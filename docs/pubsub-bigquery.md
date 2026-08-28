# Landing a Pub/Sub feed in BigQuery

The Pub/Sub target publishes a changelog: one message per row change, carrying the row's
columns plus `_id`, `_CHANGE_TYPE` (`UPSERT` or `DELETE`) and `_CHANGE_SEQUENCE_NUMBER`.
That is exactly the shape a [BigQuery subscription][bq-sub] applies through
[BigQuery CDC][bq-cdc], so no consumer code is needed — Pub/Sub writes into the table
itself, and the table converges on the producer's current state.

The subscription does not have to live in the producer's project. A topic in one project
and a subscription plus table in another is a supported Pub/Sub topology, and it is what
makes the feed consumable by someone who only ever sees the topic name.

This page is the receiving end: the table to create, the subscription to attach, the
grants each side needs, and the limits that come with the design.

## Column types

The producer encodes rows with a canonical JSON codec. The encoding is fixed, so it
decides what each destination column may be declared as. The safe mappings below avoid
both rejected messages and accepted conversions with the wrong units.

| Value in the pipe | On the wire | Destination column |
|---|---|---|
| `string` | string | `STRING` |
| safe integer `number` | number | `INT64` or `FLOAT64` |
| non-integer `number` | number | `FLOAT64` — **not `INT64`** |
| `bigint` | decimal string, `"1000000000000000000"` | `STRING`, or `NUMERIC` / `BIGNUMERIC` when the value fits — **never `INT64`** |
| `boolean` | bool | `BOOL` |
| `Date` | RFC 3339, `"2023-11-14T22:13:20.999Z"` | `TIMESTAMP` |
| `Uint8Array`, `ArrayBuffer` | `0x` hex string | `STRING` — **not `BYTES`** |
| `null` | null | any `NULLABLE` column |
| array | JSON array | a matching `ARRAY<T>` (`REPEATED`) column |
| nested object | JSON object | a matching `STRUCT<...>` column |
| JSON text supplied as a string | escaped JSON string, `"{\"key\":\"value\"}"` | `JSON` |

Three of these bite in practice:

- **A `bigint` arrives as a string, and BigQuery rejects a string written to `INT64`.**
  `NUMERIC` and `BIGNUMERIC` parse the string only while it is in range. A full-range
  `uint256` does not fit even `BIGNUMERIC` (~76 significant digits, while `uint256` reaches
  78), so `STRING` is the safe column for an unconstrained token amount. Validate or clamp
  the value before publishing if the destination needs a numeric column.
- **A `Date` arrives as a string, not a number.** This is deliberate: BigQuery reads a
  JSON *number* in a `TIMESTAMP` column as *microseconds* since the epoch, so the old
  seconds value silently landed timestamps near 1970. Pub/Sub
  converts a string into a `TIMESTAMP` as long as it matches BigQuery's canonical format,
  which RFC 3339 does. If a dead-letter message ever shows a timestamp conversion error
  instead, emit epoch microseconds as a number from the route's `map` — the direct
  BigQuery target does exactly that, for the same reason.
- **A BigQuery `JSON` column does not accept a raw JSON object or array through
  `use_table_schema`.** The corresponding message field must be an escaped JSON string
  ([the documented table-schema mapping][bq-table-schema]). Call `JSON.stringify` in the
  route's `map`, or use a matching `STRUCT` / `REPEATED` destination instead.

A route that needs a different shape overrides the whole encoding with `TopicRoute.encode`.

## Create the table

The table needs a primary key — CDC has nothing to match an `UPSERT` or a `DELETE`
against otherwise. `_CHANGE_TYPE` and `_CHANGE_SEQUENCE_NUMBER` are pseudocolumns: they
are read from the message but must **not** appear in the DDL.

For the ERC-20 feed in [`examples/evm/18.pubsub.example.ts`](examples/evm/18.pubsub.example.ts):

```sql
CREATE TABLE `my-project.my_dataset.erc20_transfers` (
  _id STRING NOT NULL PRIMARY KEY NOT ENFORCED,
  token STRING,
  `from` STRING,
  `to` STRING,
  amount STRING,
  block INT64,
  timestamp TIMESTAMP
)
OPTIONS (max_staleness = INTERVAL 15 MINUTE);
```

`max_staleness` is how far behind a query may read before BigQuery merges pending changes
at query time. Higher is cheaper and staler. Omitting it means every read pays for the
merge.

Note the backticks around `from`: it is a reserved SQL keyword, so it needs quoting in the
DDL and in every query. Renaming such columns in the route's `map` is usually less
annoying than quoting them forever.

### Composite primary keys

The key does not have to be `_id`. BigQuery accepts up to 16 primary-key columns, and a
`DELETE` carries the row's columns precisely so that every one of them reaches the table:

```sql
CREATE TABLE `my-project.my_dataset.erc20_transfers` (
  block INT64 NOT NULL,
  log_index INT64 NOT NULL,
  token STRING,
  amount STRING,
  PRIMARY KEY (block, log_index) NOT ENFORCED
)
OPTIONS (max_staleness = INTERVAL 15 MINUTE);
```

Two things follow.

`_id` is still in every message — the target owns that field and there is no way to
suppress it. Either keep an unused `_id STRING` column, or let `--drop-unknown-fields`
discard it.

**Derive `_id` from exactly the columns of the primary key.** The target tracks row
identity, fork baselines, and compensations by `_id`; BigQuery tracks it by the primary
key. When those two disagree, a fork is where it shows. Say `_id` is
`blockHash:logIndex` while the key is `(block, log_index)`: a reorg publishes a `DELETE`
for the orphaned hash and an `UPSERT` for the replacement, and because the ids differ the
target believes they are separate rows — but in BigQuery they are the same row. It happens
to come out right, since compensations are sequenced before the re-streamed blocks and the
later `UPSERT` therefore wins. Nothing checks that, though; it is an accident of ordering
rather than a guarantee. Two rows in one batch that share a key are the case with no happy
accident: BigQuery keeps the last one and the target never notices the collision.

Matching the two identities removes the whole class of problem:

```ts
_id: `${t.block.number}:${t.rawEvent.logIndex}`,
```

## Grant access

The grants must exist before Pub/Sub validates and creates the BigQuery subscription.

**On the source topic** — the producer's side. Grant `pubsub.topics.attachSubscription` to
the identity that will run the subscription-creation command. `roles/pubsub.subscriber`
carries it:

```bash
gcloud pubsub topics add-iam-policy-binding evm.base.erc20-transfers \
  --project=producer-project \
  --member='serviceAccount:someone@my-project.iam.gserviceaccount.com' \
  --role=roles/pubsub.subscriber
```

**On the BigQuery table** — the subscriber's side. Pub/Sub writes as the service agent of
the project that owns the *subscription*:

```bash
PROJECT_NUMBER="$(gcloud projects describe my-project --format='value(projectNumber)')"
PUBSUB_SERVICE_AGENT="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

bq add-iam-policy-binding \
  --member="serviceAccount:${PUBSUB_SERVICE_AGENT}" \
  --role=roles/bigquery.dataEditor \
  my-project:my_dataset.erc20_transfers
```

Publishing is billed to the topic's project; delivery is billed to the subscription's
project. A producer's delivery bill therefore does not grow with the number of subscribers.

## Create the dead-letter topic

The topic in `--dead-letter-topic` must already exist, and it needs its own subscription;
otherwise forwarded messages have nowhere to be retained:

```bash
gcloud pubsub topics create erc20-transfers-dlq \
  --project=my-project

gcloud pubsub subscriptions create erc20-transfers-dlq-reader \
  --project=my-project \
  --topic=erc20-transfers-dlq
```

## Create the BigQuery subscription

```bash
gcloud pubsub subscriptions create erc20-transfers-bq \
  --project=my-project \
  --topic=projects/producer-project/topics/evm.base.erc20-transfers \
  --bigquery-table=my-project:my_dataset.erc20_transfers \
  --use-table-schema \
  --drop-unknown-fields \
  --dead-letter-topic=projects/my-project/topics/erc20-transfers-dlq \
  --max-delivery-attempts=5
```

After the source subscription exists, grant its service agent permission to forward failed
messages to the dead-letter topic and acknowledge them on the source subscription
([the complete dead-letter policy requirements][dlq]):

```bash
PROJECT_NUMBER="$(gcloud projects describe my-project --format='value(projectNumber)')"
PUBSUB_SERVICE_AGENT="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud pubsub topics add-iam-policy-binding erc20-transfers-dlq \
  --project=my-project \
  --member="serviceAccount:${PUBSUB_SERVICE_AGENT}" \
  --role=roles/pubsub.publisher

gcloud pubsub subscriptions add-iam-policy-binding erc20-transfers-bq \
  --project=my-project \
  --member="serviceAccount:${PUBSUB_SERVICE_AGENT}" \
  --role=roles/pubsub.subscriber
```

`--use-table-schema` maps the JSON body onto the table's columns. (`--use-topic-schema` is
not available: the producer registers no Avro or Protobuf schema on the topic.)

`--drop-unknown-fields` makes additive producer changes survivable. Without it, each message
carrying a field the table lacks is not written and remains in the backlog unless a dead-letter
policy forwards it. That individual failure is not a global stop for an unordered subscription,
but repeated incompatible messages build a backlog. With the flag, unknown fields are discarded;
add the destination column before consumers need the new value.

The dead-letter topic preserves the failed message and adds a
`CloudPubSubDeadLetterSourceDeliveryErrorMessage` attribute with the message-specific reason.
It is not the only failure signal: `pubsub.googleapis.com/subscription/push_request_count`
with `response_code=invalid_argument` reports rejected message values. Destination-wide errors
such as a missing table or revoked permission put the subscription itself into an error state;
while it is in that state, messages stay in the source backlog and are not forwarded to the DLQ.
Monitor both the subscription state and delivery metrics
([BigQuery subscription troubleshooting][bq-troubleshooting]).

Message ordering is not needed. BigQuery resolves concurrent changes to the same row by
`_CHANGE_SEQUENCE_NUMBER`, which the producer assigns and never rewinds.

## Verify

```sql
SELECT COUNT(*), MAX(timestamp) FROM `my-project.my_dataset.erc20_transfers`;
```

Rows appearing with a plausible `MAX(timestamp)` — not 1970 — means the type mapping is
right end to end. Then verify all three delivery signals:

- `pubsub.googleapis.com/subscription/num_undelivered_messages` drains on
  `erc20-transfers-bq`;
- `pubsub.googleapis.com/subscription/push_request_count` has no
  `response_code=invalid_argument` deliveries;
- `pubsub.googleapis.com/subscription/num_undelivered_messages` stays at zero on
  `erc20-transfers-dlq-reader`.

## Limits

**No automatic history.** By default, a subscription receives only what is published after
it is created. Topic-level message retention can make up to 31 days seekable, but this target
does not coordinate that replay with a destination bootstrap. A consumer that needs the full
dataset has to bootstrap from a snapshot elsewhere and then attach or explicitly manage the
retained-message replay.

**A lost producer sequencer is silent and unrecoverable downstream.**
`_CHANGE_SEQUENCE_NUMBER` comes from a single counter in the producer's local state file.
If that file is lost or restored from a backup, the counter restarts, and BigQuery starts
discarding the republished lower numbers as stale — every affected row freezes at its old
value with no error on either side. Recovery is a fresh namespace on the producer and a
re-bootstrap of every destination table, which means the producer needs a way to reach its
subscribers out of band. Producers: keep the state file on durable storage, one process
per path, and watch the `sqd_pubsub_cold_start` gauge.

**Schema changes must be additive and nullable.** Dropping or retyping a column, or adding
a `REQUIRED` one, breaks subscribers whose tables no longer match.

**No finality marker on a CDC row.** The feed converges after a chain reorg — a fork
publishes a `DELETE` or a restoring `UPSERT` with a higher sequence number — but nothing in
the CDC envelope says when a row became reorg-proof, and a BigQuery subscription could not act on
it anyway. A stateful consumer that needs the source's finalized head reads it off the control
route's finality watermark below, as a reference value.

**Quotas.** 10 000 attached subscriptions per topic, and a per-region cap on BigQuery
subscription throughput.

## The sequence is the completeness barrier

Every operation takes exactly one number from one producer-wide counter, and the number is
allocated in the same transaction that writes the operation to the outbox — a rolled-back commit
rolls the number back with it, and the outbox row is deleted only once its publish is confirmed.
So on a producer feeding **one** topic, that topic's `_CHANGE_SEQUENCE_NUMBER` run is gapless, and
a subscriber holding a contiguous run is missing no operation the producer committed inside it.

That is what a stateful consumer actually reads, for two different questions:

- **a fork is retired** once the compensations are in hand — they are sequenced ahead of the rows
  that replace them, so "on a `DELETE` at block *N* with sequence *S*, drop every contribution from
  a row with block ≥ *N* and sequence < *S*" is correct even though delivery is unordered;
- **a block is whole** once the run reaches the first operation of a higher block. This is also the
  only way to distinguish "that block produced no rows" from "its rows have not arrived".

Out-of-order delivery costs a reorder buffer, not correctness: contiguity is a property of the set
a subscriber has accumulated, not of the order it arrived in. Two conditions apply, and both are
easy to break by accident:

- **the producer keeps one topic.** A second data topic splits the counter across both, and neither
  run is contiguous on its own. So does `control.topic`.
- **the subscription does not filter.** A server-side filter removes messages, and every removal
  reads as a gap. A BigQuery landing may filter — it uses no barrier — but a filtered subscription
  cannot double as a stateful consumer's.

`_CHANGE_SEQUENCE_NUMBER` is the canonical carrier and is always present. `publish.uidAttribute`
adds a decimal copy of the same number as the fourth element of `_uid`, so a subscriber can
gap-check without decoding the body; it is deduplication metadata, not a second contract.

The barrier is about transport, and nothing else. It says the subscriber has every operation the
producer committed — not that the source or the mapper produced every row it should have, and not
that a row is final.

## The control route

The optional `control` route carries records that state something about the feed rather than carry
a table row. Two kinds today.

### The fork announcement

```ts
control: {
  fork: ({ epoch, rollbackTo, deadEnd }) => ({
    data: { type: 'fork', epoch, rollbackTo: rollbackTo?.number ?? null, deadEnd },
  }),
}
```

One message per resolved fork, built inside the transaction that rewinds the cursor and raises the
epoch — so a rewound cursor whose new epoch was never announced cannot exist.

**This is an optimisation, not the rollback mechanism.** A consumer folding an aggregate already
has an exact rule without it: the compensation rule above. What the announcement buys is latency —
one message retires a whole fork, where the rule converges only as the last compensation lands —
and generality: a `materialized` route compensates by restoring the surviving revision, so such a
topic can pass through a fork without a single `DELETE`, and a consumer inferring forks from
`DELETE`s is blind to it.

### The finality watermark

```ts
control: {
  finality: {
    everyBlocks: 100,
    map: ({ finalized, observedAt }) => ({
      data: { type: 'finality', finalBlock: finalized.number, observedAt: observedAt.number },
    }),
  },
}
```

The source's own finalized head, republished each time it advances by `everyBlocks` (default 100).
This is the one input a consumer cannot derive from the row stream, and it needs one: per-block
state cannot be kept forever, and folding it into a base a retraction can no longer reach takes an
answer to "have all corrections below this height already arrived?" that the rows do not give.

It is a **reference value, not an authority over a row.** Finality is a policy — providers disagree
about it, and a pipe may widen its window deliberately — so the number is an input to whatever
confirmation policy the consumer chose. Reading it needs no ceremony: the value is monotone, so a
consumer keeps the maximum of what it has seen, with no sequence comparison involved, which makes
the read robust to duplicates and reordering for free. A watermark skipped by a rolled-back commit
or a restart costs nothing, because the next one carries a higher value.

It comes off the batch commit rather than off row traffic, so it keeps advancing while the table is
quiet. Stamped on rows instead, it would stop advancing exactly when a sparse table's consumer
needs it most.

### Both kinds ride the data topics

Neither is a separate feed — both are statements about the rows already on those topics — so they
go to every topic the producer's `topics` routes feed. One subscription for a consumer, and no
number burned out of the topic's own sequence.

Each is an ordinary CDC row — `_id`, `_CHANGE_TYPE`, `_CHANGE_SEQUENCE_NUMBER` and all. `_id`
defaults to `<namespace>:control:fork:<epoch>` and `<namespace>:control:finality:<block>`,
write-once either way; the route may set its own.

### `_type` selects what a subscription receives

Every message carries a reserved `_type` attribute: `cdc` for a table row change, `control` for a
producer statement about the feed. A BigQuery subscription takes the rows:

```
attributes._type = "cdc"
```

Pub/Sub filters match **attributes only**, never the message body — that is why the discriminator
is an attribute rather than a field. Filtered messages are acknowledged by Pub/Sub and never
delivered, and carry no outbound charge.

`_type` is deliberately coarse. A subscription's filter cannot be changed after it is created, so
`attributes._type = "cdc"` has to stay correct when a new kind of control record appears later —
which that record's own body then discriminates.

> **The filter is immutable.** A subscription's filter cannot be changed after it is created
> ([subscription filters][filter-docs]). It therefore has to be in place **before** the producer
> publishes its first control record — otherwise that record reaches a table whose schema is not
> its own and lands in the dead-letter topic. Note that a finality watermark starts flowing as soon
> as the route is configured, without waiting for a fork. To add a control route to a feed whose
> subscriptions already exist, either recreate those subscriptions with the filter, or set
> `control.topic` to publish on a separate topic and leave them untouched — at the cost of the
> shared sequence.

### Give it the attributes your subscribers filter on

The target adds `_type` and nothing else. If your consumers subscribe with a filter —
`attributes.chain = "ethereum"`, say — a control record without a `chain` attribute is silently
invisible to them, in exactly the case it exists for. Set the filter attributes your data rows
carry:

```ts
control: {
  fork: ({ epoch, rollbackTo, deadEnd }) => ({
    data: { type: 'fork', epoch, safeBlock: rollbackTo?.number ?? 0, deadEnd },
    attributes: { chain: 'ethereum', table: 'blocks' },
  }),
}
```

Only the attributes that are constant for the topic can be mirrored — a per-row one such as
`block_number` cannot be, since a fork spans many. A subscriber filtering on one of those has to
admit control records explicitly (and gives up the sequence barrier by filtering at all):

```
attributes.block_number = "19000000" OR attributes._type = "control"
```

### Why the epoch matters

The announcement is enqueued inside the transaction that rewinds the cursor and raises the epoch,
so a rewound cursor with an unannounced epoch cannot exist — a crash leaves it unpublished, never
unrecorded. `map` receives the same `epoch`, so data rows can carry the epoch they were published
under: after the announcement raises it, a row still stamped with the old one is recognisable as
coming from the abandoned branch. That is what lets an unordered consumer tell a late duplicate
from live data.

The announcement does not replace the per-row compensations — both go out, and the compensations
are what keep a BigQuery destination correct. A consumer that acts on the announcement can ignore
them; their inherited attributes carry the pre-fork epoch, so an epoch fence discards them anyway.

`docs/examples/evm/19.pubsub-control.example.ts` is a runnable example.

## Producer metrics

Exported by the pipe's metrics server. Alert on the first two.

| Metric | Type | Meaning |
| --- | --- | --- |
| `sqd_pubsub_block_to_commit_lag_seconds` | histogram | Seconds from the committed block's chain timestamp to the Pub/Sub ack. End-to-end freshness, including block production and chain-to-portal propagation — a service level, not an attribution. Empty below the go-live block, or when the pipe's query selects no block `timestamp`. |
| `sqd_pubsub_portal_to_commit_lag_seconds` | histogram | Seconds from the portal stamping the batch to the Pub/Sub ack. In-process latency, covering stream-buffer dwell and the pipe's transformers as well as this target. |
| `sqd_pubsub_operations_total` | counter | Operations enqueued, by `topic`, `kind` (`cdc`/`control`) and `operation` (`upsert`/`delete`). |
| `sqd_pubsub_publish_duration_seconds` | histogram | Wallclock of one outbox drain. Split a `portal_to_commit` regression against this. |
| `sqd_pubsub_published_bytes_total` | counter | Payload bytes confirmed published, attributes excluded. |
| `sqd_pubsub_outbox_depth` | gauge | Rows queued but unconfirmed. Sustained growth means the publisher is not keeping up. |
| `sqd_pubsub_compensations_per_fork` | histogram | Compensating operations per resolved fork — fork blast radius. The fork announcement is excluded; it compensates nothing. |
| `sqd_pubsub_manifest_rows` | gauge | Rows in the rollback manifest — the unfinalized window the target can still repair. |
| `sqd_pubsub_publish_saturation_seconds` | counter | Message ordering only: seconds spent publishing a partition at ≥80% of Pub/Sub's 1 MB/s per-ordering-key cap. Growth means the headroom is eroding. |
| `sqd_pubsub_cold_start` | gauge | 1 when the state file started empty. See the lost-sequencer limit above. |

Both lag histograms are observed once per batch above the go-live block, including a batch
whose routes mapped nothing: they measure how current the destination is, and a route that
legitimately publishes nothing for a stretch is still current. Gating them on a non-empty
publish would make a sparse feed's freshness series go silent and read as a dead pipe.

## When a shared dataset is the better answer

Pub/Sub is the right tool when the consumer needs the stream — changes within seconds,
applied to their own table, on their own schedule. It is the wrong tool for "give me your
data". Consumers who want the whole history, in their own BigQuery, with the schema as the
contract, are better served by sharing the dataset directly ([Analytics Hub][ahub]): no
per-subscriber cost, no backfill problem, and no sequencer to lose.

[bq-sub]: https://cloud.google.com/pubsub/docs/bigquery
[bq-cdc]: https://cloud.google.com/bigquery/docs/change-data-capture
[bq-table-schema]: https://cloud.google.com/pubsub/docs/create-bigquery-subscription#use_table_schema
[filter-docs]: https://cloud.google.com/pubsub/docs/subscription-message-filter
[bq-troubleshooting]: https://cloud.google.com/pubsub/docs/bigquery-troubleshooting
[dlq]: https://cloud.google.com/pubsub/docs/dead-letter-topics
[ahub]: https://cloud.google.com/bigquery/docs/analytics-hub-introduction
