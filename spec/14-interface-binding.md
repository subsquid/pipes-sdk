# 14 — Interface binding (IB-n)

The only normative doc naming concrete surfaces. Bands: 1–19 portal wire protocol,
20–39 persisted state formats, 40–49 observability HTTP surface, 50–59 error registry.

## Portal wire protocol

**IB-1 — Routes.** Relative to a per-dataset base URL:
`GET metadata?expand[]=metadata` (dataset info: `real_time`, `start_block`, kind) ·
`GET head` / `GET finalized-head` → `{hash, number}` ·
`GET timestamps/{seconds}/block` → `{block_number}` ·
`POST stream` / `POST finalized-stream` (the data stream). Full mode uses `GET head` +
`POST stream`; finalized-only mode uses `GET finalized-head` +
`POST finalized-stream` (IB-11). The base URL MAY embed basic credentials
(`user:pass@`), forwarded as an `Authorization: Basic` header; requests carry a client
`User-Agent`.

**IB-2 — Stream request.** JSON body:
`{ type, fields, fromBlock, toBlock?, parentBlockHash?, ...requests }` where `type` is
a network tag (IB-10), `fields` the per-entity boolean selection tree, and request
arrays are per-entity filter lists (IB-8).

**IB-3 — Anchor advance.** After each received block: `fromBlock ← number+1`,
`parentBlockHash ← hash`. The anchor is the fork-detection mechanism; the first request
of a resumed run carries the recovered cursor's hash — but only on the range that
continues directly from it (`fromBlock = cursor.number+1`). A later disjoint range starts
unanchored (no `parentBlockHash`) and re-establishes the anchor from its own first block;
carrying the cursor's hash across the gap would fault a 409 against a block it never
precedes (ADR-20).

**IB-4 — Fork signal.** HTTP **409** on stream requests; body key `previousBlocks`: the
canonical chain as `[{number, hash}, …]` (spec name: DEF-10). The wire key is frozen —
renames happened SDK-side only.

**IB-5 — Stream response.** **200**: NDJSON — one JSON block per `\n`-delimited line;
lines may split across transport chunks; a trailing unterminated line flushes at
end-of-stream. **204**: at head, no data (head-only signal; re-poll per WP-13).
**200 with empty body**: range has no data upstream (range end). Compression:
gzip/zstd, transparent to content (the reference client negotiates zstd only on
runtimes supporting it, Node ≥ 24.4).

**IB-6 — Head headers.** Each stream response carries
`X-Sqd-Finalized-Head-Number` + `X-Sqd-Finalized-Head-Hash` (→ `head.finalized`) and
`X-Sqd-Head-Number` (→ `head.latest`, number only).

**IB-7 — Retry surface.** Retryable statuses: 429, 502, 503, 504, 521–524, plus any
response bearing `Retry-After` (seconds or HTTP-date; server pacing wins over the
local schedule P-RETRY-SCHEDULE-MS).

**IB-8 — Per-network request families.** Closed per-network filter/entity sets
(illustrative, authoritative source = the reference query schemas): evm — `logs`
(address, topic0–3, relational inclusions `transaction`/`transactionTraces`/
`transactionLogs`/`transactionStateDiffs`), `transactions` (to/from/sighash/type),
`traces`, `stateDiffs`, `includeAllBlocks`; solana — `instructions` (programId,
discriminators d1/d2/d4/d8 — one width per instruction request, an ABI is single-width
(ADR-17); the wire has no d0 — account slots a0–a9), `transactions`, `logs`, `balances`,
`tokenBalances`, `rewards`; bitcoin/tron/hyperliquidFills — their reference sets.
A conforming implementation reproduces these request JSON shapes byte-compatibly.

**IB-9 — Block payload schemas.** Per network: the entity shapes (header, transaction,
log, trace, instruction, …) with numeric-width rules (values exceeding 2^53 are decoded
as arbitrary-precision integers, e.g. nonce). The selection tree projects these shapes:
selected keys present, unselected absent, absent collections decode as `[]`.

**IB-10 — Network tags (closed set).** `evm` · `solana` · `substrate` · `bitcoin` ·
`tron` · `hyperliquidFills` (exact casing; the last is deliberately camel-case).

**IB-11 — Finalized-only route selection.** A target may declare that it accepts only
finalized-stream delivery (the TypeScript reference binding names the capability
`requiresFinalizedStream`; other language APIs may differ). Before resolving symbolic
ranges, the pipe constructs one effective finalized portal view and uses it for
`GET finalized-head`, transformer startup context, cache access, and
`POST finalized-stream`. A cache receives `finalized: true` and the effective portal
view itself is pinned to finalized operations, so an older cache that ignores the
flag remains safe when it uses the supplied client. The target receives the effective
mode, not merely the configured one. Overriding configured full mode emits one warning;
already-finalized mode is quiet. A target without the capability keeps the configured
mode. Decision: ADR-22.

## Persisted state formats

Cross-implementation contract: any conforming implementation MUST read and write these
formats (CN-45). JSON cursor encoding everywhere: `{"number": ℕ, "hash": string?,
"timestamp": ℕ (epoch seconds)?}`.

**IB-20 — ClickHouse binding (class A).** Sync table (default `<db>.sync`):
`id String, current String (cursor JSON), finalized String (cursor JSON | '' when
absent), rollback_chain String (JSON array), timestamp DateTime(3), sign Int8`,
`ENGINE = CollapsingMergeTree(sign) ORDER BY (timestamp, id)`. Cursor rows append with
`sign=+1`; retention cancels with `sign=-1`; newest row per id = resume state
*(wall-clock ordering: GAP-10)*. Rollback mechanism by engine family:
Collapsing-family engines → net-cancel rows computed via `sum(sign)` grouping (with
insert-dedup and pre-merge-collapse disabled on those inserts); other engines →
lightweight DELETE (materialized views keep rolled-back rows — warned);
`Distributed` engines → refused (coded). A minmax skip index on the block-number
column is auto-created for rollback pruning. Repair (recovery + fork) is delegated to
an author `onRollback` handler (RP-42; the binding is schema-blind and cannot enumerate
the tables to clean — ADR-15). When it is absent, the binding logs a startup warning
(the recovery crash window applies to any restart, finalized stream included); and on the
hot stream a fork that actually arrives is refused (coded, E2007) rather than silently
returning an un-rolled-back cursor *(the no-hook recovery window is warned, not repaired —
an accepted deviation, ADR-15)*.

**IB-21 — Postgres binding (class T).** Sync table (default `public.sync`):
`id text, current_number numeric, current_hash text, "current_timestamp" timestamptz,
finalized jsonb ('{}' when absent), rollback_chain jsonb, PRIMARY KEY (id,
current_number)`. Data + cursor in one transaction, serializable by default
*(operator-downgradable — weaker levels void CN-10's lost-update protection; GAP-26)*.
Undo log: per tracked
table a `<table>__snapshots` before/after-image table + row trigger, gated per
transaction to unfinalized blocks; fork replays images newest-first. Retention:
P-PG-UNFINALIZED-RETENTION blocks of snapshots. Legacy migration: single guarded
`UPDATE` (atomic).

**IB-22 — Parquet binding (class K).** State sidecar `_sqd_parquet_state.<id>.json`
(id-less form `_sqd_parquet_state.json` — written only when the target is configured
without an id, unreachable through a source-connected pipe, WP-3): `{id?, cursor,
finalized?, coverage: {<table>: next-window-start}}`, written temp-file → fsync →
rename → dir-fsync. Data files `<table>/<from>-<to>.parquet`, names zero-padded to 12
digits, window = coverage (DEF-14) not row min/max; publish refuses to overwrite
(coded). Recovery deletes `*.tmp-*` always and any file with `to > cursor`; a file
straddling the cursor is a coded refusal unless its `from` equals the table's recorded
coverage start, which makes it the interrupted checkpoint's own file. A recorded coverage
start ahead of what the cursor allows is clamped with a warning, not refused. Column types:
INT64/INT32/UTF8/BYTE_ARRAY/BOOLEAN/DOUBLE/
TIMESTAMP(millis)/DATE/JSON/LIST/STRUCT; DECIMAL deliberately unsupported; block
column INT64/INT32 required non-null. Compression: SNAPPY (default), GZIP, BROTLI,
UNCOMPRESSED. The target declares IB-11 finalized-only delivery, appends every delivered
row directly, and exposes no fork-resolution operation (CN-32).

**IB-23 — BigQuery binding (class W).** Sync/WAL table `<dataset>.sync`:
`id, op ('commit'|'rollback'), current (cursor JSON), finalized (cursor JSON),
rollback_chain (JSON), range_low, range_high, committed BOOL, timestamp (client µs,
strictly monotonic per process; across restarts ordering is wall-clock — GAP-10)`.
States: in-flight-commit → committed;
in-flight-rollback → rolled-back; recovery of an in-flight record deletes
`[range_low, range_high]` from every tracked table and appends a rolled-back marker
with an **empty** chain. Appends: committed-stream cumulative offsets (server
dedupe), requests ≤ P-BQ-APPEND-MAX-BYTES. Tracked tables: range-partitioned on an
INT64 NOT NULL block column (refusals coded); partitioning MAY be explicitly disabled
by the operator — the partition check is then skipped, the INT64 NOT NULL check
remains. Orphan guard: tracked data without sync rows → coded refusal *(probed
table-wide rather than per key, so a first run into a table a co-resident pipe populates
is refused — GAP-20)*.

**IB-28 — Google Pub/Sub binding (class C).** Combined local state schema v4, one SQLite
file per pipe (`synchronous = FULL`, exclusive locking mode as the single-writer lock):
`meta(key, value)` (schema version, cursor key, go-live block, producer-wide sequence,
wire configuration, and materialized-route id sources), `cursor(id, latest, finalized,
updated_at)`, `outbox(row_id AUTOINCREMENT, route, topic, op, id, ordering_key, seq,
attributes, payload, block_number)`, `ledger_blocks(number, hash, timestamp)` — the
rollback chain for ancestor resolution — `manifest(route, topic, ordering_key, seq,
block_number, mode, op, id, attributes, payload)`, `materialized_baseline(route, topic,
ordering_key, id, op, attributes, payload, block_number)`, `materialized_identity(id,
route, topic, ordering_key, attributes)`, and `rollback_inverse(route, topic,
ordering_key, id, op, payload)`. An older schema version is refused rather than migrated in
place, and a state file that started empty is refused unless `allowColdStart` declares the
bootstrap (E2420, CN-46).

Each payload is a plain-object BigQuery CDC row containing the author's columns plus
target-owned `_id`, `_CHANGE_TYPE` (`UPSERT` | `DELETE`), and
`_CHANGE_SEQUENCE_NUMBER` (the producer-wide integer sequence in uppercase hexadecimal,
because BigQuery compares matching primary-key changes as unsigned hexadecimal values; see
[BigQuery CDC custom ordering](https://cloud.google.com/bigquery/docs/change-data-capture#manage_custom_ordering)).
`_id` resolves in order from `data._id`, `MessageDraft.id`, the route's `deriveId`, or the
block-derived fallback. `DELETE` retains the supplied row columns so every destination
primary-key column is present. The default encoder is normative canonical JSON (bigint →
decimal string, byte views → lowercase `0x` hex, `Date` → RFC 3339, keys sorted by
UTF-16 code unit, unrepresentable values refused — RP-24); a route may encode the complete
CDC row itself; the destination column types the encoding implies are normative for a
BigQuery subscription and are tabulated in `docs/pubsub-bigquery.md`. Nothing but such a row
is published on a topic (RP-45). User attributes remain attributes; the protocol attributes
are `_finalized` — the source's finalized block number in decimal, on every message, absent
only on a dataset that reports no finalized head — and optional `_uid`, a canonical JSON
tuple `[namespace, topic, orderingKey, seq]` with decimal `seq`.

One producer feeds one topic: `_CHANGE_SEQUENCE_NUMBER` is producer-wide, so a second topic
is refused (E2428) and a batch whose operations step backwards in block order is refused
(E2429), unless `sequenceBarrier: false` declares that no consumer reads the barrier. The
order checked is the sequencing order, which is route by route, so the barrier admits one
multi-block route per producer — a second route on the same topic restarts at the batch's
first block and is refused (GAP-40).

`publish.messageOrdering` is false by default. When enabled, the topic name is the default
ordering key and a draft may override it; the subscription must also enable ordered
delivery. Route modes are `event` (write-once ids) and `materialized` (revisable ids). A
materialized route uses one id source, and each id keeps its route, topic, ordering key,
and attributes across revisions. Go-live block (`publishFrom`, default: the head at first
start, then persisted) gates publishing and recording, so a pipe may read deeper history
than it publishes. Topic administration is validate-only by default. Codes E24xx.

**IB-24 — Lock declarations.** Postgres: per-batch advisory transaction lock on the
cursor key — dual instance fails coded. ClickHouse, BigQuery, Parquet: **no lock**
(convention/guards only; NG2 applies) *(BigQuery single-writer assumption: GAP-12)*.

**IB-25 — Cache store.** SQLite table `data(query_hash TEXT, block_from INTEGER,
block_to INTEGER, value BLOB, PRIMARY KEY(block_from, block_to, query_hash))`; `value`
= compressed (zstd, else gzip) JSON of the finalized-narrowed batch; query hash =
SHA-256 hex of the query JSON minus positional fields (RS-21). The codec is not marked
in the store; readers MUST discriminate zstd vs gzip by content magic *(the reference
reader feature-detects by runtime instead — GAP-27)*.

**IB-26 — Legacy cursor key.** The literal `stream` (DEF-9, RP-31).

**IB-27 — Binding declarations.** Per-binding enforcement, indexed here; the cited
entries remain authoritative for detail. *Exclusivity* = whether a binding's tracked
tables are declared to belong to a single cursor key; it is the precondition that lets
the orphan-data guard run at all (CN-44).

| Binding | Class | Lock (IB-24) | Legacy migration (RP-31) | Tracked-table exclusivity |
|---|---|---|---|---|
| ClickHouse | A | none | yes | not declared — guard off |
| Postgres | T | advisory, per batch | yes | not declared — guard off |
| Parquet | K | none | no | not declared — guard off *(yet refuses shared directories via filename collision: GAP-35)* |
| BigQuery | W | none | no | assumed exclusive — guard on, probed table-wide *(GAP-20)* |
| Pub/Sub | C | exclusive state-file lock, process-lifetime | no | n/a — the sink owns no tables; ids are namespaced per producer |

A binding MAY expose exclusivity as operator configuration; where it does, the declared
value governs whether CN-44 applies. Declaring exclusivity over tables that co-resident
pipes in fact share is an operator error the spec does not detect.

*Decision index.* Applying to every binding: ADR-2 (cursor keyed by pipe id), ADR-4
(coded error taxonomy), ADR-5 (durability classes), ADR-16 (exclusivity, proposed).
Binding-specific, with the entry carrying each one's detail:

- **ClickHouse** — IB-20, IB-24 · ADR-7 (sign-netting rollback), ADR-15 (class-A repair
  hook, proposed)
- **Postgres** — IB-21, IB-24 · none binding-specific
- **Parquet** — IB-22 · ADR-6 (coverage-window naming)
- **BigQuery** — IB-23 · none binding-specific
- **Pub/Sub** — IB-28 · ADR-21 (compensating append-only class)

Source- and decoder-side decisions (ADR-1, ADR-3, ADR-9…ADR-12) are out of scope here —
they bind the ingestion path, not a sink.

## Observability HTTP surface

**IB-40 — Server.** Default port P-METRICS-PORT; JSON endpoints wrap payloads as
`{"payload": …}`; CORS allows localhost origins only (remote dashboards proxy) *(the
reference matches `localhost` as a substring — GAP-34)*; no authentication; read-only.

**IB-41 — `GET /stats`.** `{payload: {sdk: {version}, runtime: {name, version},
entrypoint, pipes: [{id, dataset, portal: {url, query}, progress: {from, current, to,
percent, etaSeconds}, speed: {blocksPerSecond, bytesPerSecond}}], usage: {memory}}}`.
Pre-first-batch: progress/speed fields read 0 (indistinguishable from block 0 on this
surface; the −1 sentinel exists only on /metrics, IB-46).

**IB-42 — `GET /metrics`.** Prometheus text exposition. Required names (all labeled
`id`): `sqd_processed_block` (gauge, −1 sentinel = not started, IB-46),
`sqd_end_block`, `sqd_progress_ratio`, `sqd_eta_seconds`,
`sqd_blocks_processed_total`, `sqd_bytes_downloaded_total`, `sqd_forks_total`,
`sqd_portal_requests_total` (+ labels `classification` ∈ success|rate_limited|error,
`status`), `sqd_batch_size_blocks` (histogram), `sqd_batch_size_bytes` (histogram).
Per-sink metric sets (`sqd_parquet_*` with a `table` label, `sqd_bigquery_*` with a
`kind` label) and the runtime's default process metrics MAY additionally be present;
consumers MUST tolerate them.

**IB-43 — `GET /profiler?id=&from=`.** `{payload: {enabled, profiles: [{name,
totalTime, startOffset, children[]}…]}}` — per-batch span trees; retention
P-PREVIEW-HISTORY snapshots.

**IB-44 — `GET /preview/transformation?id=`.** `{payload: {transformation: {name,
data, elapsed?, startOffset?, dataSize?, labels?, children[]}, batch?: {from, to,
blocksCount, bytesSize}}}`. Truncation rules (normative — dashboards depend on them):
arrays longer than P-PREVIEW-ARRAY-LIMIT collapse to `[first, "... N more ..."]`;
big integers serialize with an `n` suffix; dates as ISO strings. `transformation.data`
is a pre-serialized JSON **string**; `dataSize` is its length.

**IB-45 — `GET /health`.** Text `ok` while the server lives.

**IB-46 — Sentinels.** `sqd_processed_block = −1` means "no batch processed yet";
consumers MUST NOT read it as a block number.

*(This whole surface currently lacks golden fixtures and version markers — GAP-16.)*

## Error registry

**IB-50 — Code bands.** `PipeError` carries a stable code + docs URL suffix
`/errors/<code>`; match on code or type, never message text (ADR-4).

| Band | Area | Codes in use |
|---|---|---|
| E0xxx | pipe configuration | E0001 blank/default pipe id *(currently dead — GAP-4)*, E0002 invalid range/date, E0003 unusable instruction discriminator set (mixed widths across the decoder, shared discriminator, or an instruction with none or several) |
| E1xxx | fork handling | E1001 sink lacks fork support, E1002 empty canonical chain, E1003 ancestor unresolvable, E1004 portal contract violation (canonical below cursor), E1005 fork reported on the finalized-only route |
| E20xx | ClickHouse binding | E2001–E2007 (retention, table name, distributed-rollback, collapse-column, missing sign, rollback-index, fork-without-rollback-handler) |
| E21xx | Postgres binding | E2101–E2106 (client, config, advisory lock, untracked table, missing PK, FK cycle) |
| E22xx | BigQuery binding | E2201–E2213 (schema/partition guards, orphan data E2212, append rejection E2213) |
| E23xx | Parquet binding | E2301–E2317 and E2320 in use (schema/config; file collision E2309, state corrupt E2310, recovery delete failure E2314, nested-schema E2315; coverage guards E2316 invalid range, E2317 state/data disagreement; engine-output verification E2320 non-Parquet segment refused). E2318–E2319 retired, unassigned |
| E24xx | Pub/Sub binding | E2401–E2424, with E2420 retired (topic/attribute/request guards E2401–E2404, checked pre-commit and again after recovery encoding; canonical-codec refusals E2405–E2406; finality guards E2407–E2409; state guards E2410–E2411, E2417, state identity/wire binding E2419/E2421, and sequence exhaustion E2424; row-identity guards E2412–E2413, E2418, E2422; route configuration E2414–E2416, E2423) |

**IB-51 — Transport errors.** Non-coded, typed: HTTP error (with response), request
timeout, body-stall timeout. Retryability: request timeouts and connection-class errors
are retryable in addition to IB-7's statuses; the body-stall timeout is currently
**not** retried. The fork signal is internal (consumed by T-FORK), never surfaced to
authors.

**IB-52 — Registry sync.** The set of codes an implementation can emit MUST equal this
registry; CI SHOULD enforce it *(no such check exists — GAP-13)*.

## Input-side binding (simulator obligations)

The CT portal simulator MUST implement: IB-1 routes, including IB-11 mode selection;
NDJSON framing with adversarial
chunk splits; 200/204/empty-body/409 semantics; head headers per batch; retry-after
pacing; scripted fork signals with canonical chains; scripted head regressions.
Responses MUST be derived from the request anchor (IB-3) against a held chain, never
selected by request ordinal — a restarted SUT re-requests from its recovered cursor,
which an ordinal script cannot answer, making CT-2 unimplementable against it.
Sink-store probes MUST read state via IB-20…IB-25 formats directly.

## Versioning

A change to any surface in this doc updates this file and the CT-5 fixtures in the
same change. Anything not specified here is unspecified — conformance tests and
consumers MUST NOT pin it.
