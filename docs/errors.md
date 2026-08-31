<!--
  Single source of truth for @subsquid/pipes error codes.

  Every SDK error ends its message with
  `See: https://docs.sqd.dev/en/sdk/pipes-sdk/errors/<code>`.
  The docs site imports THIS file and serves it at /en/sdk/pipes-sdk/errors,
  routing /en/sdk/pipes-sdk/errors/<code> to the matching section anchor below.

  Keep in sync with the code the messages come from:
    - packages/pipes/src/core/errors.ts                          (E0xxx, E1xxx)
    - packages/pipes/src/targets/clickhouse/errors.ts            (E20xx)
    - packages/pipes/src/targets/drizzle/node-postgres/errors.ts (E21xx)
    - packages/pipes/src/targets/bigquery/errors.ts              (E22xx)
    - packages/pipes/src/targets/parquet/errors.ts               (E23xx)
    - packages/pipes/src/targets/pubsub/errors.ts                (E24xx)
-->

# Error reference

Every error `@subsquid/pipes` raises carries a stable code and ends its message with a link back to
this page:

```
Pipe requires a non-default ID when used with targets.
...
See: https://docs.sqd.dev/en/sdk/pipes-sdk/errors/E0001
```

Match on the code (or the `instanceof` class), not the message text — messages may be reworded, codes
are stable. Codes are grouped by where they originate:

| Prefix  | Area                        |
| ------- | --------------------------- |
| `E0xxx` | Source / pipe configuration |
| `E1xxx` | Fork handling & rollback    |
| `E20xx` | ClickHouse target           |
| `E21xx` | Postgres (Drizzle) target   |
| `E22xx` | BigQuery target             |
| `E23xx` | Parquet target              |
| `E24xx` | Google PubSub target       |

---

## Pipe configuration

### E0001 · Pipe requires a unique id

A pipe was connected to a target (`.pipeTo(...)`) while still using the default id. Targets persist
their resume cursor under the pipe's `id`, so a shared/default id would let two pipes silently
overwrite each other's progress.

**Fix** — set a stable, globally unique, non-empty `id`:

```ts
evmPortalStream({ id: 'eth-transfers', portal: '...', outputs })
```

### E0002 · Invalid block range

A `range` (on the stream or a decoder) is misconfigured — an inverted range (`from` after `to`), an
invalid date, or a timestamp that can't be resolved to a block. The message names the exact problem.

**Fix** — ensure `from ≤ to` and use a resolvable bound: `'latest'`, a block number (`'12,000,000'`),
an ISO date (`'2024-01-01'`), or a `Date`.

### E0003 · Unusable instruction discriminator set

A `solanaInstructionDecoder` was built with a discriminator set it can't use: an instruction with no
discriminator or with more than one, discriminators of mixed widths across the decoder
(`d1`/`d2`/`d4`/`d8`), or two instructions sharing a discriminator. A decoder covers a single
program/ABI — one width, distinct discriminators — because Anchor discriminators are
program-independent, so a shared one would decode the same raw instruction under both keys.

**Fix** — give each instruction exactly one discriminator, keep a single width per decoder, and split
unrelated programs into separate `solanaInstructionDecoder()` calls. If the mixed widths really do
belong to one program — an ABI whose extension instructions carry a wider discriminator than its base
ones — split it by width instead: one decoder per width, the same `programId`.

**Wrong** — two different programs in one decoder. Their `swap` instructions are unrelated, but Anchor
derives both discriminators from `sha256("global:swap")`, so they collide and each would decode the
other's data:

```ts
solanaInstructionDecoder({
  range: { from: 'latest' },
  programId: [jupiter.programId, raydium.programId],
  instructions: {
    jupSwap: jupiter.instructions.swap,
    raySwap: raydium.instructions.swap, // same discriminator as jupSwap → E0003
  },
})
```

**Right** — one decoder per program/ABI:

```ts
const jup = solanaInstructionDecoder({
  range: { from: 'latest' },
  programId: jupiter.programId,
  instructions: { swap: jupiter.instructions.swap },
})

const ray = solanaInstructionDecoder({
  range: { from: 'latest' },
  programId: raydium.programId,
  instructions: { swap: raydium.instructions.swap },
})
```

**Also right** — one ABI covering several addresses. This is the legitimate array case: the
instruction has the same discriminator, arguments and account layout at every address, so one
definition decodes it across all of them. Token and Token-2022 are distinct programs, but Token-2022
implements Token's instruction set — `transfer` is identical on both:

```ts
solanaInstructionDecoder({
  range: { from: 'latest' },
  programId: [TOKEN_PROGRAM, TOKEN_2022_PROGRAM],
  instructions: { transfer: tokenProgram.instructions.transfer },
})
```

---

## Fork handling

Raised while unwinding a chain reorganization (fork). Targets that read the hot stream — ClickHouse,
Postgres, BigQuery — handle forks themselves, so E1001–E1003 mostly surface in **custom** targets.
The finalized-only targets (Parquet, memory) read `/finalized-stream`, where a fork cannot occur;
they raise E1005 instead if one is reported anyway.

### E1001 · Target does not support fork handling

A fork was detected, but the target does not implement `resolveFork()`.

**Fix** — implement `resolveFork(canonicalBlocks)` on the target. It must remove rows above the fork
point and return the cursor to resume from. A target that commits only finalized data should instead
set `requiresFinalizedStream: true`, which puts it on a stream no fork can reach.

### E1002 · Fork with no canonical blocks

A fork was detected but no canonical blocks were supplied to resolve it — an internal invariant
violation.

**Fix** — none; please [report it as a bug](https://github.com/subsquid-labs/pipes-sdk/issues).

### E1003 · resolveFork() returned no cursor

The target's `resolveFork()` returned nothing instead of a cursor.

**Fix** — return the cursor to resume from after rolling back.

### E1004 · Portal contract violation

The portal delivered a `canonicalBlocks` set whose highest block is below the target's persisted
cursor. Rows above it would survive the fork rollback and corrupt the new chain, so the pipe refuses
to proceed. Any target that tracks a cursor can raise this.

**Fix** — none in user code; please
[report it as a bug](https://github.com/subsquid-labs/pipes-sdk/issues) against the portal contract.

### E1005 · Fork reported on the finalized stream

The pipe reads `/finalized-stream`, where every delivered block is already final, and the portal
answered a request with a fork. The target commits only finalized data and has no rollback path by
design, so the pipe stopped without changing anything it had already written.

**Fix** — [report it](https://github.com/subsquid-labs/pipes-sdk/issues) against the portal. To
restart, rewind (or clear) the target's persisted cursor to a block the portal still considers
canonical — for the Parquet target that is the `_sqd_parquet_state.<pipe-id>.json` file in its
output directory.

---

## ClickHouse target

### E2001 · Invalid `maxRows`

The `maxRows` batching option is not a positive number.

**Fix** — set `maxRows` to a value greater than 0 (or omit it for the default).

### E2002 · Unparseable table name

A table identifier could not be parsed as `table` or `database.table`.

**Fix** — pass `table` or `database.table`; quote identifiers that themselves contain dots.

### E2003 · Cannot roll back a Distributed table

Rollback targeted a `Distributed` table. Multi-shard rollback is not supported.

**Fix** — point the rollback at the underlying local table instead.

### E2004 · Rollback engine collapses on the wrong column

The table's collapsing engine collapses on a column other than `sign`. Rollback inserts cancel rows
with `sign = -1`, so the collapse column must be named `sign`.

**Fix** — rename the collapse column to `sign`.

### E2005 · Rollback table has no `sign` column

The table has no `sign` column, so cancel-row rollback cannot work.

**Fix** — add a `sign` column and use a `CollapsingMergeTree`-family engine for tables you roll back.

### E2006 · Invalid rollback index column

The column passed to `ensureRollbackIndex` is not a plain identifier.

**Fix** — pass a plain identifier (letters, digits, underscores; no spaces or expressions).

### E2007 · Chain fork with no rollback handler

A chain fork was detected, but the ClickHouse target has no `onRollback` handler, so the rows
written above the fork point cannot be removed. Returning a rewound cursor without removing them
would leave diverged data, so the target refuses instead. On the hot stream a startup warning
announces this risk before a fork ever arrives.

**Fix** — configure `onRollback` on the ClickHouse target. The typical implementation calls
`store.removeAllRows` with `where: 'block_number > {latest:UInt32}'` and
`params: { latest: safeCursor.number }`. A pipe reading `/finalized-stream` never forks and does
not need one.

---

## Postgres (Drizzle) target

### E2101 · Drizzle client missing

The `db` passed to `drizzleTarget` has no underlying client (`$client`).

**Fix** — pass a Drizzle instance created with a real driver, e.g. `drizzle(pool)`.

### E2102 · Invalid retention

`unfinalizedBlocksRetention` is not a positive number.

**Fix** — set it to a value greater than 0.

### E2103 · Advisory lock not acquired

Another process is holding the PostgreSQL advisory lock for this state id.

**Fix** — ensure only one process writes to a given pipe `id` at a time.

### E2104 · Untracked table

A write targeted a table that isn't registered for rollback tracking.

**Fix** — include the table in the `tables` array passed to `drizzleTarget`.

### E2105 · Missing primary key

A snapshot trigger cannot be built for a tracked table without primary key columns.

**Fix** — declare a primary key on the tracked table.

### E2106 · Circular foreign keys

The tracked tables' foreign keys form a cycle, so no safe delete order can be determined.

**Fix** — break the foreign-key cycle among the tracked tables.

### E2107 · Column name unresolved

A column declared without an explicit database name (`integer()`) keeps its JS property key
until the Drizzle dialect's casing cache resolves the real one, and that lookup returned
nothing — so the snapshot trigger and rollback SQL cannot name the column.

**Fix** — name the column explicitly, e.g. `integer('item_id')`, or pass a `casing` option to
`drizzle(..., { casing: 'snake_case' })` so the dialect can resolve it.

---

## BigQuery target

### E2201 · Cannot determine GCP project id

`bigqueryTarget` could not resolve a project id at construction.

**Fix** — pass `projectId` explicitly, or construct `new BigQuery({ projectId })` so
`client.bigquery.projectId` is set.

### E2202 · Partition column missing from schema

A tracked table's declared `schema` omits its block-number / partition column.

**Fix** — add the column to `tables[].schema` as `INT64 NOT NULL`. The target forces that type and
mode, but the column itself must be declared.

### E2203 · Partition column has the wrong type

The partition column is not `INT64`. `FLOAT64`/`NUMERIC` lose precision above 2^53 (Solana slot
numbers exceed this) and non-integer types break `RANGE_BUCKET` pruning, so reorg-cleanup `BETWEEN`
predicates become inexact.

**Fix** — type the partition column as `INT64`.

### E2204 · Partition column is nullable

The partition column is `NULLABLE`. Under SQL three-valued logic, rows with a `NULL` block number
never match the fork `DELETE` predicate and would linger forever.

**Fix** — make the column `REQUIRED` (`NOT NULL`).

### E2205 · Table is not range-partitioned

An existing live table is not range-partitioned on the declared column. A reorg `DELETE` without
partition pruning scans the whole table — unaffordable at scale.

**Fix** — recreate the table with `RANGE_BUCKET` partitioning on the column (the error prints
suggested DDL).

### E2206 · Unsupported field shape for auto-create

Auto-creation cannot emit DDL for `REPEATED` (array) or `RECORD`/`STRUCT` fields.

**Fix** — pre-create the table manually with the proper `ARRAY<...>` / `STRUCT<...>` column and
re-run; the target validates it without recreating.

### E2207 · Declared column missing from live table

A column declared in the schema does not exist in the live table.

**Fix** — add the column to the table, or drop it from the declared schema.

### E2208 · Column type or mode mismatch

A declared column's type or mode (`NULLABLE`/`REQUIRED`/`REPEATED`) differs from the live table.

**Fix** — align the declared schema with the live table, or migrate the table to match.

### E2209 · Write to an unregistered table

Data was written to a table that isn't listed in `tables[]`, so its rows can't be cleaned up on a
reorg.

**Fix** — add the table to `bigqueryTarget({ tables: [...] })`.

### E2210 · Internal schema-map mismatch

Internal invariant violation (schema map and allowlist disagree).

**Fix** — none; please [report it as a bug](https://github.com/subsquid-labs/pipes-sdk/issues).

### E2211 · Corrupt in-flight sync row

A sync row left in `IN_FLIGHT` state is missing its `range_low`/`range_high` bounds, so recovery
can't proceed.

**Fix** — manual intervention: inspect the sync table row for this pipe `id` and repair or clear the
in-flight state.

### E2212 · Orphaned tracked data

The sync table has no row for this pipe `id`, but tracked tables still hold data from a prior run.
Restarting from the initial cursor would re-process every block and duplicate every row, so the
target refuses to start.

**Fix** — if you deliberately reset the sync table, also `TRUNCATE`/drop the tracked tables it names.
If you're upgrading from a pre-`id`-keyed cursor, keep the old cursor by pinning
`settings: { state: { id: 'stream' } }` — see the
[migration guide](https://github.com/subsquid-labs/pipes-sdk/blob/main/packages/pipes/MIGRATION.md#10-target-cursors-are-now-keyed-by-the-pipe-id).

### E2213 · BigQuery rejected rows in AppendRows

`AppendRows` returned per-row errors (proto-schema mismatch, `NOT NULL` violation, value out of
range). The affected rows are **not** written.

**Fix** — compare the live table schema against the descriptor the writer uses and fix the offending
column or values.

---

## Parquet target

### E2301 · No tables declared

`parquetTarget` was given an empty `tables` list — nothing to write.

**Fix** — declare at least one table in `parquetTarget({ tables: [...] })`.

### E2302 · Duplicate table name

Two declared tables share a name.

**Fix** — make each table name unique.

### E2303 · Empty schema

A table declared no columns.

**Fix** — declare at least one column.

### E2304 · Block-number column missing

A table's schema omits the block-number column.

**Fix** — add it as an integer column (`INT64`), or set `blockNumberColumn` to the column that
carries the block number.

### E2305 · Block-number column has the wrong type

The block-number column is not an integer type.

**Fix** — declare it `INT64` (or `INT32`).

### E2306 · Unsupported compression codec

A column declared a compression codec the target doesn't support.

**Fix** — use one of the supported codecs (the message lists them).

### E2307 · Unsupported column type

A column declared a type the target doesn't support.

**Fix** — use a supported leaf type, or `LIST` / `STRUCT` (the message lists the supported set).

### E2308 · Write to an unregistered table

Data was written to a table that isn't declared in `tables[]`.

**Fix** — add the table to `parquetTarget({ tables: [...] })`.

### E2309 · File collision

`publish()` would overwrite an existing Parquet file for a block range — a sign of overlapping
segments or a dirty output directory.

**Fix** — write to a clean output directory and don't point two writers at the same one.

### E2310 · Corrupt state file

The persisted state file exists but could not be parsed.

**Fix** — inspect or remove the state file to recover.

### E2311 · Block-number column is optional

The block-number column is declared `optional`, but every row must be attributable to a block for
file-range reasoning and crash recovery. Null or missing values are rejected before append.

**Fix** — declare the block-number column required (remove `optional`).

### E2312 · Invalid block-number value

A row's block-number column held a missing or non-finite value.

**Fix** — ensure every row carries a finite integer block number.

### E2313 · Row value does not match column type

A dev-mode value check failed: a required value was null, a `STRUCT` column got a non-object, a
`LIST` column got a non-array, or a leaf value didn't match its declared type.

**Fix** — correct the row so it matches the declared schema.

### E2314 · Crash recovery could not delete an over-cursor file

After a crash, a Parquet file whose blocks exceed the committed cursor could not be deleted; leaving
it would duplicate data on resume.

**Fix** — clear the filesystem error (permissions, locks) and remove the file so recovery can finish.

### E2315 · Invalid nested schema

A nested column declaration is malformed — an empty `STRUCT`, a `LIST` without `element`, a
non-object column declaration, or nesting too deep (possibly cyclic).

**Fix** — a `STRUCT` needs at least one field and a `LIST` needs an `element`; correct the
declaration.

### E2316 · Invalid segment coverage range

Files are named `<from>-<to>.parquet` for the block window they cover, and a segment was about to be
named for a range that could not be formed — inverted (`from` above `to`), or with no coverage start
recorded for the table. This is an internal invariant; it should not be reachable from user code.

**Fix** — not user-serviceable. Please report it with the surrounding logs.

### E2317 · State file disagrees with the data files

The data files were written by a run that committed further than the state file records — typically a
restored older state file, or a cursor rewound by hand. A published `<from>-<to>.parquet` file
straddles the committed cursor (`from <= cursor < to`) *and* does not start where the persisted
coverage says that table was next due to publish from. Such a file holds committed data (blocks at or
below the cursor) that a resume would never re-fetch, so deleting it as an incomplete-checkpoint
remnant would lose data. The refusal happens **before** anything is deleted or published, so the data
files are intact.

A straddling file whose `from` *does* match the table's persisted coverage start is not this error:
that is the file an interrupted checkpoint was publishing, and a sparse table's stretched file
straddles the cursor as a matter of course. It is deleted and re-derived like any other remnant.

**Fix** — restore the state file that matches the data files, or delete both the state file and the
affected table directories to re-index that range from scratch.

**Upgrading from a version that kept no coverage record**: if the previous version's last run
crashed, its checkpoint remnant can itself straddle the cursor (a row keyed below the cursor puts a
row-min/max name's `from` at or below it), and with no coverage to explain the straddle this error
fires on the first post-upgrade start. That remnant is an ordinary incomplete-checkpoint leftover:
deleting just that file and restarting is enough — recovery re-fetches and regenerates it. To avoid
the manual step entirely, restart the pipe once on the old version (letting its own recovery run)
before upgrading.

A related condition is *not* fatal: if the persisted coverage for a table starts ahead of the
furthest block a file could consistently cover from for the resume cursor, it is clamped back to
that block and logged as a warning. The usual cause is an edit to the configured query ranges
(a gap the recorded start referred to no longer exists), and clamping keeps the blocks after the
cursor claimed by a file.

### E2320 · Engine output is not a Parquet file

A segment writer engine (`settings.engine`) finished a segment file that fails the Parquet
envelope check. The refusal names which condition failed:

- the file is smaller than any valid Parquet file (12 bytes);
- it does not start **and** end with the `PAR1` magic bytes;
- its footer length field — the little-endian uint32 immediately before the trailing magic —
  claims a footer that cannot fit inside the file (this is what rejects arbitrary bytes merely
  wrapped in `PAR1`);
- or the file could not be opened or read at all during verification.

The target refuses to publish at the checkpoint, before the file gets a published name, so
downstream readers never see it and the run is fully recoverable (the cursor never advanced
past the affected rows). This is an envelope check, not a decode: a structurally valid Parquet
file with wrong contents is beyond it.

**Fix** — the engine implementation is broken: it must write a complete Parquet file (including
the footer) to the temp path it was given before resolving `finish()`. The built-in
`parquetjsEngine` cannot produce this error.

---

## Google PubSub target

PubSub is append-only: a published message cannot be read back, updated, or deleted. Most of the
codes below fire *before* anything is published, because after that there is nothing to take back.

### E2401 · Topic does not exist

A configured route names a topic that is missing from the project. The default `topicSetup:
'validate'` checks this once at start, before any data is accepted.

**Fix** — create the topic, or set `topicSetup: 'create'` (dev convenience; needs admin IAM).
`topicSetup: 'none'` skips topic administration entirely for least-privilege deployments.

### E2402 · Reserved attribute name

A user attribute collides with a reserved namespace: names starting with `_` belong to the target
(`_finalized`, `_uid`), names starting with `goog` to Google Cloud. The CDC fields `_id`, `_CHANGE_TYPE`, and
`_CHANGE_SEQUENCE_NUMBER` belong in the message body and are added by the target.

**Fix** — rename the attribute. Every unprefixed name is free, deliberately including common
business names like `id` and `op`.

### E2403 · Attribute budget exceeded

The message exceeds PubSub's per-message attribute limits: 100 attributes (99 for the user beside
`_finalized`, or 98 with `publish.uidAttribute`), 256 bytes per key, 1024 bytes per value. Non-string values are
refused here too — PubSub attributes are strings, and filters compare them as strings.

**Fix** — publish the value in the payload instead, or shorten it. Filter attributes should stay
short and low-cardinality.

### E2404 · Message too large

The encoded data exceeds PubSub's 10 MB message limit, or the data plus attributes, ordering
key, topic resource name, and protobuf framing exceeds the 10 MB publish-request limit.

**Fix** — split the row, shorten its attributes, or use a smaller route-level encoding.

### E2405 · Canonical codec cannot encode this value

The canonical codec met a value outside the protocol: an unsafe integer, `NaN`/`±Infinity`, a
`Map`/`Set`/`RegExp`/class instance, a function or symbol, an `undefined` inside an array, or an
invalid `Date`. The message names the exact path (`$.swap.amounts[1]`).

The codec is deliberately total rather than lenient: "same operation ⇒ same bytes" is a protocol
guarantee, and a silent coercion (an unsafe integer rounding, an `undefined` becoming `null`) would
break it invisibly.

**Fix** — pass a `bigint` or a string for large integers, a number for sub-second timestamps, and a
plain object/array for structures. A route-level `encode` receives the complete CDC row.

### E2406 · Canonical codec met a cycle

The payload references itself.

**Fix** — break the cycle.

### E2407 · Dataset reports no finalized head

The dataset never reports a finalized watermark, so the target cannot keep a rollback manifest —
and nothing it publishes could be compensated if the chain forked.

**Fix** — read the finalized stream, or set `assumeNoForks: true` to assert that this dataset
cannot fork. The absence of a watermark is not evidence that forks cannot happen, which is why the
assertion has to be explicit.

### E2408 · Fork reported under `assumeNoForks`

A chain fork arrived on a pipe that declared the dataset fork-free. Nothing was recorded to
compensate with, and the affected operations are already published.

**Fix** — remove `assumeNoForks`, and re-bootstrap the consumers of the affected topics.

### E2409 · Block has no hash

A fork-capable dataset delivered a block without a hash, and the route relies on generated ids.
Bare block numbers repeat after a fork, so the generated id would alias an orphaned row with a
canonical one.

**Fix** — supply a string `_id` in `MessageDraft.data`, set `MessageDraft.id`, or include block
hashes in the source data.

### E2410 · State file is locked

Another process holds the state file. Exactly one producer may own one: it is the authoritative
sequencer for every id it publishes, and a second writer would hand consumers change sequence
numbers they already hold.

**Fix** — run one instance per state path.

### E2411 · State schema version mismatch

The state file was written by a different schema version of the target. State schemas are not
migrated in place.

**Fix** — run the SDK version that owns the state, or start a fresh state and re-bootstrap the
destination. A new state file is a new sequencer; see the cold-start warning in the target's logs.

### E2412 · Ordering key while message ordering is disabled

A route set a per-draft `orderingKey`, but `publish.messageOrdering` is disabled.

**Fix** — set `publish.messageOrdering: true`, or drop the key. With ordering enabled, the topic
name is the default key and a draft can override it to create a separate ordered partition.

### E2413 · Materialized id moved

A materialized route changed how it resolves ids between `data._id`, `MessageDraft.id`, its
`deriveId` callback, and the generated fallback; or an existing id changed its topic, ordering key,
or filter attributes between revisions. Either change can leave an old row behind or make a
subscription filter stop receiving the row mid-life.

**Fix** — use one id source throughout a materialized route, and keep each row's resolved identity
and attributes stable for its whole lifetime.

### E2414 · Delete-free window route without an empty value

`windowTopic({ emptyWindows: 'upsert' })` was declared without `emptyValues`. The two go together: a
fork can orphan every revision of a window id, and the compensation for that has no row behind it —
the target must synthesize one, and only the route knows what an empty window looks like.

**Fix** — supply `emptyValues`. Without it the route would be delete-free in normal operation but
emit a `delete` on a fork, which is the worst of both: its consumers skipped tombstone retention on
the strength of the guarantee.

### E2415 · Draft without a usable block

A route produced an operation with no `block.number`. Every operation is attributed to its block —
that attribution is what makes fork compensation possible.

**Fix** — carry the row's block through `map`.

### E2416 · `publishFrom: 'latest'` cannot be resolved

The dataset reports neither a chain head nor a finalized head, so there is no head to go live at.

**Fix** — pass an explicit `publishFrom` block.

### E2417 · State file unavailable

The state file could not be opened. It is the producer's sequencer, so it must live on a persistent
volume — not ephemeral container storage.

**Fix** — check the path and its permissions, and mount it on durable storage.

### E2418 · Two drafts with the same id in one batch

An `event` route produced two operations sharing an id in one batch. On an event route every id is
write-once, so the second would silently overwrite the first for every consumer.

**Fix** — make the id unique per row, or declare `mode: 'materialized'` if the row is meant to be
revised.

### E2419 · State file belongs to another producer

The state file records a different cursor key than the one this pipe binds. Only the cursor row
is keyed — the outbox, the manifest and the sequence counters are producer-wide — so adopting
another producer's file would report a clean warm start while publishing its pending operations
under this pipe's identity.

**Fix** — one state file per producer. Give this pipe its own `state.path`, or pin
`settings.id` to the key the file was written under if this pipe really is that producer
renamed.

### E2420 · Cold start refused

The run started with no state at the configured path, so it would restart the producer's change
sequence at zero. If that namespace has already published, BigQuery discards the republished lower
numbers as stale and every affected row freezes at its old value — with no error on either side.
That is why recovery from lost state is not a restart.

**Fix** — to bootstrap a namespace that has never published, set `allowColdStart: true`. To recover
a namespace whose state was lost, publish under a fresh namespace and re-bootstrap every
destination; restoring the state file from a backup reintroduces the same failure.

### E2421 · State wire configuration changed

The `namespace`, `attributes`, `publish.uidAttribute`, or `publish.messageOrdering` setting changed
while reusing PubSub state. Those settings determine published metadata and ordering keys. Continuing could
rebuild an unconfirmed outbox row differently during crash recovery. The same error is raised when
a route switches between the canonical and a custom encoder while that route still has pending
outbox rows.

**Fix** — restore the settings and encoder kind used to create the pending rows, drain the outbox,
and then change the encoder; or treat the change as a new feed with fresh state and a fresh
namespace.

### E2422 · Invalid BigQuery CDC row

A draft's `data` is not a plain object, its `_id` is not a non-empty string, or it owns
`_CHANGE_TYPE` or `_CHANGE_SEQUENCE_NUMBER`. The target must add the change fields to produce a
valid CDC message.

**Fix** — wrap primitive, array, or binary values in an object field; use a non-empty string `_id`
(or omit it to let the target derive one); and remove the target-owned change fields from the row.

### E2423 · Route missing during recovery

The durable outbox contains an operation for a route that is no longer configured. The route is
needed to encode the final CDC message during recovery.

**Fix** — restore the route configuration, drain the state, and only then remove the route.

### E2424 · Change sequence exhausted

The producer-wide BigQuery CDC sequence reached the largest integer the state can increment without
precision loss, or a sequence outside that supported range reached the encoder. Reusing a sequence
would make later changes look stale to BigQuery.

**Fix** — start a new feed with fresh state and a fresh namespace, then re-bootstrap its destination.

### E2425 · No routes configured

The target was constructed with no `topics` entry, so it would open a state file, take its
exclusive lock, and publish nothing.

**Fix** — configure at least one topic route.

### E2428 · Producer feeds more than one topic

The change sequence is one producer-wide counter, so a producer that feeds several topics leaves
none of them with a contiguous run — and a consumer reading a gap-free run as a completeness
barrier would see holes it cannot distinguish from lost messages.

**Fix** — run one producer per topic, or set `sequenceBarrier: false` to declare that no consumer
of this producer relies on a contiguous sequence run.

Several routes may name the same topic, but with the barrier on only one of them can span more
than one block per batch: routes are mapped one after another, so the second restarts at the
batch's first block and is refused by E2429. Merge such streams into one route's `map`.

### E2429 · Operations step backwards in block order

A batch mapped an operation for a lower block than one already sequenced. Operations take their
numbers in the order they are mapped, and consumers read a contiguous run reaching block *N+1* as
proof that block *N* is complete — a backwards step makes that reading wrong.

**Fix** — emit drafts in block order. A fork's rewind is not a backwards step: the target resets
the check when it resolves one. Set `sequenceBarrier: false` if no consumer relies on block
boundaries.

Note that the check walks the batch in sequencing order, which is route by route rather than
block by block. A second route on the same topic therefore restarts at the batch's first block
and lands here on any batch spanning more than one block, with no ordering the mapper can supply
to avoid it — the barrier admits one multi-block route per producer. Merge the streams into one
`map`, or turn the barrier off.
