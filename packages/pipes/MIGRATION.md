# Migration guide

Step-by-step instructions for updating from the previous release.

---

## ⚠️ Naming overhaul: hard renames and removals

The public API naming overhaul renames symbols **without** compatibility aliases — this lands in a major release, so old names simply stop existing and the compiler will point you at each one (`resolveFork`, `canonicalBlocks`, `rollback` hooks, `PortalStream`, `add*Request` builder methods, `evmEventDecoder`, `chunkForInsert`, `contractFactorySqliteStore`, CLI `target` config key, `/preview/transformation`, `sqd_processed_block`/`sqd_end_block`, and friends). All previously deprecated APIs are removed as well: the aliases `evmPortalSource`/`solanaPortalSource`/`hyperliquidFillsPortalSource`, `factory`, `factorySqliteDatabase`, `chunk`, and `createClickhouseTarget`; the `DecodedInstruction.blockNumber` duplicate of `block.number`; the ClickHouse `onRollback` context's `cursor` duplicate of `safeCursor`; and the Parquet `'TIMESTAMP_MILLIS'` column-type alias (write `'TIMESTAMP'` — identical wire format).

`finalizationBuffer`, `FinalizationBuffer` and the `Finalization` state type are removed. A target
that commits only finalized data now declares `requiresFinalizedStream: true` and writes rows as
they arrive; see section 14.

Other silent-at-compile-time changes to check: the ClickHouse `onRollback` callback receives `reason: 'recovery' | 'fork'` instead of `type: 'offset_check' | 'blockchain_fork'`; Prometheus dashboards must move to `sqd_processed_block`/`sqd_end_block`; Pipes UI older than this release reads endpoints/payload keys that no longer exist.

---

## 1. Rename portal sources to portal streams

All portal source functions have been renamed to portal streams. The old names are removed — there are no compatibility aliases.

```ts
// before
import { evmPortalSource } from '@subsquid/pipes/evm'
import { solanaPortalSource } from '@subsquid/pipes/solana'
import { hyperliquidFillsPortalSource } from '@subsquid/pipes/hyperliquid'

// after
import { evmPortalStream } from '@subsquid/pipes/evm'
import { solanaPortalStream } from '@subsquid/pipes/solana'
import { hyperliquidFillsPortalStream } from '@subsquid/pipes/hyperliquid'
```

---

## 2. Move decoders from `.pipe()` into `outputs`

This is the most common change. Instead of chaining `.pipe(decoder)` after the source, pass your decoder through the `outputs` option. The EVM decoder itself is renamed: `evmDecoder` → `evmEventDecoder` (it decodes event logs specifically, matching `solanaInstructionDecoder`).

### Single decoder

```ts
// before
const stream = evmPortalSource({
  portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
}).pipe(
  evmDecoder({
    range: { from: 'latest' },
    events: { transfers: commonAbis.erc20.events.Transfer },
  }),
)

// after
const stream = evmPortalStream({
  portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  outputs: evmEventDecoder({
    range: { from: 'latest' },
    events: { transfers: commonAbis.erc20.events.Transfer },
  }),
})
```

### Multiple decoders (was `.pipeComposite()`)

```ts
// before
const stream = evmPortalSource({
  portal: 'https://portal.sqd.dev/datasets/base-mainnet',
}).pipeComposite({
  transfers: erc20Transfers({ range }),
  swaps:     uniswapV3Decoder({ range }),
})

// after
const stream = evmPortalStream({
  portal: 'https://portal.sqd.dev/datasets/base-mainnet',
  outputs: {
    transfers: erc20Transfers({ range }),
    swaps:     uniswapV3Decoder({ range }),
  },
})
```

The `data` shape is unchanged — `data.transfers`, `data.swaps` etc. still work as before.

---

## 3. Add a pipe `id` (now required)

Every portal stream now requires an `id`. It must be **globally unique, stable and non-empty** — targets use it as a cursor key to persist progress (see section 10). Two pipes that share the same `id` will overwrite each other's cursor. The `id` is also used to scope log lines and Prometheus metric labels.

Calling `.pipeTo()` without an `id` throws `DefaultPipeIdError` (E0001) at startup; an empty or blank `id` throws at stream construction.

```ts
// before
await evmPortalSource({ portal: '...' })
  .pipe(evmDecoder({ ... }))
  .pipeTo(clickhouseTarget({ ... }))

// after
await evmPortalStream({
  id: 'eth-transfers',     // globally unique, stable ID for cursor persistence
  portal: '...',
  outputs: evmEventDecoder({ ... }),
}).pipeTo(clickhouseTarget({ ... }))
```

---

## 4. Rename `factory()` to `contractFactory()`

The SQLite store is backend-qualified: `factorySqliteDatabase` → `contractFactorySqliteStore` (an intermediate 1.0-alpha name `contractFactoryStore` was renamed again — the backend is part of the contract, since a `path` option writes a file to disk).

```ts
// before
import { factory, factorySqliteDatabase } from '@subsquid/pipes/evm'

factory({
  address: '0x1f98...',
  event: factoryAbi.PoolCreated,
  parameter: 'pool',
  database: factorySqliteDatabase({ path: './pools.sqlite' }),
})

// after
import { contractFactory, contractFactorySqliteStore } from '@subsquid/pipes/evm'

contractFactory({
  address: '0x1f98...',
  event: factoryAbi.PoolCreated,
  childAddressField: 'pool',            // renamed from `parameter`
  database: contractFactorySqliteStore({ path: './pools.sqlite' }),
})
```

`childAddressField` also accepts a function for custom extraction logic:

```ts
contractFactory({
  address: '0x1f98...',
  event: factoryAbi.PoolCreated,
  childAddressField: (decoded) => decoded.pool,
  database: contractFactorySqliteStore({ path: './pools.sqlite' }),
})
```

---

## 5. Update Solana sources

`solanaPortalStream` dropped the `query` option and `.pipeComposite()`. Use `outputs` instead.

```ts
// before
const stream = solanaPortalSource({
  portal: 'https://portal.sqd.dev/datasets/solana-mainnet',
}).pipeComposite({
  orcaSwaps: createSolanaInstructionDecoder({ range: { from: '340,000,000' }, ... }),
  raydiumSwaps: createSolanaInstructionDecoder({ range: { from: '340,000,000' }, ... }),
})

// after
const stream = solanaPortalStream({
  portal: 'https://portal.sqd.dev/datasets/solana-mainnet',
  outputs: {
    orcaSwaps:    solanaInstructionDecoder({ range: { from: '340,000,000' }, ... }),
    raydiumSwaps: solanaInstructionDecoder({ range: { from: '340,000,000' }, ... }),
  },
})
```

Note: `createSolanaInstructionDecoder` → `solanaInstructionDecoder` (rename, no alias).

---

## 6. Update runner configuration

The runner factory is now `devRunner` (was `createDevRunner`), its `stream` field is now `handler`, and `RunConfig` is now `PipeContext`.

```ts
// before
import { RunConfig, createDevRunner } from '@subsquid/pipes/runtime/node'

async function indexTransfers({ id, params }: RunConfig<{ portal: string }>) { ... }

createDevRunner([
  { id: 'eth', params: { portal: '...' }, stream: indexTransfers },
])

// after
import { PipeContext, devRunner } from '@subsquid/pipes/runtime/node'

async function indexTransfers({ id, params }: PipeContext<{ portal: string }>) { ... }

devRunner([
  { id: 'eth', params: { portal: '...' }, handler: indexTransfers },
])
```

---

## 7. Update custom transformers that read raw portal data

If you wrote a custom transformer that accesses `data.blocks`, remove the `.blocks` accessor — `data` is now the array directly.

```ts
// before
source.pipe({
  profiler: { name: 'my transformer' },
  transform: (data, ctx) => {
    return data.blocks.map((block) => ({
      number: block.header.number,
      logs:   block.logs ?? [],
    }))
  },
})

// after
source.pipe({
  profiler: { name: 'my transformer' },
  transform: (data, ctx) => {
    return data.map((block) => ({
      number: block.header.number,
      logs:   block.logs ?? [],
    }))
  },
})
```

---

## 8. Update custom query builder usage (`.build()`)

If you use `evmQuery().build(...)` directly (e.g. in a custom decoder), separate the transform from the build call. The transformer's fork hook is renamed `fork` → `rollback` — it receives an already-resolved safe cursor and its job is the destructive undo, not fork resolution.

```ts
// before
const decoder = evmQuery()
  .addFields(myFields)
  .build({
    setupQuery: ({ query }) => query.merge(extraQuery),
    profiler: { name: 'my-decoder' },
    transform: (data, ctx) => data.blocks.map(decode),
    fork: async (cursor, ctx) => { /* rollback state */ },
  })

// after
const decoder = evmQuery()
  .addFields(myFields)
  .build({ setupQuery: ({ query }) => query.merge(extraQuery) })
  .pipe({
    profiler: { name: 'my-decoder' },
    transform: (data, ctx) => data.map(decode),
    rollback: async (cursor, ctx) => { /* undo state above the cursor */ },
  })
```

The data-request methods on every query builder gained a `Request` suffix — the argument is a request/filter for the entity, not the entity itself (`addLog` → `addLogRequest`, `addTransaction` → `addTransactionRequest`, `addTrace` → `addTraceRequest`, `addStateDiff` → `addStateDiffRequest`, `addInstruction` → `addInstructionRequest`, and so on across the EVM, Solana, Bitcoin, Tron and Hyperliquid builders). `addFields` and `addRange` are unchanged — they add actual fields and ranges.

```ts
// before
evmQuery().addLog({ range: { from: 0 }, request: { topic0: [transferTopic] } })

// after
evmQuery().addLogRequest({ range: { from: 0 }, request: { topic0: [transferTopic] } })
```

---

## 9. Update progress tracker callback types

```ts
// before
import { StartState, ProgressState } from '@subsquid/pipes'

evmPortalSource({
  portal: '...',
  outputs: evmDecoder({ ... }),
  progress: {
    onStart:    (data: StartState)    => console.log(`starting from block ${data.initial}`),
    onProgress: (data: ProgressState) => console.log(`${data.state.current.number}`),
  },
})

// after
import { StartEvent, ProgressEvent } from '@subsquid/pipes'

evmPortalStream({
  portal: '...',
  outputs: evmEventDecoder({ ... }),
  progress: {
    onStart:    (event: StartEvent)    => console.log(`starting from block ${event.state.initial}`),
    onProgress: (event: ProgressEvent) => console.log(`${event.progress.state.current}`),
  },
})
```

Inside `ProgressEvent`, the range bounds are named `from`/`to` (previously `initial`/`last`), matching the `range: { from, to }` option vocabulary — `to` is the end of the indexed range (the configured `to` bound, or the chain head when unbounded). `current` is a plain block number, and per-interval activity stats live under `progress.intervalStats` (previously `interval`).

---

## 10. Target cursors are now keyed by the pipe `id`

Previously every target persisted its cursor under the static default key `"stream"`, no matter
which pipe wrote it — two pipes sharing one offset table silently overwrote each other's progress.
Cursors are now keyed by the pipe's `id`. An explicit per-target id still wins and disables
everything described below:

```ts
clickhouseTarget({ settings: { id: 'my-key' } })   // ClickHouse
drizzleTarget({ settings: { state: { id: 'my-key' } } })  // Postgres
bigqueryTarget({ settings: { state: { id: 'my-key' } } }) // BigQuery
parquetTarget({ settings: { id: 'my-key' } })      // Parquet
```

### What happens on the first restart after upgrading

| Target | Behaviour |
|---|---|
| **ClickHouse** | Sync rows left under the legacy `"stream"` key are re-keyed to the pipe `id` automatically (one-time, logged as a warning), and indexing resumes from the migrated cursor. |
| **Postgres (Drizzle)** | Same — the legacy `"stream"` sync rows are re-keyed to the pipe `id` in a single atomic `UPDATE` and indexing resumes from the migrated cursor. |
| **BigQuery** | **No automatic migration.** A deployment with WAL rows under `"stream"` and data in tracked tables refuses to start with `ORPHAN_TRACKED_DATA` (a deliberate guard against silent re-processing). To resume the old cursor, pin the legacy key explicitly: `settings: { state: { id: 'stream' } }`. |
| **Parquet** | **No automatic migration.** The state file moved from `_sqd_parquet_state.json` to `_sqd_parquet_state.<pipe-id>.json`. Rename the file on disk to the new name before restarting — otherwise the pipe restarts from the beginning and fails on colliding parquet file names. (Deployments that already set an explicit `settings.id` were using the suffixed name before and are unaffected.) |

### Several pipes sharing one offset table under the old default

Under the shared `"stream"` key only one cursor ever survived, and it belonged to only **one** of
those pipes. After the upgrade, the first pipe to start consumes the legacy rows — including a
finalized watermark that is monotonic and cannot be lowered afterwards. For such setups:

1. Pin an explicit per-target id on the pipe that should keep the cursor **before** upgrading.
2. Let the other pipes start fresh under their own ids (or backfill them deliberately).
3. Avoid starting the upgraded pipes concurrently on the very first run — the migration itself is
   not serialized on ClickHouse.

Single-pipe deployments (the common case) need no action: the cursor migrates automatically and a
one-time warning is logged.

---

## 11. Rename types

| Before | After |
|---|---|
| `ResultOf<T>` | `OutputOf<T>` |
| `BatchCtx` | `BatchContext` |
| `RunConfig` | `PipeContext` |
| `FactoryOptions` | `ContractFactoryOptions` |
| `StartState` | `StartEvent` |
| `ProgressState` | `ProgressEvent` |
| `PortalSource` / `PortalSourceOptions` | `PortalStream` / `PortalStreamOptions` |
| `Ctx` | `HookContext` |
| `StartCtx` / `StopCtx` | `StartContext` / `StopContext` |
| `BatchStreamContext` | `StreamInfo` |
| `SdkError` | `SdkErrorName` |
| `Settings` (ClickHouse) | `ClickhouseSettings` |
| `ForkNoPreviousBlocksError` | `MissingForkAncestorError` (code E1002 unchanged) |

The `PortalClientOptions` duration keys are unit-suffixed: `maxIdleTime` → `maxIdleTimeMs`, `maxWaitTime` → `maxWaitTimeMs`, `headPollInterval` → `headPollIntervalMs` (all were already milliseconds).

---

## 12. Rename utility functions

| Before | After |
|---|---|
| `chunk` (also `batchForInsert` in earlier 1.0 alphas) | `chunkForInsert` |
| `createDefaultLogger` | `defaultLogger` |
| `createFinalizationBuffer` | Removed — use `requiresFinalizedStream` on the target (section 14) |
| `toSnakeKeys` | `toSnakeCaseKeys` |
| `displayEstimatedTime` | `formatEta` |
| `coerceFinalized` | `normalizeFinalized` |
| `lines` | `joinLines` |
| `parseBlockFormatting` | `parseFormattedBlock` |
| `BQ_ERR` / `PQ_ERR` | `BIGQUERY_ERROR_CODES` / `PARQUET_ERROR_CODES` |
| `BigQueryState` / `BigQueryStore` / `BigQueryTracker` | `BigQuerySyncState` / `BigQueryWriter` / `BigQueryTableRegistry` |

---

## 13. Rename removed imports

| Before | After | Notes |
|---|---|---|
| `createEvmPortalSource` | `evmPortalStream` | Alias removed |
| `createSolanaPortalSource` | `solanaPortalStream` | Alias removed |
| `createSolanaInstructionDecoder` | `solanaInstructionDecoder` | Renamed, no alias |
| `evmDecoder` | `evmEventDecoder` | Renamed, no alias |
| `createClickhouseTarget` | `clickhouseTarget` | Alias removed |
| `contractFactoryStore` (1.0 alphas only) | `contractFactorySqliteStore` | Renamed, no alias |
| `createDevRunner` (1.0 alphas only) | `devRunner` | Renamed, no alias |
| `new EvmQueryBuilder()` | `evmQuery()` | Shorthand factory, old still works |
| `new SolanaQueryBuilder()` | `solanaQuery()` | Shorthand factory, old still works |
| `new HyperliquidFillsQueryBuilder()` | `hyperliquidFillsQuery()` | Shorthand factory, old still works |

---

## 14. Update custom targets and remove finalization buffers

If you implement the `Target` interface directly, the contract method the engine calls on a
detected chain fork is renamed `fork(previousBlocks)` → `resolveFork(canonicalBlocks)`: it must
find the common ancestor with the given canonical chain, undo everything above it, and return the
resume cursor. The parameter rename matters too — the blocks are the portal's view of the
*canonical* chain (a.k.a. `previousBlocks` in the Portal API's 409 response body), not the blocks
you just processed.

The word *rollback* is reserved for hooks that receive an **already-resolved** cursor and only
undo state: the transformer hook (`fork` → `rollback`, see section 8) and the target callbacks
(`onRollback`, `onBeforeRollback`/`onAfterRollback`), which fire for forks *and* startup recovery.

For a target that writes only finalized data, remove its `FinalizationBuffer` entirely. Declare
`requiresFinalizedStream: true`; the source then resolves `latest` against the finalized head
(falling back to the hot head on a dataset that finalizes nothing) and reads `/finalized-stream`,
even when the portal was configured hot. Every delivered block is final, so write rows immediately
and remove the target's `resolveFork` implementation:

```ts
// before
const buffer = finalizationBuffer<Row>({ getBlockNumber: (row) => row.blockNumber })
const target = createTarget<Row[]>({
  write: async ({ read }) => {
    for await (const { data, ctx } of read()) {
      const rows = buffer.push(data, {
        finalized: ctx.stream.head.finalized,
        rollbackChain: ctx.stream.state.rollbackChain,
      })
      if (rows.length > 0) await append(rows)
    }
  },
  resolveFork: (canonicalBlocks) => buffer.resolveFork(canonicalBlocks),
})

// after
const target = createTarget<Row[]>({
  requiresFinalizedStream: true,
  write: async ({ read }) => {
    for await (const { data } of read()) {
      if (data.length > 0) await append(data)
    }
  },
})
```

Hot targets that can undo writes still use `resolveFork(canonicalBlocks)` as described above.
Custom `PortalCache` implementations receive both `finalized: true` and a client whose
finality-sensitive methods are pinned to the finalized endpoints; use the client passed to
`getStream` rather than a separately retained client.

If you drive `ParquetStore` directly, call `flushBatch()` without finalization state;
`ParquetStore.resolveFork` is removed.

Check your ranges before upgrading: a bounded pipe whose `to` sits above the finalized head used to
finish once the portal had delivered the range, and now waits for it to finalize. If a job is
expected to terminate — a CI check, a cron backfill — end it at or below the finalized head, or
point it at a target that reads the hot stream.

---

## 15. ClickHouse rollbacks are engine-aware

The `onRollback` discriminator changed: the callback now receives `reason: 'recovery' | 'fork'`
instead of `type: 'offset_check' | 'blockchain_fork'` (`'recovery'` fires on every restart with a
persisted cursor; `'fork'` on chain forks). The context also no longer carries the `cursor`
duplicate — use `safeCursor`. Both are runtime-visible only if you branched on the old values, so
grep for them.

Beyond that, no code changes are required — `onRollback` implementations calling
`store.removeAllRows` keep working. What changes is what happens under the hood, depending on each
table's engine:

| Table engine | Behaviour after upgrading |
|---|---|
| `CollapsingMergeTree` family with a `sign` column | Cancel rows (`sign = -1`), netted with a `GROUP BY / sum(sign)` query instead of `SELECT * FINAL` — correct under insert-retry duplicates and fast on large tables. A minmax skip index `_sqd_rollback_idx` on `block_number` is created on first rollback. |
| Any other engine (`MergeTree`, `ReplacingMergeTree`, ...) | Lightweight `DELETE` with a logged warning. Previously cancel rows were inserted blindly, which failed or silently corrupted such tables. Requires ClickHouse ≥ 23.3. **Materialized views on these tables keep the rolled-back data** — switch the table to `CollapsingMergeTree(sign)` if you rely on MVs. |
| `Distributed` | Explicit error — roll back the underlying local table instead. |

Recommended follow-ups:

1. Call `store.ensureRollbackIndex({ table })` in `onStart` for existing large tables — the index is
   built by an async mutation, so creating it eagerly avoids one slow first rollback.
2. If the rolling client cannot read `system.tables` / `system.columns`, rollbacks log a warning and
   fall back to the previous `FINAL`-based cancel-row behavior; grant read access to get the new
   mechanics.

---

## 16. Parquet: rename `TIMESTAMP_MILLIS` to `TIMESTAMP`

The Parquet format spec deprecates the `TIMESTAMP_MILLIS` converted type in favor of the `TIMESTAMP` logical type. The column type is renamed accordingly; the old name is removed (no alias). Both spellings write byte-identical files (int64 epoch-ms, readable by every Parquet reader as `TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS)`), so existing data needs no migration — only schemas change.

```ts
// before
schema: { timestamp: { type: 'TIMESTAMP_MILLIS', optional: true } }

// after
schema: { timestamp: { type: 'TIMESTAMP', optional: true } }
```

New column types are also available: `DATE` (int32 days since the Unix epoch), `JSON` (stringified into an annotated BYTE_ARRAY), `STRUCT` (nested groups — insert plain nested objects) and `LIST` (canonical 3-level lists — insert plain arrays):

```ts
schema: {
  blockNumber: { type: 'INT64' },
  day: { type: 'DATE' },
  meta: { type: 'JSON', optional: true },
  user: { type: 'STRUCT', fields: { name: { type: 'UTF8' } } },
  topics: { type: 'LIST', element: { type: 'UTF8' } },
}
```

---

## 17. Observability: metric and endpoint renames

- Prometheus gauges: `sqd_current_block` → `sqd_processed_block`, and `sqd_last_block` →
  `sqd_end_block`. The second rename is semantic too: the value is the **end of the indexed range**
  (the configured `to` bound, or the chain head when unbounded) — for a range-bounded run it is not
  the chain head, and dashboards labelling it that way were wrong. Update dashboards and alerts.
- Metrics server HTTP API: `GET /exemplars/transformation` → `GET /preview/transformation`
  ("exemplar" collides with the Prometheus/OpenMetrics term of art); the `/profiler` payload key
  `profilers` → `profiles`; the `/stats` payload's `code.filename` → `entrypoint`.
- **Pipes UI:** upgrade `@subsquid/pipes-ui` together with the SDK — older UI versions read the
  removed endpoints and payload keys and will show no data against a 1.0 pipe.

---

## 18. Migrate Google Pub/Sub output to BigQuery CDC

The Pub/Sub target now publishes BigQuery CDC JSON rows instead of the previous custom attribute
envelope. Update the producer and every consumer as one protocol migration.

Configuration changes:

| Before | After |
|---|---|
| `publish: { delivery: 'lww' }` | omit `messageOrdering`, or set `messageOrdering: false` |
| `publish: { delivery: 'ordered' }` | `publish: { messageOrdering: true }` |
| `heartbeat: { everyBlocks: ... }` | removed; use destination-side freshness monitoring |

Drafts and encoders also change:

- `MessageDraft.data` and `RollbackInverse.data` must be plain objects. Wrap strings or binary data
  in an object field.
- Put a non-empty string `_id` in `data` to define the BigQuery row key, or continue using
  `MessageDraft.id`. The target removes the input `_id` before storing the row and restores it in
  the final CDC message.
- `TopicRoute.encode` now receives the complete `BigQueryCdcMessage`, including `_id`,
  `_CHANGE_TYPE`, and `_CHANGE_SEQUENCE_NUMBER`, rather than only the route payload.
- A `delete` draft must carry the row columns containing the destination's primary key. The target
  publishes those columns alongside the CDC metadata; this also applies to fork compensation.
- Pub/Sub attributes now contain only user attributes and the optional `_uid`. The public
  `WIRE_VERSION`, `ENVELOPE_ATTRIBUTES`, and `DeliveryProfile` exports are removed, and E2420 is
  retired.
- The canonical encoder writes a `Date` as an RFC 3339 string (`"2023-11-14T22:13:20.999Z"`)
  instead of unix seconds. A BigQuery subscription reads a JSON *number* in a `TIMESTAMP` column as
  microseconds since the epoch, so the previous encoding landed every timestamp in 1970. If a
  destination column was declared `INT64` to receive the old value, redeclare it as `TIMESTAMP`.
  Milliseconds now survive the round trip.

The other encoder outputs are unchanged but constrain the destination column type: a `bigint`
becomes a decimal string, which BigQuery accepts into `NUMERIC` or `BIGNUMERIC` only when the value
fits, and into `STRING` across the full range, but rejects into `INT64`; byte views become `0x` hex
strings, which belong in `STRING`, not `BYTES`. `docs/pubsub-bigquery.md` tabulates the full mapping
and the DDL it implies.

State schema v2 is not migrated in place. A v1 state file fails at startup with E2411. Before
upgrading, let the old producer drain its outbox. Then start the CDC feed with a fresh state path and
a fresh namespace, and re-bootstrap the destination so its rows and sequence history belong to the
new feed. Reusing the old namespace or destination state can make new CDC changes appear stale.

---

## 19. RPC latency watcher emits an array of two-sided samples

`evmRpcLatencyWatcher` / `solanaRpcLatencyWatcher` / `bitcoinRpcLatencyWatcher` used to emit
`LatencySample | null` at the moment the portal delivered a head, joining against whatever the
reference RPC happened to hold *right then*. Heads the reference had not reported yet produced
`null` and were dropped — which is precisely the case where the portal won. The delay distribution
was therefore truncated at zero and its tail described the reference node, not the portal.

Each head is now held until both sides have reported it (or the wait window closes), and every batch
emits the samples that became decidable, so iterate the result:

```ts
// before
for await (const { data } of stream) {
  if (!data) continue
  console.table(data.rpc)
}

// after
for await (const { data } of stream) {
  for (const sample of data) {
    console.table(sample.rpc)
  }
}
```

`rpc[]` now carries one row per configured endpoint — including endpoints that did not report the
head — and the rows changed shape:

- **`portalDelayMs` is signed.** Negative means the portal delivered the head first. Histogram
  buckets and any `max(0, …)` clamping downstream must be revisited, or portal leads will be folded
  back into the zero bucket.
- **`portalDelayMs` and `receivedAt` are absent when `unresolved` is set.** `rpc-behind` — the
  endpoint had not reached the head before the window closed, so the portal is ahead by at least
  that window; `rpc-missing` — the endpoint is already past the head but never recorded it (reorged
  away, dropped update, or evicted while the portal was backfilling), which carries no latency
  information at all. Count these, do not chart them, and never read a missing delay as zero.

The window defaults to 60s and is configurable per watcher via `resolveTimeoutMs`. Samples surface
one portal batch after they become decidable — the delay they carry is computed from recorded
timestamps and is unaffected by that.

---

## Quick checklist

- [ ] `evmPortalSource` → `evmPortalStream`
- [ ] `solanaPortalSource` → `solanaPortalStream`
- [ ] `hyperliquidFillsPortalSource` → `hyperliquidFillsPortalStream`
- [ ] `.pipe(decoder)` → `outputs: decoder`
- [ ] `.pipeComposite({ ... })` → `outputs: { ... }`
- [ ] Add a globally unique, non-empty `id` to every portal stream
- [ ] Cursor re-keying: nothing to do for single-pipe ClickHouse/Postgres (auto-migrated); BigQuery: pin `state: { id: 'stream' }` to keep the old cursor; Parquet: rename `_sqd_parquet_state.json` to `_sqd_parquet_state.<pipe-id>.json`
- [ ] Pipes sharing one offset table under the old default: pin explicit per-target ids before upgrading
- [ ] `evmDecoder` → `evmEventDecoder`
- [ ] `factory()` → `contractFactory()`
- [ ] `factorySqliteDatabase()` → `contractFactorySqliteStore()`
- [ ] `parameter` → `childAddressField` in factory options
- [ ] `createDevRunner` → `devRunner`, `stream` → `handler` in runner config
- [ ] `RunConfig` → `PipeContext`
- [ ] `ResultOf` → `OutputOf`
- [ ] `chunk` → `chunkForInsert`
- [ ] `createSolanaInstructionDecoder` → `solanaInstructionDecoder`
- [ ] Query builders: `addLog` / `addTransaction` / `addInstruction` / … → `addLogRequest` / `addTransactionRequest` / `addInstructionRequest` / …
- [ ] Custom transformers: `data.blocks` → `data`; `fork` hook → `rollback`
- [ ] Custom `.build({ transform })` → `.build().pipe()`
- [ ] Hot custom targets: `fork(previousBlocks)` → `resolveFork(canonicalBlocks)`
- [ ] Finalized-only custom targets: remove `FinalizationBuffer` / `Finalization` / `resolveFork`, set `requiresFinalizedStream: true`, and write delivered rows immediately
- [ ] `StartState` → `StartEvent`, `ProgressState` → `ProgressEvent` (progress state reads `from`/`to`; interval stats under `intervalStats`)
- [ ] ClickHouse `onRollback`: `type: 'offset_check' | 'blockchain_fork'` → `reason: 'recovery' | 'fork'`; `cursor` → `safeCursor`
- [ ] ClickHouse rollbacks: nothing to do for CollapsingMergeTree tables (optionally call `store.ensureRollbackIndex` in `onStart` on large tables); non-collapsing tables now roll back via `DELETE` (needs ClickHouse ≥ 23.3) and their MVs keep rolled-back data
- [ ] Parquet schemas: `TIMESTAMP_MILLIS` → `TIMESTAMP` (alias removed; files unchanged)
- [ ] Prometheus dashboards: `sqd_current_block` → `sqd_processed_block`, `sqd_last_block` → `sqd_end_block`
- [ ] Pub/Sub: migrate delivery configuration and consumers to BigQuery CDC rows; use fresh v2 state, a fresh namespace, and a re-bootstrapped destination
- [ ] Pub/Sub destination tables: `Date` columns are now RFC 3339 `TIMESTAMP`, not `INT64` unix seconds
- [ ] RPC latency watchers: output is now `LatencySample[]` — iterate it instead of null-checking a single sample
- [ ] RPC latency consumers: `portalDelayMs` is signed (negative = portal first) and absent on `unresolved` rows; stop treating a missing delay as `0`
- [ ] Upgrade `@subsquid/pipes-ui` together with the SDK
