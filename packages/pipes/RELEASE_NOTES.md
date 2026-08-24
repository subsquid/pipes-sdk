# Pipes SDK 1.0 — Release Notes

## Breaking changes

### 1. New chainable `outputs` option (required)

Portal sources no longer expose `.pipe()` / `.pipeComposite()` on the source level.
Instead, every portal source now requires a chainable `outputs` option that defines what data to fetch 
and how to initially transform it.

An output is built with the `query().build().pipe()` chain:

```ts
evmStream({
  source: '...',
  outputs: evmQuery()
    .addLogRequest({
      range: { from: 'latest' },
      request: { topic0: [erc20.events.Transfer.topic] },
    })
    .build()                          // finalizes the query (no further query changes allowed)
    .pipe((blocks) => decode(blocks)) // chain transformers via .pipe()
    .pipe((decoded) => filter(decoded)),
})
```

After `.build()` the query is frozen — you can only chain `.pipe()` transformers from that point.

**Decoders** like `evmEventDecoder()` and `solanaInstructionDecoder()` are
convenience shorthands that wrap `query().build().pipe()` internally. 

They are also chainable:

```ts
// decoder is a shorthand for query().build().pipe()
evmStream({
  source: '...',
  outputs: evmEventDecoder({
    range: { from: 'latest' },
    events: { transfers: erc20.events.Transfer },
  }).pipe((e) => e.transfers), // chain .pipe() on top of a decoder
})
```

Multiple outputs (previously `.pipeComposite()`) are now a named record:

```ts
// before
evmPortalSource({ portal: '...' })
  .pipeComposite({
    transfers: erc20Transfers({ range }),
    swaps:     uniswapV3Decoder({ range }),
  })

// after
evmStream({
  source: '...',
  outputs: {
    transfers: erc20Transfers({ range }),
    swaps:     uniswapV3Decoder({ range }),
  },
})
```

The same change applies to every portal stream (`solanaPortalStream`, `hyperliquidFillsPortalStream`, etc.).

### 2. Raw outputs are now plain block arrays

When using a portal source without a decoder, `data` is now the block array directly — no more `.blocks` wrapper.

```ts
// before
const stream = evmPortalSource({
  portal: '...',
  query: new EvmQueryBuilder().addLog({ topic0: [erc20.events.Transfer.topic] }),
})

for await (const { data } of stream) {
  data.blocks // Block[]
}

// after
const stream = evmStream({
  source: '...',
  // evmQuery() is a shorthand for new EvmQueryBuilder()
  outputs: evmQuery()
    .addLogRequest({ range: { from: 0 }, request: { topic0: [erc20.events.Transfer.topic] } })
    .build(),
})

for await (const { data } of stream) {
  data // Block[]
}
```

Query builder constructors now have shorthand factory functions:

| Before | After |
|---|---|
| `new EvmQueryBuilder()` | `evmQuery()` |
| `new SolanaQueryBuilder()` | `solanaQuery()` |
| `new HyperliquidFillsQueryBuilder()` | `hyperliquidFillsQuery()` |

### 3. Pipe `id` is now required on all portal sources

Every portal source now requires an `id`. It must be **globally unique and stable** — targets use it as a cursor key to persist progress. Two pipes that share the same `id` will overwrite each other's cursor. The `id` is also used to scope log lines and Prometheus metric labels.

Calling `.pipeTo()` without an `id` throws `DefaultPipeIdError` (E0001); an empty or blank `id` throws at stream construction.

```ts
// before — id was optional
evmPortalSource({ portal: '...' })
  .pipe(evmDecoder({ ... }))
  .pipeTo(myTarget)

// after — id is required
evmStream({ id: 'eth-transfers', source: '...', outputs: evmEventDecoder({ ... }) })
  .pipeTo(myTarget) // cursor stored under key "eth-transfers"

solanaPortalStream({ id: 'sol-swaps', portal: '...', outputs: solanaInstructionDecoder({ ... }) })
  .pipeTo(myTarget) // cursor stored under key "sol-swaps"
```

### 4. Renamed functions and types

Functions and types have been renamed for clarity and consistency. These are **hard renames** — this is a major release, and the old names are removed without compatibility aliases, so the compiler will point you at each one.

**Functions:**

| Before | After |
|---|---|
| `createEvmPortalSource` / `evmPortalSource` | `evmStream` |
| `solanaPortalSource` | `solanaPortalStream` |
| `hyperliquidFillsPortalSource` | `hyperliquidFillsPortalStream` |
| `evmDecoder` | `evmEventDecoder` |
| `createSolanaInstructionDecoder` | `solanaInstructionDecoder` |
| `factory` | `contractFactory` |
| `factorySqliteDatabase` | `contractFactorySqliteStore` |
| `chunk` | `chunkForInsert` |
| `createClickhouseTarget` | `clickhouseTarget` |
| `createDefaultLogger` | `defaultLogger` |
| `createFinalizationBuffer` | Removed — use `requiresFinalizedStream` on finalized-only targets (breaking change 14) |
| `addLog` / `addTransaction` / `addInstruction` / … (all query builders) | `addLogRequest` / `addTransactionRequest` / `addInstructionRequest` / … (`addFields` / `addRange` unchanged) |
| `toSnakeKeys` | `toSnakeCaseKeys` |
| `displayEstimatedTime` | `formatEta` |
| `coerceFinalized` | `normalizeFinalized` |
| `lines` | `joinLines` |
| `parseBlockFormatting` | `parseFormattedBlock` |

**Types:**

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
| `BQ_ERR` / `PQ_ERR` | `BIGQUERY_ERROR_CODES` / `PARQUET_ERROR_CODES` |
| `Finalization` | Removed — the buffer state it described no longer exists (breaking change 14) |
| `BigQueryState` / `BigQueryStore` / `BigQueryTracker` | `BigQuerySyncState` / `BigQueryWriter` / `BigQueryTableRegistry` |

`PortalClientOptions` duration keys are unit-suffixed: `maxIdleTime` → `maxIdleTimeMs`, `maxWaitTime` → `maxWaitTimeMs`, `headPollInterval` → `headPollIntervalMs` (all were already milliseconds).

**New types:** `SingleOutput`, `MultiOutput`, `EventFilter<T>`

### 5. Factory option `parameter` renamed to `childAddressField`

The `parameter` field in factory options is now `childAddressField`. It also accepts a function for custom extraction logic:

```ts
// before
factory({
  event: factoryAbi.PoolCreated,
  parameter: 'pool',
  database: factorySqliteDatabase({ ... }),
})

// after
contractFactory({
  event: factoryAbi.PoolCreated,
  childAddressField: 'pool',
  database: contractFactorySqliteStore({ ... }),
})

// new — function extractor
contractFactory({
  event: factoryAbi.PoolCreated,
  childAddressField: (decoded) => decoded.pool,
  database: contractFactorySqliteStore({ ... }),
})
```

### 6. Runner `stream` field renamed to `handler`, `RunConfig` to `PipeContext`

```ts
// before
const runner = createDevRunner([
  { id: 'eth', params: { portal: '...' }, stream: indexTransfers },
])

async function indexTransfers({ id, params }: RunConfig<{ portal: string }>) { ... }

// after
const runner = devRunner([
  { id: 'eth', params: { portal: '...' }, handler: indexTransfers },
])

async function indexTransfers({ id, params }: PipeContext<{ portal: string }>) { ... }
```

### 7. Progress tracker event types renamed and restructured

| Before | After |
|---|---|
| `StartState` | `StartEvent` |
| `ProgressState` | `ProgressEvent` |

`ProgressEvent` data is nested under a `.progress` key. Both types now include a `logger` field.
Range bounds inside `progress.state` are named `from`/`to` (matching the `range` option vocabulary — `to` is the end of the indexed range, or the chain head when unbounded), and per-interval activity stats live under `progress.intervalStats`.

```ts
// before
evmPortalSource({
  progress: {
    onStart:    (s: StartState)    => console.log(s.initial),
    onProgress: (s: ProgressState) => console.log(s.state.current),
  },
})

// after
evmStream({
  progress: {
    onStart:    (e: StartEvent)    => console.log(e.state.initial),
    onProgress: (e: ProgressEvent) => console.log(e.progress.state.current),
  },
})
```

### 8. `TransactionFields.nonce` is now `bigint`

The `nonce` field in EVM `TransactionFields` changed from `number` to `bigint` to support values exceeding `Number.MAX_SAFE_INTEGER`. The validator now accepts both numeric and string inputs from the Portal API.

### 9. Fork handling: `resolveFork`, `rollback` hooks, `canonicalBlocks`

The fork/rollback vocabulary is now consistent: *fork* names the blockchain event, *resolveFork* names handling it (find the common ancestor, undo above it, return the resume cursor), and *rollback* names the destructive undo alone — which can also happen outside forks (e.g. startup recovery).

- The `Target` contract method is `resolveFork(canonicalBlocks)` (was `fork(previousBlocks)`). The parameter rename is semantic: the blocks are the portal's view of the **canonical** chain (`previousBlocks` in the Portal API's 409 body), not the blocks you just processed.
- The transformer lifecycle hook and `Factory` method that receive an **already-resolved** safe cursor are named `rollback` (were `fork`) — their whole job is the undo.
- `ForkNoPreviousBlocksError` is renamed `MissingForkAncestorError` (code E1002 unchanged).

### 10. ClickHouse `onRollback` discriminator: `reason: 'recovery' | 'fork'`

The callback's discriminator key `type` is renamed `reason`, and the values now name the rollback's cause at a consistent level: `'recovery'` (was `'offset_check'`) fires on every restart with a persisted cursor and cleans up rows a possibly-interrupted previous run wrote past it; `'fork'` (was `'blockchain_fork'`) fires on chain forks. The context's `cursor` duplicate is removed — use `safeCursor`.

```ts
// before
onRollback: async ({ type, store, cursor }) => { ... }      // 'offset_check' | 'blockchain_fork'

// after
onRollback: async ({ reason, store, safeCursor }) => { ... } // 'recovery' | 'fork'
```

### 11. Metrics server endpoints and Prometheus gauges renamed

- Prometheus gauges: `sqd_current_block` → `sqd_processed_block`; `sqd_last_block` → `sqd_end_block`. The second is a semantic fix: the value is the **end of the indexed range** (the configured `to` bound, or the chain head when unbounded) — for range-bounded runs it was never the chain head. Update dashboards and alerts.
- HTTP API: `GET /exemplars/transformation` → `GET /preview/transformation` ("exemplar" and "sample" are Prometheus/OpenMetrics terms of art; the payload is a truncated *preview* of each transformation stage's last batch). The `/profiler` payload key `profilers` → `profiles`, and the `/stats` payload's `code.filename` → `entrypoint`.
- **Pipes UI:** upgrade `@subsquid/pipes-ui` together with the SDK — older UI versions read the removed endpoints/payload keys and will show no data against a 1.0 pipe.

### 12. `profiler.id` renamed to `profiler.name`

The `id` property in `ProfilerOptions` and `Profiler` has been renamed to `name` to avoid confusion with the pipe `id`.

```ts
// before
evmDecoder({
  profiler: { id: 'ERC20 transfers' },
  ...
})

// after
evmEventDecoder({
  profiler: { name: 'ERC20 transfers' },
  ...
})
```

### 13. `DecodedInstruction` block info moved under `block`

Solana `DecodedInstruction` now exposes a `block` object with both `number` and `hash`. The top-level `blockNumber` field is removed.

```ts
// before
event.blockNumber // number

// after
event.block.number // number
event.block.hash   // string
```

### 14. Finalized-only targets read the finalized stream instead of buffering

A target that commits only finalized data now declares `requiresFinalizedStream: true`. The source
uses a finalized client view for range resolution, caches and block streaming, warning when it had
to override a hot portal configuration. The selection is per pipe: piping the same source to another
target afterwards leaves that one on the stream it asked for. The Parquet and memory targets set the
flag.

They previously took a hot stream and held unfinalized rows in a `finalizationBuffer`. That could
not make bounded completion correct: a hot stream completes when the range is delivered, even if
the target still holds an uncommitted tail above the reported finalized head.

**This changes when such a pipe finishes.** A bounded range whose `to` sits above the finalized head
used to complete as soon as the portal had delivered it; it now waits for those blocks to finalize.
On a chain with slow finality that is minutes, and on a dataset that finalizes nothing it never
returns — so a bounded backfill in CI or cron should end at or below the finalized head, or run
against a target that reads the hot stream.

Consequences for custom targets and caches:

- `finalizationBuffer` / `FinalizationBuffer` / `Finalization` are **removed**. Declare
  `requiresFinalizedStream: true` and write rows as they arrive.
- A finalized-only target no longer needs `resolveFork`; no fork can reach it.
- A custom `PortalCache` should use the client passed to `getStream`. It receives the effective
  finalized client as well as `finalized: true`.
- `ParquetStore.flushBatch()` no longer accepts finalization state, and
  `ParquetStore.resolveFork()` is removed.
- Reorg-safety comes from the dataset's own finality. For a dataset that never finalizes anything,
  files are not reorg-safe.

### 15. Google Pub/Sub now publishes BigQuery CDC rows

The Pub/Sub target replaces its custom `_op` / `_id` / `_seq` / `_v` attribute envelope with a
BigQuery CDC JSON body containing `_id`, `_CHANGE_TYPE`, and `_CHANGE_SEQUENCE_NUMBER`. Delete
messages retain the row columns so business and composite primary keys reach BigQuery. Pub/Sub
attributes are now limited to user attributes plus the optional `_uid`.

This is an API and state compatibility break:

- `publish.delivery` is replaced by `publish.messageOrdering`; `heartbeat` is removed.
- `MessageDraft.data` and `RollbackInverse.data` accept plain objects only.
- `TopicRoute.encode` receives the complete `BigQueryCdcMessage`.
- `WIRE_VERSION`, `ENVELOPE_ATTRIBUTES`, and `DeliveryProfile` are no longer exported; E2420 is
  retired.
- A `Date` encodes as an RFC 3339 string rather than unix seconds, so it lands in a `TIMESTAMP`
  column instead of 1970. A BigQuery subscription reads a JSON number in a `TIMESTAMP` column as
  microseconds since the epoch.
- State schema v2 has no in-place migration. Drain the old outbox, then use fresh state, a fresh
  namespace, and a re-bootstrapped destination. See `MIGRATION.md` for the full procedure.

`docs/pubsub-bigquery.md` is the setup guide for the receiving end: the destination column type each
encoded value requires, the table DDL, the subscription flags, the IAM grants for a subscription
that lives in a different project than the topic, and the limits that come with the design.

---

## New features


### 1. Time-based ranges

Ranges now accept ISO date strings and `Date` objects in addition to block numbers. Dates are automatically resolved to the corresponding block numbers via the portal API.

```ts
evmStream({
  source: '...',
  outputs: evmEventDecoder({
    range: { from: '2024-01-01' },              // date string
    events: { transfers: erc20.events.Transfer },
  }),
})

// Date objects work too
evmEventDecoder({
  range: {
    from: new Date('2024-01-01T00:00:00Z'),
    to:   new Date('2024-02-01T00:00:00Z'),
  },
  events: { ... },
})
```

Supported `from` / `to` formats:

| Format | Example |
|---|---|
| Block number | `18908900` |
| Formatted block number | `'1,000,000'` or `'1_000_000'` |
| ISO date string | `'2024-01-01'` or `'2024-01-01T00:00:00Z'` |
| `Date` object | `new Date('2024-01-01')` |
| Latest block | `'latest'` (only `from`) |

Date-only strings (e.g. `'2024-01-01'`) are treated as UTC midnight. Identical timestamps across multiple ranges are deduplicated into a single portal API call.

#### `NaturalRange` type refinement

`NaturalRange` is now a discriminated union. When `from` is `'latest'`, `to` only accepts a block number — `Date` and date strings are no longer allowed because the portal cannot resolve timestamps for blocks that have not been produced yet.

```ts
// before — single object type
type NaturalRange = { from: number | 'latest' | Date; to?: number | Date }

// after — discriminated union
type NaturalRange =
  | { from: number | Date; to?: number | Date }
  | { from: 'latest'; to?: number }
```

`from: 'latest'` with a numeric `to` now correctly preserves both values in the resolved range. Previously, `to` was silently dropped.

```ts
// ✅ Valid — block number as `to`
evmEventDecoder({ range: { from: 'latest', to: 20_000_000 }, ... })

// ❌ Throws BlockRangeConfigurationError
evmEventDecoder({ range: { from: 'latest', to: new Date('2025-01-01') }, ... })
```

#### Range validation

Block ranges are validated after timestamp resolution. The following conditions throw a `BlockRangeConfigurationError` (E0002):

| Condition | Example | Error |
|---|---|---|
| Inverted range (`from` > `to`) | `{ from: 1000, to: 500 }` | `Invalid block range: 'from' (1000) must be less than or equal to 'to' (500)` |
| `Date` for `to` with `from: 'latest'` | `{ from: 'latest', to: new Date(...) }` | `Cannot use a Date for 'to' when 'from' is 'latest'…` |
| Unresolvable timestamp (e.g. future date) | `{ from: new Date('2030-01-01') }` | `Failed to resolve timestamp 2030-01-01T00:00:00.000Z to a block number…` |

The portal's `No chunk found for timestamp` error is now wrapped with context identifying which timestamp failed and why.

### 2. `defineAbi` — use standard JSON ABIs without code generation

`defineAbi()` converts a standard JSON ABI (Solidity compiler output, Hardhat/Foundry artifact) into subsquid decoder objects at runtime — no `squid-evm-typegen` step required. Uses `@subsquid/evm-codec` under the hood for 10x faster decoding compared to viem.

```ts
import erc20Json from './erc20.json'
import { defineAbi } from '@subsquid/pipes/evm'

const erc20 = defineAbi(erc20Json)

evmEventDecoder({
  range: { from: 'latest' },
  events: {
    transfers: erc20.events.Transfer,
    approvals: erc20.events.Approval,
  },
})
```

Accepts a plain ABI array, an `as const` literal for full type inference, or a Hardhat/Foundry artifact with an `abi` field:

```ts
// Inline with `as const` — fully typed decode results
const erc20 = defineAbi([
  {
    type: 'event',
    name: 'Transfer',
    inputs: [
      { indexed: true, name: 'from', type: 'address' },
      { indexed: true, name: 'to', type: 'address' },
      { indexed: false, name: 'value', type: 'uint256' },
    ],
  },
] as const)
// erc20.events.Transfer.decode() returns { from: string, to: string, value: bigint }

// From Hardhat artifact
import artifact from './artifacts/MyContract.json'
const myContract = defineAbi(artifact)
```

The returned object has `.events` and `.functions` maps that work directly with `evmEventDecoder()`, `evmQuery()`, and `contractFactory()`.

### 3. Testing utilities — `@subsquid/pipes/testing`

A new public entry point with helpers for writing unit and integration tests against portal streams. Create mock portals, test loggers, mock metrics, and read stream output — without hitting real infrastructure.

```ts
import { mockPortal, testLogger, mockMetricsServer, readAll } from '@subsquid/pipes/testing'

// Spin up a mock portal HTTP server with canned responses
const portal = await mockPortal(mockResponses)

// Use portal.url with any portal stream in your test
const stream = evmStream({
  id: 'test',
  source: portal.url,
  logger: testLogger(),
  metrics: mockMetricsServer(),
  outputs: evmEventDecoder({ ... }),
})

for await (const { data } of stream) {
  // process data
}

// Clean up
await portal.close()
```

| Utility | Description |
|---|---|
| `mockPortal(responses, options?)` | Starts a local HTTP server that serves canned portal responses. Returns a `MockPortal` with `.url` and `.close()` |
| `finalizedMockPortal(responses)` | Same as above but marks all blocks as finalized |
| `testLogger()` | Creates a pino logger configured for test output |
| `mockMetricsServer()` | Creates mock counter, gauge, and histogram metrics |
| `readAll(stream)` | Drains a stream and returns its concatenated `data` |

### 4. EVM testing utilities — `@subsquid/pipes/testing/evm`

A new public entry point with helpers for writing tests against EVM portal streams. Encode events with full type inference from viem ABIs, build mock blocks with auto-generated metadata, and spin up a mock portal server — all in a few lines.

```ts
import { encodeEvent, mockBlock, mockEvmPortalStream } from '@subsquid/pipes/testing/evm'

const transfer = encodeEvent({
  abi: erc20Abi,
  eventName: 'Transfer',
  address: '0xA0b8...3606eB48',
  args: { from: '0x...', to: '0x...', value: 100n }, // fully typed from ABI
})

const portal = await mockEvmPortalStream({
  blocks: [
    mockBlock({ transactions: [{ logs: [transfer] }] }),
    mockBlock({ transactions: [{ logs: [transfer] }] }),
  ],
})

// Use portal.url with evmStream in your test
```

Works end-to-end with `evmEventDecoder` and `contractFactory()` for testing Uniswap-style factory/child event patterns. Requires `viem` as an optional peer dependency.

### 5. OpenTelemetry integration — `@subsquid/pipes/opentelemetry`

Export profiler spans to Jaeger, Tempo, or any OTEL-compatible backend:

```ts
import { opentelemetryProfiler } from '@subsquid/pipes/opentelemetry'

evmStream({
  source: '...',
  profiler: opentelemetryProfiler(), // drop-in for profiler: true
  outputs: evmEventDecoder({ ... }),
})
```

Requires `@opentelemetry/api` (optional peer dependency) plus an OTEL SDK in the app.

### 6. Runner — multi-pipe management (local development only)

Define your pipe logic once, then run it against multiple datasets concurrently with shared metrics and automatic retries:

```ts
import { devRunner } from '@subsquid/pipes/runtime/node'

// one pipe function, reused across chains
async function indexTransfers({ id, params, logger, metrics }: PipeContext<{ portal: string }>) {
  const stream = evmStream({
    id,
    source: params.portal,
    logger,
    metrics,
    outputs: evmEventDecoder({
      range: { from: '2024-01-01' },
      events: { transfers: erc20.events.Transfer },
    }),
  })

  for await (const { data } of stream) {
    logger.info(`Got ${data.transfers.length} transfers`)
  }
}

const runner = devRunner(
  [
    { id: 'eth-transfers',  params: { portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet' },  handler: indexTransfers },
    { id: 'base-transfers', params: { portal: 'https://portal.sqd.dev/datasets/base-mainnet' },      handler: indexTransfers },
  ],
  { retry: 5, metrics: { port: 9090 } },
)

await runner.start()
```

All pipes run concurrently in a single process, share a Prometheus metrics server, 
and each gets its own scoped logger and cursor persistence keyed by `id`.


### 7. Typed error system with documentation links

All framework errors extend `PipeError` and carry a unique code linking to the docs (`https://docs.sqd.dev/en/sdk/pipes-sdk/errors/<code>`).

| Error | Code | Thrown when |
|---|---|---|
| `DefaultPipeIdError` | E0001 | `.pipeTo()` called without a pipe `id` |
| `BlockRangeConfigurationError` | E0002 | Block range is misconfigured (inverted range, invalid date with `'latest'`, unresolvable timestamp) |
| `TargetForkNotSupportedError` | E1001 | Fork detected but target has no `resolveFork()` method |
| `MissingForkAncestorError` | E1002 | Fork exception carried an empty canonical block list |
| `ForkCursorMissingError` | E1003 | Target `resolveFork()` returned `null` |
| `PortalContractViolationError` | E1004 | Portal delivered `canonicalBlocks` whose highest block is below the persisted cursor |
| `ForkOnFinalizedStreamError` | E1005 | Portal reported a fork on the finalized stream, where every delivered block is already final |

Targets carry their own codes in the `E2xxx` band — `E20xx` ClickHouse, `E21xx` Postgres, `E22xx` BigQuery, `E23xx` Parquet — thrown as `ClickhouseTargetError`, `PostgresTargetError`, `BigQueryTargetError` and `ParquetTargetError`. Every code is documented in the [error reference](https://docs.sqd.dev/en/sdk/pipes-sdk/errors).


### 8. New Prometheus metrics

The following metrics are now collected automatically for every source:

| Metric | Type | Description |
|---|---|---|
| `sqd_processed_block{id}` | gauge | Last processed block number |
| `sqd_end_block{id}` | gauge | End of the indexed range: the configured `to` bound, or the chain head when unbounded |
| `sqd_progress_ratio{id}` | gauge | Indexing progress as a ratio from 0 to 1 |
| `sqd_eta_seconds{id}` | gauge | Estimated time to full sync in seconds |
| `sqd_blocks_processed_total{id}` | counter | Total number of blocks processed |
| `sqd_bytes_downloaded_total{id}` | counter | Total bytes downloaded from portal |
| `sqd_forks_total{id}` | counter | Chain reorganizations detected |
| `sqd_portal_requests_total{id, classification, status}` | counter | HTTP requests to the portal by status code |
| `sqd_batch_size_blocks{id}` | histogram | Number of blocks per batch |
| `sqd_batch_size_bytes{id}` | histogram | Size of each batch in bytes |

All metrics are labelled with the pipe `id`.

### 9. Engine-aware ClickHouse rollbacks

`store.removeAllRows` now picks the removal mechanism by table engine:

- **CollapsingMergeTree family** (incl. `Replicated*` and ClickHouse Cloud `Shared*` variants) with a
  `sign` column: rows are cancelled with `sign = -1` rows — the only removal mechanism that propagates
  through materialized views. The read-back uses a `GROUP BY all columns / sum(sign)` netting query
  instead of `SELECT * FINAL`, so it is correct under insert-retry duplicates, idempotent on re-run,
  and no longer scans the whole table.
- **Any other engine**: falls back to a lightweight `DELETE` with a logged warning — the table itself
  is cleaned, but materialized views built on it keep the removed data (ClickHouse fires MVs on
  `INSERT` only). Requires ClickHouse ≥ 23.3.
- **`Distributed` tables** are rejected with an explicit error — roll back the underlying local table.

A minmax skip index `_sqd_rollback_idx` on `block_number` is created automatically on first rollback,
so rollback reads prune old parts regardless of the table's `ORDER BY`. Call the new
`store.ensureRollbackIndex({ table })` in `onStart` to set it up eagerly and avoid a slow first
rollback on an existing large table.

Reading table metadata requires access to `system.tables` / `system.columns`; without it the store
logs a warning and falls back to the previous `FINAL`-based cancel-row behavior.

### 10. `DecodedInstruction` now includes `block` with hash

Solana `DecodedInstruction` exposes a `block` object with both `number` and `hash` (the old top-level `blockNumber` is removed — see breaking change 13).

---

## Fixes

### Fixed D2/D4 discriminator matching in Solana instruction decoder

`getInstructionD2` and `getInstructionD4` used incorrect hex slice offsets for `0x`-prefixed strings, extracting 3/6 bytes instead of 2/4. Programs using 2-byte or 4-byte discriminators (e.g. Solana System Program) silently matched zero instructions.

---

## Removals

There are **no deprecated aliases** in this release — every rename in breaking change 4 is a hard rename, and all previously deprecated APIs are gone:

- `CompositeTransformer` / `compositeTransformer` / `composite-transformer.ts` removed — use named `outputs`
- `.pipeComposite()` removed — use named `outputs`
- `query` option removed from `evmStream` and `solanaPortalStream`
- Deprecated aliases removed: `evmPortalSource` / `createEvmPortalSource`, `solanaPortalSource` / `createSolanaPortalSource`, `hyperliquidFillsPortalSource`, `factory`, `factorySqliteDatabase`, `chunk`, `createClickhouseTarget`
- `createSolanaInstructionDecoder` removed — use `solanaInstructionDecoder`
- `ResultOf<T>` removed — use `OutputOf<T>`
- `DecodedInstruction.blockNumber` removed — use `block.number`
- ClickHouse `onRollback` context's `cursor` removed — use `safeCursor`
- Parquet `'TIMESTAMP_MILLIS'` column type removed — write `'TIMESTAMP'` (identical wire format)
- `finalizationBuffer` / `FinalizationBuffer` / `Finalization` removed — see breaking change 14
- `TransformerFn` removed from public exports
- `Subset<T, U>` removed from `query-builder.ts` exports — now a recursive type in `types.ts`
