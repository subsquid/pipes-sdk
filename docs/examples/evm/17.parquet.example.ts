/**
 * Parquet target — write a stream to rotating, finalized-only Parquet files.
 *
 * Indexes ERC20 Transfer + Approval events from Ethereum mainnet into local Parquet files
 * under `<OUT>/transfers/` and `<OUT>/approvals/`. Each file is named for the block range it
 * covers (`<from>-<to>.parquet`) and is immutable once published — the standard columnar format
 * read directly by DuckDB, Spark, Athena and ClickHouse `s3()` (no import step).
 *
 * Why this target:
 *   - **Finalized-only.** The target reads `/finalized-stream`, so every delivered block is final
 *     and a reorg never touches a file on disk. This example uses a fixed historical range; a live
 *     `{ from: 'latest' }` range resolves against the finalized head and follows finalized blocks.
 *   - **Constant memory.** Rows stream to a temp file that rotates by byte size
 *     (`rollover.maxBytes`), so a multi-GB backfill never lands wholly in RAM. The engine's
 *     `rowGroupSize` factory option bounds the writer's in-memory buffer.
 *   - **Crash-safe.** A durable cursor (`<OUT>/_sqd_parquet_state.json`) advances only at a
 *     checkpoint; on restart, any file above the cursor is dropped and re-fetched.
 *   - **Coverage you can read off the filename.** `<from>-<to>` is the window the file covers,
 *     not the min/max block of the rows in it, so a table that goes quiet for a stretch still
 *     names those blocks in whatever it writes next. A missing range means "not indexed", never
 *     "indexed, no data" — and the files of one table are contiguous.
 *
 * Note: `onData` must be a pure function of the batch for finalized blocks (no wall-clock / RNG
 * affecting a row's identity) — recovery re-processes finalized blocks and relies on
 * regenerating byte-identical rows. Parquet has no server-side dedupe.
 *
 * Prerequisites:
 *   - `@dsnp/parquetjs` is an optional peer dependency — install it: `npm i @dsnp/parquetjs`.
 *
 * To run:
 * ```bash
 * PARQUET_OUT=./parquet-out tsx docs/examples/evm/17.parquet.example.ts
 * ```
 *
 * Inspect with DuckDB (query the files directly):
 * ```bash
 * duckdb -c "SELECT count(*), min(blockNumber), max(blockNumber) FROM './parquet-out/transfers/*.parquet'"
 * duckdb -c "SELECT token, count(*) c FROM './parquet-out/transfers/*.parquet' GROUP BY 1 ORDER BY c DESC LIMIT 10"
 * ```
 */

import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'
import { parquetTarget, parquetjsEngine } from '@subsquid/pipes/targets/parquet'

const OUT = process.env['PARQUET_OUT'] ?? './parquet-out'

async function main() {
  await evmStream({
    id: 'erc20-parquet',
    source: { url: 'https://portal.sqd.dev/datasets/ethereum-mainnet', finalized: true },
    outputs: evmEventDecoder({
      // A small historical range so files appear immediately and the run terminates. Swap to
      // `{ from: 'latest' }` to follow the finalized head without an in-memory finality buffer.
      range: { from: 21_000_000, to: 21_000_100 },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
        approvals: commonAbis.erc20.events.Approval,
      },
    }),
  }).pipeTo(
    // The decoded data type is inferred from the evmEventDecoder events config above.
    parquetTarget({
      dir: OUT,
      // The block-number column defaults to `blockNumber` and must be present + integer-typed.
      // Writing to a table not declared here throws synchronously from `store.insert`.
      tables: [
        {
          table: 'transfers',
          schema: {
            blockNumber: { type: 'INT64' },
            logIndex: { type: 'INT32' },
            txIndex: { type: 'INT32' },
            timestamp: { type: 'TIMESTAMP', optional: true },
            // The same instant stored as its UTC calendar day (int32) — cheap day-partitioned scans.
            day: { type: 'DATE', optional: true },
            // Raw log topics as a Parquet LIST — declare the element type, insert a plain array.
            topics: { type: 'LIST', element: { type: 'UTF8' } },
            token: { type: 'UTF8' },
            from: { type: 'UTF8' },
            to: { type: 'UTF8' },
            // A uint256 amount fits no Parquet numeric — keep the exact decimal as text.
            amount: { type: 'UTF8' },
          },
        },
        {
          table: 'approvals',
          schema: {
            blockNumber: { type: 'INT64' },
            logIndex: { type: 'INT32' },
            txIndex: { type: 'INT32' },
            timestamp: { type: 'TIMESTAMP', optional: true },
            token: { type: 'UTF8' },
            owner: { type: 'UTF8' },
            spender: { type: 'UTF8' },
            amount: { type: 'UTF8' },
          },
        },
      ],
      settings: {
        // Small cap so the example rotates into several files instead of one big one. Production
        // defaults to 128 MiB. `maxBytes` is a soft cap, checked at each batch boundary.
        rollover: { maxBytes: 8 * 1024 * 1024 },
        // Encoding options belong to the engine (this is the default engine, spelled out to
        // show where they go). SNAPPY (the default) is a good speed/ratio tradeoff; GZIP /
        // BROTLI compress harder. `rowGroupSize` (default 100k rows) bounds staging memory.
        engine: parquetjsEngine({ compression: 'SNAPPY' }),
      },
      onData: ({ store, data, ctx }) => {
        ctx.logger.debug(`batch: ${data.transfers.length} transfers, ${data.approvals.length} approvals`)

        // Rows are staged per table; the finalized ones flush to the open Parquet writer after
        // `onData` returns. The JS → Parquet input contract: INT64 ← number/bigint, TIMESTAMP ←
        // Date (or null for an optional column), DATE ← Date truncated to its UTC day, LIST ←
        // plain array, UTF8 ← string.
        store.insert(
          'transfers',
          data.transfers.map((t) => ({
            blockNumber: t.block.number,
            logIndex: t.rawEvent.logIndex,
            txIndex: t.rawEvent.transactionIndex,
            timestamp: t.timestamp ?? null,
            day: t.timestamp ?? null,
            topics: t.rawEvent.topics,
            token: t.rawEvent.address,
            from: t.event.from,
            to: t.event.to,
            amount: t.event.value.toString(),
          })),
        )

        store.insert(
          'approvals',
          data.approvals.map((a) => ({
            blockNumber: a.block.number,
            logIndex: a.rawEvent.logIndex,
            txIndex: a.rawEvent.transactionIndex,
            timestamp: a.timestamp ?? null,
            token: a.rawEvent.address,
            owner: a.event.owner,
            spender: a.event.spender,
            amount: a.event.value.toString(),
          })),
        )
      },
    }),
  )

  console.log(`Done. Parquet files written under ${OUT}/ — query them with DuckDB (see the header).`)
}

void main()
