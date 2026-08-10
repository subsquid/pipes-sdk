import {
  type BlockCursor,
  type Counter,
  type Histogram,
  type HookContext,
  type Logger,
  type Metrics,
  type Profiler,
  type Range,
  createTarget,
  formatBlock,
  formatNumber,
  humanBytes,
} from '~/core/index.js'

import type { ParquetEngine } from './engine.js'
import { ParquetState } from './parquet-state.js'
import { ParquetStore } from './parquet-store.js'
import { parquetjsEngine } from './parquetjs/parquetjs-engine.js'
import { type ParquetTable, validateTables } from './schema.js'

/** Default rollover byte size — 128 MiB is a good Parquet file size for data-lake query engines. */
const DEFAULT_MAX_BYTES = 128 * 1024 * 1024

export type ParquetRollover = {
  /** Soft byte cap per file — checked at each batch boundary, so a huge batch can overshoot. Default 128 MiB. */
  maxBytes?: number
  /** Optional row cap per file, checked alongside `maxBytes`. */
  maxRows?: number
  /** Optional wall-clock checkpoint floor (ms) — recommended for live tailing so finalized data isn't stuck in an open file. */
  intervalMs?: number
  /** Optional block-count checkpoint floor — checkpoint once the cursor advances this many blocks. */
  intervalBlocks?: number
}

export type ParquetSettings = {
  rollover?: ParquetRollover
  /**
   * Segment writer engine. Omitted → the default `parquetjsEngine()`, the SDK's only
   * built-in. Pass any {@link ParquetEngine} implementation to swap the writer: an engine
   * translates the declared schema and plain-JS rows privately and writes each segment file
   * at a temp path the target assigns; the target names files for their coverage window,
   * verifies engine output (Parquet magic bytes) and owns publication, so naming, durability
   * and crash recovery are engine-invariant by construction (see `engine.ts` for the full
   * contract).
   *
   * Encoding options (row-group size, compression, backend tuning) are engine-owned: pass
   * them to the engine's factory — `parquetjsEngine({ rowGroupSize, compression })` — where
   * they are typed to what that engine can actually do. The target neither consumes nor
   * forwards them.
   */
  engine?: ParquetEngine
  /**
   * Namespace for the state file, so multiple pipes can share one `dir`. Defaults to the
   * pipe's source `id`; set explicitly only to pin the state file independent of the source id.
   */
  id?: string
}

type ParquetTargetMetrics = {
  rowsWritten: Counter<'id' | 'table'>
  bytesWritten: Counter<'id' | 'table'>
  filesPublished: Counter<'id' | 'table'>
  checkpointDuration: Histogram<'id'>
}

/**
 * Writes a stream to rotating, **finalized-only** Parquet files — the columnar format read by
 * DuckDB, Spark, Athena and ClickHouse's `s3()`.
 *
 * **Finalized-only.** Parquet files are immutable once written, so a block that could still reorg
 * is never written. The target gets that from the stream rather than from buffering: it sets
 * `requiresFinalizedStream`, so the pipe reads `/finalized-stream` whatever the portal options say
 * (warning when it had to override them). Every delivered block is final, so no row is ever held
 * back and no fork can arrive.
 *
 * **Constant memory.** Finalized rows stream straight to a temp file and the file rotates by byte
 * size, so a multi-gigabyte finalized backfill never lands wholly in RAM (the default parquetjs
 * engine flushes row groups to disk incrementally — verified by the Step 0 spike — with its
 * `rowGroupSize` factory option bounding the in-memory buffer; alternative engines own their
 * staging bounds through their own factory options). For an engine whose byte reporting is
 * estimated rather than measured, `rollover.maxRows` is an exact, engine-independent backstop —
 * the target counts appended rows itself.
 *
 * **Coverage-named files.** Files are named `<from>-<to>.parquet` for the block window they
 * **cover** — the window the pipe processed — not for the min/max block of the rows inside. Within
 * a configured query range, a table's files tile it end to end: a table with no rows for a while
 * publishes nothing and names the whole span when it next writes, and whatever span is still owed
 * when the stream ends is claimed by a zero-row file. So for files written by this version, a block
 * range absent from a table means "not indexed", never "indexed, no data".
 *
 * Three things that follow, and are easy to get wrong:
 * - Gaps **between** configured ranges stay absent — those blocks are never fetched, so no file
 *   claims them. That is the same statement, not an exception to it.
 * - The name bounds coverage, not content. `onData` may key a row outside the window that emitted
 *   it (an aggregate stamped with its window's first block), so do not use the filename to locate
 *   a given block's rows — use the row-group statistics in the footer, which is what DuckDB, Spark
 *   and Athena prune on anyway.
 * - Files written **before** this version were named for their rows' min/max, are indistinguishable
 *   by name, and their gaps are not healed retroactively. The guarantee covers the range written
 *   from the upgrade onward.
 *
 * **Crash safety.** A writer holding ≥1 finalized row is always published at the very next
 * checkpoint, and the persisted cursor advances only at a checkpoint — so no finalized row is ever
 * held past the cursor. On restart, recovery deletes any published file above the cursor (an
 * incomplete checkpoint) and any temp file, then `read(cursor)` re-fetches the deterministic,
 * finalized data after the cursor, which regenerates identically.
 *
 * **Contract (load-bearing): `onData` must be a pure function of the batch for finalized blocks.**
 * Recovery re-processes finalized blocks after a crash and relies on regenerating byte-identical
 * rows. Do not let wall-clock time, RNG, or external mutable state affect a row's identity. Unlike
 * BigQuery (which dedupes server-side via committed-stream offsets), Parquet has no server-side
 * dedupe, so this purity contract is the recovery guarantee.
 *
 * Known trade-offs:
 * - One busy table's checkpoint rotates **all** open writers, so low-volume tables get smaller
 *   files (the "small files" problem) — inherent to a single global cursor.
 * - A table that produces no rows at all still gets one zero-row file per run (and one per query
 *   range), which is the price of coverage being readable off the filenames.
 * - Byte-only rotation can stall the cursor on a slow live tail (finalized data stays invisible
 *   until the file closes; a crash re-fetches more). Set `rollover.intervalMs`/`intervalBlocks`
 *   for live tailing.
 * - Reorg-safety is inherited from `/finalized-stream`, so it is only as good as the dataset's own
 *   notion of finality: for a dataset that never finalizes anything, the files are **not**
 *   reorg-safe.
 *
 * @param options.dir - Output directory. It is the isolation unit — **one pipe per dir**. Holds a
 *   `<table>/` sub-directory per table plus a `_sqd_parquet_state.<pipe-id>.json` cursor file
 *   (namespaced by the pipe's source id, or by `settings.id` when set explicitly).
 * @param options.tables - Declared tables with explicit schemas; the block-number column must be
 *   present and integer-typed. Writing to an undeclared table from `onData` throws.
 * @param options.onData - Per-batch handler; call `store.insert(table, rows)` to stage rows.
 */
export function parquetTarget<T>(options: {
  dir: string
  tables: ParquetTable[]
  settings?: ParquetSettings
  onStart?: (ctx: { store: ParquetStore; logger: Logger }) => Promise<unknown> | unknown
  onData: (ctx: { store: ParquetStore; data: T; ctx: HookContext }) => Promise<unknown> | unknown
}) {
  const { dir, tables, settings = {}, onStart, onData } = options

  validateTables(tables)

  // The store constructor runs the engine's per-table capability checks at construction,
  // so config mistakes surface at startup, not deep in the first batch.
  const engine = settings.engine ?? parquetjsEngine()

  const maxBytes = settings.rollover?.maxBytes ?? DEFAULT_MAX_BYTES
  const { maxRows, intervalMs, intervalBlocks } = settings.rollover ?? {}

  const store = new ParquetStore({ dir, tables, engine })

  return createTarget<T>({
    // Parquet files are immutable once published, so the target may only ever see final blocks.
    requiresFinalizedStream: true,
    write: async ({ read, logger, id }) => {
      // Lazy: registered on the first batch from `ctx.metrics`.
      let metrics: ParquetTargetMetrics | undefined

      // Namespace the state file by the pipe's source id (an explicit settings.id wins), so
      // several pipes can share one dir without clobbering each other's cursor.
      const state = new ParquetState({ dir, tables: tables.map((t) => t.table), id: settings.id ?? id, logger })

      // Tracks checkpoint progress; the closure below mutates it.
      let lastCheckpointMs = Date.now()
      let lastCheckpointBlock = -1

      // The try/finally wraps the ENTIRE write() body, so a startup throw still discards any open
      // temp files. fork() runs inside read()'s generator while this for-await is suspended, so no
      // write races with it and close-in-finally is race-free.
      try {
        const resumeState = await state.getCursor()
        const startCursor = resumeState?.latest
        lastCheckpointBlock = startCursor?.number ?? -1

        await onStart?.({ store, logger })

        // Captured for the checkpoint closure's metric labels — assigned on the first batch.
        let metricsId = ''
        let lastBoundary: BlockCursor | undefined = startCursor
        // The configured range the stream is currently inside; a change means a gap was skipped.
        let openRange: Range | undefined
        // Latest (source-clamped) finalized head, persisted alongside the cursor at each checkpoint
        // so the source can re-seed its watermark after an unclean restart. Seeded from resume state.
        let lastFinalized: BlockCursor | undefined = resumeState?.finalized ?? undefined

        // The current batch's target span, which checkpoint() hangs its own spans off. Undefined
        // outside a batch: the source ends the batch span as soon as the loop body returns, so the
        // stream-end checkpoint has no live parent to attach to and simply runs unmeasured.
        let batchSpan: Profiler | undefined

        const checkpoint = async (
          cursor: BlockCursor,
          reason: string,
          closeTails = false,
          owingTables?: string[],
        ): Promise<void> => {
          const startedMs = Date.now()
          const span = batchSpan?.start({ name: `checkpoint (${reason})`, labels: 'db' })

          try {
            // Files are named for the window they cover, which ends at the cursor being committed —
            // so publish must see the same cursor that is about to be persisted.
            const published = await measure(span, 'publish files', () =>
              store.publishAll(cursor.number, { closeTails, tables: owingTables }),
            )

            // A stalled boundary can re-trigger rotation on every batch while no writer has a window
            // to name yet; rewriting an identical state file (two fsyncs) each time buys nothing.
            if (published.length === 0 && cursor.number === lastCheckpointBlock) {
              lastCheckpointMs = Date.now()

              return
            }

            await measure(span, 'save cursor', () => state.saveCursor(cursor, lastFinalized, store.coverage()))

            if (metrics) {
              for (const file of published) {
                metrics.bytesWritten.inc({ id: metricsId, table: file.table }, file.bytes)
                metrics.filesPublished.inc({ id: metricsId, table: file.table }, 1)
              }
              metrics.checkpointDuration.observe({ id: metricsId }, (Date.now() - startedMs) / 1000)
            }

            lastCheckpointMs = Date.now()
            lastCheckpointBlock = cursor.number

            if (published.length > 0) {
              const rows = published.reduce((s, f) => s + f.rows, 0)
              const bytes = published.reduce((s, f) => s + f.bytes, 0)
              logger.info({
                message: `checkpoint (${reason}): published ${published.length} file(s), ${formatNumber(rows)} rows / ${humanBytes(bytes)}, cursor → block ${formatBlock(cursor.number)}`,
                files: published.map((f) => f.path),
              })
            } else {
              logger.debug(`checkpoint (${reason}): no open files, cursor → block ${formatBlock(cursor.number)}`)
            }
          } finally {
            span?.end()
          }
        }

        for await (const { data, ctx } of read(resumeState)) {
          // Everything this target does for the batch hangs off one span, so a trace shows the
          // target's share of the batch next to the source's `fetch data` and the transformers.
          const target = ctx.profiler.start({ name: 'parquet', labels: 'db' })
          batchSpan = target

          try {
            if (!metrics) {
              metrics = registerParquetMetrics(ctx.metrics)
              metricsId = ctx.id
              // Seeded on the first batch because `stream.state` only exists once the source has
              // resolved its ranges. Fallback for a table the state doesn't cover yet:
              //   - Resuming: cursor + 1. `initial` is the *configured* query start (pre-resume), so
              //     using it here would let the first file claim blocks an earlier run already wrote.
              //   - Cold start: `initial` — the stream's configured start. Hardcoding 0 instead would
              //     make a backfill from block N claim to cover 0..N of blocks it never looked at.
              const clamped = store.seedCoverage(
                state.coverage,
                startCursor ? startCursor.number + 1 : ctx.stream.state.initial,
                ctx.stream.state.ranges,
              )
              for (const { table, persisted, seeded } of clamped) {
                logger.warn(
                  `Persisted coverage for table '${table}' starts at block ${formatBlock(persisted)}, ahead of ` +
                    `block ${formatBlock(seeded)} — the furthest a file could start for the committed cursor. ` +
                    `Seeding ${formatBlock(seeded)} instead, so the blocks after the cursor stay claimed. This is ` +
                    `expected if the configured query ranges changed since the state file was written.`,
                )
              }
            }

            // Crossing from one configured range into a later one: the blocks between them are never
            // fetched, so every table must close its coverage at the old range's end before anything
            // names a window on the far side of the gap. The tail closes at `lastBoundary` itself and
            // never at a re-labelled copy of it — a cursor pairing one block's number with another
            // block's hash would be persisted, and the next resume would hand the portal that hash as
            // its parent. `lastBoundary` is the previous batch's last block and `openRange` is the
            // range that block fell in, so it is inside the range; the guard covers `openRange`
            // having gone stale over a batch outside every range.
            const range = rangeOf(ctx.stream.state.ranges, ctx.stream.state.current.number)
            const tail = lastBoundary
            if (
              openRange !== undefined &&
              range !== undefined &&
              range.from > openRange.from &&
              tail !== undefined &&
              tail.number <= (openRange.to ?? Number.POSITIVE_INFINITY)
            ) {
              const owing = store.tablesOwingCoverage(tail.number)
              if (owing.length > 0) {
                await checkpoint(tail, 'range-end', true, owing)
              }
            }
            openRange = range ?? openRange

            // A configured range the stream skipped entirely (it yielded no batch, so the crossing
            // above never fired for it) must not be folded into the next file's coverage. Advance any
            // table still lagging below the current range's start up to it — this also runs on the
            // first batch after a restart, where there is no `openRange` transition to detect.
            if (range !== undefined) {
              store.advanceCoverageInto(range.from)
            }

            // 1. user stages rows via store.insert(...)
            await target.measure('data handler', async (profiler) => {
              await onData({ store, data, ctx: { logger, profiler } })
            })

            // 2. the cursor this batch could checkpoint to, carrying the HASH needed for resume.
            //    It is the batch's last block, NOT min(finalized, current): the stream is
            //    finalized-only, so every delivered block is final, and `flushBatch` below appends
            //    all of them. Clamping to the reported finalized head — which lags whenever finality
            //    advances inside one response — would publish a file holding rows above the cursor
            //    it persists, and the restart would re-fetch and duplicate them.
            const finalized = ctx.stream.head.finalized
            if (finalized) {
              // Already clamped by the source, so it never regresses — persist it as the
              // restart-seed floor for the watermark.
              lastFinalized = finalized
            }
            const boundaryCursor = ctx.stream.state.current
            lastBoundary = boundaryCursor

            // 3. append the batch's rows into the (lazily-opened) writers.
            const appended = await target.measure({ name: 'flush batch', labels: 'db' }, () => store.flushBatch())
            for (const stat of appended) {
              metrics.rowsWritten.inc({ id: ctx.id, table: stat.table }, stat.rows)
            }

            // 4. checkpoint on any rollover trigger.
            const reasons: string[] = []
            if (await store.shouldRotate({ maxBytes, maxRows })) reasons.push('size')
            if (intervalMs !== undefined && Date.now() - lastCheckpointMs >= intervalMs) reasons.push('interval-ms')
            if (intervalBlocks !== undefined && boundaryCursor.number - lastCheckpointBlock >= intervalBlocks) {
              reasons.push('interval-blocks')
            }

            if (reasons.length > 0) await checkpoint(boundaryCursor, reasons.join('+'))
          } finally {
            batchSpan = undefined
            target.end()
          }
        }

        // 5. stream end — flush open writers, close every table's trailing coverage and persist the
        //    final cursor. Tails must close here: stretching only claims a sat-out window once the
        //    table publishes again, and after the stream ends it never will.
        if (lastBoundary !== undefined) {
          // A resume can complete without a single batch (backfill already done), leaving the
          // first-batch seeding unrun while the persisted map still owes a tail from a run that
          // crashed between its last checkpoint and stream end. Seed from the map alone: an owed
          // [coverage, cursor] pair never spans an un-queried gap (coverage is advanced into a
          // range before any checkpoint inside it), and the next run that does see batches
          // re-clamps every start through the real ranges.
          if (!metrics && startCursor) {
            store.seedCoverage(state.coverage, startCursor.number + 1)
          }

          const owing = store.tablesOwingCoverage(lastBoundary.number)
          if (owing.length > 0 || store.hasOpenWriters || lastBoundary.number > lastCheckpointBlock) {
            await checkpoint(lastBoundary, 'stream-end', true, owing)
          }
        }
      } finally {
        await store.close()
      }
    },
  })
}

/**
 * `span.measure`, tolerant of there being no span. A checkpoint runs both inside a batch (where it
 * nests under the batch's target span) and at stream end (where the source has already closed the
 * batch span, so there is nothing to nest under and the work just runs untimed).
 */
function measure<T>(span: Profiler | undefined, name: string, fn: () => Promise<T>): Promise<T> {
  return span ? span.measure({ name, labels: 'db' }, fn) : fn()
}

/** The configured range `block` falls in, or `undefined` when no ranges are recorded. */
function rangeOf(ranges: Range[], block: number): Range | undefined {
  return ranges.find((r) => block >= r.from && (r.to === undefined || block <= r.to))
}

function registerParquetMetrics(metrics: Metrics): ParquetTargetMetrics {
  // Sub-second checkpoints on a healthy pipeline up to tens of seconds when a large file is being
  // closed + fsynced; the long tail beyond 30s lands in +Inf.
  const checkpointBuckets = [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30]

  return {
    rowsWritten: metrics.counter({
      name: 'sqd_parquet_rows_written_total',
      help: 'Finalized rows appended to Parquet writers, by table.',
      labelNames: ['id', 'table'] as const,
    }),
    bytesWritten: metrics.counter({
      name: 'sqd_parquet_bytes_written_total',
      help: 'Bytes of published Parquet files, by table (on-disk size including footer).',
      labelNames: ['id', 'table'] as const,
    }),
    filesPublished: metrics.counter({
      name: 'sqd_parquet_files_published_total',
      help: 'Parquet files published (closed + atomically renamed), by table.',
      labelNames: ['id', 'table'] as const,
    }),
    checkpointDuration: metrics.histogram({
      name: 'sqd_parquet_checkpoint_duration_seconds',
      help: 'Wallclock duration (seconds) of one checkpoint (publish open files + persist cursor).',
      labelNames: ['id'] as const,
      buckets: checkpointBuckets,
    }),
  }
}
