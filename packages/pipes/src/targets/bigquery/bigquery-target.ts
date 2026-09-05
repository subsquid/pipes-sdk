import type { BigQuery } from '@google-cloud/bigquery'
import { managedwriter } from '@google-cloud/bigquery-storage'

import {
  type BlockCursor,
  type Counter,
  type Histogram,
  type HookContext,
  type Logger,
  type Metrics,
  blockTimestampSeconds,
  createTarget,
  formatBlock,
  formatNumber,
  humanBytes,
} from '~/core/index.js'

import { type BigQueryStateOptions, BigQuerySyncState } from './bigquery-state.js'
import { BigQueryWriter } from './bigquery-store.js'
import { BigQueryTableRegistry, type TrackedTableLocation } from './bigquery-tracker.js'
import { BIGQUERY_ERROR_CODES, BigQueryTargetError } from './errors.js'
import { type PartitioningSetting, type TrackedTable, ensureTrackedTable, partitioningWithDefaults } from './tables.js'
import { classifyBqError } from './utils.js'

export type BigQuerySettings = {
  state?: Omit<BigQueryStateOptions, 'projectId' | 'dataset'>
  /**
   * `false` disables partitioning DDL emission for tracked tables. Production must keep
   * partitioning enabled (default) — without it, fork DELETEs scan the whole table on every
   * reorg. The sync table is always unpartitioned (small enough that pruning buys nothing).
   */
  partitioning?: PartitioningSetting
  /**
   * Optional proto Writer factory passed through to BigQueryWriter. Tests inject a fake here
   * to avoid the fragile vi.mock-on-Storage-Write-API path under v8 coverage.
   */
  protoWriterFactory?: import('./bigquery-store.js').ProtoWriterFactory
}

export type BigQueryClients = {
  bigquery: BigQuery
  /**
   * Storage Write API client. Optional — if omitted, the target constructs one with the
   * same `projectId` and the default `bigquerystorage.googleapis.com` endpoint. Pass an
   * explicit instance when you need custom credentials, a non-default endpoint (e.g.
   * non-default endpoint, or retry settings.
   */
  writer?: managedwriter.WriterClient
}

/**
 * Creates a BigQuery target with fork-aware reorg handling.
 *
 * The target uses the Storage Write API with **Committed streams**: one long-lived stream
 * per table, opened lazily on first write and reused for every batch. Rows are immediately
 * visible after `AppendRows` acks (the 2025 GA closed the streaming-buffer DML lockout that
 * would otherwise block fork DELETEs on freshly streamed rows). Cumulative per-stream
 * offsets give exactly-once semantics — a retried AppendRows resends the same offset and
 * BQ server-dedupes.
 *
 * Atomicity model: the Storage Write API has no atomic flush across tables, so atomicity
 * lives at the WAL level. The `sync` table records every batch's intent (`IN_FLIGHT_COMMIT`)
 * before any data write and the completion marker (`COMMITTED`) only after every tracked
 * table has acked its AppendRows. Forks follow the same shape: `IN_FLIGHT_ROLLBACK` →
 * per-table DELETEs → `ROLLED_BACK`. A crash mid-batch leaves the cursor unmoved; the next
 * `getCursor()` re-runs the bounded DELETE on every tracked table, idempotently cleaning
 * any partial write before resuming.
 *
 * @param options.tables - Tracked tables. Auto-created with RANGE_BUCKET partitioning on
 *   `blockNumberColumn` if missing; existing tables validated against the declared schema
 *   on every restart. Writes to any non-listed table from `onData` throw.
 * @param options.onData - Per-batch handler. Use `store.insert(table, rows)` to buffer rows;
 *   they flush when `onData` returns successfully and the WAL marks the batch COMMITTED.
 * @param options.onBeforeRollback / onAfterRollback - Hooks around the fork DELETE phase.
 *   These receive only the safe cursor (no `store`) — the fork path has no commit point,
 *   so writes would either be lost or sit outside the WAL recovery range.
 */
export function bigqueryTarget<T>(options: {
  client: BigQueryClients
  /** GCP project id; defaults to `client.bigquery.projectId`. */
  projectId?: string
  /** BQ dataset that hosts both tracked tables and the sync table. */
  dataset: string
  tables: TrackedTable[]
  settings?: BigQuerySettings
  onStart?: (ctx: { store: BigQueryWriter; logger: Logger }) => Promise<unknown> | unknown
  onData: (ctx: { store: BigQueryWriter; data: T; ctx: HookContext }) => Promise<unknown> | unknown
  onBeforeRollback?: (ctx: { cursor: BlockCursor }) => Promise<unknown> | unknown
  onAfterRollback?: (ctx: { cursor: BlockCursor }) => Promise<unknown> | unknown
}) {
  const { client, dataset, tables, settings = {}, onStart, onData, onBeforeRollback, onAfterRollback } = options

  const projectId = options.projectId ?? (client.bigquery.projectId as string | undefined)
  if (!projectId) {
    throw new BigQueryTargetError(
      BIGQUERY_ERROR_CODES.PROJECT_ID,
      `bigqueryTarget: cannot determine GCP project id. Pass options.projectId explicitly or ` +
        `construct BigQuery({ projectId }) so client.bigquery.projectId is set.`,
    )
  }

  const partitioning = partitioningWithDefaults(settings.partitioning)
  const stateTableName = settings.state?.table ?? 'sync'

  // BQ REST (`bigquery.googleapis.com`) and Storage Write API (`bigquerystorage.googleapis.com`)
  // are separate services; do not forward the REST `apiEndpoint` to the writer. For
  // non-default endpoints the user must pass `client.writer` themselves.
  const writer = client.writer ?? new managedwriter.WriterClient({ projectId })
  // We own the writer only when we constructed it. User-supplied writers are their
  // responsibility to shut down; closing them inside write()'s finally would yank a long-lived
  // gRPC handle out from under any other code holding a reference.
  const ownsWriter = !client.writer

  const store = new BigQueryWriter(client.bigquery, writer, {
    projectId,
    dataset,
    trackedTables: tables,
    syncTable: { dataset, table: stateTableName },
    protoWriterFactory: settings.protoWriterFactory,
  })

  const trackedLocations: TrackedTableLocation[] = tables.map((t) => ({
    table: t.table,
    fqn: `${projectId}.${dataset}.${t.table}`,
    blockNumberColumn: t.blockNumberColumn,
  }))

  const tracker = new BigQueryTableRegistry({ store, tables, dataset, projectId })

  const state = new BigQuerySyncState({
    store,
    bigquery: client.bigquery,
    trackedTables: trackedLocations,
    options: { projectId, dataset, ...settings.state },
  })

  return createTarget<T>({
    write: async ({ read, logger, id }) => {
      // Key the WAL by the pipe's source id (unless an explicit settings.state.id was given), so
      // progress is isolated per pipe. Must run before getCursor so recovery and writes agree.
      state.bindCursorKey(id)
      store.bindLogger(logger)

      // Lazy: registered on the first batch from `ctx.metrics`. The slot is local to
      // `write()`, but the metrics-server registry caches by name — every call to `write()`
      // (including re-entries after a crash) resolves to the same Histogram/Counter handles
      // for a given pipe id, so the in-process state stays consistent across restarts.
      let metrics: BqTargetMetrics | undefined

      // The try/finally wraps the ENTIRE write() body — including ensureTrackedTable,
      // onStart, and getCursor — so the internally-constructed WriterClient is closed
      // even if startup throws. fork() runs inside read()'s generator while this
      // for-await is suspended; once we exit the for-await (normally or via throw) no
      // more forks fire, so close-in-finally is race-free.
      //
      // Drop any rows the previous invocation buffered without committing — if `onData`
      // threw between `insert` and `commitBatch`, those rows would leak into our first
      // commit on this re-entry and duplicate.
      store.resetBuffer()
      try {
        // Validate / auto-create tracked tables BEFORE accepting any data so schema
        // mismatches surface at startup, not deep in the first batch. projectId is
        // passed explicitly so validation runs against the same project as writes/deletes.
        for (const table of tables) {
          await ensureTrackedTable({
            bigquery: client.bigquery,
            projectId,
            dataset,
            trackedTable: table,
            partitioning,
          })
        }

        await onStart?.({ store, logger })

        // Recovery: getCursor re-executes any outstanding IN_FLIGHT operation before returning.
        const resumeState = await state.getCursor({ logger })
        for await (const { data, ctx } of read(resumeState)) {
          if (!metrics) metrics = registerBqTargetMetrics(ctx.metrics)
          const span = ctx.profiler.start({ name: 'bigquery', labels: 'db' })
          try {
            const next = ctx.stream.state.current
            // The source has already clamped the finalized head + rollback chain through the
            // pipe's monotonic watermark; feed the same values to both WAL writes so the
            // IN_FLIGHT and COMMITTED rows agree.
            const finalized = ctx.stream.head.finalized
            const rollbackChain = ctx.stream.state.rollbackChain
            // Pre-batch cursor = the last durably persisted position. `state` is the single source
            // of truth: saveCommitPost advances it, saveRollbackPost rewinds it on a fork, so after
            // a reorg it already points at the safe cursor — no separate copy in write() to drift.
            const previousCursor = state.lastCommittedCursor
            // Range of new blocks this batch is about to write: [low, next.number].
            //   - Subsequent batches: low = previousCursor.number + 1.
            //   - Very first batch (no previousCursor): low = stream.state.initial — the
            //     stream's configured starting block. Hardcoding 0 here would tell recovery
            //     to DELETE everything from block 0 to the crash point, destroying any rows
            //     written by a prior run (or by a backfill starting at a non-zero block).
            const low = previousCursor ? previousCursor.number + 1 : ctx.stream.state.initial
            const high = next.number

            await trackBqErrors(metrics, ctx.id, () =>
              span.measure('wal pre-commit', () =>
                state.saveCommitPre({ cursor: previousCursor, finalized, rollbackChain, range: { low, high } }),
              ),
            )

            await span.measure('user onData', () =>
              Promise.resolve(onData({ store, data, ctx: { logger, profiler: span } })),
            )

            // `getBufferStats` encodes rows lazily and caches the result so the eventual
            // `commitBatch` doesn't re-encode — single source of truth for both logs.
            const tableStats = store.getBufferStats()
            const totalRows = Object.values(tableStats).reduce((s, t) => s + t.rows, 0)
            const totalBytes = Object.values(tableStats).reduce((s, t) => s + t.bytes, 0)
            const range =
              low === high ? `block ${formatBlock(low)}` : `blocks ${formatBlock(low)} → ${formatBlock(high)}`

            logger.debug({
              message: `in-flight batch: ${formatNumber(totalRows)} rows / ${humanBytes(totalBytes)}, ${range}`,
              tables: tableStats,
            })

            // `commit_duration` measures only the parallel AppendRows phase, not the WAL
            // bracket — the help text claims this scope, and a wider timer would conflate
            // commit latency with WAL/onData latency on the dashboard.
            const commitStartMs = Date.now()
            await trackBqErrors(metrics, ctx.id, () => span.measure('commit data tables', () => store.commitBatch()))
            const commitEndMs = Date.now()

            await trackBqErrors(metrics, ctx.id, () =>
              span.measure('wal post-commit', () =>
                state.saveCommitPost({ logger, cursor: next, finalized, rollbackChain }),
              ),
            )

            // Observe AFTER post-commit so a failed WAL post-commit (which leaves the batch
            // uncommitted from the recovery POV) doesn't emit phantom success observations.
            // commitEndMs was captured before post-commit so the duration / lag still reflect
            // the AppendRows ack moment, not the post-commit delay.
            metrics.commitDuration.observe({ id: ctx.id }, (commitEndMs - commitStartMs) / 1000)
            // Portal block times are per-network (tron and substrate report ms), so they are
            // normalized here rather than trusted verbatim; an unusable one is skipped, not
            // observed as a wildly wrong value derived from `0`.
            const blockTimestamp = blockTimestampSeconds(next.timestamp)
            if (blockTimestamp !== undefined) {
              metrics.blockToCommitLag.observe({ id: ctx.id }, commitEndMs / 1000 - blockTimestamp)
            }

            logger.info({
              message: `committed batch: ${formatNumber(totalRows)} rows / ${humanBytes(totalBytes)} across ${Object.keys(tableStats).length} tables, ${range}`,
              tables: tableStats,
              totalRows,
              totalBytes,
            })
          } finally {
            span.end()
          }
        }
      } finally {
        if (ownsWriter) writer.close()
      }
    },
    resolveFork: async (canonicalBlocks) => {
      const { safeCursor, upper } = await state.fork(canonicalBlocks)
      if (!safeCursor) return null

      await state.saveRollbackPre({
        cursor: safeCursor,
        finalized: undefined,
        rollbackChain: [],
        range: { low: safeCursor.number + 1, high: upper },
      })

      await onBeforeRollback?.({ cursor: safeCursor })

      // Per-table parallel DELETEs.
      await tracker.fork(safeCursor.number, upper)

      // WAL post-rollback: marks the rollback complete; restart will not re-execute.
      await state.saveRollbackPost({ cursor: safeCursor, finalized: undefined, rollbackChain: [] })

      await onAfterRollback?.({ cursor: safeCursor })

      return safeCursor
    },
  })
}

type BqTargetMetrics = {
  blockToCommitLag: Histogram<'id'>
  commitDuration: Histogram<'id'>
  appendErrors: Counter<'id' | 'kind'>
}

function registerBqTargetMetrics(metrics: Metrics): BqTargetMetrics {
  // Buckets cover the operational range per stage — sub-second commits on a healthy pipeline
  // up to a 10-minute lag where an alert should already be firing. Long tail beyond 10m goes
  // into the +Inf bucket; count/sum still capture SLO violations.
  const blockToCommitLagBuckets = [0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600]
  const commitDurationBuckets = [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30]

  return {
    blockToCommitLag: metrics.histogram({
      name: 'sqd_bigquery_block_to_commit_lag_seconds',
      help: 'Per-batch lag (seconds) between the last block timestamp and the BQ AppendRows ack. PRIMARY commit-stage SLO.',
      labelNames: ['id'] as const,
      buckets: blockToCommitLagBuckets,
    }),
    commitDuration: metrics.histogram({
      name: 'sqd_bigquery_commit_duration_seconds',
      help: 'Wallclock duration (seconds) of one batch commit (parallel AppendRows across tracked tables).',
      labelNames: ['id'] as const,
      buckets: commitDurationBuckets,
    }),
    appendErrors: metrics.counter({
      name: 'sqd_bigquery_append_errors_total',
      help: 'AppendRows failures classified by kind. Kind ∈ {not_found, invalid_argument, resource_exhausted, transient, unknown}.',
      labelNames: ['id', 'kind'] as const,
    }),
  }
}

// BQ errors from any of pre/data/post AppendRows go through this classifier so
// `append_errors_total{kind}` represents end-to-end commit-stage failures, not just the
// data-table phase. `onData` is excluded — those are user-code throws, not BQ.
async function trackBqErrors<R>(metrics: BqTargetMetrics, id: string, op: () => Promise<R>): Promise<R> {
  try {
    return await op()
  } catch (error) {
    metrics.appendErrors.inc({ id, kind: classifyBqError(error) }, 1)
    throw error
  }
}
