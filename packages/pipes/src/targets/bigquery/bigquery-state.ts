import type { BigQuery, TableField } from '@google-cloud/bigquery'

import {
  type BlockCursor,
  CursorKey,
  LEGACY_DEFAULT_CURSOR_ID,
  type Logger,
  PortalContractViolationError,
  type RollbackRecord,
  type TargetState,
  formatBlock,
  normalizeFinalized,
  resolveForkCursor,
} from '~/core/index.js'

import type { BigQueryWriter } from './bigquery-store.js'
import type { TrackedTableLocation } from './bigquery-tracker.js'
import { BIGQUERY_ERROR_CODES, BigQueryTargetError } from './errors.js'
import { syncTableDdl } from './tables.js'
import { isNotFoundError } from './utils.js'

/**
 * Schema of the sync table for proto-encoding writes. Includes `timestamp` so we send a
 * client-generated value with each row — the column default exists as a safety net but we
 * don't depend on it.
 */
const SYNC_WRITE_SCHEMA: TableField[] = [
  { name: 'id', type: 'STRING', mode: 'REQUIRED' },
  { name: 'op', type: 'STRING', mode: 'REQUIRED' },
  { name: 'current', type: 'STRING', mode: 'NULLABLE' },
  { name: 'finalized', type: 'STRING', mode: 'NULLABLE' },
  { name: 'rollback_chain', type: 'STRING', mode: 'REQUIRED' },
  { name: 'range_low', type: 'INT64', mode: 'NULLABLE' },
  { name: 'range_high', type: 'INT64', mode: 'NULLABLE' },
  { name: 'committed', type: 'BOOL', mode: 'REQUIRED' },
  { name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
]

type SyncRow = {
  id: string
  op: 'commit' | 'rollback'
  /** JSON-encoded BlockCursor; null when the WAL row records "no prior cursor" (first batch). */
  current: string | null
  finalized: string | null
  rollback_chain: string
  range_low: number | string | null
  range_high: number | string | null
  committed: boolean
  /** Microseconds since epoch — client-assigned at write time. */
  timestamp: number
}

/**
 * Common WAL args bag — every save* method takes a superset of this. Bundling args
 * into an options bag keeps method signatures at ≤3 positional arguments.
 */
export type WalCommitArgs = {
  cursor: BlockCursor | undefined
  finalized: BlockCursor | undefined
  rollbackChain: BlockCursor[]
}

export type BigQueryStateOptions = {
  /** GCP project id. */
  projectId: string
  /** BQ dataset id where the sync table lives. */
  dataset: string
  /** Sync table name. Defaults to 'sync'. */
  table?: string
  /**
   * Stream identifier — isolates multiple logical streams sharing the same sync table.
   * Defaults to the pipe's source `id`. Set explicitly only to pin a cursor key independent
   * of the source id.
   */
  id?: string
  /** Maximum number of sync rows to retain per stream id. Defaults to 10_000. */
  maxRows?: number
  /** Run `cleanupOldRows` once every Nth `saveCommitPost`. Defaults to 25. */
  cleanupEverySaves?: number
}

/**
 * WAL state machine + crash recovery + fork resolution for the BigQuery target.
 *
 * Wraps the sync table — the only framework-managed table per the project's "no extra tables"
 * constraint — and exposes typed methods for each WAL transition:
 *
 *   saveCommitPre  → IN_FLIGHT_COMMIT     (op='commit',   committed=false, range set)
 *   saveCommitPost → COMMITTED            (op='commit',   committed=true,  range NULL)
 *   saveRollbackPre  → IN_FLIGHT_ROLLBACK (op='rollback', committed=false, range set)
 *   saveRollbackPost → ROLLED_BACK        (op='rollback', committed=true,  range NULL)
 *
 * Plus three orchestration entry points:
 *
 *   - `getCursor` — lazy auto-create + crash recovery routine. If the latest row is IN_FLIGHT,
 *     re-executes the bounded DELETEs across every tracked table (idempotent), writes the
 *     completed marker, and returns the pre-batch / safe-fork cursor. This is the keystone of
 *     the no-corruption guarantee — without it, mid-fork crashes leave permanent inconsistency
 *     across tables, since BQ DML is exact (unlike ClickHouse CollapsingMergeTree).
 *
 *   - `fork` — pages sync rows newest-first from BigQuery to find the common ancestor with
 *     `canonicalBlocks`. Asserts the portal invariant `upper >= cursor.number` to refuse
 *     silently dropping orphan rows above a truncated `canonicalBlocks`.
 *
 *   - `cleanupOldRows` — periodic maintenance to keep the sync table small, gated on save
 *     count to avoid running every batch.
 */
export class BigQuerySyncState {
  readonly #store: BigQueryWriter
  readonly #bigquery: BigQuery
  readonly #trackedTables: TrackedTableLocation[]
  readonly #fqn: string
  readonly options: Required<BigQueryStateOptions>
  #saves = 0
  #lastCommittedCursor: BlockCursor | undefined

  // The id every WAL row is keyed by: an explicit `options.id`, else the pipe's source id once
  // `bindCursorKey` runs, else the legacy default (e.g. when bind never runs in unit tests).
  readonly #key: CursorKey

  constructor({
    store,
    bigquery,
    trackedTables,
    options,
  }: {
    store: BigQueryWriter
    bigquery: BigQuery
    trackedTables: TrackedTableLocation[]
    options: BigQueryStateOptions
  }) {
    this.#store = store
    this.#bigquery = bigquery
    this.#trackedTables = trackedTables
    this.options = {
      projectId: options.projectId,
      dataset: options.dataset,
      table: options.table ?? 'sync',
      id: options.id ?? LEGACY_DEFAULT_CURSOR_ID,
      maxRows: options.maxRows ?? 10_000,
      cleanupEverySaves: options.cleanupEverySaves ?? 25,
    }
    this.#fqn = `${this.options.projectId}.${this.options.dataset}.${this.options.table}`

    this.#key = new CursorKey(options.id)
  }

  /**
   * Resolve the cursor key from the pipe's source id, unless an explicit `options.id` was given
   * (explicit always wins). Called once by the target before any read so getCursor, the WAL
   * writes, fork and cleanup all key by the same value.
   */
  bindCursorKey(sourceId: string | undefined): void {
    this.#key.bind(sourceId)
  }

  /** The id every WAL row is keyed by. Exposed for tests. */
  get cursorKey(): string {
    return this.#key.value
  }

  /**
   * The last durably persisted cursor — advanced by `saveCommitPost`, rewound by
   * `saveRollbackPost` on a fork, and restored by `getCursor` on recovery. The write loop reads
   * this as the single source of truth for the pre-batch cursor, so a reorg cannot leave a stale
   * copy behind.
   */
  get lastCommittedCursor(): BlockCursor | undefined {
    return this.#lastCommittedCursor
  }

  /**
   * Reads the last committed cursor and runs crash recovery if needed.
   *
   * Lazy auto-creates the sync table on Not Found — `getCursor` may be called by the framework
   * BEFORE `onStart` (Drizzle ordering), so onStart-only creation would crash on first deploy.
   *
   * Recovery transitions:
   *   - latest is COMMITTED        → return cursor.
   *   - latest is IN_FLIGHT_COMMIT → DELETE [range_low, range_high] from all tables, write
   *                                  rollback marker, return PRE-batch cursor (the IN_FLIGHT
   *                                  row's `current`, NOT the new batch's end).
   *   - latest is ROLLED_BACK      → return cursor.
   *   - latest is IN_FLIGHT_ROLLBACK → re-DELETE [range_low, range_high] (idempotent), write
   *                                    rollback marker, return cursor (safe block).
   */
  async getCursor({ logger }: { logger: Logger }): Promise<TargetState | undefined> {
    let row: SyncRow | undefined
    try {
      row = await this.#fetchLatestRow()
    } catch (e) {
      if (!isNotFoundError(e)) throw e

      logger.debug(`Sync table ${this.#fqn} not found; creating it.`)
      await this.#bigquery.query({
        query: syncTableDdl({
          fqn: this.#fqn,
          dataset: this.options.dataset,
          table: this.options.table,
        }),
      })

      // Same defense as the empty-table branch below: a freshly-created sync table next to
      // tracked tables that already hold prior data is unsafe — restarting from the initial
      // cursor would re-process every block and duplicate everything.
      await this.#assertNoOrphanTrackedData()

      return undefined
    }

    if (!row) {
      // Sync table exists but has no rows for our id. Two distinct cases:
      //   (a) genuine first run for this id — tracked tables are also empty, restart from
      //       the configured initial cursor is correct
      //   (b) sync state was lost out-of-band (manual DELETE / TRUNCATE / drop+recreate /
      //       cleanup misconfigured to keep too few rows) but tracked tables still hold
      //       prior runs' data — silently restarting from `initial` would re-process every
      //       block and duplicate everything in tracked tables
      // Distinguish by sniffing the tracked tables for any row. If empty (a), proceed; if
      // non-empty (b), refuse with a clear error pointing at the recovery path.
      await this.#assertNoOrphanTrackedData()
      return undefined
    }

    const cursor = decodeCursor(row.current)

    // Hand the persisted finalized head back as resume state so the source can seed its
    // monotonic watermark (survives an unclean restart mid-fork). It is the latest WAL row's
    // floor; a higher finalized left by a pre-fix regression in an older row is not recovered
    // — defensive max-across-rows seed is deferred (PR #88 review). Explicit `null` when no
    // finalized head was ever stored.
    const finalized = normalizeFinalized(decodeCursor(row.finalized)) ?? null

    if (row.committed) {
      this.#lastCommittedCursor = cursor
      return cursor ? { latest: cursor, finalized } : undefined
    }

    // Recovery path — clean the in-flight range across every tracked table.
    const low = parseIntStrict(row.range_low)
    const high = parseIntStrict(row.range_high)
    if (low === null || high === null) {
      throw new BigQueryTargetError(
        BIGQUERY_ERROR_CODES.CORRUPT_INFLIGHT_ROW,
        `Internal: sync row in ${row.op} IN_FLIGHT state has NULL range_low/range_high; ` +
          `cannot recover. Manual intervention needed: inspect ${this.#fqn} for id=${this.#key.value}.`,
      )
    }

    const range = low === high ? `block ${formatBlock(low)}` : `blocks ${formatBlock(low)} → ${formatBlock(high)}`
    const action = row.op === 'commit' ? 'unfinished write' : 'unfinished rollback'
    logger.warn(
      `Crash recovery (id=${this.#key.value}): previous run left an ${action}; ` +
        `cleaning up ${range} from ${this.#trackedTables.length} tracked table(s) before resuming.`,
    )

    if (low <= high) await this.#recoveryDeleteRange(low, high)

    await this.#writeRow({
      id: this.#key.value,
      op: 'rollback',
      current: cursor ? encodeCursor(cursor) : null,
      finalized: row.finalized,
      // Empty rollback_chain on the recovery row, NOT row.rollback_chain. The IN_FLIGHT row's
      // chain came from ctx.stream.state.rollbackChain at pre-commit time — it lists blocks
      // that were ABOUT to be written but never made it. Carrying that chain forward into the
      // ROLLED_BACK marker would make resolveForkCursor consider those phantom blocks as valid
      // ancestors during a later deep fork, causing the framework to resume from a position
      // with a gap in the data tables.
      rollback_chain: '[]',
      range_low: null,
      range_high: null,
      committed: true,
      timestamp: nowMicros(),
    })

    this.#lastCommittedCursor = cursor ?? undefined
    return cursor ? { latest: cursor, finalized } : undefined
  }

  /**
   * WAL pre-commit row (IN_FLIGHT_COMMIT). Called BEFORE writing data tables.
   *
   * `previousCursor` is the cursor BEFORE this batch (review fix #1) — recovery returns
   * to this point on crash. `low/high` is the inclusive range of new blocks the batch is
   * about to write. The COMMITTED row written by `saveCommitPost` carries the post-batch
   * cursor.
   *
   * On the very first batch (no prior state) `previousCursor` is undefined and the WAL
   * row stores `current = null`; recovery interprets this as "no cursor", returns
   * undefined, and the framework restarts from the configured starting point.
   */
  async saveCommitPre({
    cursor,
    finalized,
    rollbackChain,
    range,
  }: WalCommitArgs & { range: { low: number; high: number } }): Promise<void> {
    await this.#writeRow({
      id: this.#key.value,
      op: 'commit',
      current: cursor ? encodeCursor(cursor) : null,
      finalized: finalized ? encodeCursor(finalized) : null,
      rollback_chain: JSON.stringify(rollbackChain),
      range_low: range.low,
      range_high: range.high,
      committed: false,
      timestamp: nowMicros(),
    })
  }

  /**
   * WAL post-commit row (COMMITTED). Called AFTER data tables are committed.
   * `cursor` is the post-batch cursor; resumption from this point is consistent.
   */
  async saveCommitPost({
    logger,
    cursor,
    finalized,
    rollbackChain,
  }: WalCommitArgs & { logger: Logger; cursor: BlockCursor }): Promise<void> {
    await this.#writeRow({
      id: this.#key.value,
      op: 'commit',
      current: encodeCursor(cursor),
      finalized: finalized ? encodeCursor(finalized) : null,
      rollback_chain: JSON.stringify(rollbackChain),
      range_low: null,
      range_high: null,
      committed: true,
      timestamp: nowMicros(),
    })
    this.#lastCommittedCursor = cursor
    this.#saves++
    if (this.#shouldCleanup()) await this.cleanupOldRows(logger)
  }

  /**
   * WAL pre-rollback row (IN_FLIGHT_ROLLBACK). Called BEFORE per-table fork DELETEs.
   * If the process crashes between the pre-rollback row and the post-rollback row,
   * recovery on next `getCursor()` re-executes the same DELETEs (idempotent).
   *
   * `cursor` is the safe (post-fork) cursor; `range` covers the forked blocks to delete.
   */
  async saveRollbackPre({
    cursor,
    finalized,
    rollbackChain,
    range,
  }: WalCommitArgs & { cursor: BlockCursor; range: { low: number; high: number } }): Promise<void> {
    await this.#writeRow({
      id: this.#key.value,
      op: 'rollback',
      current: encodeCursor(cursor),
      finalized: finalized ? encodeCursor(finalized) : null,
      rollback_chain: JSON.stringify(rollbackChain),
      range_low: range.low,
      range_high: range.high,
      committed: false,
      timestamp: nowMicros(),
    })
  }

  /**
   * WAL post-rollback row (ROLLED_BACK). Called AFTER per-table fork DELETEs succeed.
   */
  async saveRollbackPost({ cursor, finalized, rollbackChain }: WalCommitArgs & { cursor: BlockCursor }): Promise<void> {
    await this.#writeRow({
      id: this.#key.value,
      op: 'rollback',
      current: encodeCursor(cursor),
      finalized: finalized ? encodeCursor(finalized) : null,
      rollback_chain: JSON.stringify(rollbackChain),
      range_low: null,
      range_high: null,
      committed: true,
      timestamp: nowMicros(),
    })
    this.#lastCommittedCursor = cursor
  }

  /**
   * Resolve the safe cursor for a fork.
   *
   * Pages committed sync rows newest-first from BigQuery (`ORDER BY timestamp DESC`) and
   * walks them through `resolveForkCursor` to find the common ancestor with `canonicalBlocks`.
   *
   * Asserts the portal invariant `upper >= currentCursor.number`. If violated — the portal
   * sent a canonicalBlocks set whose highest block is below our persisted cursor — we throw
   * rather than silently DELETE only part of the divergence range and leave orphan rows above
   * `upper` to corrupt the new chain.
   */
  async fork(canonicalBlocks: BlockCursor[]): Promise<{ safeCursor: BlockCursor | null; upper: number }> {
    const upper = canonicalBlocks.reduce((m, b) => (b.number > m ? b.number : m), Number.NEGATIVE_INFINITY)
    if (this.#lastCommittedCursor && upper < this.#lastCommittedCursor.number) {
      throw new PortalContractViolationError(
        `Portal invariant violated: max(canonicalBlocks).number=${upper} is below the persisted cursor ` +
          `(${this.#lastCommittedCursor.number}). The portal must include every block above the safe ` +
          `cursor in canonicalBlocks, otherwise rows in (${upper}, ${this.#lastCommittedCursor.number}] ` +
          `would survive the fork DELETE and corrupt the new chain. Refusing to proceed — file a bug ` +
          `against the portal contract.`,
      )
    }

    const safeCursor = await resolveForkCursor(this.#streamRollbackRecords(), canonicalBlocks)
    return { safeCursor, upper }
  }

  /**
   * Removes sync rows older than `maxRows`. Runs every 25 saves to amortize cost.
   * The 7-day partition expiration on the sync table catches rows we missed.
   */
  async cleanupOldRows(logger: Logger): Promise<void> {
    // `current` and `timestamp` are BigQuery reserved keywords; backtick-quote uniformly.
    const sql = `
      DELETE FROM \`${this.#fqn}\`
      WHERE \`id\` = @id
        AND \`timestamp\` < (
          SELECT MIN(\`timestamp\`) FROM (
            SELECT \`timestamp\` FROM \`${this.#fqn}\`
            WHERE \`id\` = @id
            ORDER BY \`timestamp\` DESC
            LIMIT @keep
          )
        )
    `
    try {
      const { rowCount } = await this.#store.executeDml(sql, { id: this.#key.value, keep: this.options.maxRows })
      if (rowCount > 0) logger.debug(`Cleaned ${rowCount} old sync rows from ${this.#fqn}`)
    } catch (e) {
      logger.warn(`Sync cleanup failed (non-fatal): ${e instanceof Error ? e.message : String(e)}`)
    }
  }

  // -------------------------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------------------------

  async #writeRow(row: SyncRow) {
    await this.#store.commitSyncRow(SYNC_WRITE_SCHEMA, row as unknown as Record<string, unknown>)
  }

  /**
   * Throws `ORPHAN_TRACKED_DATA` if any tracked table has at least one row. Used as a
   * defense-in-depth guard when sync state is unexpectedly empty — without it a manual
   * `DELETE FROM sync` (or TRUNCATE / drop+recreate) would silently degrade into "restart
   * from initial cursor", duplicating every previously processed block in tracked tables.
   *
   * Cost: one cheap `SELECT 1 FROM <table> LIMIT 1` per tracked table. Runs only on the
   * "sync empty" code path — i.e. on first start (cheap) and after operator intervention.
   */
  async #assertNoOrphanTrackedData(): Promise<void> {
    if (this.#trackedTables.length === 0) return
    for (const t of this.#trackedTables) {
      const probe = await this.#store
        .query<{ has_row: boolean }>(`SELECT TRUE AS has_row FROM \`${t.fqn}\` LIMIT 1`)
        .catch((e) => {
          // If the tracked table doesn't exist yet (genuine cold start before
          // `ensureTrackedTable`), there is no orphan data by definition.
          if (isNotFoundError(e)) return [] as { has_row: boolean }[]
          throw e
        })
      if (probe.length > 0) {
        throw new BigQueryTargetError(
          BIGQUERY_ERROR_CODES.ORPHAN_TRACKED_DATA,
          `Sync table \`${this.#fqn}\` has no rows for id='${this.#key.value}', but tracked ` +
            `table \`${t.fqn}\` still holds data from a prior run. Refusing to restart from the ` +
            `initial cursor — that would re-process every block and duplicate every row.\n\n` +
            `If this is intentional (you manually reset the sync table and want a fresh run), ` +
            `also TRUNCATE / drop the tracked tables: ${this.#trackedTables.map((x) => x.fqn).join(', ')}.`,
        )
      }
    }
  }

  async #fetchLatestRow(): Promise<SyncRow | undefined> {
    const rows = await this.#store.query<SyncRow>(
      `SELECT * FROM \`${this.#fqn}\` WHERE \`id\` = @id ORDER BY \`timestamp\` DESC LIMIT 1`,
      { id: this.#key.value },
    )
    return rows[0]
  }

  async #recoveryDeleteRange(low: number, high: number): Promise<void> {
    if (this.#trackedTables.length === 0) return
    await Promise.all(
      this.#trackedTables.map((t) =>
        this.#store.executeDml(
          `DELETE FROM \`${t.fqn}\` WHERE \`${t.blockNumberColumn}\` >= @low AND \`${t.blockNumberColumn}\` <= @high`,
          { low, high },
        ),
      ),
    )
  }

  /**
   * Async iterable of RollbackRecords newest-first, paged directly from BigQuery via
   * `ORDER BY timestamp DESC`. Drives `resolveForkCursor`, which reads every record to find the
   * highest floor.
   *
   * Each page is bounded by the timestamp of the previous page's last row, so the same row
   * is never yielded twice — server-assigned `CURRENT_TIMESTAMP()` gives µs precision and is
   * unique per write within a stream. A page holds `maxRows` rows, the bound cleanup keeps,
   * so a fork costs one BQ query unless cleanup has fallen behind.
   */
  async *#streamRollbackRecords(): AsyncGenerator<RollbackRecord> {
    const pageSize = this.options.maxRows
    let cutoff: string | number | undefined
    while (true) {
      const params: Record<string, unknown> = { id: this.#key.value, limit: pageSize }
      let where = `\`id\` = @id AND \`committed\` = TRUE`
      if (cutoff !== undefined) {
        params['cutoff'] = cutoff
        where += ` AND \`timestamp\` < @cutoff`
      }
      const rows = await this.#store.query<SyncRow>(
        `SELECT * FROM \`${this.#fqn}\` WHERE ${where} ORDER BY \`timestamp\` DESC LIMIT @limit`,
        params,
      )
      if (rows.length === 0) break
      for (const row of rows) {
        yield this.#rowToRollbackRecord(row)
      }
      if (rows.length < pageSize) {
        break
      }

      const last = rows[rows.length - 1].timestamp
      if (last == null) break // unreachable on a server-managed timestamp, but defensive
      cutoff = last
    }
  }

  #rowToRollbackRecord(row: SyncRow): RollbackRecord {
    return {
      rollbackChain: row.rollback_chain ? (JSON.parse(row.rollback_chain) as BlockCursor[]) : [],
      finalized: row.finalized ? (JSON.parse(row.finalized) as BlockCursor) : undefined,
    }
  }

  #shouldCleanup(): boolean {
    return this.#saves === 1 || this.#saves % this.options.cleanupEverySaves === 0
  }
}

/**
 * Client wall-clock time in microseconds with a within-ms tie-breaker counter — wire format
 * for BQ TIMESTAMP, **strictly monotonic per process** (under the documented single-writer
 * invariant). JS `Date.now()` has only ms precision; `Date.now() * 1000` would let two
 * writes in the same ms collide on `==`, which would let `WHERE timestamp < @cutoff` page
 * boundaries silently drop the boundary row in fork resolution. Clamping to `lastMs` on
 * NTP steps backwards preserves monotonicity even if the system clock doesn't.
 */
let lastMs = 0
let withinMsCounter = 0
function nowMicros(): number {
  const ms = Math.max(Date.now(), lastMs)
  if (ms > lastMs) {
    lastMs = ms
    withinMsCounter = 0
  } else {
    withinMsCounter += 1
  }

  return ms * 1000 + withinMsCounter
}

function encodeCursor(c: BlockCursor): string {
  return JSON.stringify(c)
}

function decodeCursor(s: string | null): BlockCursor | undefined {
  if (s == null) return undefined
  const parsed = JSON.parse(s)
  return parsed == null ? undefined : (parsed as BlockCursor)
}

function parseIntStrict(v: number | string | null): number | null {
  if (v === null) return null
  if (typeof v === 'number') return v
  const n = Number.parseInt(v, 10)
  return Number.isFinite(n) ? n : null
}
