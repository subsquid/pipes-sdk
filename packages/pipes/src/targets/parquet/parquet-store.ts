import { unlink } from 'node:fs/promises'
import path from 'node:path'

import { type Range } from '~/core/index.js'

import type { ParquetEngine, ParquetTableWriter } from './engine.js'
import { PARQUET_ERROR_CODES, ParquetTargetError } from './errors.js'
import {
  type ParquetColumn,
  type ParquetColumns,
  type ParquetLeafType,
  type ParquetTable,
  blockColumnOf,
} from './schema.js'
import { type PublishedSegment, type SegmentWriter, finalizeSegmentFile, nextTmpPath } from './segment.js'

/** Per-table first block the next published file will cover, keyed by table name. */
export type CoverageStarts = Record<string, number>

/** A published segment plus the table and coverage window it belongs to. */
export type PublishedFile = PublishedSegment & { table: string; from: number; to: number }

/** A persisted coverage start that could not be honoured, and what was seeded instead. */
export type ClampedCoverage = { table: string; persisted: number; seeded: number }

type Row = Record<string, unknown>

/** Per-table immutable config resolved once at construction. */
type TableConfig = {
  blockColumn: string
  /** Reads + validates a row's block number (always-on guard against null/NaN). */
  getBlockNumber: (row: Row) => number
  /** Declared column shape, kept for the dev-mode value check. */
  columns: ParquetColumns
  dir: string
  /** Creates this table's segment writers; engine-specific state lives behind it. */
  tableWriter: ParquetTableWriter
}

/**
 * A table's currently-open segment: the engine's writer plus the bookkeeping the target owns —
 * the temp path it assigned and the row count that drives rotation and the published-file
 * stats. Engines never see or report any of these.
 */
type OpenSegment = {
  writer: SegmentWriter
  tmpPath: string
  rows: number
}

/** Rotation thresholds checked at each batch boundary. */
export type RotationLimits = {
  maxBytes: number
  maxRows?: number
}

/** Per-table count of rows appended in a `flushBatch` call (for metrics). */
export type AppendStat = { table: string; rows: number }

/**
 * Staging + open segment writers for the Parquet target.
 *
 * `insert(table, rows)` stages a batch's rows (rejecting unknown tables synchronously, like
 * `bigquery-store`). `flushBatch` appends each table's staged rows to its segment writer —
 * opening a writer lazily only when there is at least one row, so empty/low-volume tables never
 * create degenerate files.
 *
 * The target reads `/finalized-stream` (`requiresFinalizedStream`), so every staged row is already
 * final: there is nothing to hold back and no fork to roll back.
 */
export class ParquetStore {
  readonly #configs = new Map<string, TableConfig>()
  readonly #staged = new Map<string, Row[]>()
  // The current open segment per table — at most one. Reset (published/discarded) at checkpoints.
  readonly #segments = new Map<string, OpenSegment>()
  // For error messages in the publish tail.
  readonly #engineName: string
  // First block each table's NEXT published file will claim to cover. Seeded by `seedCoverage`
  // and advanced only when that table actually publishes — see `publishAll`.
  readonly #coverageStart = new Map<string, number>()
  // Configured query ranges, ascending. Empty means "one implicit range" (no gaps to skip).
  #ranges: Range[] = []
  // Per-cell type checking is a hot-path cost, so it only runs outside production.
  readonly #validateValues = process.env.NODE_ENV !== 'production'

  constructor(options: { dir: string; tables: ParquetTable[]; engine: ParquetEngine }) {
    this.#engineName = options.engine.name
    for (const table of options.tables) {
      const blockColumn = blockColumnOf(table)
      const getBlockNumber = (row: Row): number => readBlockNumber(table.table, blockColumn, row)
      const dir = path.join(options.dir, table.table)
      this.#configs.set(table.table, {
        blockColumn,
        getBlockNumber,
        columns: table.schema,
        dir,
        tableWriter: options.engine.table(table),
      })
    }
  }

  /**
   * Stages rows for `table` until the next `flushBatch`. Throws synchronously if `table` was not
   * declared in `tables[]` — an unknown table can never be finalized, rotated or recovered, so
   * surfacing it immediately (before any I/O) is safer than silently dropping the rows.
   */
  insert(table: string, rows: Row[]): void {
    const config = this.#configs.get(table)
    if (!config) {
      throw new ParquetTargetError(
        PARQUET_ERROR_CODES.UNREGISTERED_TABLE,
        `Table '${table}' is not declared. Declared tables: ${[...this.#configs.keys()].sort().join(', ')}. ` +
          `Add it to parquetTarget({ tables: [...] }).`,
      )
    }
    if (rows.length === 0) return

    if (this.#validateValues) {
      for (const row of rows) validateRowValues(table, config.columns, row)
    }

    const existing = this.#staged.get(table)
    if (existing) {
      existing.push(...rows)
    } else {
      this.#staged.set(table, [...rows])
    }
  }

  /**
   * Appends each table's staged rows to its (lazily-opened) segment writer and clears staging.
   * Returns per-table appended-row counts for metrics.
   */
  async flushBatch(): Promise<AppendStat[]> {
    const stats: AppendStat[] = []

    for (const [table, config] of this.#configs) {
      const staged = this.#staged.get(table) ?? []

      if (staged.length > 0) {
        // Every row must be attributable to a block for recovery to reason about what a file
        // covers. Nothing else reads the number now, so this call IS the guard.
        for (const row of staged) {
          config.getBlockNumber(row)
        }

        const segment = this.#getOrCreateSegment(table, config)
        await segment.writer.append(staged)
        segment.rows += staged.length
        stats.push({ table, rows: staged.length })
      }
    }

    this.#staged.clear()

    return stats
  }

  /** True if any open writer has reached the byte or row rotation threshold. */
  async shouldRotate(limits: RotationLimits): Promise<boolean> {
    for (const segment of this.#segments.values()) {
      if (limits.maxRows !== undefined && segment.rows >= limits.maxRows) return true
      if ((await segment.writer.size()) >= limits.maxBytes) return true
    }

    return false
  }

  /** Whether any segment writer is currently open (contains at least one finalized row). */
  get hasOpenWriters(): boolean {
    return this.#segments.size > 0
  }

  /**
   * Seeds each table's next-file coverage start and records the configured query ranges (which
   * `publishAll` needs to skip blocks the pipe will never fetch).
   *
   * `persisted` wins; a table absent from it — a cold start, a newly declared table, or state
   * written before coverage was tracked — falls back to `fallbackStart`, which the caller must set
   * to the first block this run may publish (NOT the source's `initial`, which is the configured
   * query start and ignores any resume).
   *
   * A persisted value that isn't a non-negative integer is treated as absent rather than trusted:
   * `pad(-5)` renders as `0000000000-5`, which the data-file pattern cannot parse, so crash
   * recovery would go blind to that file and let it duplicate rows.
   *
   * Whichever start is chosen is clamped forward to the first actually-queried block, so a
   * `fallbackStart` of `cursor + 1` (or a persisted value) that lands in an un-queried gap between
   * ranges can't seed a file that names blocks the pipe never fetched.
   *
   * A persisted value ahead of `nextQueriedBlock(fallbackStart)` — the furthest a file could
   * consistently start for the resume point — is clamped back down to it rather than trusted:
   * honouring it would leave the blocks between the cursor and that start claimed by no file at
   * all. The two things that produce it are both handled by clamping — the query ranges were
   * edited since the state was written (the recorded start refers to a gap that no longer exists,
   * and the clamped value covers the newly-queried blocks correctly), or the cursor was rewound by
   * hand (recovery has already deleted the files above it, so re-indexing from the clamp is exactly
   * right). Returns what it clamped so the caller can say so out loud.
   *
   * @internal — lifecycle, driven by the target.
   */
  seedCoverage(persisted: CoverageStarts | undefined, fallbackStart: number, ranges: Range[] = []): ClampedCoverage[] {
    this.#ranges = [...ranges].sort((a, b) => a.from - b.from)

    const ceiling = this.#nextQueriedBlock(fallbackStart)
    const clamped: ClampedCoverage[] = []

    for (const table of this.#configs.keys()) {
      const start = persisted?.[table]
      const valid = Number.isInteger(start) && (start as number) >= 0

      if (valid && (start as number) > ceiling) {
        clamped.push({ table, persisted: start as number, seeded: ceiling })
        this.#coverageStart.set(table, ceiling)
        continue
      }

      this.#coverageStart.set(table, this.#nextQueriedBlock(valid ? (start as number) : fallbackStart))
    }

    return clamped
  }

  /**
   * Bumps every table whose next-file start still sits below `rangeFrom` up to it. Called on entry
   * to a configured range so a range the stream skipped entirely — it produced no batch, so no tail
   * was closed for it — is not folded into the next file's coverage, which would claim the
   * un-queried gap after it. A table owing a window *within* the current range (start already at or
   * past `rangeFrom`) is left untouched.
   *
   * @internal — lifecycle, driven by the target.
   */
  advanceCoverageInto(rangeFrom: number): void {
    const target = this.#nextQueriedBlock(rangeFrom)
    for (const [table, start] of this.#coverageStart) {
      if (start < target) {
        this.#coverageStart.set(table, target)
      }
    }
  }

  /**
   * Per-table coverage starts, to persist alongside the checkpoint cursor.
   *
   * @internal — lifecycle, driven by the target.
   */
  coverage(): CoverageStarts {
    return Object.fromEntries(this.#coverageStart)
  }

  /**
   * Tables whose coverage has fallen behind `to` — i.e. that owe a file for a window they sat out.
   *
   * @internal — lifecycle, driven by the target.
   */
  tablesOwingCoverage(to: number): string[] {
    return [...this.#configs.keys()].filter((table) => {
      const from = this.#coverageStart.get(table)

      return from !== undefined && from <= to
    })
  }

  /**
   * Publishes a segment per table for the window ending at `to`, and resets the open writers.
   * Call immediately before persisting the checkpoint cursor.
   *
   * Each file is named for the **window it covers** — `[the table's coverage start, to]` — not the
   * min/max block of its rows, so a consumer reads coverage off the filename instead of guessing
   * whether a gap means "no data" or "not indexed yet".
   *
   * By default only tables with an open writer publish; a table with no rows keeps its coverage
   * start, so the next file it does publish stretches back across the windows it sat out. That
   * keeps a sparse table gap-free without a tiny file per window.
   *
   * With `closeTails`, every table still owing coverage publishes — writing an **empty** segment if
   * it has no rows. Stretching alone can't close a table's final window (there is no next file to
   * stretch), so the caller must force this where coverage would otherwise end: at stream end, and
   * before crossing into a later query range.
   *
   * `tables` lets a caller that has already computed the owing set (via {@link tablesOwingCoverage})
   * pass it in rather than have it recomputed here.
   *
   * A writer this call does not publish — its table was left out of `tables`, or its coverage start
   * is still ahead of `to` (the boundary cursor has not advanced since it last published, so there
   * is no window to name yet) — is deliberately left **open**, to be published by a later
   * checkpoint or discarded by {@link close}. Dropping it here would lose its finalized rows and
   * orphan its temp file.
   *
   * @internal — lifecycle, driven by the target.
   */
  async publishAll(to: number, options: { closeTails?: boolean; tables?: string[] } = {}): Promise<PublishedFile[]> {
    const published: PublishedFile[] = []
    const tables = options.tables ?? (options.closeTails ? this.tablesOwingCoverage(to) : [...this.#segments.keys()])

    for (const table of tables) {
      // A declared table always has a config; checking it first keeps the two failure modes of a
      // caller-supplied `tables` entry distinct — unknown table vs. declared-but-never-seeded.
      const config = this.#configs.get(table)
      if (config === undefined) {
        throw new ParquetTargetError(
          PARQUET_ERROR_CODES.UNREGISTERED_TABLE,
          `Internal: publishAll() was asked to publish table '${table}', which is not declared. ` +
            `Declared tables: ${[...this.#configs.keys()].sort().join(', ')}.`,
        )
      }

      const from = this.#coverageStart.get(table)
      if (from === undefined) {
        throw new ParquetTargetError(
          PARQUET_ERROR_CODES.COVERAGE_RANGE_INVALID,
          `Internal: table '${table}' has an open writer but no coverage start — seedCoverage() ` +
            `must run before the first publish.`,
        )
      }
      if (from > to) continue

      // Only tail-closing invents a segment for a table that has none; a plain checkpoint publishes
      // what is open and nothing else, so it never writes a zero-row file.
      const open = this.#segments.get(table)
      if (!open && !options.closeTails) continue

      const segment = open ?? this.#getOrCreateSegment(table, config)

      // The engine only completes the temp file; the publish tail — magic-bytes check, fsync,
      // collision refusal, atomic rename to the coverage-window name, dir fsync — runs here, so
      // naming and durability are engine-invariant.
      await segment.writer.finish()
      const file = await finalizeSegmentFile({
        dir: config.dir,
        tmpPath: segment.tmpPath,
        rows: segment.rows,
        range: { from, to },
        engine: this.#engineName,
      })

      published.push({ table, from, to, ...file })
      this.#segments.delete(table)
      this.#coverageStart.set(table, this.#nextQueriedBlock(to + 1))
    }

    return published
  }

  /**
   * The first block at or after `block` that the pipe actually queries.
   *
   * Coverage must never advance into the gap between two configured ranges: those blocks are never
   * fetched, so a later file naming itself across them would claim to cover data the pipe never
   * looked at — the exact inverse of the guarantee the naming exists to give. With no ranges
   * recorded (a single implicit range) `block` is already correct.
   */
  #nextQueriedBlock(block: number): number {
    if (this.#ranges.length === 0) return block

    for (const range of this.#ranges) {
      if (range.to !== undefined && block > range.to) continue

      return Math.max(block, range.from)
    }

    // Past every configured range — nothing further will be queried, so leave it where it is.
    return block
  }

  /**
   * Best-effort teardown for the error path: aborts every open segment's writer (releasing its
   * resources) and deletes its temp file — file removal is target-owned, like naming. The
   * discarded rows are finalized-but-not-checkpointed, so they regenerate from the portal on the
   * next run since the cursor never advanced past them.
   */
  async close(): Promise<void> {
    for (const segment of this.#segments.values()) {
      await segment.writer.abort().catch(() => {})
      // Best-effort — the engine may never have created the file, or it may already be gone.
      await unlink(segment.tmpPath).catch(() => {})
    }
    this.#segments.clear()
    this.#staged.clear()
  }

  #getOrCreateSegment(table: string, config: TableConfig): OpenSegment {
    let segment = this.#segments.get(table)
    if (!segment) {
      const tmpPath = nextTmpPath(config.dir)
      segment = { writer: config.tableWriter.createSegment(tmpPath), tmpPath, rows: 0 }
      this.#segments.set(table, segment)
    }

    return segment
  }
}

// JS has no built-in INT32 bounds (only Number.MAX_SAFE_INTEGER, which is 53-bit).
const INT32_MIN = -2_147_483_648
const INT32_MAX = 2_147_483_647

/**
 * Reads a row's block number and fails loudly on a missing/non-finite value. The block column is
 * load-bearing for file-range attribution and recovery, so coercing `null` to 0 or accepting
 * `undefined` as `NaN` would corrupt durability. This guard is always on, including in production.
 */
function readBlockNumber(table: string, blockColumn: string, row: Row): number {
  const raw = row[blockColumn]
  const value = Number(raw)
  if (raw == null || !Number.isFinite(value)) {
    throw new ParquetTargetError(
      PARQUET_ERROR_CODES.BLOCK_VALUE_INVALID,
      `Table '${table}' has a row whose block-number column '${blockColumn}' is ${describe(raw)}. ` +
        `It must be a finite integer on every row.`,
    )
  }

  return value
}

/**
 * Dev-mode (non-production) check that each cell matches its declared column type, catching silent
 * corruption the Parquet encoder would otherwise wave through — most notably a hex *string* handed
 * to a `BYTE_ARRAY` column (stored as the ASCII bytes of `"0x…"`, not the decoded bytes) and a
 * `number` above 2^53 for an `INT64` column (precision already lost before the write). Recurses
 * into STRUCT/LIST declarations so errors carry the offending path — the writer library's own
 * errors for nested data name a bare field with no table/row context.
 */
function validateRowValues(table: string, columns: ParquetColumns, row: Row): void {
  for (const [name, column] of Object.entries(columns)) {
    validateValue(table, name, column, row[name])
  }
}

function validateValue(table: string, path: string, column: ParquetColumn, value: unknown): void {
  if (value == null) {
    if (!column.optional) {
      throw new ParquetTargetError(
        PARQUET_ERROR_CODES.VALUE_INVALID,
        `Table '${table}' column '${path}' is required but the row value is ${describe(value)}.`,
      )
    }

    return
  }

  if (column.type === 'STRUCT') {
    // Map/Set are rejected because their entries are invisible to the property access the
    // writer shreds with (`value[field]`) — required fields would fail as "undefined" and
    // optional ones silently write null. Class instances stay accepted: their fields ARE
    // readable as properties, so they write correctly.
    if (
      typeof value !== 'object' ||
      Array.isArray(value) ||
      value instanceof Date ||
      value instanceof Uint8Array ||
      value instanceof Map ||
      value instanceof Set
    ) {
      throw new ParquetTargetError(
        PARQUET_ERROR_CODES.VALUE_INVALID,
        `Table '${table}' column '${path}' (declared STRUCT) expects a plain object, got ${describe(value)}.`,
      )
    }
    for (const [name, child] of Object.entries(column.fields)) {
      validateValue(table, `${path}.${name}`, child, (value as Row)[name])
    }

    return
  }

  if (column.type === 'LIST') {
    if (!Array.isArray(value)) {
      throw new ParquetTargetError(
        PARQUET_ERROR_CODES.VALUE_INVALID,
        `Table '${table}' column '${path}' (declared LIST) expects an array, got ${describe(value)}.`,
      )
    }
    for (let i = 0; i < value.length; i++) {
      validateValue(table, `${path}[${i}]`, column.element, value[i])
    }

    return
  }

  const problem = checkValueType(column.type, value)
  if (problem) {
    throw new ParquetTargetError(
      PARQUET_ERROR_CODES.VALUE_INVALID,
      `Table '${table}' column '${path}' (declared ${column.type}) ${problem}, got ${describe(value)}.`,
    )
  }
}

/**
 * Returns a human description of why `value` is wrong for `type`, or `undefined` if it is fine.
 * Exhaustive over {@link ParquetLeafType} with no `default` on purpose: adding a leaf type
 * without deciding its dev-mode check here fails to compile (`noImplicitReturns`).
 */
function checkValueType(type: ParquetLeafType, value: unknown): string | undefined {
  switch (type) {
    case 'BYTE_ARRAY':
      if (!(value instanceof Uint8Array)) {
        return 'expects a Buffer/Uint8Array (a hex string is stored as raw ASCII — use Buffer.from(hex, "hex"))'
      }

      return undefined

    case 'INT32':
      if (typeof value === 'bigint') return value < INT32_MIN || value > INT32_MAX ? 'is out of INT32 range' : undefined
      if (typeof value !== 'number' || !Number.isInteger(value)) return 'expects an integer'

      return value < INT32_MIN || value > INT32_MAX ? 'is out of INT32 range' : undefined

    case 'INT64':
      if (typeof value === 'bigint') return undefined
      if (typeof value !== 'number' || !Number.isInteger(value)) return 'expects an integer or bigint'

      return Number.isSafeInteger(value)
        ? undefined
        : 'exceeds 2^53-1 as a JS number and would lose precision — pass a bigint'

    case 'DOUBLE':
      return typeof value === 'number' ? undefined : 'expects a number'

    case 'BOOLEAN':
      return typeof value === 'boolean' ? undefined : 'expects a boolean'

    case 'UTF8':
      return typeof value === 'string' ? undefined : 'expects a string'

    case 'TIMESTAMP':
      return value instanceof Date || typeof value === 'number' ? undefined : 'expects a Date or epoch-millis number'

    case 'DATE': {
      // The writer library divides a Date's epoch-ms by 86,400,000 and the int32 encoder
      // truncates toward zero — the correct calendar day for 1970+, the *wrong* day before
      // 1970 — and it rejects negative day numbers outright, so pre-1970 input is refused here.
      if (value instanceof Date) {
        return value.getTime() >= 0 ? undefined : 'does not support pre-1970 Dates'
      }
      if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
        return 'expects a Date or a non-negative integer of whole days since the Unix epoch'
      }

      return value > INT32_MAX
        ? 'is out of INT32 range for days since the Unix epoch — this looks like epoch millis; pass a Date instead'
        : undefined
    }

    case 'JSON': {
      // The writer does Buffer.from(JSON.stringify(value)) at flush time; a bigint or circular
      // reference crashes there without table/column context, and a function/symbol/undefined
      // stringifies to undefined and crashes Buffer.from. Serialize once here (dev-only) to
      // fail early with context.
      try {
        return JSON.stringify(value) === undefined ? 'expects a JSON-serializable value' : undefined
      } catch {
        return 'expects a JSON-serializable value (bigints and circular references are not)'
      }
    }
  }
}

/** Safe, never-throwing rendering of an arbitrary value for error messages (handles bigint/null). */
function describe(value: unknown): string {
  if (value === undefined) return 'undefined'
  if (value === null) return 'null'
  if (typeof value === 'bigint') return `${value}n`
  if (typeof value === 'string') return JSON.stringify(value)

  return String(value)
}
