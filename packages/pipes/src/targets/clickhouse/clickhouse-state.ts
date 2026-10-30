import {
  BatchContext,
  BlockCursor,
  CursorKey,
  LEGACY_DEFAULT_CURSOR_ID,
  Logger,
  Profiler,
  RollbackRecord,
  TargetState,
  normalizeFinalized,
  resolveForkCursor,
} from '~/core/index.js'

import { ClickhouseStore, RollbackCursor } from './clickhouse-store.js'
import { CLICKHOUSE_ERROR_CODES, ClickhouseTargetError } from './errors.js'

// FIXME: we need refactor it to make order more deterministic and predictable - WHY?
// ORDER BY (timestamp, id) isn't a good choice
const table = (table: string) => `
CREATE TABLE IF NOT EXISTS ${table}
(
    id               String COMMENT 'Stream identifier to differentiate multiple logical streams',
    current          String COMMENT 'Current offset, corresponds to the most recent indexed block',
    finalized        String COMMENT 'Finalized offset, usually corresponds to the most recent known block',
    rollback_chain   String COMMENT 'JSON-encoded list of block references starting from the finalized block and including all unfinalized blocks',
    timestamp        DateTime(3) COMMENT 'Timestamp of the record, in milliseconds with 3 decimal precision',
    sign             Int8 COMMENT 'Marker used by CollapsingMergeTree to distinguish insertions (+1) and deletions (-1)'
) ENGINE = CollapsingMergeTree(sign)
  ORDER BY (timestamp, id)
`

type SyncRow = { current: string; finalized: string; timestamp: string } & Record<string, unknown>

/** The blocks a first batch covers: from the range start up to the batch's last block. */
export type FirstBatchBlocks = { from: number; to: number }

/**
 * The cursor row written before a pipe's first batch, one block below the range start, with that
 * batch's last block: see `saveStartCursor`.
 */
export type StartCursor = { kind: 'start'; number: number; batchEnd: number }

/** A cursor as the sync table stores it. */
export type StoredCursor = BlockCursor | StartCursor

export function isStartCursor(cursor: StoredCursor): cursor is StartCursor {
  return 'kind' in cursor && cursor.kind === 'start'
}

/** The safe cursor the repair hook gets for a stored one: a start cursor bounds the removal at its batch end. */
export function rollbackCursorOf(cursor: StoredCursor): RollbackCursor {
  if (!isStartCursor(cursor)) {
    return cursor
  }

  return { number: cursor.number, upTo: cursor.batchEnd }
}

/**
 * Configuration options for ClickhouseState.
 */
export type Options = {
  /**
   * Name of the ClickHouse database to use.
   * Defaults to "default" if not provided.
   */
  database?: string

  /**
   * Name of the table to store offset data.
   */
  table?: string

  /**
   * Stream identifier used to isolate offset records within the same table.
   * Defaults to the pipe's source `id`. Set explicitly only to pin a cursor key
   * independent of the source id (e.g. several pipes writing to one table).
   */
  id?: string

  /**
   * Maximum number of rows to retain per unique stream id in the offset table.
   * Older rows beyond this count will be removed.
   * Default is 10,000.
   */
  maxRows?: number
}

export class ClickhouseState {
  options: Options & Required<Pick<Options, 'database' | 'id' | 'table' | 'maxRows'>>

  readonly #qualifiedName: string
  #saves = 0
  #lastTimestamp = 0
  // The start cursor this run resumed from, as stored, so the run's own start cursor can cancel it
  #resumedStart?: Record<string, unknown>

  // The id every cursor row is keyed by: an explicit `settings.id`, else the pipe's source id
  // once `bindCursorKey` runs, else the legacy default (e.g. when bind never runs in unit tests).
  readonly #key: CursorKey
  #logger?: Logger

  constructor(
    private store: ClickhouseStore,
    options: Options,
  ) {
    // Accessing connectionParams as any due to private typing in ClickHouseClient
    const client = store.client as any

    const maxRows = options.maxRows ?? 10_000
    if (maxRows <= 0) {
      throw new ClickhouseTargetError(CLICKHOUSE_ERROR_CODES.MAX_ROWS, 'Max rows must be greater than 0')
    }

    this.options = {
      database: client.connectionParams?.database || 'default',
      table: 'sync',
      ...options,
      // override after spread so an explicit `undefined` cannot clobber the defaults
      id: options.id ?? LEGACY_DEFAULT_CURSOR_ID,
      maxRows,
    }

    this.#key = new CursorKey(options.id)

    this.#qualifiedName = `"${this.options.database}"."${this.options.table}"`
  }

  /**
   * Resolve the cursor key from the pipe's source id, unless an explicit `settings.id` was given
   * (explicit always wins). Called once by the target before any read so getCursor, saveCursor and
   * fork all key by the same value.
   */
  bindCursorKey(sourceId: string | undefined, logger?: Logger): void {
    this.#logger = logger
    this.#key.bind(sourceId)
  }

  /** The id every cursor row is keyed by. Exposed for tests. */
  get cursorKey(): string {
    return this.#key.value
  }

  encodeCursor(cursor: StoredCursor): string {
    return JSON.stringify(cursor)
  }
  decodeCursor(cursor: string): BlockCursor {
    return JSON.parse(cursor)
  }

  async saveCursor(
    {
      stream: {
        state: { current, rollbackChain },
        head,
      },
      profiler,
    }: BatchContext,
    parentSpan: Profiler = profiler,
  ) {
    const timestamp = this.#nextTimestamp()

    await parentSpan.measure({ name: 'insert cursor', labels: 'db' }, async () => {
      await this.store.insert({
        table: this.#qualifiedName,
        values: [
          {
            id: this.#key.value,
            current: this.encodeCursor(current),
            // The source has already clamped this finalized head + rollback chain through the
            // pipe's monotonic watermark, so persisting them verbatim keeps the stored floor
            // non-regressing without any target-local clamp.
            finalized: head.finalized ? this.encodeCursor(head.finalized) : '',
            rollback_chain: JSON.stringify(rollbackChain),
            sign: 1,
            timestamp,
          },
        ],
        format: 'JSONEachRow',
      })
    })

    this.#saves++

    // Debounce cleanup for large retention windows, but enforce small limits
    // eagerly so `maxRows` stays an effective per-id bound after each save.
    // For maxRows < 25 we clean every save (overshoot is only 1 row); for the
    // common maxRows=10_000 case we clean every 25 saves (overshoot ≤ 0.25%).
    const cleanupInterval = this.options.maxRows < 25 ? 1 : 25

    if (this.#saves === 1 || this.#saves % cleanupInterval === 0) {
      await parentSpan.measure({ name: 'cleanup cursors', labels: 'db' }, async () => {
        // Filter by id so cleanup of one stream cannot evict another stream's rows
        // when multiple streams share the same offset table.
        await this.store.removeAllRowsByQuery({
          table: this.#qualifiedName,
          query: `
            SELECT *
            FROM ${this.#qualifiedName} FINAL
            WHERE id = {id:String}
            ORDER BY "timestamp" DESC
            OFFSET ${this.options.maxRows}
          `,
          params: { id: this.#key.value },
        })
      })
    }
  }

  async getCursor(): Promise<TargetState | undefined> {
    try {
      const row = await this.#readLatestRow(this.#key.value)
      if (row) {
        this.#continueStampsAfter(row)
        const primary = this.#stateOf(row)
        this.#resumedStart = isStartCursor(primary.latest) ? row : undefined

        return primary
      }

      return await this.#migrateLegacyCursor()
    } catch (e: unknown) {
      if (e instanceof Error && 'type' in e && e.type === 'UNKNOWN_TABLE') {
        await this.store.command({ query: table(this.#qualifiedName) })

        return
      }

      throw e
    }
  }

  /**
   * Writes the start cursor before the first batch's data goes in: a cursor row one block below the
   * range start, with the batch's last block. The batch's own cursor row supersedes it by being
   * newer. Until then a restart resumes from it, and the repair hook removes up to that block.
   *
   * A run that resumed from a start cursor writes one of its own, since its first batch may end
   * later, and cancels the old one in the same insert, so a crash loop does not pile them up.
   */
  async saveStartCursor({ from, to }: FirstBatchBlocks) {
    const cursor: StartCursor = { kind: 'start', number: from - 1, batchEnd: to }
    const superseded = this.#resumedStart ? [{ ...this.#resumedStart, sign: -1 }] : []
    this.#resumedStart = undefined

    await this.store.insert({
      table: this.#qualifiedName,
      values: [
        ...superseded,
        {
          id: this.#key.value,
          current: this.encodeCursor(cursor),
          finalized: '',
          rollback_chain: '[]',
          sign: 1,
          timestamp: this.#nextTimestamp(),
        },
      ],
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }

  // The newest row is the cursor, so two rows in one millisecond would leave it undecided; the start
  // cursor and the first batch's cursor can be that close
  #nextTimestamp(): number {
    this.#lastTimestamp = Math.max(Date.now(), this.#lastTimestamp + 1)

    return this.#lastTimestamp
  }

  // The stamps go on from the newest stored row, so a clock set back across a restart cannot order
  // a new row below it
  #continueStampsAfter(row: SyncRow) {
    this.#lastTimestamp = Math.max(this.#lastTimestamp, Date.parse(row.timestamp))
  }

  /**
   * Automatic one-time migration from the legacy default key. A pipe upgraded from an SDK version
   * that keyed every cursor by the static "stream" id finds its progress there — re-key those rows
   * under the pipe's own cursor key (timestamps preserved, so the rollback chain stays ordered for
   * fork resolution) and resume from the migrated cursor.
   *
   * Skipped when an explicit `settings.id` is set: an explicitly pinned key either already holds
   * its own rows or deliberately names a fresh cursor, and inheriting the shared legacy cursor
   * would be wrong.
   *
   * Caveats when several source-keyed pipes share one offset table (ClickHouse has no advisory
   * lock, so nothing serializes this migration):
   * - The first pipe to start consumes the legacy rows — but under the shared legacy key only one
   *   cursor ever survived, and it belonged to only ONE of those pipes. The winner may inherit a
   *   foreign block position and a foreign finalized floor (the source's watermark is monotonic,
   *   so a too-high floor does not self-correct). Pin explicit `settings.id`s before upgrading
   *   such setups, or reset the migrated cursor if the resumed position looks foreign.
   * - Two pipes starting concurrently can both read the legacy rows before either re-keys them
   *   and both resume from the same cursor. The warning below fires in each pipe's log either
   *   way, so the overlap is visible.
   */
  async #migrateLegacyCursor(): Promise<TargetState | undefined> {
    if (this.#key.isExplicit || this.#key.value === LEGACY_DEFAULT_CURSOR_ID) return

    const legacyRow = await this.#readLatestRow(LEGACY_DEFAULT_CURSOR_ID)
    if (!legacyRow) {
      return
    }

    // The re-keyed rows keep their stamps
    this.#continueStampsAfter(legacyRow)
    const legacy = this.#stateOf(legacyRow)

    this.#logger?.warn(
      `Found a ClickHouse cursor under the legacy default id "${LEGACY_DEFAULT_CURSOR_ID}" in ` +
        `${this.#qualifiedName}; migrating its rows to this pipe's id "${this.#key.value}" and resuming ` +
        `from the migrated cursor (block ${legacy.latest.number}). If several pipes shared this offset ` +
        `table under the legacy default, this cursor belonged to only one of them — pin an explicit ` +
        `settings.id per pipe before upgrading the rest, and reset this pipe's cursor if the resumed ` +
        `position looks foreign.`,
    )

    await this.#rekeyLegacyRows()

    return legacy
  }

  /**
   * Physically move every legacy row to the current cursor key. Each streamed batch is written as
   * ONE insert carrying both the re-keyed copy (same timestamp, sign=+1 under the pipe's id) and
   * the CollapsingMergeTree cancellation of the original (sign=-1 under the legacy id), so the
   * re-key and the cancel land together — a crash cannot leave the rows duplicated under both
   * keys, and the legacy chain is scanned once instead of twice.
   */
  async #rekeyLegacyRows(): Promise<void> {
    const res = await this.store.query({
      query: `SELECT * FROM ${this.#qualifiedName} FINAL WHERE id = {id:String}`,
      format: 'JSONEachRow',
      query_params: { id: LEGACY_DEFAULT_CURSOR_ID },
      clickhouse_settings: {
        date_time_output_format: 'iso',
        output_format_json_quote_64bit_floats: 1,
        output_format_json_quote_64bit_integers: 1,
      },
    })

    for await (const rows of res.stream<Record<string, unknown>>()) {
      await this.store.insert({
        table: this.#qualifiedName,
        values: rows.flatMap((r) => {
          const row = r.json() as Record<string, unknown>

          return [
            { ...row, id: this.#key.value },
            { ...row, sign: -1 },
          ]
        }),
        format: 'JSONEachRow',
        clickhouse_settings: {
          date_time_input_format: 'best_effort',
        },
      })
    }
  }

  // As stored, timestamps in ISO, so a row can be written back as its own cancel row
  async #readLatestRow(id: string): Promise<SyncRow | undefined> {
    const res = await this.store.query({
      query: `SELECT * FROM ${this.#qualifiedName} WHERE id = {id:String} ORDER BY timestamp DESC LIMIT 1`,
      format: 'JSONEachRow',
      query_params: { id },
      clickhouse_settings: { date_time_output_format: 'iso' },
    })
    const [row] = await res.json<SyncRow>()

    return row
  }

  #stateOf(row: SyncRow): TargetState {
    // Hand the persisted finalized head back as resume state so the source can seed its monotonic
    // watermark (survives an unclean restart mid-fork). It is the newest row's floor — a higher
    // finalized left by a pre-fix regression in an older row is not recovered; a defensive
    // max-across-rows seed is deferred (PR #88 review). Explicit `null` when none was ever stored.
    return {
      latest: this.decodeCursor(row.current),
      finalized: normalizeFinalized(row.finalized ? this.decodeCursor(row.finalized) : undefined) ?? null,
    }
  }

  async fork(canonicalBlocks: BlockCursor[]): Promise<BlockCursor | null> {
    // Only the pipe's own rows participate: getCursor migrated any legacy-keyed rows to the
    // current key before the first read, so by fork time no other key can hold our chain.
    return resolveForkCursor(this.#records(this.#key.value), canonicalBlocks)
  }

  async *#records(id: string): AsyncIterable<RollbackRecord> {
    // Filter by id (like getCursor and cleanup do): when multiple streams share one sync table,
    // an unfiltered scan would mix other streams' rollback chains into this fork's resolution and
    // could resolve to a foreign cursor, corrupting the rollback.
    const res = await this.store.query({
      query: `SELECT * FROM ${this.#qualifiedName} WHERE id = {id:String} ORDER BY "timestamp" DESC`,
      format: 'JSONEachRow',
      query_params: { id },
    })

    for await (const rows of res.stream<{ rollback_chain: string; finalized: string }>()) {
      for (const row of rows) {
        const raw = row.json()

        yield {
          rollbackChain: JSON.parse(raw.rollback_chain) as BlockCursor[],
          // A row persisted before the source reported any finalized head stores '' (and always
          // an empty rollback chain). Decode it to `undefined` so resolveForkCursor skips it
          // gracefully — matching the postgres/bigquery targets — instead of crashing on
          // JSON.parse('').
          finalized: raw.finalized ? (JSON.parse(raw.finalized) as BlockCursor) : undefined,
        }
      }
    }
  }
}
