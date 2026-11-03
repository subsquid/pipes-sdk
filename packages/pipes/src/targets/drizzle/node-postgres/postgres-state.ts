import { sql } from 'drizzle-orm'

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
import { doWithRetry } from '~/internal/function.js'
import { parseNumber } from '~/internal/number.js'
import { Transaction } from '~/targets/drizzle/node-postgres/drizzle-target.js'
import { syncTable, tableNotExists } from '~/targets/drizzle/node-postgres/tables.js'

import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'

type DeleteResult = {
  rowCount: number
}
type SelectResult<T> = {
  rows: T[]
}
type StateSelect = SelectResult<{
  rollback_chain: BlockCursor[]
  finalized: BlockCursor
  current_number: string
  current_hash: string
  current_timestamp: Date
  id: string
}>

interface PgClient {
  query<T = any>(query: string, params?: any[]): Promise<T>
}

/** @internal */
export type Table = {
  fqnName: string
  name: string
  schema: string
}

/**
 * Configuration options for PostgresState.
 */
export type StateOptions = {
  /**
   * Name of the PostgreSQL schema to use.
   * Defaults to "public" if not provided.
   */
  schema?: string

  /**
   * Name of the table to store offset data.
   */
  table?: string

  /**
   * Stream identifier used to isolate offset records within the same table.
   * Defaults to the pipe's source `id`. Set explicitly only to pin a cursor key
   * independent of the source id (e.g. several pipes writing to one table).
   *
   * When left to default, sync rows written by an older SDK under the legacy static
   * `"stream"` id are migrated to the pipe's id automatically on first resume.
   */
  id?: string

  unfinalizedBlocksRetention?: number
}

export class PostgresState {
  options: Required<StateOptions>

  readonly #sync: Table

  /** Internal counter to track the number of saves for cleanup operations. */
  #saves = 0

  // The id every sync row is keyed by: an explicit `options.id`, else the pipe's source id
  // once `bindCursorKey` runs, else the legacy default (e.g. when bind never runs in unit tests).
  readonly #key: CursorKey

  constructor(
    private client: PgClient,
    options?: StateOptions,
  ) {
    this.options = {
      schema: 'public',
      table: 'sync',
      unfinalizedBlocksRetention: 1000,
      ...options,
      // override after spread so an explicit `undefined` cannot clobber the default
      id: options?.id ?? LEGACY_DEFAULT_CURSOR_ID,
    }

    if (this.options?.unfinalizedBlocksRetention && this.options?.unfinalizedBlocksRetention <= 0) {
      throw new PostgresTargetError(POSTGRES_ERROR_CODES.RETENTION_INVALID, 'Retention strategy must be greater than 0')
    }

    this.#key = new CursorKey(options?.id)

    this.#sync = {
      name: this.options.table,
      schema: this.options.schema,
      fqnName: `"${this.options.schema}"."${this.options.table}"`,
    }
  }

  /**
   * Resolve the cursor key from the pipe's source id, unless an explicit `options.id` was given
   * (explicit always wins). Called once by the target before any read so getCursor, saveCursor,
   * fork and the advisory lock all key by the same value.
   */
  bindCursorKey(sourceId: string | undefined): void {
    this.#key.bind(sourceId)
  }

  /** The id every sync row is keyed by. Exposed for tests. */
  get cursorKey(): string {
    return this.#key.value
  }

  /**
   * Acquires a PostgreSQL advisory lock for the current state ID using
   * the pg_try_advisory_xact_lock function. This ensures that only one
   * process can write to this state at a time. The lock is automatically
   * released at the end of the transaction.
   */
  async acquireLock(tx: Transaction): Promise<void> {
    const res = await tx.execute<{ got_lock: boolean }>(
      sql`SELECT pg_try_advisory_xact_lock(hashtext(${this.#key.value})::bigint) AS got_lock;`,
    )

    if (res.rows[0]?.got_lock) return

    throw new PostgresTargetError(
      POSTGRES_ERROR_CODES.ADVISORY_LOCK_FAILED,
      [
        `Could not acquire advisory lock for state id "${this.#key.value}".`,
        `Another process might be holding the lock.`,
        `Please ensure that only one process is writing to this state at a time.`,
      ].join(' '),
    )
  }

  async saveCursor(
    tx: Transaction,
    {
      stream: {
        state: { current, rollbackChain },
        head,
      },
      logger,
      profiler,
    }: BatchContext,
    parentSpan: Profiler = profiler,
  ) {
    // The source has already clamped this finalized head + rollback chain through the
    // pipe's monotonic watermark, so persist them verbatim — the stored finalized floor
    // stays non-regressing without a target-local clamp.
    const finalizedBlock = head.finalized?.number

    logger.debug(`Saving cursor at block ${current.number} for ${this.#key.value} row...`)
    await parentSpan.measure({ name: 'insert cursor', labels: 'db' }, async () => {
      await tx.execute(
        sql`
          INSERT INTO ${sql.raw(this.#sync.fqnName)} (
             id, current_number, current_hash, "current_timestamp", finalized, rollback_chain
          )
          VALUES (
              ${this.#key.value},
              ${current.number},
              ${current.hash},
              ${current.timestamp ? new Date(current.timestamp * 1000) : sql.raw('NULL')},
              ${JSON.stringify(head.finalized || {})},
              ${JSON.stringify(rollbackChain || [])}
          )
        `,
      )
    })
    this.#saves++
    if (this.#saves === 1 || this.#saves % 25 === 0) {
      // Clean up old unfinalized blocks beyond retention
      const safeBlockNumber = Math.max(
        Math.min(current.number, finalizedBlock ?? Infinity) - this.options.unfinalizedBlocksRetention,
        0,
      )

      logger.info(`Cleaning up old offsets less than ${safeBlockNumber} block for ${this.#key.value} row...`)

      const res = await parentSpan.measure({ name: 'cleanup cursors', labels: 'db' }, async () => {
        return tx.execute<DeleteResult>(sql`
          DELETE FROM ${sql.raw(this.#sync.fqnName)}
          WHERE "id" = ${this.#key.value} AND "current_number" <= ${safeBlockNumber}
        `)
      })

      logger.debug(`Removed unused offsets from ${res.rowCount} rows from ${this.options.table}`)

      return { safeBlockNumber }
    }

    return {
      safeBlockNumber: -1,
    }
  }

  async getCursor({ logger }: { logger: Logger }): Promise<TargetState | undefined> {
    try {
      const primary = await this.#readLatest()
      if (primary) return primary

      return await this.#migrateLegacyCursor(logger)
    } catch (e) {
      if (!tableNotExists(e)) {
        throw e
      }

      logger.debug(`Creating table ${this.#sync.fqnName} for state management...`)
      await doWithRetry(() => this.client.query(syncTable(this.#sync)))
      logger.debug(`Table ${this.#sync.fqnName} created!`)
    }

    return
  }

  async #readLatest(): Promise<TargetState | undefined> {
    const { rows } = await this.client.query<StateSelect>(
      `SELECT * FROM ${this.#sync.fqnName} WHERE id = $1 ORDER BY "current_number" DESC LIMIT 1`,
      [this.#key.value],
    )
    const [row] = rows
    if (!row) return

    // Hand the persisted finalized head back as resume state so the source can seed its
    // monotonic watermark (survives an unclean restart mid-fork). Taken from the resume row
    // (the latest by current_number), so `latest` and `finalized` stay consistent. In steady
    // state that row carries the highest finalized; right after a fork it can briefly hold an
    // older finalized, but the source re-clamps the floor from the live head on the first batch
    // (and re-forks off the stale `latest`), so it self-heals. `null` when none was stored.
    return {
      latest: {
        number: parseNumber(row.current_number),
        hash: row.current_hash,
        timestamp: row.current_timestamp ? row.current_timestamp.getTime() / 1000 : undefined,
      },
      finalized: normalizeFinalized(row.finalized) ?? null,
    }
  }

  /**
   * Automatic one-time migration from the legacy default key. A pipe upgraded from an SDK version
   * that keyed every sync row by the static "stream" id finds its progress there — re-key those
   * rows under the pipe's own cursor key and resume from the migrated cursor.
   *
   * The re-key is a single UPDATE guarded by NOT EXISTS on the new key, so it is atomic: when
   * several source-keyed pipes race the migration on a shared table, exactly one UPDATE re-keys
   * the rows and the others match nothing and start fresh. Skipped when an explicit `options.id`
   * is set — a pinned key either already holds its own rows or deliberately names a fresh cursor,
   * and inheriting the shared legacy cursor would be wrong.
   */
  async #migrateLegacyCursor(logger: Logger): Promise<TargetState | undefined> {
    if (this.#key.isExplicit || this.#key.value === LEGACY_DEFAULT_CURSOR_ID) return

    const res = await this.client.query<{ rowCount?: number }>(
      `UPDATE ${this.#sync.fqnName} SET id = $1
       WHERE id = $2 AND NOT EXISTS (SELECT 1 FROM ${this.#sync.fqnName} WHERE id = $1)`,
      [this.#key.value, LEGACY_DEFAULT_CURSOR_ID],
    )
    if (!res.rowCount) return

    logger.warn(
      `Found ${res.rowCount} sync row(s) under the legacy default id "${LEGACY_DEFAULT_CURSOR_ID}" in ` +
        `${this.#sync.fqnName}; migrated them to this pipe's id "${this.#key.value}" and resuming from the ` +
        `migrated cursor. If several pipes shared this table under the legacy default, this cursor belonged ` +
        `to only one of them — pin an explicit id per pipe before upgrading the rest, and reset this pipe's ` +
        `cursor if the resumed position looks foreign.`,
    )

    return this.#readLatest()
  }

  /**
   * Drop sync rows above the fork's safe cursor. Their cursors describe the now-dead chain, so
   * keeping them would (a) let `getCursor` resume from a stale `current_number` (it is not
   * monotonic in write order while reprocessing climbs back to the pre-fork height) and (b)
   * collide with the primary key when reprocessing re-writes those block numbers. Runs inside the
   * fork transaction so the rollback and this cleanup commit atomically.
   */
  async removeForkedRows(tx: Transaction, cursor: BlockCursor): Promise<void> {
    await tx.execute(sql`
      DELETE FROM ${sql.raw(this.#sync.fqnName)}
      WHERE "id" = ${this.#key.value} AND "current_number" > ${cursor.number}
    `)
  }

  async fork(canonicalBlocks: BlockCursor[]): Promise<BlockCursor | null> {
    const PAGE_SIZE = 1000
    const client = this.client
    const fqnName = this.#sync.fqnName
    const id = this.#key.value

    async function* records(): AsyncIterable<RollbackRecord> {
      let offset = 0

      while (true) {
        const res = await client.query<StateSelect>(
          `SELECT * FROM ${fqnName} WHERE "id" = $1 ORDER BY "current_number" DESC LIMIT ${PAGE_SIZE} OFFSET ${offset}`,
          [id],
        )
        if (!res.rows.length) break

        for (const row of res.rows) {
          yield {
            rollbackChain: row.rollback_chain || [],
            finalized: row.finalized,
          }
        }

        offset += PAGE_SIZE
      }
    }

    return resolveForkCursor(records(), canonicalBlocks)
  }
}
