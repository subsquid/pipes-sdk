import type { ClickHouseClient } from '@clickhouse/client'

import { BlockCursor, HookContext, Logger, createTarget } from '~/core/index.js'

import { ClickhouseState, isStartCursor, rollbackCursorOf } from './clickhouse-state.js'
import { ClickhouseStore, RollbackCursor } from './clickhouse-store.js'
import { CLICKHOUSE_ERROR_CODES, ClickhouseTargetError } from './errors.js'

/**
 * Configuration options for ClickhouseState.
 */
export type ClickhouseSettings = {
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
   *
   * When left to default, a cursor written by an older SDK under the legacy static
   * `"stream"` id is migrated to the pipe's id automatically on first resume. If several
   * pipes shared one offset table under that legacy default, only one of them owned the
   * surviving cursor — pin an explicit id per pipe before upgrading such setups.
   */
  id?: string

  /**
   * Maximum number of rows to retain per unique stream id in the offset table.
   * Older rows beyond this count will be removed.
   * Default is 10,000.
   */
  maxRows?: number
}

export function clickhouseTarget<T>({
  client,
  onStart,
  onData,
  onRollback,
  settings = {},
}: {
  client: ClickHouseClient
  settings?: ClickhouseSettings
  onStart?: (ctx: { store: ClickhouseStore; logger: Logger }) => unknown | Promise<unknown>
  onData: (ctx: { store: ClickhouseStore; data: T; ctx: HookContext }) => unknown | Promise<unknown>
  /**
   * Called when previously written blocks must be removed. `reason: 'recovery'` fires on
   * every restart with a persisted cursor — it cleans up rows a possibly-interrupted
   * previous run wrote past the saved cursor. `reason: 'fork'` fires on chain forks.
   * The typical implementation is `store.removeRowsAfter(safeCursor, { tables })`, which
   * removes every row above the cursor: on a table several pipes write, also pass a `where`
   * that picks this pipe's rows.
   *
   * Where the target knows the highest block this pipe may have stored above the cursor, it
   * passes it as `safeCursor.upTo`, and `removeRowsAfter` stops there: on a fork, the cursor the
   * fork rolls back from; after a run that stopped inside its first batch, that batch's last
   * block. In the second case the cursor is one block below the range start, saved before the
   * batch's data, so a handler that writes its own condition removes every row from the range
   * start on, and at a range start of 0 gets `number: -1`.
   *
   * On CollapsingMergeTree / VersionedCollapsingMergeTree tables (or their Replicated
   * variants) with a `sign` column, `store.removeRowsAfter` removes rows by inserting
   * cancel rows (sign = -1) — the only removal mechanism that propagates through
   * materialized views. On any other engine it falls back to a lightweight `DELETE` with
   * a warning: the table itself is cleaned, but materialized views built on it keep the
   * removed data. It also auto-creates a minmax skip index on `block_number` so the
   * rollback stays fast on large tables; call `store.ensureRollbackIndex({ table })` in
   * `onStart` to set the index up eagerly.
   */
  onRollback?: (ctx: {
    reason: 'recovery' | 'fork'
    store: ClickhouseStore
    safeCursor: RollbackCursor
  }) => unknown | Promise<unknown>
}) {
  // TODO Can we generate row ID based on query?

  const store = new ClickhouseStore(client)
  const state = new ClickhouseState(store, settings)

  // The cursor of the last batch saved: no row of this pipe lies above it once recovery has run
  let lastCommit: BlockCursor | undefined

  return createTarget<T>({
    write: async ({ read, logger, id, finalized }) => {
      // Key the cursor by the pipe's source id (unless an explicit settings.id was given), so
      // progress is isolated per pipe. Must run before getCursor so read and write agree.
      state.bindCursorKey(id, logger)
      store.bindLogger(logger)

      // The recovery crash window applies to any restart, finalized stream included — the
      // data-then-cursor write is non-atomic regardless of finality. The fork half is hot-only.
      if (!onRollback) {
        const forkNote = finalized
          ? ''
          : ' On the hot stream a chain fork will be refused (E2007) rather than rolled back.'
        logger.warn(
          'No onRollback handler is configured. Rows written above the saved cursor are not removed ' +
            'on recovery, so an unclean restart re-delivers them as duplicates.' +
            forkNote,
        )
      }

      await onStart?.({ store, logger })
      const cursor = await state.getCursor()

      if (cursor) {
        await onRollback?.({
          reason: 'recovery',
          store,
          safeCursor: rollbackCursorOf(cursor.latest),
        })
      }

      // A start cursor makes a crash in the first batch recoverable like any other. Resuming from
      // one, the replayed batch may end later, so it gets a start cursor of its own.
      const resumedFromStart = cursor !== undefined && isStartCursor(cursor.latest)
      let firstBatch = !cursor || resumedFromStart
      lastCommit = resumedFromStart ? undefined : cursor?.latest

      for await (const { data, ctx } of read(cursor)) {
        const target = ctx.profiler.start({ name: 'clickhouse', labels: 'db' })

        try {
          if (firstBatch) {
            const blocks = { from: ctx.stream.state.initial, to: ctx.stream.state.current.number }

            await target.measure({ name: 'insert start cursor', labels: 'db' }, () => state.saveStartCursor(blocks))
            firstBatch = false
          }

          await target.measure('data handler', async (profiler) => {
            await onData({
              store,
              data: data,
              ctx: {
                logger,
                profiler,
              },
            })
          })

          await state.saveCursor(ctx, target)
          lastCommit = ctx.stream.state.current
        } finally {
          target.end()
        }
      }

      await store.close()
    },
    resolveFork: async (canonicalBlocks) => {
      // A fork requires removing rows written above the fork point. Without a handler that
      // cleanup cannot happen, so returning a rewound cursor would strand diverged data —
      // refuse loudly instead. The startup warning announced this; here it becomes fatal.
      if (!onRollback) {
        throw new ClickhouseTargetError(
          CLICKHOUSE_ERROR_CODES.MISSING_ROLLBACK_ON_FORK,
          'A chain fork was detected, but no onRollback handler is configured to remove the rows ' +
            'written above the fork point. Configure onRollback on the ClickHouse target to make it ' +
            'fork-safe.',
        )
      }

      const cursor = await state.fork(canonicalBlocks)
      if (!cursor) return cursor

      await onRollback({
        reason: 'fork',
        store,
        safeCursor: lastCommit ? { ...cursor, upTo: lastCommit.number } : cursor,
      })
      return cursor
    },
  })
}
