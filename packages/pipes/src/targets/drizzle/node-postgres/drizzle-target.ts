import { Table } from 'drizzle-orm'
import { NodePgDatabase } from 'drizzle-orm/node-postgres/driver'
import { NodePgQueryResultHKT } from 'drizzle-orm/node-postgres/session'
import { PgTransaction } from 'drizzle-orm/pg-core'
import type { PgTransactionConfig } from 'drizzle-orm/pg-core/session'

import { BlockCursor, HookContext, createTarget } from '~/core/index.js'
import { nonNullable } from '~/internal/array.js'
import { doWithRetry } from '~/internal/function.js'

import { DrizzleTracker } from './drizzle-tracker.js'
import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'
import { PostgresState, StateOptions } from './postgres-state.js'
import { orderTablesForDelete } from './rollback.js'

export type Transaction = PgTransaction<NodePgQueryResultHKT, any, any>

/**
 * Creates a PostgreSQL target using Drizzle ORM with automatic rollback table creation.
 *
 * @param options - Configuration options
 * @param options.db - Drizzle database instance
 * @param options.tables - Array of Drizzle tables that will be used for tracking rollbacks
 * @param options.onStart - Optional callback that runs before processing starts
 * @param options.onData - Callback that processes each batch of data within a transaction
 * @param options.onBeforeRollback - Optional callback that runs before a rollback is performed
 * @param options.onAfterRollback - Optional callback that runs after a rollback is performed
 * @param options.settings - Optional settings for state management and transaction configuration
 * @returns Target implementation that can be used with pipe()
 * @example
 * ```ts
 * drizzleTarget({
 *   db: drizzle('postgresql://...'),
 *   tables: [myTable],
 *   onData: async ({tx, data}) => {
 *     await tx.insert(myTable).values(data)
 *   }
 * })
 * ```
 */
export function drizzleTarget<T>({
  db,
  tables,
  onStart,
  onData,
  onBeforeRollback,
  onAfterRollback,
  settings,
}: {
  db: NodePgDatabase
  settings?: {
    state?: StateOptions
    transaction?: {
      isolationLevel?: 'read uncommitted' | 'read committed' | 'repeatable read' | 'serializable'
    }
  }
  tables: Table[] | Record<string, Table>
  onStart?: (ctx: { db: NodePgDatabase }) => Promise<unknown>
  onData: (ctx: { tx: Transaction; data: T; ctx: HookContext }) => Promise<unknown>
  onBeforeRollback?: (ctx: { tx: Transaction; cursor: BlockCursor }) => Promise<unknown> | unknown
  onAfterRollback?: (ctx: { tx: Transaction; cursor: BlockCursor }) => Promise<unknown> | unknown
}) {
  const tracker = new DrizzleTracker()
  const client = (db as any).$client
  if (!client) {
    throw new PostgresTargetError(
      POSTGRES_ERROR_CODES.DRIZZLE_CLIENT_MISSING,
      'Drizzle client not found on the provided database instance',
    )
  }

  const state = new PostgresState(client, settings?.state)
  const sortedTables = orderTablesForDelete(Array.isArray(tables) ? tables : Object.values(tables))
  const config: PgTransactionConfig = {
    isolationLevel: settings?.transaction?.isolationLevel || 'serializable',
  }

  return createTarget<T>({
    write: async ({ read, logger, id }) => {
      // Key the cursor by the pipe's source id (unless an explicit settings.state.id was given),
      // so progress is isolated per pipe. Must run before getCursor so read and write agree.
      state.bindCursorKey(id)

      const cursor = await state.getCursor({
        logger,
      })

      await onStart?.({ db })

      const triggers = sortedTables.map((table) => tracker.add(table)).filter(nonNullable)

      logger.debug(`Configuring PG triggers for ${triggers.length} tables...`)
      if (triggers.length) {
        await doWithRetry(
          () =>
            db.transaction(async (tx) => {
              // Acquire an advisory lock to prevent concurrent trigger modifications
              await tx.execute(`SELECT pg_advisory_xact_lock(hashtext('sqd_drizzle_triggers')::bigint);`)
              // Execute all trigger creation statements
              await Promise.all(triggers.map(async (trigger) => tx.execute(trigger)))
              // Lock is released automatically at the end of the transaction
            }, config),
          {
            title: 'postgres triggers configuration',
            retries: 3,
            delayMs: 100,
          },
        )
      }
      logger.debug(`PG triggers configured`)

      // Undo snapshots are tagged with the batch's last block, so a block's writes are only
      // rollback-safe when that block has the batch to itself and no finalized block shares it.
      for await (const { data, ctx } of read(cursor, { perBlockUnfinalized: true })) {
        const target = ctx.profiler.start({ name: 'postgres', labels: 'db' })

        try {
          await doWithRetry(
            () =>
              db.transaction(async (tx) => {
                await state.acquireLock(tx)
                // `!= null`: a finalized head of 0 would otherwise skip the undo log entirely.
                const finalizedHead = ctx.stream.head.finalized?.number
                const snapshotEnabled =
                  finalizedHead != null && ctx.stream.state.current.number >= finalizedHead ? 'true' : 'false'

                /*
                 * Enable snapshotting for this transaction
                 *
                 * We set the block number to the current batch's block number
                 * so that any changes made during this transaction can be
                 * rolled back to this point if needed.
                 */
                await tx.execute(`
                  SET LOCAL sqd.snapshot_enabled = ${snapshotEnabled};
                  SET LOCAL sqd.snapshot_block_number = ${ctx.stream.state.current.number};
                `)

                // Skipped for a batch built from zero source blocks (the finality catch-up a
                // bounded stream ends with): it carries no rows, and user handlers never saw
                // blockless batches before. The cursor save below still runs.
                if (ctx.batch?.blocksCount !== 0) {
                  await target.measure('data handler', async (profiler) => {
                    await onData({
                      tx: tracker.wrapTransaction(tx),
                      data,
                      ctx: {
                        logger,
                        profiler,
                      },
                    })
                  })
                }

                const { safeBlockNumber } = await state.saveCursor(tx, ctx, target)
                if (safeBlockNumber > 0) {
                  logger.debug(`Safe block number updated to ${safeBlockNumber}`)

                  await target.measure({ name: 'cleanup snapshots', labels: 'db' }, () => {
                    return tracker.cleanup(tx, safeBlockNumber)
                  })
                }
              }, config),
            {
              title: 'postgres batch insert transaction',
              retries: 3,
              delayMs: 100,
            },
          )
        } finally {
          target.end()
        }
      }
    },
    resolveFork: async (canonicalBlocks) => {
      const cursor = await state.fork(canonicalBlocks)
      if (!cursor) return cursor

      await db.transaction(async (tx) => {
        await onBeforeRollback?.({ tx, cursor })
        await tracker.fork(tx, cursor)
        // Drop the now-dead sync rows above the safe cursor so the resume row stays the last write
        // and reprocessing can re-insert those block numbers without a primary-key collision.
        await state.removeForkedRows(tx, cursor)
        await onAfterRollback?.({ tx, cursor })
      }, config)

      return cursor
    },
  })
}
