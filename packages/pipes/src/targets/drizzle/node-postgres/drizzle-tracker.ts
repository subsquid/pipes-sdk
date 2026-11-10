import { Column, Table } from 'drizzle-orm'

import { BlockCursor } from '~/core/index.js'

import { createColumnNameResolver, getDrizzleTableName } from './consts.js'
import { Transaction } from './drizzle-target.js'
import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'
import {
  SNAPSHOT_BLOCK_COLUMN,
  SnapshotPlan,
  buildSnapshotPlan,
  generateForkDeleteSQL,
  generateForkRestoreSQL,
  generateTriggerSQL,
} from './rollback.js'

/** @internal */
export class DrizzleTracker {
  // Insertion order is the FK-safe delete order (children → parents); reversed, it is the insert order.
  #plans = new Map<Table, SnapshotPlan>()
  readonly #resolveColumnName: (col: Column) => string

  constructor(db: unknown) {
    this.#resolveColumnName = createColumnNameResolver(db)
  }

  add(table: Table) {
    if (this.#plans.has(table)) {
      return
    }

    const plan = buildSnapshotPlan(table, this.#resolveColumnName)

    this.#plans.set(table, plan)

    return generateTriggerSQL(plan)
  }

  async cleanup(tx: Transaction, blockNumber: number) {
    const statements = [...this.#plans.values()].map(
      (plan) => `DELETE FROM "${plan.snapshotName}" WHERE "${SNAPSHOT_BLOCK_COLUMN}" <= ${blockNumber};`,
    )
    if (!statements.length) return

    await tx.execute(statements.join('\n'))
  }

  async fork(tx: Transaction, cursor: BlockCursor) {
    const plans = [...this.#plans.values()]
    if (!plans.length) return

    const deletes = plans.map((plan) => generateForkDeleteSQL(plan, cursor.number))
    // Reversed: a restored child row needs its parent back first.
    const restores = plans
      .slice()
      .reverse()
      .map((plan) => generateForkRestoreSQL(plan, cursor.number))
    // Drop the consumed snapshots; those at or below the cursor stay for a later deeper rollback.
    const dropSnapshots = plans.map(
      (plan) => `DELETE FROM "${plan.snapshotName}" WHERE "${SNAPSHOT_BLOCK_COLUMN}" > ${cursor.number};`,
    )

    // No bind parameters, so the driver sends the whole script in one round trip.
    await tx.execute([...deletes, ...restores, ...dropSnapshots].join('\n'))
  }

  wrapTransaction(tx: any): Transaction {
    for (const method of ['insert', 'delete', 'update']) {
      const orig = tx[method].bind(tx)

      tx[method] = (table: Table, ...args: any[]) => {
        if (!this.#plans.has(table)) {
          throw new PostgresTargetError(
            POSTGRES_ERROR_CODES.UNTRACKED_TABLE,
            `Table "${getDrizzleTableName(table)}" is not tracked for rollbacks. Make sure to include it in the "tables" array when creating the target.`,
          )
        }

        return orig(table, ...args)
      }
    }

    return tx
  }
}
