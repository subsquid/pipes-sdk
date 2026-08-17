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
    for (const plan of this.#plans.values()) {
      await tx.execute(`DELETE FROM "${plan.snapshotName}" WHERE "${SNAPSHOT_BLOCK_COLUMN}" <= ${blockNumber};`)
    }
  }

  async fork(tx: Transaction, cursor: BlockCursor) {
    const plans = [...this.#plans.values()]

    for (const plan of plans) {
      await tx.execute(generateForkDeleteSQL(plan, cursor.number))
    }

    // Reversed: a restored child row needs its parent back first.
    for (const plan of plans.slice().reverse()) {
      await tx.execute(generateForkRestoreSQL(plan, cursor.number))
    }

    // Drop the consumed snapshots; those at or below the cursor stay for a later deeper rollback.
    for (const plan of plans) {
      await tx.execute(`DELETE FROM "${plan.snapshotName}" WHERE "${SNAPSHOT_BLOCK_COLUMN}" > ${cursor.number}`)
    }
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
