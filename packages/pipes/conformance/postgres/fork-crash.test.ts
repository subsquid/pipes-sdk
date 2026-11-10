import { describe, expect } from 'vitest'

import { TABLE_NAMES, numbers } from '../scenarios/erc20.js'
import { S3 } from '../scenarios/fork.js'
import {
  COMMIT,
  CURSOR_WRITE,
  FORKED_ROWS,
  type PostgresStore,
  S3_POSTGRES,
  batchAt,
  crashAt,
  it,
  resumeToEnd,
} from './harness.js'
import type { StatementKillPoint } from './kill-switch.js'

/**
 * The steps of S3 that S1 never reaches: a batch above the floor, whose transaction writes the undo
 * log too (CN-30), and the fork's own transaction, which rolls the tables back and drops the sync
 * rows above the ancestor (CN-34). Above the floor every block is a batch of its own.
 */
const POINTS = {
  undoBeforeCursor: {
    name: 'inside a batch above the floor, after its rows and undo records',
    statement: CURSOR_WRITE,
    inTransaction: batchAt(22),
    at: 1,
    when: 'before',
  },
  undoAfterCommit: {
    name: 'after the commit of a batch above the floor',
    statement: COMMIT,
    inTransaction: batchAt(22),
    at: 1,
    when: 'after',
  },
  forkBeforeCommit: {
    name: 'inside the fork transaction, after the rollback',
    statement: COMMIT,
    inTransaction: FORKED_ROWS,
    at: 1,
    when: 'before',
  },
  forkAfterCommit: {
    name: 'after the commit of the fork transaction',
    statement: COMMIT,
    inTransaction: FORKED_ROWS,
    at: 1,
    when: 'after',
  },
} satisfies Record<string, StatementKillPoint>

const scenario = S3_POSTGRES

/**
 * The blocks above the starting floor that the undo log of `transfers` tags records with. The batch
 * that ends on the floor writes some too, tagged with the floor: they can never be rolled back.
 */
function undoAboveFloor(store: PostgresStore): number[] {
  return store.tables.transfers.undo.filter((block) => block > S3.finalized)
}

/**
 * S3 into Postgres, killed by SIGKILL in a batch above the floor and in the fork's transaction
 * (CT-2). The fork's commit is a commit point too: the cursor at the ancestor, the floor unchanged
 * and the chain trimmed (CN-34). Every test runs the same two checks as on S1: `crashAt` fails
 * unless the crash left a commit point intact (CN-40), and `resumeToEnd` fails unless the restart
 * ends in S3's end state.
 */
describe('Postgres (class T) · S3 one fork · killed at each commit step (CT-2)', () => {
  it('inside a batch above the floor: neither its rows nor its undo records are visible (CN-10, CN-30)', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.undoBeforeCursor,
    })

    expect(killed.statement).toBe('insert sync')
    expect(killed.transaction).toContain('insert transfers')
    expect(batches.at(-1)?.current.number).toBe(22)
    expect(store.state?.current?.number).toBe(21)
    // Every batch above the floor before it left its undo records; this one left none.
    expect(undoAboveFloor(store)).toEqual([20, 21])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('after the commit of a batch above the floor: its undo records are in, and the restart goes on', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, killed } = await crashAt({ scenario, simulator, database, model, point: POINTS.undoAfterCommit })

    expect(killed.transaction.at(-1)).toBe('commit')
    expect(store.state?.current?.number).toBe(22)
    expect(undoAboveFloor(store)).toEqual([20, 21, 22])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the fork transaction: the store keeps the forked branch, and the restart forks again', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, forks, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkBeforeCommit,
    })

    // The rollback goes as one script, labelled by its first statement: a delete from a tracked table.
    const rollbackDeletes = TABLE_NAMES.map((table) => `delete ${table}`)
    const rolledBack = killed.transaction.some((label) => rollbackDeletes.includes(label))

    expect(rolledBack).toBe(true)
    expect(killed.transaction).toContain('delete sync')
    expect(forks).toHaveLength(1)
    expect(store.state?.current?.number).toBe(S3.latest)

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a second crash inside the fork transaction: the rollback runs again and converges (WP-46)', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.forkBeforeCommit })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.forkBeforeCommit })

    expect(second.forks).toHaveLength(1)
    expect(second.batches).toEqual([])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it("after the fork's commit: the ancestor keeps the current floor and a trimmed chain", async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store } = await crashAt({ scenario, simulator, database, model, point: POINTS.forkAfterCommit })
    const chain = store.state?.rollbackChain.map((block) => block.number)

    expect(store.state?.current?.number).toBe(S3.ancestor)
    expect(store.state?.finalized?.number).toBe(S3.raisedTo)
    expect(chain).toEqual(numbers(S3.raisedTo + 1, S3.ancestor))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })
})
