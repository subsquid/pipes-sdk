import { describe, expect } from 'vitest'

import { TABLE_NAMES } from '../scenarios/erc20.js'
import {
  COMMIT,
  DATA_WRITE,
  type PostgresStore,
  S1_POSTGRES,
  TRIGGER_SET_UP,
  batchAt,
  crashAt,
  it,
  resumeToEnd,
} from './harness.js'
import type { StatementKillPoint } from './kill-switch.js'

/**
 * The class-T commit protocol (CN-10) statement by statement. A run first sets up the undo-log
 * triggers in a transaction of its own; then each batch commits in one transaction. Its first
 * statement takes the pipe's lock, tags the undo log with the batch's last block and writes the
 * cursor row; the data rows follow. A point names its transaction by the block it tags: S1's first batch ends at 6, its
 * second at 13.
 */
const POINTS = {
  beforeWrites: { name: 'before the batch writes anything', statement: batchAt(13), at: 1, when: 'before' },
  afterCursor: {
    name: 'inside the transaction, after the cursor write',
    statement: DATA_WRITE,
    inTransaction: batchAt(13),
    at: 1,
    when: 'before',
  },
  afterData: {
    name: 'inside the transaction, after the data writes',
    statement: COMMIT,
    inTransaction: batchAt(13),
    at: 1,
    when: 'before',
  },
  commitSent: { name: 'commit sent, unanswered', statement: COMMIT, inTransaction: batchAt(13), at: 1, when: 'sent' },
  afterCommit: { name: 'after the transaction', statement: COMMIT, inTransaction: batchAt(13), at: 1, when: 'after' },
  firstTransaction: {
    name: 'inside the first batch transaction',
    statement: COMMIT,
    inTransaction: batchAt(6),
    at: 1,
    when: 'before',
  },
  recovery: {
    name: 'recovery, inside the trigger set-up',
    statement: COMMIT,
    inTransaction: TRIGGER_SET_UP,
    at: 1,
    when: 'before',
  },
} satisfies Record<string, StatementKillPoint>

/** What a batch transaction sends before its data writes: `lock` also tags the undo log and writes the cursor. */
const OPENING = ['begin', 'lock']
const DATA_WRITES = TABLE_NAMES.map((table) => `insert ${table}`)

/** The statements of `transaction` after its opening that write data rows. */
function dataWritesOf(transaction: string[]): string[] {
  return transaction.slice(OPENING.length).filter((label) => DATA_WRITES.includes(label))
}

function cursorOf(store: PostgresStore) {
  return store.state?.current
}

const scenario = S1_POSTGRES

/**
 * S1 into Postgres, killed by SIGKILL at each statement of the commit protocol and of recovery
 * (CT-2). A batch commits whole or not at all, so every crash must leave the store at the last
 * commit the server acknowledged. Every test runs the same two checks:
 *
 * - `crashAt` fails unless the crash left a commit point intact (CN-40). It returns where the kill
 *   landed: how many batches the pipe delivered, and what the open transaction had sent;
 * - `resumeToEnd` fails unless the restart starts right after the recovered cursor and ends in S1's
 *   end state (INV-40…INV-42).
 */
describe('Postgres (class T) · S1 backfill · killed at each commit step (CT-2)', () => {
  it('before the batch writes anything: the store holds the batch before, and the restart starts after it', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.beforeWrites,
    })

    expect(killed).toEqual({ statement: 'lock', transaction: ['begin'] })
    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(batches[0].current)

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the transaction, after the cursor write: the cursor is not visible (CN-10)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterCursor,
    })

    expect(DATA_WRITES).toContain(killed.statement)
    expect(killed.transaction).toEqual(OPENING)
    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(batches[0].current)

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the transaction, after the data writes: neither the rows nor the cursor is visible (CN-10)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({ scenario, simulator, database, model, point: POINTS.afterData })

    expect(killed.statement).toBe('commit')
    expect(killed.transaction.slice(0, OPENING.length)).toEqual(OPENING)
    expect(killed.transaction).not.toContain('commit')
    // Which tables the batch writes is free; that it wrote rows is not.
    expect(dataWritesOf(killed.transaction).length).toBeGreaterThan(0)
    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(batches[0].current)

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('commit sent, unanswered: the store holds the batch or the one before, and the restart reads which', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({ scenario, simulator, database, model, point: POINTS.commitSent })
    const commitPoints = batches.map((batch) => batch.current)

    expect(killed.statement).toBe('commit')
    expect(killed.transaction.at(-1)).toBe('commit')
    expect(killed.transaction.slice(0, OPENING.length)).toEqual(OPENING)
    expect(batches).toHaveLength(2)
    // The pipe cannot know whether the server committed; either is a commit point (CN-40).
    expect(commitPoints).toContainEqual(cursorOf(store))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('after the transaction: the store holds the batch, and the restart resumes right after it', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterCommit,
    })

    expect(killed.statement).toBe('commit')
    expect(killed.transaction.at(-1)).toBe('commit')
    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(batches[1].current)

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the first batch transaction: no state and no row, and the restart starts cold', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.firstTransaction,
    })

    expect(killed.statement).toBe('commit')
    expect(killed.transaction.slice(0, OPENING.length)).toEqual(OPENING)
    expect(batches).toHaveLength(1)
    expect(store.sync).toEqual([])
    expect(store.state).toBeUndefined()

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a crash in the first run’s set-up, before any batch: the restart sets up again and converges', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({ scenario, simulator, database, model, point: POINTS.recovery })
    const ddl = killed.transaction.slice(2)

    expect(killed.transaction.slice(0, 2)).toEqual(['begin', 'lock'])
    expect(ddl.length).toBeGreaterThan(0)
    expect(ddl.filter((label) => label === 'create')).toEqual(ddl)
    expect(batches).toEqual([])
    expect(store.state).toBeUndefined()

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a crash in recovery, inside the trigger set-up: recovery runs again and converges (INV-41)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.afterData })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.recovery })

    // The restart died before its first batch, and left the store as the first crash did.
    expect(second.killed.transaction.slice(0, 2)).toEqual(['begin', 'lock'])
    expect(second.batches).toEqual([])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it("a second crash in the restart's first transaction: nothing piles up (CN-42)", async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    // The first crash leaves the batch at 6, so the restart's first batch is the one at 13 again.
    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.afterData })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.afterData })
    const delivered = second.batches.map((batch) => batch.current.number)

    expect(second.killed.transaction.slice(0, OPENING.length)).toEqual(OPENING)
    expect(delivered).toEqual([13])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })
})
