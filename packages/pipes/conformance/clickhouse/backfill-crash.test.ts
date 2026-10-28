import { describe, expect } from 'vitest'

import { S1, S1_ROWS } from '../scenarios/backfill.js'
import { inStoreOrder } from '../scenarios/erc20.js'
import {
  S1_CLICKHOUSE,
  crashAt,
  cursorOf,
  it,
  readClickhouseStore,
  resumeToEnd,
  rowsBetween,
  runUnwatched,
  warningsLogger,
} from './harness.js'
import type { RequestKillPoint } from './kill-switch.js'
import { rowsOfClickhouse } from './probe.js'

/**
 * The class-A commit protocol (CN-13) request by request. Each batch inserts its rows table by
 * table (transfers first, a table with no rows in the batch skipped), then appends its cursor row;
 * the first batch appends a start cursor before its rows, and runs the sync table's retention read
 * after its cursor row. A restart first calls the repair hook,
 * which cancels the rows above the cursor table by table, both tables at once. A point names its
 * batch by its last block: S1's first batch ends at 6, its second at 13.
 */
const POINTS = {
  beforeWrites: {
    name: 'before the batch writes anything',
    request: '^insert transfers$',
    inBatch: 13,
    at: 1,
    when: 'before',
  },
  betweenTables: {
    name: 'between the data inserts of two tables',
    request: '^insert approvals$',
    inBatch: 13,
    at: 1,
    when: 'before',
  },
  afterData: {
    name: 'after the data inserts, before the cursor write',
    request: '^insert sync$',
    inBatch: 13,
    at: 1,
    when: 'before',
  },
  cursorSent: { name: 'cursor write sent, unanswered', request: '^insert sync$', inBatch: 13, at: 1, when: 'sent' },
  afterCursor: { name: 'after the cursor write', request: '^insert sync$', inBatch: 13, at: 1, when: 'after' },
  firstBatchStarted: {
    name: 'the first batch, after its start cursor, before its data',
    request: '^insert transfers$',
    inBatch: 6,
    at: 1,
    when: 'before',
  },
  firstBatch: {
    name: 'the first batch, after its data, before the first cursor write',
    request: '^insert sync$',
    inBatch: 6,
    at: 2,
    when: 'before',
  },
  firstCursor: {
    name: 'the first batch, after the first cursor write',
    request: '^insert sync$',
    inBatch: 6,
    at: 2,
    when: 'after',
  },
  setUp: { name: 'the first run, after the sync table is made', request: '^create sync$', at: 1, when: 'after' },
  recovery: { name: 'recovery, after the cancel rows of one table', request: '^cancel ', at: 1, when: 'after' },
} satisfies Record<string, RequestKillPoint>

const DATA_WRITES = ['insert transfers', 'insert approvals']

/** Below the floor S1 reports from the start: a portal whose finalized head went back. */
const REGRESSED_FLOOR = 30

const scenario = S1_CLICKHOUSE

/**
 * S1 into ClickHouse, killed by SIGKILL at each request of the commit protocol and of recovery
 * (CT-2). The cursor row goes in after the data, so a crash between them leaves rows above the
 * cursor: the crash window, which the repair hook removes on restart. Every test runs the same two
 * checks:
 *
 * - `crashAt` fails unless the crash left a commit point intact (CN-40): the rows at or below the
 *   cursor are exactly S1's. It returns the window, which each test pins to the step it names;
 * - `resumeToEnd` fails unless the restart starts right after the recovered cursor and ends in S1's
 *   end state (INV-40…INV-42).
 */
describe('ClickHouse (class A) · S1 backfill · killed at each commit step (CT-2)', () => {
  it('before the batch writes anything: the store holds the batch before, and nothing above it', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.beforeWrites,
    })

    expect(killed).toEqual({ request: 'insert transfers', sent: [] })
    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(batches[0].current)
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('between the data inserts of two tables: one table holds the batch above the cursor', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.betweenTables,
    })
    const transfers = rowsBetween(scenario, batches[0].current, batches[1].current).filter(
      (row) => row.table === 'transfers',
    )

    expect(killed).toEqual({ request: 'insert approvals', sent: ['insert transfers'] })
    expect(cursorOf(store)).toStrictEqual(batches[0].current)
    expect(window).toEqual(transfers)

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('after the data inserts, before the cursor write: the batch is above the cursor, and recovery removes it', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterData,
    })

    expect(killed).toEqual({ request: 'insert sync', sent: DATA_WRITES })
    expect(cursorOf(store)).toStrictEqual(batches[0].current)
    expect(window).toEqual(rowsBetween(scenario, batches[0].current, batches[1].current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('cursor write sent, unanswered: the store holds the batch or the one before, and the restart reads which', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.cursorSent,
    })
    const commitPoints = batches.map((batch) => batch.current)
    const cursor = cursorOf(store)

    expect(killed).toEqual({ request: 'insert sync', sent: [...DATA_WRITES, 'insert sync'] })
    // The pipe cannot know whether the server took the row; either is a commit point (CN-40).
    expect(commitPoints).toContainEqual(cursor)
    expect(window).toEqual(rowsBetween(scenario, cursor, batches[1].current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('after the cursor write: the store holds the batch, and the restart resumes right after it', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterCursor,
    })

    expect(killed).toEqual({ request: 'insert sync', sent: [...DATA_WRITES, 'insert sync'] })
    expect(cursorOf(store)).toStrictEqual(batches[1].current)
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a crash in the first run, after the sync table is made: no state and no row, and the restart starts cold', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.setUp,
    })

    // The first read of the cursor finds no sync table, and makes it.
    expect(killed).toEqual({ request: 'create sync', sent: ['select sync', 'create sync'] })
    expect(batches).toEqual([])
    expect(store.sync).toEqual([])
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a crash in recovery, after the cancel rows of one table: recovery runs again and converges (CN-41)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.afterData })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.recovery })
    const cancelled = second.killed.request.replace('cancel ', '')
    const left = first.window.filter((row) => row.table !== cancelled)

    // The restart died before its first batch, with the window of one table cancelled; whether the
    // other table's cancel went out first is free.
    expect(second.batches).toEqual([])
    expect(second.store.sync).toStrictEqual(first.store.sync)
    expect([[], left]).toContainEqual(second.window)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it("a second crash in the restart's first batch: recovery removes the window again, and nothing piles up (CN-42)", async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    // The first crash leaves the batch at 6, so the restart's first batch is the one at 13 again.
    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.afterData })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.afterData })
    const delivered = second.batches.map((batch) => batch.current.number)

    expect(delivered).toEqual([13])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it('a restart whose portal reports a lower floor keeps the one it persisted (WP-2, INV-12)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store } = await crashAt({ scenario, simulator, database, model, point: POINTS.afterCursor })
    const persisted = store.state?.finalized?.number
    simulator.finalize(REGRESSED_FLOOR)

    // `resumeToEnd` wants the highest floor reported, which only the resume state still holds.
    expect(persisted).toBe(S1.to)
    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('without a repair hook: the restart warns, and stores the window a second time (ADR-15)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { batches, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterData,
      repair: 'none',
    })
    const { logger, warnings } = warningsLogger()
    const end = await runUnwatched({ scenario, simulator, database, repair: 'none', logger })
    const twice = inStoreOrder([...S1_ROWS, ...window])

    expect(window).toEqual(rowsBetween(scenario, batches[0].current, batches[1].current))
    // The accepted deviation: loud, and no worse than the window delivered again (RP-42).
    expect(warnings).toEqual([expect.stringContaining('No onRollback handler is configured')])
    expect(end.state).toStrictEqual(scenario.end(simulator).state)
    expect(rowsOfClickhouse(end)).toEqual(twice)
  })

  it('the first batch, after its data: the restart removes it from the start cursor, and converges', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.firstBatch,
    })

    expect(killed).toEqual({ request: 'insert sync', sent: ['insert sync', ...DATA_WRITES] })
    expect(store.state).toBeUndefined()
    expect(store.start).toEqual({ from: scenario.range.from, to: batches[0].current.number })
    expect(window).toEqual(rowsBetween(scenario, undefined, batches[0].current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('the first batch, after its start cursor, before its data: the restart removes nothing, and converges', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.firstBatchStarted,
    })

    expect(killed).toEqual({ request: 'insert transfers', sent: ['insert sync'] })
    expect(store.start).toEqual({ from: scenario.range.from, to: batches[0].current.number })
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('the first batch, after the first cursor write: the cursor supersedes the start cursor, and the restart resumes after it', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.firstCursor,
    })

    expect(killed).toEqual({ request: 'insert sync', sent: ['insert sync', ...DATA_WRITES, 'insert sync'] })
    expect(store.start).toBeUndefined()
    expect(cursorOf(store)).toStrictEqual(batches[0].current)
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a crash in the recovery of the first batch, after the cancel rows of one table: recovery runs again and converges (CN-41)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.firstBatch })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.recovery })
    const cancelled = second.killed.request.replace('cancel ', '')
    const left = first.window.filter((row) => row.table !== cancelled)

    // Whether the other table's cancel went out first is free, as in any recovery
    expect(second.batches).toEqual([])
    expect(second.store.start).toEqual(first.store.start)
    expect([[], left]).toContainEqual(second.window)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it('a second crash in the replayed first batch: its own start cursor replaces the first, and the restart converges (CN-42)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.firstBatch })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.firstBatch })

    expect(second.store.start).toEqual(first.store.start)
    expect(second.window).toEqual(first.window)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it('the first batch without a repair hook: the restart warns, and stores the batch a second time (ADR-15)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.firstBatch,
      repair: 'none',
    })
    const { logger, warnings } = warningsLogger()
    const end = await runUnwatched({ scenario, simulator, database, repair: 'none', logger })

    expect(warnings).toEqual([expect.stringContaining('No onRollback handler is configured')])
    expect(end.state).toStrictEqual(scenario.end(simulator).state)
    expect(rowsOfClickhouse(end)).toEqual(inStoreOrder([...S1_ROWS, ...window]))
  })

  it('the first batch, with a hook that binds the cursor as unsigned: the restart fails at block -1 and removes nothing', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.firstBatch,
      repair: 'predicate',
    })
    const requests = simulator.chain.requests.length
    const restart = runUnwatched({ scenario, simulator, database, repair: 'predicate' })

    await expect(restart).rejects.toThrow('Value -1 cannot be parsed as UInt64')
    expect(simulator.chain.requests, 'the requests of the failed restart').toHaveLength(requests)
    expect(await readClickhouseStore(database), 'the store after the failed restart').toStrictEqual(store)
  })
})
