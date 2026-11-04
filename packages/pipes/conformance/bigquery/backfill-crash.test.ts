import { describe, expect } from 'vitest'

import { type BigqueryStore, S1_BIGQUERY, crashAt, cursorOf, it, resumeToEnd, rowsBetween } from './harness.js'
import type { CallKillPoint } from './kill-switch.js'

/** Without a project the harness may write, the class-W suite skips wholesale. */
const RUN = !!process.env['BIGQUERY_TEST_PROJECT']

/**
 * The class-W commit protocol (CN-14) call by call. Each batch writes a WAL row that names the
 * range it is about to write, streams the batch's rows into every tracked table, then writes the
 * WAL row that completes the batch. A point names its batch by the block it ends at: S1's first
 * batch ends at 6, its second at 13.
 */
const POINTS = {
  beforePreCommit: {
    name: 'after the batch was delivered, before its pre-commit WAL row',
    call: '^append sync$',
    inBatch: 13,
    at: 1,
    when: 'before',
  },
  afterData: {
    name: 'after the batch’s data, before its post-commit WAL row',
    call: '^append sync$',
    inBatch: 13,
    at: 2,
    when: 'before',
  },
  postCommitSent: {
    name: 'the post-commit WAL row sent, unanswered',
    call: '^append sync$',
    inBatch: 13,
    at: 2,
    when: 'sent',
  },
  afterPostCommit: {
    name: 'after the post-commit WAL row',
    call: '^append sync$',
    inBatch: 13,
    at: 2,
    when: 'after',
  },
  firstBatch: {
    name: 'inside the first batch, after its data',
    call: '^append sync$',
    inBatch: 6,
    at: 2,
    when: 'before',
  },
  recovery: {
    name: 'in the restart’s recovery, before its first DELETE',
    call: '^delete ',
    at: 1,
    when: 'before',
  },
} satisfies Record<string, CallKillPoint>

const scenario = S1_BIGQUERY

function inFlightOf(store: BigqueryStore) {
  return store.inFlight
}

/**
 * S1 into BigQuery, killed by SIGKILL at each call of the commit protocol and of recovery (CT-2,
 * the matrix GAP-14 asked for). Rows above the recovery cursor are the class's crash window: they
 * are residue of the write the in-flight WAL row names, and the restart's recovery DELETEs them.
 * Every test runs the same two checks:
 *
 * - `crashAt` fails unless the crash left a commit point intact (CN-40): with the window set apart,
 *   the store holds exactly the scenario's rows up to its recovery cursor;
 * - `resumeToEnd` fails unless the restart starts right after the recovered cursor and ends in S1's
 *   end state (INV-40…INV-42).
 */
describe.skipIf(!RUN)('BigQuery (class W) · S1 backfill · killed at each commit step (CT-2)', () => {
  it('before the pre-commit WAL row: the store holds the batch before, and nothing of the batch', {
    timeout: 420_000,
  }, async ({ simulateS1, database }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.beforePreCommit,
    })

    expect(killed.call).toBe('append sync')
    // The kill came before the call went out, and the batch before it had sent nothing new.
    expect(killed.sent).toEqual([])
    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(batches[0].current)
    expect(inFlightOf(store)).toBeUndefined()
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('after the batch’s data: its rows are above the cursor, and the in-flight row names their range (CN-14)', {
    timeout: 420_000,
  }, async ({ simulateS1, database }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterData,
    })
    const [before, batch] = batches

    expect(killed.call).toBe('append sync')
    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(before.current)
    expect(inFlightOf(store)).toEqual({
      op: 'commit',
      current: before.current,
      range: { low: before.current.number + 1, high: batch.current.number },
    })
    expect(window).toEqual(rowsBetween(scenario, before.current, batch.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('the post-commit WAL row sent, unanswered: the store holds the batch or the one before, and the restart reads which', {
    timeout: 420_000,
  }, async ({ simulateS1, database }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.postCommitSent,
    })
    const commitPoints = batches.map((batch) => batch.current)

    expect(killed.call).toBe('append sync')
    expect(killed.sent.at(-1)).toBe('append sync')
    expect(batches).toHaveLength(2)
    // The pipe cannot know whether the row landed; either is a commit point (CN-40).
    expect(commitPoints).toContainEqual(cursorOf(store))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('after the post-commit WAL row: the store holds the batch, and the restart resumes right after it', {
    timeout: 420_000,
  }, async ({ simulateS1, database }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterPostCommit,
    })

    expect(killed.call).toBe('append sync')
    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(batches[1].current)
    expect(inFlightOf(store)).toBeUndefined()
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the first batch, after its data: no state and no committed row, and the restart starts cold', {
    timeout: 420_000,
  }, async ({ simulateS1, database }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.firstBatch,
    })
    const [batch] = batches

    expect(killed.call).toBe('append sync')
    expect(batches).toHaveLength(1)
    expect(cursorOf(store)).toBeUndefined()
    expect(inFlightOf(store)).toEqual({
      op: 'commit',
      current: undefined,
      range: { low: scenario.range.from, high: batch.current.number },
    })
    expect(window).toEqual(rowsBetween(scenario, undefined, batch.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a crash in the restart’s recovery, before its first DELETE: recovery runs again and converges (INV-41)', {
    timeout: 420_000,
  }, async ({ simulateS1, database }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.afterData })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.recovery })

    // The restart died inside its recovery, before any DELETE or WAL row; the store stands as the first crash left it.
    expect(second.killed.call).toBe('delete transfers')
    expect(second.batches).toEqual([])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it("a second crash in the restart's first batch: nothing piles up (CN-42)", { timeout: 420_000 }, async ({
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
    // Each crash leaves one recovery marker more in the WAL; the tables, the cursor and the
    // in-flight range are what must not pile up.
    expect(cursorOf(second.store)).toStrictEqual(cursorOf(first.store))
    expect(second.store.inFlight).toStrictEqual(first.store.inFlight)
    expect(second.store.tables).toStrictEqual(first.store.tables)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })
})
