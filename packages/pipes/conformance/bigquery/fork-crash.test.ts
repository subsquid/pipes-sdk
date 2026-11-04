import { describe, expect } from 'vitest'

import type { BlockCursor } from '~/core/index.js'
import { testLogger } from '~/testing/index.js'

import { TABLE_NAMES } from '../scenarios/erc20.js'
import { S3 } from '../scenarios/fork.js'
import {
  type BigqueryStore,
  S3_BIGQUERY,
  crashAt,
  cursorOf,
  expectEndState,
  it,
  lateRollbackDelete,
  resumeToEnd,
  rowsBetween,
  runKilledOverlapping,
  runToEnd,
  settledJobs,
} from './harness.js'
import type { CallKillPoint } from './kill-switch.js'
import { recoverBigquery } from './pipe.js'

/** Without a project the harness may write, the class-W suite skips wholesale. */
const RUN = !!process.env['BIGQUERY_TEST_PROJECT']

/**
 * The steps of S3 that S1 never reaches: a batch above the floor, and the fork. A batch is a portal
 * answer: the one from 21 to 27 lies above the floor at 19. The fork comes once the pipe has reached
 * the head: the target pages its WAL for the ancestor, writes the WAL row that names the range the
 * rollback will clear, DELETEs both tables over it, then writes the WAL row that completes the
 * rollback. A fork answers from the ancestor's child, so the batch after it ends at
 * `ancestor + blocksPerResponse`.
 */
const POINTS = {
  aboveFloor: {
    name: 'inside a batch above the floor, after its data, before its post-commit WAL row',
    call: '^append sync$',
    inBatch: 27,
    at: 2,
    when: 'before',
  },
  forkBeforeWal: {
    name: 'inside the fork, before its pre-rollback WAL row',
    call: '^append sync$',
    inFork: true,
    at: 1,
    when: 'before',
  },
  forkBeforeDelete: {
    name: 'inside the fork, after its pre-rollback WAL row, before any DELETE',
    call: '^delete ',
    inFork: true,
    at: 1,
    when: 'before',
  },
  forkAfterDelete: {
    name: 'inside the fork, after the DELETE of the second table was created',
    call: '^delete ',
    inFork: true,
    at: 2,
    when: 'after',
  },
  afterForkBatch: {
    name: 'the first batch after the fork, after its data, before its post-commit WAL row',
    call: '^append sync$',
    inBatch: S3.ancestor + S3.blocksPerResponse,
    at: 2,
    when: 'before',
  },
} satisfies Record<string, CallKillPoint>

const scenario = S3_BIGQUERY

/** The ancestor of the last fork the run began, as the canonical chain the journal holds names it. */
function ancestorOf(forks: BlockCursor[][]): BlockCursor | undefined {
  const canonical = forks.at(-1) ?? []
  return canonical.find((block) => block.number === S3.ancestor)
}

/**
 * The fork state a class-W crash leaves: the cursor back on the ancestor. The in-flight rollback WAL
 * row carries no floor or chain of its own, so the cursor is all there is to check against the model.
 */
function expectForkState(store: BigqueryStore, forks: BlockCursor[][]) {
  const ancestor = ancestorOf(forks)

  expect(ancestor).toBeDefined()
  expect(cursorOf(store)).toStrictEqual(ancestor)
}

/**
 * S3 into BigQuery, killed by SIGKILL in a batch above the floor and across the fork (CT-2). Every
 * test runs the same two checks as on S1: `crashAt` fails unless the crash left a commit point intact
 * (CN-40), and `resumeToEnd` fails unless the restart ends in S3's end state. The middle of the fork
 * is where the 2026-11-02 incident lived: a fault while the rollback DELETEs ran left the pipe
 * replaying the fork onto a store it had already cleared.
 */
describe.skipIf(!RUN)('BigQuery (class W) · S3 one fork · killed at each commit step (CT-2)', () => {
  it('inside a batch above the floor: its rows are above the cursor, and recovery DELETEs them', {
    timeout: 420_000,
  }, async ({ simulateS3, database }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.aboveFloor,
    })
    const [before, batch] = batches.slice(-2)

    expect(killed.call).toBe('append sync')
    expect(batch.current.number).toBe(27)
    expect(cursorOf(store)).toStrictEqual(before.current)
    expect(store.inFlight).toEqual({
      op: 'commit',
      current: before.current,
      range: { low: before.current.number + 1, high: batch.current.number },
    })
    expect(window).toEqual(rowsBetween(scenario, before.current, batch.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the fork, before its pre-rollback WAL row: the store keeps the last batch commit', {
    timeout: 420_000,
  }, async ({ simulateS3, database }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkBeforeWal,
    })

    expect(killed.call).toBe('append sync')
    // The ancestor read went out through the client's fast query path, which sends no job to kill
    // on; the pre-rollback WAL row is the fork's first call the switch can see.
    expect(killed.sent).toEqual([])
    expect(forks).toHaveLength(1)
    expect(cursorOf(store)).toStrictEqual(batches.at(-1)?.current)
    expect(store.inFlight).toBeUndefined()
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a second crash before the pre-rollback WAL row: the restart forks again, and converges (WP-46)', {
    timeout: 420_000,
  }, async ({ simulateS3, database }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.forkBeforeWal })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.forkBeforeWal })

    expect(second.forks).toHaveLength(1)
    expect(second.batches).toEqual([])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it('after the pre-rollback WAL row, before any DELETE: the cursor sits on the ancestor and the forked rows are its window', {
    timeout: 420_000,
  }, async ({ simulateS3, database }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkBeforeDelete,
    })
    const ancestor = ancestorOf(forks)

    // The 2026-11-02 incident shape: everything of the rollback is named but none of it ran.
    expect(killed.call).toBe('delete transfers')
    expect(killed.sent).toEqual(['append sync'])
    expectForkState(store, forks)
    expect(store.inFlight).toEqual({
      op: 'rollback',
      current: ancestor,
      range: { low: S3.ancestor + 1, high: batches.at(-1)!.current.number },
    })
    expect(window).toEqual(rowsBetween(scenario, ancestor, batches.at(-1)!.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('after the rollback DELETEs were created: the forked rows are gone, and the WAL row still names the range', {
    timeout: 420_000,
  }, async ({ simulateS3, database }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkAfterDelete,
    })
    const ancestor = ancestorOf(forks)

    expect(killed.call).toBe('delete approvals')
    expectForkState(store, forks)
    expect(store.inFlight).toEqual({
      op: 'rollback',
      current: ancestor,
      range: { low: S3.ancestor + 1, high: batches.at(-1)!.current.number },
    })
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a restart that begins while the killed run’s rollback DELETEs still run converges (INV-41)', {
    timeout: 420_000,
  }, async ({ simulateS3, database }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    // A supervisor restarts the process at once; it cannot wait for the killed run's jobs. The
    // harness hands the journal over the same way — no store read, no wait — so the restart runs
    // against whatever of the killed run's jobs BigQuery still holds, as it schedules them; the
    // controlled order the next test pins is what makes this safe.
    const run = await runKilledOverlapping({ scenario, simulator, database, model, point: POINTS.forkAfterDelete })
    const ancestor = ancestorOf(run.forks)
    const first = simulator.chain.requests.length

    await runToEnd({ scenario, simulator, database, model })

    const restart = simulator.chain.requests[first]?.query

    expect(restart?.fromBlock, 'the first block the restart asks for').toBe(S3.ancestor + 1)
    expect(restart?.parentBlockHash, 'the anchor of the restart').toBe(ancestor?.hash)

    // A rollback DELETE the killed run left can finish after the restart's own writes: wait for
    // every one of them, then read the store again — the end state must still stand.
    await settledJobs(database, run.jobs)
    await expectEndState({ scenario, simulator, database })
  })

  it('a rollback DELETE that completes after the restart’s recovery takes nothing, and the run converges', {
    timeout: 420_000,
  }, async ({ simulateS3, database }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    // The killed run's rollback DELETEs were created and everything landed: the store holds the
    // in-flight rollback row and nothing above the ancestor.
    const { store, batches } = await crashAt({ scenario, simulator, database, model, point: POINTS.forkAfterDelete })
    const range = { low: S3.ancestor + 1, high: batches.at(-1)!.current.number }

    expect(store.inFlight?.range).toEqual(range)

    // The restart's recovery alone: the rollback DELETEs re-run, the marker lands. The run itself
    // has not written a row yet.
    await recoverBigquery({ projectId: database.projectId, dataset: database.dataset, logger: testLogger() })

    // The killed run's outstanding DELETE completes now — after the restart's recovery, before its
    // writes: the latest landing BigQuery's own-table DML order can hand a restart. Everything it
    // names is already gone, so it must take nothing; a completion after the restart's writes would
    // eat its rows, which that order does not allow.
    const taken = []

    for (const table of TABLE_NAMES) {
      taken.push(await lateRollbackDelete(database, table, range))
    }

    expect(taken).toEqual(TABLE_NAMES.map(() => 0))

    await runToEnd({ scenario, simulator, database, model })
  })

  it('in the first batch after the fork: its rows sit above the ancestor cursor until its own commit', {
    timeout: 420_000,
  }, async ({ simulateS3, database }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterForkBatch,
    })
    const [batch] = batches.slice(-1)
    const ancestor = ancestorOf(forks)

    expect(killed.call).toBe('append sync')
    expect(forks).toHaveLength(1)
    expect(batch.current.number).toBe(POINTS.afterForkBatch.inBatch)
    expectForkState(store, forks)
    expect(store.inFlight).toEqual({
      op: 'commit',
      current: ancestor,
      range: { low: S3.ancestor + 1, high: batch.current.number },
    })
    expect(window).toEqual(rowsBetween(scenario, ancestor, batch.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })
})
