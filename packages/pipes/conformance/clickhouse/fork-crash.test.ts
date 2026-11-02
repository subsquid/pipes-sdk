import { describe, expect } from 'vitest'

import type { BlockCursor } from '~/core/index.js'

import type { Observation } from '../scenarios/erc20.js'
import { S3 } from '../scenarios/fork.js'
import { type ClickhouseStore, S3_CLICKHOUSE, crashAt, cursorOf, it, resumeToEnd, rowsBetween } from './harness.js'
import type { RequestKillPoint } from './kill-switch.js'

/**
 * The steps of S3 that S1 never reaches: a batch above the floor, and the fork. A batch is a portal
 * answer: the one from 21 to 27 lies above the floor at 19. The fork comes once the pipe has reached
 * the head: the target reads its sync rows for the ancestor, persists it, then the repair hook
 * cancels the rows above it, both tables at once, each after a netting read.
 */
const POINTS = {
  aboveFloor: {
    name: 'inside a batch above the floor, after its data, before its cursor write',
    request: '^insert sync$',
    inBatch: 27,
    at: 1,
    when: 'before',
  },
  forkBeforeCursor: {
    name: 'inside the fork, before the ancestor cursor write',
    request: '^insert sync$',
    inFork: true,
    at: 1,
    when: 'before',
  },
  forkBeforeCancel: {
    name: 'inside the fork, after the ancestor cursor write and before any cancel row',
    request: '^cancel ',
    inFork: true,
    at: 1,
    when: 'before',
  },
  forkMidRollback: {
    name: 'inside the fork, after the cancel rows of one table',
    request: '^cancel ',
    inFork: true,
    at: 1,
    when: 'after',
  },
  forkAfterRollback: {
    name: 'after the fork rolled both tables back, before the next commit',
    request: '^cancel ',
    inFork: true,
    at: 2,
    when: 'after',
  },
  // The first answer after the fork runs from the ancestor's child.
  afterForkBatch: {
    name: 'the first batch after the fork, after its data, before its cursor write',
    request: '^insert sync$',
    inBatch: S3.ancestor + S3.blocksPerResponse,
    at: 1,
    when: 'before',
  },
} satisfies Record<string, RequestKillPoint>

const scenario = S3_CLICKHOUSE

function expectForkState(store: ClickhouseStore, batches: Observation[], forks: BlockCursor[][]) {
  const finalized = batches.at(-1)?.finalized
  const canonical = forks.at(-1) ?? []
  const ancestor = canonical.find((block) => block.number === S3.ancestor)

  expect(ancestor).toBeDefined()
  expect(store.state?.current).toStrictEqual(ancestor)
  expect(store.state?.finalized).toStrictEqual(finalized)
  expect(store.state?.rollbackChain.map(({ number, hash }) => ({ number, hash }))).toStrictEqual(
    canonical
      .filter((block) => block.number > (finalized?.number ?? -1) && block.number <= S3.ancestor)
      .map(({ number, hash }) => ({ number, hash })),
  )
}

/**
 * S3 into ClickHouse, killed by SIGKILL in a batch above the floor and in the fork (CT-2). Every
 * test runs the same two checks as on S1: `crashAt` fails unless the crash left a commit point
 * intact (CN-40), and `resumeToEnd` fails unless the restart ends in S3's end state.
 */
describe('ClickHouse (class A) · S3 one fork · killed at each commit step (CT-2)', () => {
  it('inside a batch above the floor: its rows are above the cursor, and recovery removes them', async ({
    simulateS3,
    database,
  }) => {
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

    expect(killed.request).toBe('insert sync')
    expect(batch.current.number).toBe(27)
    expect(cursorOf(store)).toStrictEqual(before.current)
    expect(window).toEqual(rowsBetween(scenario, before.current, batch.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the fork, before the ancestor cursor write: the store keeps the last batch commit', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkBeforeCursor,
    })

    expect(killed.sent[0]).toBe('select sync')
    expect(forks).toHaveLength(1)
    expect(cursorOf(store)).toStrictEqual(batches.at(-1)?.current)
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a second crash before the ancestor cursor write: the restart forks again, and converges (WP-46)', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.forkBeforeCursor })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.forkBeforeCursor })

    expect(second.forks).toHaveLength(1)
    expect(second.batches).toEqual([])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it('after the ancestor cursor write but before repair: only the forked rows are above the cursor', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkBeforeCancel,
    })

    expectForkState(store, batches, forks)
    expect(window).toEqual(rowsBetween(scenario, store.state?.current, batches.at(-1)!.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the fork rollback: the cursor stays on the ancestor and any remaining rows are crash residue', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkMidRollback,
    })

    expectForkState(store, batches, forks)
    expect(window.every((row) => row.block > S3.ancestor && row.block <= S3.latest)).toBe(true)

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('after the fork rollback: the cursor and rows are fully rewound to the ancestor', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkAfterRollback,
    })

    expectForkState(store, batches, forks)
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('in the first batch after the fork: its rows sit above the ancestor cursor until its own commit', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterForkBatch,
    })
    const [batch] = batches.slice(-1)

    expect(forks).toHaveLength(1)
    expect(batch.current.number).toBe(POINTS.afterForkBatch.inBatch)
    expectForkState(store, batches.slice(0, -1), forks)
    expect(window).toEqual(rowsBetween(scenario, store.state?.current, batch.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })
})
