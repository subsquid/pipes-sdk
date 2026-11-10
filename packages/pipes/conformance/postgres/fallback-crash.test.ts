import { describe, expect } from 'vitest'

import type { MockPortalRequest } from '~/testing/index.js'

import { S1 } from '../scenarios/backfill.js'
import { type FallbackS1Simulator, answerEnds, dataRequests, lastServed, s1Fallback } from '../scenarios/fallback.js'
import { COMMIT, CURSOR_WRITE, type PostgresStore, batchAt, crashAt, it, resumeToEnd } from './harness.js'
import type { StatementKillPoint } from './kill-switch.js'

/**
 * S1 batches end every seven blocks. The primary fails from block 28 on, so it serves the batches
 * that end at 6, 13, 20 and 27, and the standby serves the rest from 28: its first batch ends at 34.
 */
const FAIL_AT = 28
const FIRST_STANDBY_BATCH = 34
const PRIMARY_BATCHES = 4

const POINTS = {
  beforeWrites: {
    name: 'after the switch, before the batch writes anything',
    statement: batchAt(FIRST_STANDBY_BATCH),
    at: 1,
    when: 'before',
  },
  afterCursor: {
    name: 'after the switch, inside the transaction, after the cursor write',
    statement: COMMIT,
    inTransaction: batchAt(FIRST_STANDBY_BATCH),
    at: 1,
    when: 'before',
  },
  commitSent: {
    name: 'after the switch, commit sent, unanswered',
    statement: COMMIT,
    inTransaction: batchAt(FIRST_STANDBY_BATCH),
    at: 1,
    when: 'sent',
  },
  afterCommit: {
    name: 'after the switch, after the transaction',
    statement: COMMIT,
    inTransaction: batchAt(FIRST_STANDBY_BATCH),
    at: 1,
    when: 'after',
  },
  beforeSwitch: {
    name: 'before the switch, inside the second batch transaction',
    statement: CURSOR_WRITE,
    inTransaction: batchAt(13),
    at: 1,
    when: 'before',
  },
} satisfies Record<string, StatementKillPoint>

const scenario = s1Fallback('T')

function cursorOf(store: PostgresStore) {
  return store.state?.current
}

/** The first data request the standby got from `from` on in its log. */
function firstStandbyRead(simulator: FallbackS1Simulator, from = 0): MockPortalRequest['query'] | undefined {
  return dataRequests(simulator.standby.chain.requests.slice(from))[0]?.query
}

/**
 * S1 over a source list of two portals into Postgres, killed by SIGKILL around the switch to the
 * standby (CT-2, spec/16). The child reads the list on the real clock. Every test runs the checks of
 * the single-source kill matrix:
 *
 * - `crashAt` fails unless the crash left a commit point intact (CN-40);
 * - `resumeToEnd` fails unless the restart, over the same source list, starts right after the
 *   recovered cursor and ends in S1's end state (INV-40…INV-42, INV-60).
 */
describe('Postgres (class T) · source list · killed around the switch (CT-2)', () => {
  it('after the switch, before the batch writes anything: the store holds the last primary batch', async ({
    simulateFallbackS1,
    database,
  }) => {
    const simulator = await simulateFallbackS1({ failAt: FAIL_AT })
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.beforeWrites,
    })

    expect(killed).toEqual({ statement: 'set', transaction: ['begin', 'lock'] })
    expect(batches).toHaveLength(PRIMARY_BATCHES + 1)
    expect(batches.at(-1)?.current.number).toBe(FIRST_STANDBY_BATCH)
    expect(firstStandbyRead(simulator)?.fromBlock).toBe(FAIL_AT)
    expect(cursorOf(store)?.number).toBe(FAIL_AT - 1)

    await resumeToEnd({ scenario, simulator, database, model, store })

    expect(lastServed(simulator.standby.chain.requests)).toBe(S1.to)
  })

  it('after the switch, after the cursor write: neither the standby rows nor the cursor is visible', async ({
    simulateFallbackS1,
    database,
  }) => {
    const simulator = await simulateFallbackS1({ failAt: FAIL_AT })
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterCursor,
    })

    expect(killed.statement).toBe('commit')
    expect(killed.transaction).toContain('insert sync')
    expect(batches).toHaveLength(PRIMARY_BATCHES + 1)
    expect(cursorOf(store)?.number).toBe(FAIL_AT - 1)

    await resumeToEnd({ scenario, simulator, database, model, store })

    expect(lastServed(simulator.standby.chain.requests)).toBe(S1.to)
  })

  it('after the switch, commit sent: the store holds the standby batch or the one before', async ({
    simulateFallbackS1,
    database,
  }) => {
    const simulator = await simulateFallbackS1({ failAt: FAIL_AT })
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.commitSent,
    })
    const lastTwo = batches.slice(-2).map((batch) => batch.current)

    expect(killed.statement).toBe('commit')
    expect(batches).toHaveLength(PRIMARY_BATCHES + 1)
    // The pipe cannot know whether the server committed; either is a commit point (CN-40).
    expect(lastTwo).toContainEqual(cursorOf(store))

    await resumeToEnd({ scenario, simulator, database, model, store })

    expect(lastServed(simulator.standby.chain.requests)).toBe(S1.to)
  })

  it('after the switch, after the commit: the restart resumes after the standby batch', async ({
    simulateFallbackS1,
    database,
  }) => {
    const simulator = await simulateFallbackS1({ failAt: FAIL_AT })
    const model = scenario.oracle()

    const { store, batches } = await crashAt({ scenario, simulator, database, model, point: POINTS.afterCommit })

    expect(batches).toHaveLength(PRIMARY_BATCHES + 1)
    expect(cursorOf(store)).toStrictEqual(batches.at(-1)?.current)
    expect(cursorOf(store)?.number).toBe(FIRST_STANDBY_BATCH)

    const standbyBefore = simulator.standby.chain.requests.length
    await resumeToEnd({ scenario, simulator, database, model, store })

    // The restart reads the primary first; it still fails, so the standby resumes after the store.
    expect(firstStandbyRead(simulator, standbyBefore)?.fromBlock).toBe(FIRST_STANDBY_BATCH + 1)
    expect(lastServed(simulator.standby.chain.requests)).toBe(S1.to)
  })

  it('before the switch: the restart meets the failing primary and switches itself', async ({
    simulateFallbackS1,
    database,
  }) => {
    const simulator = await simulateFallbackS1({ failAt: FAIL_AT })
    const model = scenario.oracle()

    const { store, batches } = await crashAt({ scenario, simulator, database, model, point: POINTS.beforeSwitch })

    expect(batches).toHaveLength(2)
    expect(cursorOf(store)).toStrictEqual(batches[0].current)
    expect(simulator.standby.chain.requests).toHaveLength(0)

    await resumeToEnd({ scenario, simulator, database, model, store })

    expect(answerEnds(simulator.chain.requests)).toContain(FAIL_AT - 1)
    expect(lastServed(simulator.chain.requests)).toBeLessThan(FAIL_AT)
    expect(firstStandbyRead(simulator)?.fromBlock).toBe(FAIL_AT)
    expect(lastServed(simulator.standby.chain.requests)).toBe(S1.to)
  })
})
