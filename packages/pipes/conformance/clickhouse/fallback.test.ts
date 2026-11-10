import { describe, expect } from 'vitest'

import type { MockPortalRequest } from '~/testing/index.js'

import { S1 } from '../scenarios/backfill.js'
import {
  RECLAIM_OPTIONS,
  SHALLOW_FAIL_AT,
  answerEnds,
  dataRequests,
  forkStormFallback,
  lastServed,
  resumeProblem,
  s1Fallback,
  s3Fallback,
} from '../scenarios/fallback.js'
import { S3, hashAt } from '../scenarios/fork.js'
import { STORM } from '../scenarios/fork-storm.js'
import { describeTip, highestStreamed, rpcBlocksAsked, s1PortalThenRpc, s3FinalizedThenRpc } from '../scenarios/rpc.js'
import { it, runToEnd } from './harness.js'

/**
 * A pipe that reads a source list of two portals over the same chain (spec/16), one uninterrupted
 * run into ClickHouse (CT-1). The primary fails part-way and the standby serves the rest. `runToEnd`
 * fails unless the store ends in the scenario's single-source end state, so a sink cannot tell the
 * switch happened (INV-60).
 */
describe('ClickHouse (class A) · source list · switch to the standby (CT-1)', () => {
  it('S1: ends as a single-source run, and the standby resumes right after the primary (INV-60, INV-61)', async ({
    simulateFallbackS1,
    database,
  }) => {
    const failAt = 28
    const simulator = await simulateFallbackS1({ failAt })

    await runToEnd({ scenario: s1Fallback('A'), simulator, database })

    const first = dataRequests(simulator.standby.chain.requests)[0]?.query

    expect(lastServed(simulator.chain.requests)).toBeLessThan(failAt)
    expect(resumeProblem(first, simulator.chain.requests)).toBeUndefined()
    expect(lastServed(simulator.standby.chain.requests)).toBe(S1.to)
  })

  it('S1: the primary, back after one failed request, resumes right after the standby (INV-61, LIV-61)', async ({
    simulateFallbackS1,
    database,
  }) => {
    const failAt = 14
    const simulator = await simulateFallbackS1({ failAt, recover: true })

    await runToEnd({ scenario: s1Fallback('A', RECLAIM_OPTIONS), simulator, database })

    const reclaimed = dataRequests(simulator.chain.requests).find(({ query }) => query.fromBlock > failAt)?.query

    expect(lastServed(simulator.standby.chain.requests)).toBeGreaterThanOrEqual(failAt)
    expect(resumeProblem(reclaimed, simulator.standby.chain.requests)).toBeUndefined()
    expect(lastServed(simulator.chain.requests)).toBe(S1.to)
  })

  it('S3: a fork the standby signals after the switch rolls back as on a single source (INV-62)', async ({
    simulateFallbackS3,
    database,
  }) => {
    const simulator = await simulateFallbackS3('fork-after-switch')

    await runToEnd({ scenario: s3Fallback('A', { fork: true }), simulator, database })

    const standby = simulator.standby.requests
    const forks = forksIn(standby)
    const afterFork = standby.at(standby.indexOf(forks[0]) + 1)?.query

    expect(lastServed(simulator.primary.requests)).toBeLessThan(S3.raiseAt)
    expect(forks).toHaveLength(1)
    expect(afterFork?.fromBlock).toBe(S3.ancestor + 1)
    expect(afterFork?.parentBlockHash).toBe(hashAt(S3.ancestor, 0))
  })

  it('S3 without a fork: a standby with a shallower floor does not lower the floor the pipe holds (INV-64)', async ({
    simulateFallbackS3,
    database,
  }) => {
    const simulator = await simulateFallbackS3('shallow-standby')

    // The end state holds the primary's raised floor; the standby never reports it.
    await runToEnd({ scenario: s3Fallback('A', { fork: false }), simulator, database })

    const standbyFloors = dataRequests(simulator.standby.requests).map(({ head }) => head?.finalized?.number)

    expect(lastServed(simulator.primary.requests)).toBe(SHALLOW_FAIL_AT - 1)
    expect(standbyFloors.length).toBeGreaterThan(0)
    expect(standbyFloors.every((floor) => floor === S3.finalized)).toBe(true)
  })
})

/** The fork answers in a portal's log. */
function forksIn(requests: readonly MockPortalRequest[]) {
  return requests.filter(({ answer }) => answer?.kind === 'fork')
}

/**
 * The fork storm over a source list (CT-3, spec/16): three replacements, each deeper than the one
 * before, with the switch to the standby inside the storm. `runToEnd` fails unless the store ends
 * on the last branch with no superseded row, as after a storm on one portal.
 */
describe('ClickHouse (class A) · source list · fork storm across the switch (CT-3)', () => {
  it('the primary fails at the first fork: the standby signals every fork', async ({
    simulateFallbackStorm,
    database,
  }) => {
    const failAt = STORM.heads[0] + 1
    const simulator = await simulateFallbackStorm({ failAt })

    await runToEnd({ scenario: forkStormFallback('A'), simulator, database })

    expect(lastServed(simulator.primary.requests)).toBeLessThan(failAt)
    expect(forksIn(simulator.primary.requests)).toHaveLength(0)
    expect(forksIn(simulator.standby.requests)).toHaveLength(STORM.heads.length)
  })

  it('the primary fails after the first fork: the standby catches up two branches at once', async ({
    simulateFallbackStorm,
    database,
  }) => {
    const simulator = await simulateFallbackStorm({ failAt: STORM.heads[1] - 1 })

    await runToEnd({ scenario: forkStormFallback('A'), simulator, database })

    expect(forksIn(simulator.primary.requests)).toHaveLength(1)
    expect(forksIn(simulator.standby.requests)).toHaveLength(STORM.heads.length - 1)
  })
})

/**
 * S3 forks of every depth the single-portal matrix plays, signalled by the standby after the switch
 * (CT-3, INV-62). The default depth and response size are the fork-after-switch test above.
 */
describe('ClickHouse (class A) · source list · fork depth after the switch (CT-3)', () => {
  for (const ancestor of [S3.latest - 1, S3.ancestor, S3.raisedTo]) {
    for (const blocksPerResponse of [1, S3.blocksPerResponse]) {
      if (ancestor === S3.ancestor && blocksPerResponse === S3.blocksPerResponse) {
        continue
      }

      it(`rolls back ${S3.latest - ancestor} blocks with responses of ${blocksPerResponse} blocks`, {
        timeout: 30_000,
      }, async ({ simulateFallbackS3, database }) => {
        const options = { ancestor, blocksPerResponse }
        const simulator = await simulateFallbackS3('fork-after-switch', options)

        await runToEnd({ scenario: s3Fallback('A', { fork: true, ...options }), simulator, database })

        const standby = simulator.standby.requests
        const forks = forksIn(standby)
        const afterFork = standby.at(standby.indexOf(forks[0]) + 1)?.query

        expect(lastServed(simulator.primary.requests)).toBeLessThan(S3.raiseAt)
        expect(forks).toHaveLength(1)
        expect(afterFork?.fromBlock).toBe(ancestor + 1)
        expect(afterFork?.parentBlockHash).toBe(hashAt(ancestor, 0))
      })
    }
  }
})

/**
 * A source list with a JSON-RPC source (spec/16). The RPC source reads a mock portal's JSON-RPC face
 * over the same chain, and finds a fork itself.
 */
describe('ClickHouse (class A) · portal and RPC source list (CT-1)', () => {
  it('S1: the RPC standby serves the rest after the portal fails, and ends as a portal run', async ({
    simulateFallbackS1,
    database,
  }) => {
    const failAt = 28
    const simulator = await simulateFallbackS1({ failAt })

    await runToEnd({ scenario: s1PortalThenRpc('A'), simulator, database })

    const asked = rpcBlocksAsked(simulator.standby.chain)

    expect(lastServed(simulator.chain.requests)).toBeLessThan(failAt)
    expect(answerEnds(simulator.chain.requests)).toContain(Math.min(...asked) - 1)
    expect(Math.max(...asked)).toBe(S1.to)
    expect(simulator.standby.chain.requests).toHaveLength(0)
  })

  it('S3: cheap bulk, then the tip: a fork on the RPC source never sends the pipe back to the exhausted portal', {
    timeout: 30_000,
  }, async ({ simulateTipS3, database, onTestFailed }) => {
    const simulator = await simulateTipS3()
    onTestFailed(() => console.error(describeTip(simulator)))

    await runToEnd({ scenario: s3FinalizedThenRpc('A'), simulator, database })

    // The portal is asked once past its floor, where it answers at-head; a restart that selected it
    // again would ask it from inside the forked range.
    expect(highestStreamed(simulator.portal)).toBe(S3.finalized + 1)
    // The RPC fork signal carries the canonical window below the fork, so one fork finds the ancestor.
    expect(simulator.forks).toBe(1)
    expect(Math.max(...rpcBlocksAsked(simulator.rpc))).toBe(S3.to)
  })
})
