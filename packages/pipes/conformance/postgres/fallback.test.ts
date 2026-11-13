import { describe, expect } from 'vitest'

import type { MockPortalRequest } from '~/testing/index.js'

import { S1, cursorAt } from '../scenarios/backfill.js'
import { RECLAIM_OPTIONS, SHALLOW_FAIL_AT, s1Fallback, s3Fallback } from '../scenarios/fallback.js'
import { S3, hashAt } from '../scenarios/fork.js'
import { it, runToEnd } from './harness.js'

/** The highest block a portal sent in a data answer. */
function lastServed(requests: readonly MockPortalRequest[]): number {
  return Math.max(-1, ...answerEnds(requests))
}

/** The last block of each data answer: the cursor the pipe holds once it delivers that batch. */
function answerEnds(requests: readonly MockPortalRequest[]): number[] {
  return requests.flatMap(({ answer }) => (answer?.kind === 'blocks' ? [answer.blocks.at(-1)!.header.number] : []))
}

function dataRequests(requests: readonly MockPortalRequest[]) {
  return requests.filter(({ answer }) => answer?.kind === 'blocks')
}

/**
 * The client fetches the next batch ahead, so the last block a source sent need not be the last
 * one the pipe delivered from it. A resume is right when it starts after the end of a batch the
 * previous source served, anchored on that block.
 */
function expectResumeAfter(query: MockPortalRequest['query'] | undefined, previous: readonly MockPortalRequest[]) {
  expect(query, 'the resumed read').toBeDefined()

  const resumedAfter = query!.fromBlock - 1

  expect(answerEnds(previous)).toContain(resumedAfter)
  expect(query!.parentBlockHash).toBe(cursorAt(resumedAfter).hash)
}

/**
 * A pipe that reads a source list of two portals over the same chain (spec/16), one uninterrupted
 * run into Postgres (CT-1). The primary fails part-way and the standby serves the rest. `runToEnd`
 * fails unless the store ends in the scenario's single-source end state, so a sink cannot tell the
 * switch happened (INV-60).
 */
describe('Postgres (class T) · source list · switch to the standby (CT-1)', () => {
  it('S1: ends as a single-source run, and the standby resumes right after the primary (INV-60, INV-61)', async ({
    simulateFallbackS1,
    database,
  }) => {
    const failAt = 28
    const simulator = await simulateFallbackS1({ failAt })

    await runToEnd({ scenario: s1Fallback('T'), simulator, database })

    const first = dataRequests(simulator.standby.chain.requests)[0]?.query

    expect(lastServed(simulator.chain.requests)).toBeLessThan(failAt)
    expectResumeAfter(first, simulator.chain.requests)
    expect(lastServed(simulator.standby.chain.requests)).toBe(S1.to)
  })

  it('S1: the primary, back after one failed request, resumes right after the standby (INV-61, LIV-61)', async ({
    simulateFallbackS1,
    database,
  }) => {
    const failAt = 14
    const simulator = await simulateFallbackS1({ failAt, recover: true })

    await runToEnd({ scenario: s1Fallback('T', RECLAIM_OPTIONS), simulator, database })

    const reclaimed = dataRequests(simulator.chain.requests).find(({ query }) => query.fromBlock > failAt)?.query

    expect(lastServed(simulator.standby.chain.requests)).toBeGreaterThanOrEqual(failAt)
    expectResumeAfter(reclaimed, simulator.standby.chain.requests)
    expect(lastServed(simulator.chain.requests)).toBe(S1.to)
  })

  it('S3: a fork the standby signals after the switch rolls back as on a single source (INV-62)', async ({
    simulateFallbackS3,
    database,
  }) => {
    const simulator = await simulateFallbackS3('fork-after-switch')

    await runToEnd({ scenario: s3Fallback('T', { fork: true }), simulator, database })

    const standby = simulator.standby.requests
    const forks = standby.filter(({ answer }) => answer?.kind === 'fork')
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
    await runToEnd({ scenario: s3Fallback('T', { fork: false }), simulator, database })

    const standbyFloors = dataRequests(simulator.standby.requests).map(({ head }) => head?.finalized?.number)

    expect(lastServed(simulator.primary.requests)).toBe(SHALLOW_FAIL_AT - 1)
    expect(standbyFloors.length).toBeGreaterThan(0)
    expect(standbyFloors.every((floor) => floor === S3.finalized)).toBe(true)
  })
})
