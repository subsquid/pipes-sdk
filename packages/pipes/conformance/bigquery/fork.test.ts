import { describe, expect } from 'vitest'

import { S3 } from '../scenarios/fork.js'
import { STORM, forkStorm } from '../scenarios/fork-storm.js'
import { S3_BIGQUERY, it, runToEnd } from './harness.js'

/** Without a project the harness may write, the class-W suite skips wholesale. */
const RUN = !!process.env['BIGQUERY_TEST_PROJECT']

const scenario = S3_BIGQUERY

/**
 * One uninterrupted run of S3 into BigQuery (CT-1): an unfinalized tail, a floor that rises once,
 * and a fork below the head. `runToEnd` fails any run that does not end in S3's end state, the chain
 * the portal ends on; the oracle takes every block, floor and fork from the portal's log.
 */
describe.skipIf(!RUN)('BigQuery (class W) · S3 one fork · one uninterrupted run (CT-1)', () => {
  it('rolls the forked blocks back and ends on the chain the portal ends on', { timeout: 300_000 }, async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()

    await runToEnd({ scenario, simulator, database })

    const requests = simulator.chain.requests
    const forks = requests.filter((request) => request.answer?.kind === 'fork')
    const afterFork = requests.at(requests.indexOf(forks[0]) + 1)?.query

    expect(forks).toHaveLength(1)
    expect(afterFork?.fromBlock).toBe(S3.ancestor + 1)
  })
})

describe.skipIf(!process.env['BIGQUERY_TEST_PROJECT'])('bigquery · repeated forks (CT-3)', () => {
  it('replaces three branches, rolls back to finality and leaves no superseded rows', { timeout: 300_000 }, async ({
    simulateForkStorm,
    database,
  }) => {
    const simulator = await simulateForkStorm()
    await runToEnd({ scenario: forkStorm('W'), simulator, database })
    const requests = simulator.chain.requests
    const forks = requests.filter((request) => request.answer?.kind === 'fork')
    expect(forks).toHaveLength(STORM.ancestors.length)
    expect(forks.map((request) => requests[requests.indexOf(request) + 1]?.query.fromBlock)).toEqual(
      STORM.ancestors.map((number) => number + 1),
    )
  })
})
