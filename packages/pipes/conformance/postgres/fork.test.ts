import { describe, expect } from 'vitest'

import { S3 } from '../scenarios/fork.js'
import { S3_POSTGRES, it, runToEnd } from './harness.js'

const scenario = S3_POSTGRES

/**
 * One uninterrupted run of S3 into Postgres (CT-1): an unfinalized tail, a floor that rises once,
 * and a fork below the head. `runToEnd` fails any run that does not end in S3's end state, the
 * chain the portal ends on; the oracle takes every block, floor and fork from the portal's log.
 */
describe('Postgres (class T) · S3 one fork · one uninterrupted run (CT-1)', () => {
  it('rolls the forked blocks back and ends on the chain the portal ends on', async ({ simulateS3, database }) => {
    const simulator = await simulateS3()

    await runToEnd({ scenario, simulator, database })

    const requests = simulator.chain.requests
    const forks = requests.filter((request) => request.answer?.kind === 'fork')
    const afterFork = requests.at(requests.indexOf(forks[0]) + 1)?.query

    expect(forks).toHaveLength(1)
    expect(afterFork?.fromBlock).toBe(S3.ancestor + 1)
  })
})
