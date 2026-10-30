import { describe, expect } from 'vitest'

import { S3 } from '../scenarios/fork.js'
import { S3_PUBSUB, it, runToEnd } from './harness.js'

const scenario = S3_PUBSUB

/**
 * One uninterrupted run of S3 to the bus (CT-1): an unfinalized tail, a floor that rises once, and a
 * fork below the head. The bus cannot take a message back, so the sink compensates (CN-35):
 * `runToEnd` fails any run whose consumer does not end on the chain the portal ends on.
 */
describe('Pub/Sub (class C) · S3 one fork · one uninterrupted run (CT-1)', () => {
  it('compensates the forked blocks and ends on the chain the portal ends on', async ({ simulateS3, pubsub }) => {
    const simulator = await simulateS3()

    const store = await runToEnd({ scenario, simulator, pubsub })

    const requests = simulator.chain.requests
    const forks = requests.filter((request) => request.answer?.kind === 'fork')
    const afterFork = requests.at(requests.indexOf(forks[0]) + 1)?.query
    const repairs = store.wire.slice(store.wire.findIndex((operation) => operation.op === 'delete'))
    const deleted = repairs.filter((operation) => operation.op === 'delete').map(({ id }) => id)
    const firstRewrite = repairs.findIndex((operation) => operation.op === 'upsert')

    expect(forks).toHaveLength(1)
    expect(afterFork?.fromBlock).toBe(S3.ancestor + 1)
    // Each row the run published above the ancestor is deleted once, in no order the spec fixes.
    expect(deleted.sort()).toEqual(['approvals:28', 'transfers:27', 'transfers:28', 'transfers:29'])
    // Compensations go out before the source re-streams the canonical blocks (CN-35).
    expect(firstRewrite).toBe(deleted.length)
  })
})
