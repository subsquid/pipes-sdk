import { describe, expect } from 'vitest'

import { S1_PUBSUB, it, runToEnd } from './harness.js'

const scenario = S1_PUBSUB

/**
 * One uninterrupted run of S1 to the bus (CT-1). `runToEnd` fails any run that does not end in S1's
 * end state: a consumer of the bus and the oracle both hold every row of S1, the state holds the
 * cursor at the range end and the last floor reported, the outbox is empty, and the store passes
 * every structural validator. Each test adds the claim in its title.
 */
describe('Pub/Sub (class C) · S1 backfill · one uninterrupted run (CT-1)', () => {
  it('leaves exactly S1 on the bus: every row once, in one unbroken sequence, the state at the range end', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()

    const store = await runToEnd({ scenario, simulator, pubsub })

    // With no crash, nothing goes out twice.
    expect(store.wire).toHaveLength(store.sequence)
  })

  it('a restart with nothing left to read sends no request and publishes nothing (INV-10)', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    // One oracle for both runs, so it can tell the restart recovered a state it committed (CN-40).
    const model = scenario.oracle()

    const first = await runToEnd({ scenario, simulator, pubsub, model })
    const requestsBefore = simulator.chain.requests.length
    const second = await runToEnd({ scenario, simulator, pubsub, model })

    expect(second).toStrictEqual(first)
    expect(simulator.chain.requests).toHaveLength(requestsBefore)
  })
})
