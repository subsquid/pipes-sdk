import { describe, expect } from 'vitest'

import { S1_CLICKHOUSE, it, runToEnd } from './harness.js'

const scenario = S1_CLICKHOUSE

/**
 * One uninterrupted run of S1 into ClickHouse (CT-1). `runToEnd` fails any run that does not end in
 * S1's end state: the store and the oracle both hold every row of S1, the cursor at the range end
 * and the last floor reported, and the store passes every structural validator. Each test adds the
 * claim in its title.
 */
describe('ClickHouse (class A) · S1 backfill · one uninterrupted run (CT-1)', () => {
  it('leaves exactly S1 in the store: every row, the cursor at the range end, the last floor', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()

    // The claim is the end state itself, which `runToEnd` checks.
    await runToEnd({ scenario, simulator, database })
  })

  it('a restart with nothing left to read sends no request and changes nothing (INV-10)', async ({
    simulateS1,
    database,
  }) => {
    const simulator = await simulateS1()
    // One oracle for both runs, so it can tell the restart recovered a state it committed (CN-40).
    const model = scenario.oracle()

    const first = await runToEnd({ scenario, simulator, database, model })
    const requestsBefore = simulator.chain.requests.length
    const second = await runToEnd({ scenario, simulator, database, model })

    expect(second).toStrictEqual(first)
    expect(simulator.chain.requests).toHaveLength(requestsBefore)
  })
})
