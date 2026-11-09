import { describe, expect } from 'vitest'

import { S1_BIGQUERY, it, runToEnd } from './harness.js'

/** Without a project the harness may write, the class-W suite skips wholesale. */
const RUN = !!process.env['BIGQUERY_TEST_PROJECT']

const scenario = S1_BIGQUERY

/**
 * An uninterrupted run and a no-op restart of S1 into BigQuery (CT-1). `runToEnd` fails any run that does not end in
 * S1's end state: the store and the oracle both hold every row of S1, the cursor at the range end
 * and the last floor, no WAL row in flight, and the store passes every structural validator.
 */
describe.skipIf(!RUN)('BigQuery (class W) · S1 backfill · uninterrupted run and no-op restart (CT-1)', () => {
  it('a restart with nothing left to read sends no request and changes nothing (INV-10)', { timeout: 300_000 }, async ({
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
