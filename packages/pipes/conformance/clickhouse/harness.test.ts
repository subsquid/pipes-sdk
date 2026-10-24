import { setTimeout as sleep } from 'node:timers/promises'

import { describe, expect } from 'vitest'

import { S1_CLICKHOUSE, it, quiet, runKilled } from './harness.js'
import type { RequestKillPoint } from './kill-switch.js'
import { clickhouseClient } from './pipe.js'

const NEVER: RequestKillPoint = { name: 'never', request: '^insert sync$', at: 1_000, when: 'before' }

describe('runKilled', () => {
  it('refuses a run that ends before its kill point fires', async ({ simulateS1, database }) => {
    const simulator = await simulateS1()

    const run = runKilled({
      scenario: S1_CLICKHOUSE,
      simulator,
      database,
      model: S1_CLICKHOUSE.oracle(),
      point: NEVER,
    })

    await expect(run).rejects.toThrow("the run ended before the kill point 'never' fired")
  })

  it('refuses a run that fails before its kill point fires', async ({ simulateS1, database }) => {
    const simulator = await simulateS1()
    // The pipe inserts its rows into a table that is not there.
    await database.admin.command({ query: `DROP TABLE "${database.name}"."transfers"` })

    const run = runKilled({
      scenario: S1_CLICKHOUSE,
      simulator,
      database,
      model: S1_CLICKHOUSE.oracle(),
      point: NEVER,
    })

    await expect(run).rejects.toThrow(/the run failed \(exit 1, signal null\) before the kill point 'never' fired/)
  })
})

describe('quiet', () => {
  it('waits while the server runs a query of the application', async ({ database }) => {
    const application = 'ct-quiet'
    const client = clickhouseClient({ server: database.server, database: database.name, application })

    const query = client.command({ query: 'SELECT sleep(1)' })
    // Long enough for the server to start the query.
    await sleep(200)
    let done = false
    const waiting = quiet(database.admin, application).then(() => {
      done = true
    })
    await sleep(200)
    const doneWhileRunning = done
    await query
    await waiting
    await client.close()

    expect(doneWhileRunning).toBe(false)
    expect(done).toBe(true)
  })
})
