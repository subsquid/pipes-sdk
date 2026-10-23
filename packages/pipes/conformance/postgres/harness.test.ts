import { setTimeout as sleep } from 'node:timers/promises'

import pg from 'pg'
import { describe, expect } from 'vitest'

import { S1_POSTGRES, disconnected, it, runKilled } from './harness.js'
import type { StatementKillPoint } from './kill-switch.js'

const NEVER: StatementKillPoint = { name: 'never', statement: '^commit$', at: 1_000, when: 'before' }

describe('runKilled', () => {
  it('refuses a run that ends before its kill point fires', async ({ simulateS1, database }) => {
    const simulator = await simulateS1()

    const run = runKilled({
      scenario: S1_POSTGRES,
      simulator,
      database,
      model: S1_POSTGRES.oracle(),
      point: NEVER,
    })

    await expect(run).rejects.toThrow("the run ended before the kill point 'never' fired")
  })

  it('refuses a run that fails before its kill point fires', async ({ simulateS1, database }) => {
    const simulator = await simulateS1()
    // The undo-log set-up cannot put a trigger on a table that is not there.
    await database.admin.query(`DROP TABLE "${database.schema}"."transfers"`)

    const run = runKilled({
      scenario: S1_POSTGRES,
      simulator,
      database,
      model: S1_POSTGRES.oracle(),
      point: NEVER,
    })

    await expect(run).rejects.toThrow(/the run failed \(exit 1, signal null\) before the kill point 'never' fired/)
  })
})

describe('disconnected', () => {
  it('waits while the server holds a connection of the application', async ({ database }) => {
    const application = 'ct-disconnected'
    const client = new pg.Client({ connectionString: database.dsn, application_name: application })
    await client.connect()

    let done = false
    const waiting = disconnected(database.admin, application).then(() => {
      done = true
    })
    await sleep(200)
    const doneWhileOpen = done
    await client.end()
    await waiting

    expect(doneWhileOpen).toBe(false)
    expect(done).toBe(true)
  })
})
