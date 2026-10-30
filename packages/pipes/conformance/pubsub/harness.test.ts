import { writeFileSync } from 'node:fs'

import { describe, expect } from 'vitest'

import { S1_PUBSUB, it, runKilled } from './harness.js'
import type { StepKillPoint } from './kill-switch.js'

const NEVER: StepKillPoint = { name: 'never', step: '^commit$', at: 1_000, when: 'before' }

describe('runKilled', () => {
  it('refuses a run that ends before its kill point fires', async ({ simulateS1, pubsub }) => {
    const simulator = await simulateS1()

    const run = runKilled({ scenario: S1_PUBSUB, simulator, pubsub, model: S1_PUBSUB.oracle(), point: NEVER })

    await expect(run).rejects.toThrow("the run ended before the kill point 'never' fired")
  })

  it('refuses a run that fails before its kill point fires', async ({ simulateS1, pubsub }) => {
    const simulator = await simulateS1()
    // A state file that is there but empty: the run declares no bootstrap, and the target refuses a cold start.
    writeFileSync(pubsub.statePath, '')

    const run = runKilled({ scenario: S1_PUBSUB, simulator, pubsub, model: S1_PUBSUB.oracle(), point: NEVER })

    await expect(run).rejects.toThrow(/the run failed \(exit 1, signal null\) before the kill point 'never' fired/)
  })
})
