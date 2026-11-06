import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { PubSub } from '@google-cloud/pubsub'
import { describe, expect, onTestFinished, vi } from 'vitest'

import type { ReferenceModel } from '../oracle/reference-model.js'
import { validatePubsubSnapshot } from '../oracle/validators.js'
import type { S1Simulator } from '../scenarios/backfill.js'
import { type ChildRun, type StartChild, startChildFixture } from '../shutdown/spawn.js'
import { type Pubsub, S1_PUBSUB, it as harness, readPubsubStore, resumeToEnd, runToEnd } from './harness.js'
import type { StoppedRunEntry } from './stopped-run.js'

const STOPPED_RUN = fileURLToPath(new URL('./stopped-run.ts', import.meta.url))

const scenario = S1_PUBSUB

// S1 answers seven blocks per response from block 0, so two responses end here.
const SERVED_UP_TO = 13

const it = harness.extend<{ startChild: StartChild }>({ startChild: startChildFixture })

function reachedEndOfServed(entry: StoppedRunEntry) {
  return 'batch' in entry && entry.batch.current.number === SERVED_UP_TO
}

/** The chain stops answering past SERVED_UP_TO until `open` is called. */
function holdPast() {
  let held = true

  return {
    open: () => {
      held = false
    },
    delay: ({ query }: { query: { fromBlock: number } }) => {
      const hold = held && query.fromBlock > SERVED_UP_TO

      return hold ? new Promise<void>(() => {}) : undefined
    },
  }
}

function start(startChild: StartChild, simulator: S1Simulator, pubsub: Pubsub): Promise<ChildRun<StoppedRunEntry>> {
  return startChild<StoppedRunEntry>(STOPPED_RUN, {
    range: scenario.range,
    url: simulator.chain.url,
    bus: { endpoint: pubsub.bus.endpoint, projectId: pubsub.bus.projectId },
    statePath: pubsub.statePath,
    coldStart: !existsSync(pubsub.statePath),
  })
}

/** Feeds the oracle what the child delivered, as `runKilled` does. */
async function followChild(run: ChildRun<StoppedRunEntry>, simulator: S1Simulator, model: ReferenceModel) {
  const follower = scenario.follower(model, simulator, 0)

  for (const entry of await run.journal()) {
    if ('batch' in entry) {
      follower.batch(entry.batch)
    }
  }
}

/**
 * A run that a signal stops, then a restart (LIV-6). A stop leaves less to recover than a crash: the
 * batch in flight is confirmed and the outbox is empty, so the restart sends nothing twice.
 */
describe('Pub/Sub (class C) · S1 backfill · stopped by a signal', () => {
  it.for(['SIGTERM', 'SIGINT'] as const)(
    '%s drains the batch, the outbox is empty, and the restart sends nothing twice',
    async (signal, { simulateS1, pubsub, startChild }) => {
      const hold = holdPast()
      const simulator = await simulateS1({ delay: hold.delay })
      const model = scenario.oracle()
      model.recover(readPubsubStore(pubsub).state)

      const run = await start(startChild, simulator, pubsub)
      await run.entry(reachedEndOfServed)
      run.kill(signal)

      const exit = await run.exit

      // It ends by itself: the target closed its client and its state, and left no handle behind.
      expect(exit.code, exit.output).toBe(0)
      expect(await run.journal()).toContainEqual({ stopped: signal })

      const store = readPubsubStore(pubsub)

      expect(store.state?.current?.number, 'the cursor').toBe(SERVED_UP_TO)
      expect(store.outbox, 'the outbox').toEqual([])
      expect(validatePubsubSnapshot(store, scenario.ranges), 'the structural validators').toEqual([])
      expect(store.rows, 'the rows a consumer of the bus holds').toEqual(scenario.rowsUpTo(store.state?.current))

      await followChild(run, simulator, model)

      hold.open()
      const end = await runToEnd({ scenario, simulator, pubsub, model })

      expect(end.wire, 'nothing went out twice').toHaveLength(end.sequence)
    },
  )

  /**
   * What SIGKILL guarantees: no drain, but a commit point. The restart publishes what the outbox
   * holds and may repeat a message the bus already took, so a consumer applies them by version.
   */
  it.for([
    ['SIGKILL', ['SIGKILL']],
    ['SIGKILL during the drain of a SIGTERM', ['SIGTERM', 'SIGKILL']],
  ] as const)('%s: the restart converges on the end state', async ([, signals], { simulateS1, pubsub, startChild }) => {
    const hold = holdPast()
    const simulator = await simulateS1({ delay: hold.delay })
    const model = scenario.oracle()
    model.recover(readPubsubStore(pubsub).state)

    const run = await start(startChild, simulator, pubsub)
    await run.entry(reachedEndOfServed)

    for (const signal of signals) {
      run.kill(signal)
    }

    const exit = await run.exit

    expect(exit.signal, exit.output).toBe('SIGKILL')

    const store = readPubsubStore(pubsub)

    expect(validatePubsubSnapshot(store, scenario.ranges), 'the store the kill left').toEqual([])
    expect(store.rows, 'the rows the recorded changelog leaves').toEqual(scenario.rowsUpTo(store.state?.current))

    await followChild(run, simulator, model)

    hold.open()
    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })
})

describe('Pub/Sub (class C) · the client', () => {
  it('a client the target built from a config closes with the run', async ({ simulateS1, pubsub }) => {
    const close = vi.spyOn(PubSub.prototype, 'close')
    onTestFinished(() => close.mockRestore())

    const simulator = await simulateS1()

    await runToEnd({ scenario, simulator, pubsub })

    expect(close).toHaveBeenCalled()
  })
})
