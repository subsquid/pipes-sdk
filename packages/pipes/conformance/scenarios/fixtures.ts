import { it as base } from 'vitest'

import { type S1Simulator, type SimulateOptions, s1Portal } from './backfill.js'
import { type S3Options, type S3Simulator, s3Portal } from './fork.js'
import { type ForkHistory, type ForkHistorySimulator, forkHistoryPortal } from './fork-histories.js'
import { type StormSimulator, forkStormPortal } from './fork-storm.js'

/** The scenarios' portals, each closed after its test. A store's harness adds the store. */
export const it = base.extend<{
  simulateForkHistory: (history: ForkHistory) => Promise<ForkHistorySimulator>
  simulateForkStorm: () => Promise<StormSimulator>
  simulateS1: (options?: SimulateOptions) => Promise<S1Simulator>
  simulateS3: (options?: S3Options) => Promise<S3Simulator>
}>({
  simulateForkStorm: async ({}, use) => {
    const opened: StormSimulator[] = []
    await use(async () => {
      const simulator = await forkStormPortal()
      opened.push(simulator)
      return simulator
    })
    await Promise.all(opened.map(({ chain }) => chain.close()))
  },
  simulateForkHistory: async ({}, use) => {
    const opened: ForkHistorySimulator[] = []
    await use(async (history) => {
      const simulator = await forkHistoryPortal(history)
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.map(({ chain }) => chain.close()))
  },
  simulateS1: async ({}, use) => {
    const opened: S1Simulator[] = []
    await use(async (options) => {
      const simulator = await s1Portal(options)
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.map(({ chain }) => chain.close()))
  },
  simulateS3: async ({}, use) => {
    const opened: S3Simulator[] = []
    await use(async (options) => {
      const simulator = { chain: await s3Portal(options) }
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.map(({ chain }) => chain.close()))
  },
})
