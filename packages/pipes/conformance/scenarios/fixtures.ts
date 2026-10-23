import { it as base } from 'vitest'

import { type S1Simulator, type SimulateOptions, s1Portal } from './backfill.js'
import { type S3Simulator, s3Portal } from './fork.js'

/** The scenarios' portals, each closed after its test. A store's harness adds the store. */
export const it = base.extend<{
  simulateS1: (options?: SimulateOptions) => Promise<S1Simulator>
  simulateS3: () => Promise<S3Simulator>
}>({
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
    await use(async () => {
      const simulator = { chain: await s3Portal() }
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.map(({ chain }) => chain.close()))
  },
})
