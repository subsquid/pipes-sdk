import { it as base } from 'vitest'

import { type S1Simulator, type SimulateOptions, s1Portal } from './backfill.js'
import {
  type FallbackS1Simulator,
  type PortalPairSimulator,
  fallbackS1Portals,
  forkAfterSwitchPortals,
  forkStormPairPortals,
  shallowStandbyPortals,
} from './fallback.js'
import { type S3Options, type S3Simulator, s3Portal } from './fork.js'
import { type ForkHistory, type ForkHistorySimulator, forkHistoryPortal } from './fork-histories.js'
import { type StormSimulator, forkStormPortal } from './fork-storm.js'
import {
  type RandomFaultS1Simulator,
  type RandomFaultS3Simulator,
  randomFaultS1Portals,
  randomFaultS3Portals,
  randomFaults,
} from './random-faults.js'
import { type TipS3Simulator, finalizedPortalThenRpc } from './rpc.js'

/** The scenarios' portals, each closed after its test. A store's harness adds the store. */
export const it = base.extend<{
  /** The portals of S1 or S3 over a source list of three, each failing at random by `seed`. */
  simulateRandomFaultsS1: (seed: number) => Promise<RandomFaultS1Simulator>
  simulateRandomFaultsS3: (seed: number) => Promise<RandomFaultS3Simulator>
  simulateFallbackS1: (options: { failAt: number; recover?: boolean }) => Promise<FallbackS1Simulator>
  simulateFallbackS3: (
    pair: 'fork-after-switch' | 'shallow-standby',
    options?: S3Options,
  ) => Promise<PortalPairSimulator>
  simulateFallbackStorm: (options: { failAt: number }) => Promise<PortalPairSimulator>
  simulateForkHistory: (history: ForkHistory) => Promise<ForkHistorySimulator>
  simulateForkStorm: () => Promise<StormSimulator>
  simulateS1: (options?: SimulateOptions) => Promise<S1Simulator>
  /** S3 over a finalized-only portal and an RPC source over the same chain. */
  simulateTipS3: () => Promise<TipS3Simulator>
  simulateS3: (options?: S3Options) => Promise<S3Simulator>
}>({
  simulateRandomFaultsS1: async ({}, use) => {
    const opened: RandomFaultS1Simulator[] = []
    await use(async (seed) => {
      const simulator = await randomFaultS1Portals(randomFaults({ seed }))
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.flatMap(({ portals }) => portals.map((portal) => portal.close())))
  },
  simulateRandomFaultsS3: async ({}, use) => {
    const opened: RandomFaultS3Simulator[] = []
    await use(async (seed) => {
      const simulator = await randomFaultS3Portals(randomFaults({ seed }))
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.flatMap(({ portals }) => portals.map((portal) => portal.close())))
  },
  simulateFallbackS1: async ({}, use) => {
    const opened: FallbackS1Simulator[] = []
    await use(async (options) => {
      const simulator = await fallbackS1Portals(options)
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.flatMap(({ chain, standby }) => [chain.close(), standby.chain.close()]))
  },
  simulateFallbackS3: async ({}, use) => {
    const opened: PortalPairSimulator[] = []
    await use(async (pair, options) => {
      const simulator =
        pair === 'fork-after-switch' ? await forkAfterSwitchPortals(options) : await shallowStandbyPortals()
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.flatMap(({ primary, standby }) => [primary.close(), standby.close()]))
  },
  simulateFallbackStorm: async ({}, use) => {
    const opened: PortalPairSimulator[] = []
    await use(async (options) => {
      const simulator = await forkStormPairPortals(options)
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.flatMap(({ primary, standby }) => [primary.close(), standby.close()]))
  },
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
  simulateTipS3: async ({}, use) => {
    const opened: TipS3Simulator[] = []
    await use(async () => {
      const simulator = await finalizedPortalThenRpc()
      opened.push(simulator)

      return simulator
    })
    await Promise.all(opened.flatMap(({ portal, rpc }) => [portal.close(), rpc.close()]))
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
