import { describe, expect } from 'vitest'

import { numbers } from '../scenarios/erc20.js'
import {
  RANDOM_FAULT_SOURCES,
  type RandomFaults,
  describeRequests,
  portalsAsked,
  s1RandomFaults,
  s3RandomFaults,
} from '../scenarios/random-faults.js'
import { RPC_AT, s1RandomFaultsWithRpc } from '../scenarios/rpc.js'
import { it, runToEnd } from './harness.js'

const SEEDS = numbers(1, 20)

/** The faults a seed injected, for the log of a failed run: the seed replays them. */
function describeFaults(draws: RandomFaults): string {
  const lines = draws.injected.map(({ portal, fromBlock, fault }) => `portal ${portal} at ${fromBlock}: ${fault}`)

  return `seed ${draws.seed} injected:\n${lines.join('\n')}`
}

/**
 * S1 and S3 over a source list of three portals, where every stream request may fail at random:
 * a 503, a 500, a 429, a stalled answer, a cut body or a malformed line (spec/16). Whatever the
 * draws, `runToEnd` fails unless the store ends in the scenario's single-source end state (INV-60).
 */
describe('Postgres (class T) · source list · random portal faults (CT-4)', () => {
  for (const seed of SEEDS) {
    it(`S1, seed ${seed}: ends as a single-source run`, { timeout: 60_000 }, async ({
      simulateRandomFaultsS1,
      database,
      onTestFailed,
    }) => {
      const simulator = await simulateRandomFaultsS1(seed)
      onTestFailed(() => console.error(`${describeFaults(simulator.faults)}\n${describeRequests(simulator.portals)}`))

      await runToEnd({ scenario: s1RandomFaults('T'), simulator, database })

      expect(simulator.faults.injected.length).toBeGreaterThan(0)
      expect(portalsAsked(simulator.portals)).toBeGreaterThan(1)
      expect(simulator.portals).toHaveLength(RANDOM_FAULT_SOURCES)
    })

    it(`S3, seed ${seed}: ends on the chain the portals end on`, { timeout: 60_000 }, async ({
      simulateRandomFaultsS3,
      database,
      onTestFailed,
    }) => {
      const simulator = await simulateRandomFaultsS3(seed)
      onTestFailed(() => {
        console.error(
          `${describeFaults(simulator.faults)}\n${describeRequests(simulator.portals, simulator.chain.requests)}`,
        )
      })

      await runToEnd({ scenario: s3RandomFaults('T'), simulator, database })

      expect(simulator.faults.injected.length).toBeGreaterThan(0)
      expect(portalsAsked(simulator.portals)).toBeGreaterThan(1)
    })

    it(`S1 with an RPC source, seed ${seed}: ends as a single-source run`, { timeout: 60_000 }, async ({
      simulateRandomFaultsS1,
      database,
      onTestFailed,
    }) => {
      const simulator = await simulateRandomFaultsS1(seed)
      onTestFailed(() => console.error(`${describeFaults(simulator.faults)}\n${describeRequests(simulator.portals)}`))

      await runToEnd({ scenario: s1RandomFaultsWithRpc('T'), simulator, database })

      const rpcReads = simulator.portals[RPC_AT].rpcCalls.filter(({ method }) => method === 'eth_getBlockByNumber')

      expect(simulator.faults.injected.length).toBeGreaterThan(0)
      expect(rpcReads.length).toBeGreaterThan(0)
    })
  }
})
