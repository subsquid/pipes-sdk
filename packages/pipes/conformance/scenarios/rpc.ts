import type { EvmFallbackOptions } from '~/evm/evm-fallback.js'
import type { MockChainPortal } from '~/testing/index.js'

import { FALLBACK_OPTIONS, type FallbackS1Simulator } from './fallback.js'
import { highestAsked, s3ChainFollower, s3World } from './fork.js'
import { type FullStreamClass, type FullStreamScenario, s1FullStream, s3FullStream } from './full-stream.js'
import { RANDOM_FAULT_OPTIONS, type RandomFaultS1Simulator, faultyPortal, faultyRpc } from './random-faults.js'

/**
 * Source lists with a JSON-RPC source (spec/16). The RPC source reads a mock portal's JSON-RPC face
 * (`rpcUrl`), an EVM node over the same chain. It finds a fork itself and answers no 409, so the S3
 * oracle follows the chain, not a request log (`s3ChainFollower`). This module holds nothing of
 * vitest, so a child process can run it.
 */

/** S1 over a portal primary that fails at `failAt` and an RPC standby (`fallbackS1Portals`). */
export function s1PortalThenRpc(durability: FullStreamClass): FullStreamScenario<FallbackS1Simulator> {
  return {
    ...s1FullStream(durability),
    source: (simulator) => ({
      source: [simulator.chain.url, { type: 'rpc', url: simulator.standby.chain.rpcUrl }],
      fallback: FALLBACK_OPTIONS,
    }),
  }
}

/** The blocks the RPC face of `portal` was asked for by number, in order. */
export function rpcBlocksAsked(portal: MockChainPortal): number[] {
  return portal.rpcCalls.flatMap((call) => {
    const asked = highestAsked([call])

    return call.method === 'eth_getBlockByNumber' && asked >= 0 ? [asked] : []
  })
}

/**
 * S3 over "cheap bulk, then follow the tip" (spec/16): a finalized-only portal first, an RPC source
 * second, over one chain (`s3World`). The portal stops at its floor and answers at-head; the pipe
 * fails over to the RPC source once the portal is stale, follows the tip there, and meets the fork
 * there. The stream restarted after the fork must not go back to the exhausted portal.
 */
export type TipS3Simulator = {
  /** The portal's stream log, for the harness; the oracle follows the chain. */
  chain: { url: string; requests: MockChainPortal['requests'] }
  portal: MockChainPortal
  rpc: MockChainPortal
  reported: number[]
  /** Forks the pipe resolved, as the oracle saw them. */
  forks: number
}

export async function finalizedPortalThenRpc(): Promise<TipS3Simulator> {
  const world = s3World()
  const portal = await world.portal()
  const rpc = await world.portal()

  return { chain: { url: portal.url, requests: portal.requests }, portal, rpc, reported: world.reported, forks: 0 }
}

/**
 * No transport retries and no probes, as in the other source lists. A short staleness window, so
 * the exhausted portal is left fast on the real clock, and a short cooldown, so it is selectable
 * again well before the fork: the restart must pass it over because it is behind, not because it is
 * cooling down.
 */
export const TIP_OPTIONS: EvmFallbackOptions = {
  sourceRetries: 0,
  detection: {
    capabilityProbe: false,
    cooldownMs: 20,
    maxStalenessMs: 500,
    freshnessTickMs: 20,
    headTtlMs: 0,
    maxLagBlocks: null,
  },
}

export function s3FinalizedThenRpc(durability: FullStreamClass): FullStreamScenario<TipS3Simulator> {
  return {
    ...s3FullStream<TipS3Simulator>(durability),
    follower: (model, simulator) => {
      const follow = s3ChainFollower(model, () => simulator.reported)

      return {
        batch: follow.batch,
        fork: (canonical) => {
          simulator.forks++
          follow.fork(canonical)
        },
      }
    },
    source: (simulator) => ({
      source: [
        { url: simulator.portal.url, finalized: true },
        { type: 'rpc', url: simulator.rpc.rpcUrl },
      ],
      fallback: TIP_OPTIONS,
    }),
  }
}

/** The highest block the portal was asked for in a stream request. */
export function highestStreamed(portal: MockChainPortal): number {
  return Math.max(-1, ...portal.requests.map(({ query }) => query.fromBlock))
}

/**
 * The RPC source's place in the random-fault list: second, so the primary's first fault always
 * hands the read to it.
 */
export const RPC_AT = 1

/**
 * S1 over two portals and the RPC face of a third (`randomFaultS1Portals`), every request failing at
 * random: the RPC source gets an error status, a held answer, half a body or a malformed body.
 */
export function s1RandomFaultsWithRpc(durability: FullStreamClass): FullStreamScenario<RandomFaultS1Simulator> {
  return {
    ...s1FullStream(durability),
    source: (simulator) => ({
      source: simulator.portals.map((portal, index) =>
        index === RPC_AT ? faultyRpc(portal.rpcUrl) : faultyPortal(portal.url),
      ),
      fallback: RANDOM_FAULT_OPTIONS,
    }),
  }
}

/** The tip run for the log of a failed run: the portal's stream requests, and the forks resolved. */
export function describeTip(simulator: TipS3Simulator): string {
  const requests = simulator.portal.requests.map(
    ({ endpoint, query, answer }) => `portal ${endpoint} from ${query.fromBlock}: ${answer?.kind ?? 'unanswered'}`,
  )

  return [`forks resolved: ${simulator.forks}`, ...requests].join('\n')
}
