import { setTimeout as sleep } from 'node:timers/promises'

import type { EvmFallbackOptions, EvmSourceSpec } from '~/evm/evm-fallback.js'
import type { MockChainPortal, MockPortalRequest } from '~/testing/index.js'

import { type S1Simulator, s1Portal } from './backfill.js'
import { numbers } from './erc20.js'
import { highestAsked, s3ChainFollower, s3World } from './fork.js'
import { type FullStreamClass, type FullStreamScenario, s1FullStream, s3FullStream } from './full-stream.js'

/**
 * Random faults across a source list (spec/16): every stream request of every portal may fail, at
 * random, in one of the ways a real portal fails. The run must still end in the scenario's end
 * state. A seed fixes the draws, so a failing seed replays the same faults. This module holds
 * nothing of vitest, so a child process can run it.
 */

/** The faults a draw picks from, by name. `k` is how many whole blocks the answer sends first. */
const FAULTS = {
  unavailable: () => ({ status: 503 }),
  internal: () => ({ status: 500 }),
  throttled: () => ({ status: 429, retryAfter: 0 }),
  stall: (k: number) => ({ stallAfter: k }),
  cut: (k: number) => ({ cutAfter: k }),
  malformed: (k: number) => ({ lineAfter: k, line: '{"header":' }),
} as const

type FaultName = keyof typeof FAULTS

export type InjectedFault = { portal: number; fromBlock: number; fault: FaultName }

/** The draws of one run, and every fault they injected. */
export type RandomFaults = {
  seed: number
  injected: InjectedFault[]
  /** What portal `index` runs on each stream request: a short random delay, and maybe a fault. */
  onRequest(index: number, portal: MockChainPortal, query: { fromBlock: number }): Promise<void>
}

/** mulberry32: a small seeded generator, enough to replay a run's draws. */
function seededRandom(seed: number): () => number {
  let state = seed >>> 0

  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)

    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

/**
 * The draws for one run. Each portal fails a request with probability `rate`, at most `budget`
 * times, so the run ends whatever the seed: once every budget is spent, the portals only answer.
 * The primary always fails its first request, so every run reads more than one portal.
 */
export function randomFaults({
  seed,
  rate = 0.35,
  budget = 6,
}: {
  seed: number
  rate?: number
  budget?: number
}): RandomFaults {
  const random = seededRandom(seed)
  const names = Object.keys(FAULTS) as FaultName[]
  const injected: InjectedFault[] = []
  const spent = new Map<number, number>()

  return {
    seed,
    injected,
    async onRequest(index, portal, query) {
      const latencyMs = Math.floor(random() * 20)
      const primaryFirst = index === 0 && !spent.has(0)
      const fails = primaryFirst || (random() < rate && (spent.get(index) ?? 0) < budget)
      const name = names[Math.floor(random() * names.length)]
      const sentFirst = Math.floor(random() * 3)

      if (fails) {
        spent.set(index, (spent.get(index) ?? 0) + 1)
        injected.push({ portal: index, fromBlock: query.fromBlock, fault: name })
        portal.fail(FAULTS[name](sentFirst))
      }

      await sleep(latencyMs)
    },
  }
}

export const RANDOM_FAULT_SOURCES = 3

/**
 * No transport retries, so every fault moves the read; a short cooldown, so a failed portal is
 * back soon; a short staleness window, so a stalled answer fails over fast. The fallback runs on the
 * real clock: the faults are real requests.
 */
export const RANDOM_FAULT_OPTIONS: EvmFallbackOptions = {
  sourceRetries: 0,
  detection: {
    capabilityProbe: false,
    cooldownMs: 20,
    allDownPollMs: 10,
    maxStalenessMs: 300,
    freshnessTickMs: 20,
    headTtlMs: 0,
    maxLagBlocks: null,
  },
}

/**
 * How long a source may send nothing before its client gives up on the answer and asks again, as a
 * user who sets the timeout. A stall after the last block of the range needs it: every block is in,
 * no other source is ahead, so the fallback holds (a chain stall), and the default body timeout is
 * 150 s.
 */
export const READ_TIMEOUT_MS = 1_000

export function faultyPortal(url: string): EvmSourceSpec {
  return { url, http: { bodyTimeout: READ_TIMEOUT_MS } }
}

export function faultyRpc(url: string): EvmSourceSpec {
  return { type: 'rpc', url, requestTimeout: READ_TIMEOUT_MS }
}

/**
 * S1 over three portals of the same chain, each with its own faults, on its stream route and on its
 * JSON-RPC route alike. `chain` is the first one.
 */
export type RandomFaultS1Simulator = S1Simulator & { portals: MockChainPortal[]; faults: RandomFaults }

export async function randomFaultS1Portals(faults: RandomFaults): Promise<RandomFaultS1Simulator> {
  const simulators: S1Simulator[] = []
  for (const index of numbers(0, RANDOM_FAULT_SOURCES - 1)) {
    const simulator: S1Simulator = await s1Portal({
      delay: ({ query }) => faults.onRequest(index, simulator.chain, query),
      // A portal read over JSON-RPC fails the same way, through the same fault queue.
      rpcDelay: (calls) => faults.onRequest(index, simulator.chain, { fromBlock: highestAsked(calls) }),
    })
    simulators.push(simulator)
  }

  return { ...simulators[0], portals: simulators.map(({ chain }) => chain), faults }
}

/** S1 as a full-stream class over the three portals. */
export function s1RandomFaults(durability: FullStreamClass): FullStreamScenario<RandomFaultS1Simulator> {
  return {
    ...s1FullStream(durability),
    source: (simulator) => ({
      source: simulator.portals.map(({ url }) => faultyPortal(url)),
      fallback: RANDOM_FAULT_OPTIONS,
    }),
  }
}

/**
 * S3 over three portals of one chain (`s3World`). `chain` is not a portal: its `requests` is the journal of all three portals'
 * stream requests in arrival order, for the log of a failed run. The oracle follows the chain, not
 * the journal (`s3ChainFollower`): a prefetch a switch abandons leaves the journal out of order.
 */
export type RandomFaultS3Simulator = {
  chain: { url: string; requests: MockPortalRequest[] }
  portals: MockChainPortal[]
  faults: RandomFaults
  /** Every floor the chain has reported, in order. */
  reported: number[]
}

export async function randomFaultS3Portals(faults: RandomFaults): Promise<RandomFaultS3Simulator> {
  const journal: MockPortalRequest[] = []
  const world = s3World()

  for (const index of numbers(0, RANDOM_FAULT_SOURCES - 1)) {
    await world.portal({
      delay: async ({ query }, portal) => {
        // `delay` runs right after the portal records the request, so the last one is this one.
        journal.push(portal.requests.at(-1)!)

        await faults.onRequest(index, portal, query)
      },
    })
  }

  return {
    chain: { url: world.portals[0].url, requests: journal },
    portals: world.portals,
    faults,
    reported: world.reported,
  }
}

/** S3 as a full-stream class over the three portals. */
export function s3RandomFaults(durability: FullStreamClass): FullStreamScenario<RandomFaultS3Simulator> {
  return {
    ...s3FullStream<RandomFaultS3Simulator>(durability),
    follower: (model, simulator) => s3ChainFollower(model, () => simulator.reported),
    source: (simulator) => ({
      source: simulator.portals.map(({ url }) => faultyPortal(url)),
      fallback: RANDOM_FAULT_OPTIONS,
    }),
  }
}

/** The portals the run sent a stream request to: more than one means the run switched. */
export function portalsAsked(portals: readonly MockChainPortal[]): number {
  return portals.filter((portal) => portal.requests.length > 0).length
}

/**
 * A run's stream requests in arrival order, one line each, for the log of a failed run: the portal,
 * the first block asked, and the answer. Requests of one portal keep their order; across portals,
 * the S3 journal gives the order, and S1 lists the portals one after another.
 */
export function describeRequests(portals: readonly MockChainPortal[], journal?: readonly MockPortalRequest[]): string {
  const ordered = journal ?? portals.flatMap((portal) => portal.requests)

  return ordered
    .map((request) => {
      const portal = portals.findIndex((each) => each.requests.includes(request))
      const answer = request.answer
      const blocks = answer !== undefined && 'blocks' in answer ? answer.blocks.map(({ header }) => header.number) : []
      const range = blocks.length ? ` ${blocks[0]}..${blocks.at(-1)}` : ''

      return `portal ${portal} from ${request.query.fromBlock}: ${answer?.kind ?? 'unanswered'}${range}`
    })
    .join('\n')
}
