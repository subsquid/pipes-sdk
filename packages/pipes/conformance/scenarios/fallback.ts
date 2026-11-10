import type { EvmFallbackOptions } from '~/evm/evm-fallback.js'
import { type MockChainPortal, type MockPortalRequest, mockPortal } from '~/testing/index.js'

import { type S1Simulator, cursorAt, s1Portal } from './backfill.js'
import { numbers } from './erc20.js'
import { S3, type S3Options, blockAt } from './fork.js'
import { forkStorm, stormPortal } from './fork-storm.js'
import type { FullStreamClass, FullStreamScenario } from './full-stream.js'
import { s1FullStream, s3FullStream } from './full-stream.js'

/**
 * The scenarios of a pipe that reads a source list of two portals over the same chain (spec/16):
 * a primary that fails part-way, and a standby. The store must end exactly as a single-source run
 * of the same scenario leaves it (INV-60). This module holds nothing of vitest, so a child process
 * can run it.
 */

/**
 * No transport retries, so one failed request moves the read; no capability probes, so the
 * portals' logs hold only the requests the stream itself sent. The cooldown keeps a failed primary
 * out for the whole run.
 */
export const FALLBACK_OPTIONS: EvmFallbackOptions = { sourceRetries: 0, detection: { capabilityProbe: false } }

/** The highest block a portal sent in a data answer. */
export function lastServed(requests: readonly MockPortalRequest[]): number {
  return Math.max(-1, ...answerEnds(requests))
}

/** The last block of each data answer: the cursor the pipe holds once it delivers that batch. */
export function answerEnds(requests: readonly MockPortalRequest[]): number[] {
  return requests.flatMap(({ answer }) => (answer?.kind === 'blocks' ? [answer.blocks.at(-1)!.header.number] : []))
}

export function dataRequests(requests: readonly MockPortalRequest[]) {
  return requests.filter(({ answer }) => answer?.kind === 'blocks')
}

/**
 * What is wrong with a read that resumes after `previous`, the log of the source it took over from;
 * `undefined` when nothing is. The client fetches the next batch ahead, so the last block a source
 * sent need not be the last one the pipe delivered from it. A resume is right when it starts after
 * the end of a batch the previous source served, anchored on that block.
 */
export function resumeProblem(
  query: MockPortalRequest['query'] | undefined,
  previous: readonly MockPortalRequest[],
): string | undefined {
  if (query === undefined) {
    return 'no resumed read'
  }

  const resumedAfter = query.fromBlock - 1
  const ends = answerEnds(previous)
  if (!ends.includes(resumedAfter)) {
    return `resumed after ${resumedAfter}, but the previous source ended its batches at ${ends.join(', ')}`
  }

  const anchor = cursorAt(resumedAfter).hash
  if (query.parentBlockHash !== anchor) {
    return `resumed after ${resumedAfter} anchored on ${query.parentBlockHash}, not on ${anchor}`
  }

  return undefined
}

/** Fails `times` stream requests in a row, from the first one at or above `from`; by default every one. */
function failFrom(portal: () => MockChainPortal, from: number | undefined, times = Number.MAX_SAFE_INTEGER) {
  let failed = false

  return (query: { fromBlock: number }) => {
    if (from === undefined || failed || query.fromBlock < from) {
      return
    }

    failed = true
    portal().fail({ status: 503, times })
  }
}

/**
 * S1 over a primary that fails at `failAt` and a healthy standby. Both report the same floor.
 * `clock` is the fallback's clock: the standby moves it by `STANDBY_ANSWER_MS` on every request,
 * so a cooldown ends after a fixed number of standby answers, however fast the machine is.
 */
export type FallbackS1Simulator = S1Simulator & { standby: S1Simulator; clock: () => number }

const STANDBY_ANSWER_MS = 1000

/** `recover` makes the primary fail one request only, so it can come back. */
export async function fallbackS1Portals({
  failAt,
  recover = false,
}: {
  failAt: number
  recover?: boolean
}): Promise<FallbackS1Simulator> {
  let now = 0
  let primary: S1Simulator | undefined
  const fail = failFrom(() => primary!.chain, failAt, recover ? 1 : undefined)
  primary = await s1Portal({ delay: ({ query }) => fail(query) })
  const standby = await s1Portal({
    delay: () => {
      now += STANDBY_ANSWER_MS
    },
  })

  return { ...primary, standby, clock: () => now }
}

/**
 * Lets a recovered primary back after two standby answers: one liveness pass, no head cache and no
 * stall clock, which the virtual clock would distort. With no capability probe the strategy
 * reclaims it at a batch boundary.
 */
export const RECLAIM_OPTIONS: EvmFallbackOptions = {
  sourceRetries: 0,
  detection: {
    capabilityProbe: false,
    cooldownMs: 1.5 * STANDBY_ANSWER_MS,
    livenessRecoverThreshold: 1,
    headTtlMs: 0,
    maxStalenessMs: null,
  },
}

/** S1 as class T over the source list, on the simulator's clock. */
export function s1Fallback(
  durability: FullStreamClass,
  fallback: EvmFallbackOptions = FALLBACK_OPTIONS,
): FullStreamScenario<FallbackS1Simulator> {
  return {
    ...s1FullStream(durability),
    source: (simulator) => ({
      source: [simulator.chain.url, simulator.standby.chain.url],
      fallback: { ...fallback, detection: { ...fallback.detection, clock: simulator.clock } },
    }),
  }
}

/**
 * Two portals over one scenario. `chain` is not a portal: its `requests` is the journal of both
 * portals' stream requests in arrival order, so the oracle replays the run as one log (`Replay`).
 */
export type PortalPairSimulator = {
  chain: { url: string; requests: MockPortalRequest[] }
  primary: MockChainPortal
  standby: MockChainPortal
}

/** One S3 portal of a pair. Each moves its own floor and forks its own chain at the first request that reaches the step. */
type S3PortalOptions = S3Options & {
  journal: MockPortalRequest[]
  /** The floor the first request from `S3.raiseAt` on finds; none keeps the start floor. */
  raisedTo?: number
  /** Fork the chain at `S3.forkAt`, as S3 does; otherwise the chain holds branch 0 up to `S3.to`. */
  fork: boolean
  failAt?: number
}

async function s3PairPortal({
  journal,
  raisedTo,
  fork,
  failAt,
  ancestor = S3.ancestor,
  blocksPerResponse = S3.blocksPerResponse,
}: S3PortalOptions): Promise<MockChainPortal> {
  let raised = false
  let forked = false
  const latest = fork ? S3.latest : S3.to
  const fail = failFrom(() => portal, failAt)

  const portal: MockChainPortal = await mockPortal({
    blocks: numbers(S3.from, latest).map((number) => blockAt(number, 0)),
    latest,
    finalized: S3.finalized,
    blocksPerResponse,
    delay: ({ query }) => {
      // `delay` runs right after the portal records the request, so the last one is this one.
      journal.push(portal.requests.at(-1)!)

      if (raisedTo !== undefined && !raised && query.fromBlock >= S3.raiseAt) {
        raised = true
        portal.setHead({ finalized: raisedTo })
      }

      if (fork && !forked && query.fromBlock >= S3.forkAt) {
        forked = true
        portal.fork(
          ancestor,
          numbers(ancestor + 1, S3.to).map((number) => blockAt(number, 1, ancestor)),
        )
      }

      fail(query)
    },
  })

  return portal
}

/**
 * S3 with the fork on the standby (INV-62): the primary fails at `S3.raiseAt`, below the fork, so
 * the standby raises the floor, serves the head and then forks below it. `options` set the fork
 * depth and the response size, as for one portal.
 */
export async function forkAfterSwitchPortals(options: S3Options = {}): Promise<PortalPairSimulator> {
  const journal: MockPortalRequest[] = []
  const primary = await s3PairPortal({ ...options, journal, raisedTo: S3.raisedTo, fork: true, failAt: S3.raiseAt })
  const standby = await s3PairPortal({ ...options, journal, raisedTo: S3.raisedTo, fork: true })

  return { chain: { url: primary.url, requests: journal }, primary, standby }
}

/**
 * No fork, and two finality depths (INV-64): the primary raises the floor to `S3.raisedTo` and
 * delivers a batch under it, then fails; the standby still reports the start floor, below the one
 * the pipe holds.
 */
export const SHALLOW_FAIL_AT = S3.raiseAt + S3.blocksPerResponse

export async function shallowStandbyPortals(): Promise<PortalPairSimulator> {
  const journal: MockPortalRequest[] = []
  const primary = await s3PairPortal({ journal, raisedTo: S3.raisedTo, fork: false, failAt: SHALLOW_FAIL_AT })
  const standby = await s3PairPortal({ journal, fork: false })

  return { chain: { url: primary.url, requests: journal }, primary, standby }
}

/**
 * S3 as a full-stream class over the source list. `fork: false` is S3 with no fork: branch 0 to the
 * end. `options` must be the ones the portals were built with.
 */
export function s3Fallback(
  durability: FullStreamClass,
  { fork, ...options }: { fork: boolean } & S3Options,
): FullStreamScenario<PortalPairSimulator> {
  // An ancestor at the range end puts every block on branch 0: S3's rows, state and oracle with no fork.
  const scenario = s3FullStream<PortalPairSimulator>(durability, fork ? options : { ancestor: S3.to })

  return {
    ...scenario,
    source: (simulator) => ({ source: [simulator.primary.url, simulator.standby.url], fallback: FALLBACK_OPTIONS }),
  }
}

/**
 * The fork storm over two portals: the primary fails from `failAt` on, and the standby, which has
 * seen no request, catches up to the branch the pipe asks for (`stormPortal`).
 */
export async function forkStormPairPortals({ failAt }: { failAt: number }): Promise<PortalPairSimulator> {
  const journal: MockPortalRequest[] = []
  let primary: MockChainPortal | undefined
  const fail = failFrom(() => primary!, failAt)

  primary = await stormPortal({
    onRequest: (query) => {
      journal.push(primary!.requests.at(-1)!)
      fail(query)
    },
  })
  const standby: MockChainPortal = await stormPortal({
    onRequest: () => {
      journal.push(standby.requests.at(-1)!)
    },
  })

  return { chain: { url: primary.url, requests: journal }, primary, standby }
}

/** The fork storm as a full-stream class over the source list: the end state of one portal. */
export function forkStormFallback(durability: FullStreamClass): FullStreamScenario<PortalPairSimulator> {
  return {
    ...forkStorm<PortalPairSimulator>(durability),
    source: (simulator) => ({ source: [simulator.primary.url, simulator.standby.url], fallback: FALLBACK_OPTIONS }),
  }
}
