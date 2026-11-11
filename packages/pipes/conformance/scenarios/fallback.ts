import type { EvmFallbackOptions } from '~/evm/evm-fallback.js'
import { type MockChainPortal, type MockPortalRequest, mockPortal } from '~/testing/index.js'

import { type S1Simulator, s1Portal } from './backfill.js'
import { numbers } from './erc20.js'
import { S3, blockAt } from './fork.js'
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
 * stall clock, which the virtual clock would distort. With no capability probe the stock strategy
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
      url: [simulator.chain.url, simulator.standby.chain.url],
      fallback: { ...fallback, detection: { ...fallback.detection, clock: simulator.clock } },
    }),
  }
}

/**
 * Two S3 portals. `chain` is not a portal: its `requests` is the journal of both portals' stream
 * requests in arrival order, so the oracle replays the run as one log (`Replay`).
 */
export type FallbackS3Simulator = {
  chain: { url: string; requests: MockPortalRequest[] }
  primary: MockChainPortal
  standby: MockChainPortal
}

/** One S3 portal of a pair. Each moves its own floor and forks its own chain at the first request that reaches the step. */
type S3PortalOptions = {
  journal: MockPortalRequest[]
  /** The floor the first request from `S3.raiseAt` on finds; none keeps the start floor. */
  raisedTo?: number
  /** Fork the chain at `S3.forkAt`, as S3 does; otherwise the chain holds branch 0 up to `S3.to`. */
  fork: boolean
  failAt?: number
}

async function s3PairPortal({ journal, raisedTo, fork, failAt }: S3PortalOptions): Promise<MockChainPortal> {
  let raised = false
  let forked = false
  const latest = fork ? S3.latest : S3.to
  const fail = failFrom(() => portal, failAt)

  const portal: MockChainPortal = await mockPortal({
    blocks: numbers(S3.from, latest).map((number) => blockAt(number, 0)),
    latest,
    finalized: S3.finalized,
    blocksPerResponse: S3.blocksPerResponse,
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
          S3.ancestor,
          numbers(S3.ancestor + 1, S3.to).map((number) => blockAt(number, 1)),
        )
      }

      fail(query)
    },
  })

  return portal
}

/**
 * S3 with the fork on the standby (INV-62): the primary fails at `S3.raiseAt`, below the fork, so
 * the standby raises the floor, serves the head and then forks below it.
 */
export async function forkAfterSwitchPortals(): Promise<FallbackS3Simulator> {
  const journal: MockPortalRequest[] = []
  const primary = await s3PairPortal({ journal, raisedTo: S3.raisedTo, fork: true, failAt: S3.raiseAt })
  const standby = await s3PairPortal({ journal, raisedTo: S3.raisedTo, fork: true })

  return { chain: { url: primary.url, requests: journal }, primary, standby }
}

/**
 * No fork, and two finality depths (INV-64): the primary raises the floor to `S3.raisedTo` and
 * delivers a batch under it, then fails; the standby still reports the start floor, below the one
 * the pipe holds.
 */
export const SHALLOW_FAIL_AT = S3.raiseAt + S3.blocksPerResponse

export async function shallowStandbyPortals(): Promise<FallbackS3Simulator> {
  const journal: MockPortalRequest[] = []
  const primary = await s3PairPortal({ journal, raisedTo: S3.raisedTo, fork: false, failAt: SHALLOW_FAIL_AT })
  const standby = await s3PairPortal({ journal, fork: false })

  return { chain: { url: primary.url, requests: journal }, primary, standby }
}

/** S3 as a full-stream class over the source list. `fork: false` is S3 with no fork: branch 0 to the end. */
export function s3Fallback(
  durability: FullStreamClass,
  { fork }: { fork: boolean },
): FullStreamScenario<FallbackS3Simulator> {
  // An ancestor at the range end puts every block on branch 0: S3's rows, state and oracle with no fork.
  const scenario = s3FullStream<FallbackS3Simulator>(durability, fork ? {} : { ancestor: S3.to })

  return {
    ...scenario,
    source: (simulator) => ({ url: [simulator.primary.url, simulator.standby.url], fallback: FALLBACK_OPTIONS }),
  }
}
