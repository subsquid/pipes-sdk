import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { isDeepStrictEqual } from 'node:util'

import { it as base, expect } from 'vitest'

import { type MockChainPortal, type MockPortalOptions, mockPortal, testLogger } from '~/testing/index.js'

import { type ParquetUnit, readParquetSnapshot, rowsOf } from './parquet-probe.js'
import type { ReferenceModel } from './reference-model.js'
import {
  type Observation,
  PIPE_ID,
  RANGES,
  S1,
  S1_ROWS,
  TABLE_NAMES,
  type TableName,
  cursorAt,
  inStoreOrder,
  numbers,
  reportedAt,
  s1Blocks,
  s1EndState,
  s1Oracle,
  streamS1,
} from './s1.js'
import { type ParquetSnapshot, validateParquetSnapshot, validateRouteMode } from './validators.js'

/** What the simulator holds. Every expectation derives from it, never from the pipe. */
export type Simulator = {
  chain: MockChainPortal
  /**
   * Every finalized head the portal has reported, in order; none on a dataset without finality.
   * The test sets them, so they are ground truth.
   */
  reported: number[]
  /** Moves the finalized head and records the report. */
  finalize(number: number): void
}

/** `finalized: null` serves a dataset without finality. */
export type SimulateOptions = Omit<MockPortalOptions, 'blocks' | 'latest' | 'finalized'> & {
  finalized?: number | null
}

export const it = base.extend<{
  simulate: (options?: SimulateOptions) => Promise<Simulator>
  dir: string
}>({
  simulate: async ({}, use) => {
    const opened: MockChainPortal[] = []
    await use(async (options = {}) => {
      const finalized = options.finalized === undefined ? S1.to : options.finalized
      const chain = await mockPortal({
        blocksPerResponse: S1.blocksPerResponse,
        ...options,
        blocks: s1Blocks(),
        latest: S1.to,
        finalized,
      })
      opened.push(chain)

      const reported = finalized === null ? [] : [finalized]
      const finalize = (number: number) => {
        reported.push(number)
        chain.setHead({ finalized: number })
      }

      return { chain, reported, finalize }
    })
    await Promise.all(opened.map((chain) => chain.close()))
  },
  dir: async ({}, use) => {
    const dir = await mkdtemp(path.join(tmpdir(), 'sqd-ct-'))
    await use(dir)
    await rm(dir, { recursive: true, force: true })
  },
})

/** A read of S1's store. */
export type S1Store = ParquetSnapshot<ParquetUnit, TableName>

export function readStore(dir: string): Promise<S1Store> {
  return readParquetSnapshot({ dir, id: PIPE_ID, tables: TABLE_NAMES })
}

function requireSame(label: string, actual: unknown, expected: unknown): void {
  if (!isDeepStrictEqual(actual, expected)) {
    throw new Error(`${label} is ${JSON.stringify(actual)}, the simulator has ${JSON.stringify(expected)}`)
  }
}

/**
 * Feeds the oracle one batch the pipe delivered, so a transition the spec forbids fails at that
 * batch. The batch's cursor must be the chain's block and the pipe's finalized head one the portal
 * reported (INV-2); the oracle gets the finality the simulator last reported, never the pipe's view.
 */
export function follow(model: ReferenceModel, simulator: Simulator, { current, finalized }: Observation): void {
  const previous = model.readState().current?.number
  const first = previous === undefined ? S1.from : previous + 1
  const covered = numbers(first, current.number).map(cursorAt)

  requireSame('the batch cursor', current, covered.at(-1))

  const reported = simulator.reported.some((number) => isDeepStrictEqual(finalized, reportedAt(number)))
  const invented = finalized !== undefined && !reported
  if (invented) {
    throw new Error(
      `INV-2: the pipe holds the finalized head ${JSON.stringify(finalized)}, which the portal never reported`,
    )
  }

  const lastReport = simulator.reported.at(-1)
  const head = lastReport === undefined ? {} : { finalized: reportedAt(lastReport) }
  model.batch(covered, head)
}

/** The state S1 must leave in the store, from the simulator alone. */
export function endStateOf(simulator: Simulator) {
  return s1EndState(simulator.reported.at(-1))
}

/**
 * Streams S1 into a Parquet store and returns the store as the run leaves it.
 *
 * The oracle recovers from the store first, as the pipe does, then follows every batch in
 * lockstep (`follow`).
 *
 * When the run ends, the store must pass every structural validator and hold exactly S1's rows and
 * end state, and so must the oracle. Those come from the simulator, because the lockstep oracle
 * follows the pipe: it agrees with a pipe that stopped early.
 */
export async function runS1({
  simulator,
  dir,
  model = s1Oracle(),
}: {
  simulator: Simulator
  dir: string
  /** Pass one to watch it during the run, or to carry it across a restart or a crash. */
  model?: ReferenceModel
}): Promise<S1Store> {
  const before = await readStore(dir)
  model.recover(before.state)

  await streamS1({
    url: simulator.chain.url,
    dir,
    logger: testLogger(),
    observe: (observation) => follow(model, simulator, observation),
  })

  const after = await readStore(dir)
  const end = endStateOf(simulator)
  const violations = validateParquetSnapshot(after, RANGES)
  const offRoute = validateRouteMode('K', simulator.chain.requests)
  const rows = rowsOf(after)
  const oracleRows = inStoreOrder(model.data)

  expect(violations, 'the structural validators').toEqual([])
  expect(offRoute, 'route-mode coherence').toEqual([])
  expect(after.state, 'the stored state').toStrictEqual(end)
  expect(rows, 'the stored rows').toEqual(S1_ROWS)
  expect(model.readState(), 'the oracle state').toStrictEqual(end)
  expect(oracleRows, 'the oracle rows').toEqual(S1_ROWS)

  return after
}
