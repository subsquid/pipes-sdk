import { isDeepStrictEqual } from 'node:util'

import { mockBlock } from '~/testing/evm/index.js'
import { type MockChainPortal, type MockPortalOptions, mockPortal } from '~/testing/index.js'

import { type Cursor, type OracleRow, type OracleState, ReferenceModel } from '../oracle/reference-model.js'
import { type Observation, erc20Logs, inStoreOrder, numbers } from './erc20.js'

/**
 * S1: steady backfill over deep history, healthy portal (spec/11). The range ends on blocks with
 * no rows. This module holds the scenario and nothing of vitest, so a child process can run it.
 */
export const S1 = { from: 0, to: 59, lastRow: 49, blocksPerResponse: 7 }
export const RANGES = [{ from: S1.from, to: S1.to }]

/** Approval values sit apart from transfer values, so a row written to the wrong table shows. */
const APPROVAL_OFFSET = 1000

/** A sparse table: one transfer every fourth block up to `lastRow`, its value the block number. */
const hasTransfer = (number: number) => number % 4 === 1 && number <= S1.lastRow

/** A sparser one: an approval on every other transfer block, in the same block as the transfer. */
const hasApproval = (number: number) => number % 8 === 1 && number <= S1.lastRow

/** The rows S1 stores for a block, as the store reads them back: INT64 as bigint. */
export function rowsAt(number: number): OracleRow[] {
  const rows: OracleRow[] = []

  if (hasTransfer(number)) {
    rows.push({ table: 'transfers', block: number, value: { blockNumber: BigInt(number), value: String(number) } })
  }

  if (hasApproval(number)) {
    const value = String(number + APPROVAL_OFFSET)
    rows.push({ table: 'approvals', block: number, value: { blockNumber: BigInt(number), value } })
  }

  return rows
}

/** Every row S1 must leave in the store, from the scenario alone. */
export const S1_ROWS: OracleRow[] = inStoreOrder(numbers(S1.from, S1.to).flatMap(rowsAt))

export function s1Blocks() {
  return numbers(S1.from, S1.to)
    .filter(hasTransfer)
    .map((number) => {
      const approval = hasApproval(number) ? BigInt(number + APPROVAL_OFFSET) : undefined
      const logs = erc20Logs({ transfer: BigInt(number), approval })

      return mockBlock({ number, transactions: [{ logs }] })
    })
}

/** The chain's cursor for a block, as a batch carries it: given and made-up blocks share `mockBlock`'s header. */
export function cursorAt(number: number): Cursor {
  const { hash, timestamp } = mockBlock({ number }).header

  return { number, hash, timestamp }
}

/** A finalized head as the portal reports it: number and hash, no timestamp. */
export function reportedAt(number: number): Cursor {
  return { number, hash: cursorAt(number).hash }
}

/**
 * The state S1 must leave in the store: the cursor at the range end and `floor`, the highest floor
 * the portal reported. Class K keeps no rollback chain (WP-47).
 */
export function s1EndState(floor: number | undefined): OracleState {
  return {
    current: cursorAt(S1.to),
    finalized: floor === undefined ? undefined : reportedAt(floor),
    rollbackChain: [],
  }
}

/** The oracle for S1, as class K: the Parquet sink requires finalized-only delivery. */
export function s1Oracle(): ReferenceModel {
  return new ReferenceModel({
    durability: 'K',
    ranges: RANGES,
    transform: (block) => rowsAt(block.number),
  })
}

/** What the simulator holds. Every expectation derives from it, never from the pipe. */
export type S1Simulator = {
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

/** The portal of S1: every block up to the range end, finalized up to it unless `options` says otherwise. */
export async function s1Portal(options: SimulateOptions = {}): Promise<S1Simulator> {
  const finalized = options.finalized === undefined ? S1.to : options.finalized
  const chain = await mockPortal({
    blocksPerResponse: S1.blocksPerResponse,
    ...options,
    blocks: s1Blocks(),
    latest: S1.to,
    finalized,
  })

  const reported = finalized === null ? [] : [finalized]
  const finalize = (number: number) => {
    reported.push(number)
    chain.setHead({ finalized: number })
  }

  return { chain, reported, finalize }
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
export function follow(model: ReferenceModel, simulator: S1Simulator, { current, finalized }: Observation): void {
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

/** The state S1 must leave in the store, from the simulator alone: a lower report never lowers the floor (INV-12). */
export function endStateOf(simulator: S1Simulator) {
  const highest = simulator.reported.length ? Math.max(...simulator.reported) : undefined

  return s1EndState(highest)
}
