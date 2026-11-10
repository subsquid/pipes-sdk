import { isDeepStrictEqual } from 'node:util'

import type { BlockCursor } from '~/core/index.js'
import { MOCK_GENESIS_TIMESTAMP, mockBlock } from '~/testing/evm/index.js'
import { type MockChainPortal, type MockPortalOptions, type MockRpcCall, mockPortal } from '~/testing/index.js'

import {
  type Cursor,
  type DurabilityClass,
  type OracleRow,
  type OracleState,
  ReferenceModel,
} from '../oracle/reference-model.js'
import { type Observation, erc20Logs, inStoreOrder, numbers } from './erc20.js'
import type { Follower } from './full-stream.js'

/**
 * S3 reduced to one fork (spec/11), for a full-stream class. The run starts at a head whose tail is
 * unfinalized; the floor rises once while the pipe follows the tail; once the pipe has reached the
 * head, the chain forks below it and grows to the range end. Every block carries rows, so the
 * portal delivers every block and the rollback chain holds every processed block above the floor.
 * This module holds the scenario and nothing of vitest, so a child process can run it.
 */
export const S3 = {
  from: 0,
  to: 39,
  /** The head when the run starts, and the floor it reports. */
  latest: 29,
  finalized: 19,
  /** The first request from `raiseAt` on finds the floor raised to `raisedTo`. */
  raiseAt: 28,
  raisedTo: 23,
  /** The first request past the head finds every block above `ancestor` replaced, up to `to`. */
  forkAt: 30,
  ancestor: 26,
  blocksPerResponse: 7,
} as const

/** Vary rollback depth and transport partitioning without changing the original S3 defaults. */
export type S3Options = { ancestor?: number; blocksPerResponse?: number }

export const S3_RANGES = [{ from: S3.from, to: S3.to }]

/** Branch 0 is the chain the run starts on; branch 1 replaces its blocks above the ancestor. */
type Branch = 0 | 1

/** Branch 0 hashes as `mockBlock` makes them, branch 1 as the mock chain makes up its first fork. */
export function hashAt(number: number, branch: Branch): `0x${string}` {
  const digits = number.toString(16)

  return branch === 0 ? `0x${digits.padStart(64, '0')}` : `0x00000001${digits.padStart(56, '0')}`
}

/** A transfer in every block, an approval in every even one; branch 1 shifts both values. */
function valuesAt(number: number, branch: Branch) {
  const shift = 500 * branch
  const approval = number % 2 === 0 ? number + 1000 + shift : undefined

  return { transfer: number + shift, approval }
}

export function blockAt(number: number, branch: Branch, ancestor: number = S3.ancestor) {
  const { transfer, approval } = valuesAt(number, branch)
  const logs = erc20Logs({
    transfer: BigInt(transfer),
    approval: approval === undefined ? undefined : BigInt(approval),
  })
  const parentBranch: Branch = branch === 1 && number - 1 > ancestor ? 1 : 0

  return mockBlock({
    number,
    hash: hashAt(number, branch),
    parentHash: hashAt(Math.max(number - 1, 0), parentBranch),
    transactions: [{ logs }],
  })
}

function branchOf(block: Cursor, ancestor: number = S3.ancestor): Branch {
  if (block.hash === hashAt(block.number, 0)) {
    return 0
  }

  if (block.number > ancestor && block.hash === hashAt(block.number, 1)) {
    return 1
  }

  throw new Error(`S3 has no block ${block.number} with the hash ${block.hash}`)
}

/** A block's cursor, as a batch carries it. */
function cursorAt(number: number, branch: Branch): Cursor {
  return { number, hash: hashAt(number, branch), timestamp: MOCK_GENESIS_TIMESTAMP + number }
}

/** The block the chain ends with at `number`. */
function canonicalAt(number: number, ancestor: number = S3.ancestor): Cursor {
  return cursorAt(number, number > ancestor ? 1 : 0)
}

/** The rows S3 stores for a block of either branch, as the store reads them back: INT64 as bigint. */
export function s3RowsAt(block: Cursor, ancestor: number = S3.ancestor): OracleRow[] {
  const { transfer, approval } = valuesAt(block.number, branchOf(block, ancestor))
  const blockNumber = BigInt(block.number)
  const rows: OracleRow[] = [
    { table: 'transfers', block: block.number, value: { blockNumber, value: String(transfer) } },
  ]

  if (approval !== undefined) {
    rows.push({ table: 'approvals', block: block.number, value: { blockNumber, value: String(approval) } })
  }

  return rows
}

/** Every row S3 must leave in the store, from the scenario alone: the chain it ends with. */
export function s3Rows({ ancestor = S3.ancestor }: S3Options = {}): OracleRow[] {
  return inStoreOrder(
    numbers(S3.from, S3.to)
      .map((number) => canonicalAt(number, ancestor))
      .flatMap((block) => s3RowsAt(block, ancestor)),
  )
}

/**
 * The rows a store whose cursor is `cursor` holds: the blocks up to it, on the branch the cursor is
 * on above the ancestor, so a store a crash left before the fork still holds the forked rows.
 */
export function s3RowsUpTo(cursor: Cursor | undefined, { ancestor = S3.ancestor }: S3Options = {}): OracleRow[] {
  if (cursor === undefined) {
    return []
  }

  const branch = branchOf(cursor, ancestor)
  const blocks = numbers(S3.from, cursor.number).map((number) => cursorAt(number, number > ancestor ? branch : 0))

  return inStoreOrder(blocks.flatMap((block) => s3RowsAt(block, ancestor)))
}

/** The state S3 must leave in the store: the range end, the raised floor, every block above it. */
export function s3EndState({ ancestor = S3.ancestor }: S3Options = {}): OracleState {
  return {
    current: canonicalAt(S3.to, ancestor),
    finalized: { number: S3.raisedTo, hash: hashAt(S3.raisedTo, 0) },
    rollbackChain: numbers(S3.raisedTo + 1, S3.to).map((number) => canonicalAt(number, ancestor)),
  }
}

function requireSame(label: string, actual: unknown, expected: unknown): void {
  if (!isDeepStrictEqual(actual, expected)) {
    throw new Error(`${label} is ${JSON.stringify(actual)}, the S3 chain has ${JSON.stringify(expected)}`)
  }
}

/** A finalized head as the chain reports it: number and hash, no timestamp. */
function floorAt(number: number) {
  return { number, hash: hashAt(number, 0) }
}

/**
 * Feeds the oracle a run's batches and forks, checked against the S3 chain rather than a request
 * log. It is for a run no log can order: in a source list, a prefetch that a switch abandons stays
 * in the log, unanswered or unread; an RPC source finds a fork itself and answers no 409.
 *
 * A batch's blocks must be the chain's, on the branch its last block is on. A fork's canonical
 * chain must be the chain the run ends on. The pipe's floor must be one the chain reported
 * (INV-2), and the oracle gets the floor the chain last reported, never the pipe's view.
 */
export function s3ChainFollower(
  model: ReferenceModel,
  reported: () => readonly number[],
  ancestor: number = S3.ancestor,
): Follower {
  return {
    batch({ current, finalized }: Observation) {
      if (current.hash === undefined) {
        throw new Error(`the batch cursor ${current.number} has no hash`)
      }

      const branch = branchOf({ number: current.number, hash: current.hash }, ancestor)
      const previous = model.readState().current?.number
      const first = previous === undefined ? S3.from : previous + 1
      const covered = numbers(first, current.number).map((number) => cursorAt(number, number > ancestor ? branch : 0))

      requireSame('the batch cursor', current, covered.at(-1))

      const floors = reported()
      const invented =
        finalized !== undefined && !floors.some((number) => isDeepStrictEqual(finalized, floorAt(number)))
      if (invented) {
        throw new Error(
          `INV-2: the pipe holds the finalized head ${JSON.stringify(finalized)}, which the chain never reported`,
        )
      }

      const last = floors.at(-1)
      model.batch(covered, last === undefined ? {} : { finalized: floorAt(last) })
    },
    fork(canonical: BlockCursor[]) {
      const expected = canonical.map((block) => {
        const { number, hash } = canonicalAt(block.number, ancestor)

        return { number, hash }
      })

      requireSame('the canonical chain', canonical, expected)
      model.fork(expected)
    },
  }
}

/** The oracle for S3, as a full-stream class. */
export function s3Oracle(durability: DurabilityClass, { ancestor = S3.ancestor }: S3Options = {}): ReferenceModel {
  return new ReferenceModel({
    durability,
    mode: 'full',
    ranges: S3_RANGES,
    transform: (block) => s3RowsAt(block, ancestor),
  })
}

/** The portal of S3, which forks once (`s3Portal`). */
export type S3Simulator = { chain: MockChainPortal }

/**
 * The portal of S3. It raises the floor and forks the chain at the first request that reaches each
 * step, whichever process sends it, so a crash and a restart meet the same history.
 */
export async function s3Portal({
  ancestor = S3.ancestor,
  blocksPerResponse = S3.blocksPerResponse,
}: S3Options = {}): Promise<MockChainPortal> {
  let raised = false
  let forked = false

  const portal: MockChainPortal = await mockPortal({
    blocks: numbers(S3.from, S3.latest).map((number) => blockAt(number, 0)),
    latest: S3.latest,
    finalized: S3.finalized,
    blocksPerResponse,
    delay: ({ query }) => {
      if (!raised && query.fromBlock >= S3.raiseAt) {
        raised = true
        portal.setHead({ finalized: S3.raisedTo })
      }

      if (!forked && query.fromBlock >= S3.forkAt) {
        forked = true
        portal.fork(
          ancestor,
          numbers(ancestor + 1, S3.to).map((number) => blockAt(number, 1, ancestor)),
        )
      }
    },
  })

  return portal
}

/** The highest block a JSON-RPC request asks for, by number or as the end of a log range; -1 for none. */
export function highestAsked(calls: readonly MockRpcCall[]): number {
  const asked = calls.flatMap(({ method, params }) => {
    const block = method === 'eth_getLogs' ? (params[0] as { toBlock?: unknown } | undefined)?.toBlock : params[0]
    const byNumber = (method === 'eth_getBlockByNumber' || method === 'eth_getLogs') && typeof block === 'string'

    return byNumber && block.startsWith('0x') ? [Number.parseInt(block, 16)] : []
  })

  return Math.max(-1, ...asked)
}

type WorldHooks = {
  delay?: (
    request: Parameters<NonNullable<MockPortalOptions['delay']>>[0],
    portal: MockChainPortal,
  ) => Promise<void> | void
  rpcDelay?: (calls: readonly MockRpcCall[], portal: MockChainPortal) => Promise<void> | void
}

/** A request that polls the head: the call an RPC reader makes between two fetch rounds. */
function isHeadPoll(calls: readonly MockRpcCall[]): boolean {
  return calls.some(
    ({ method, params }) =>
      method === 'eth_blockNumber' || (method === 'eth_getBlockByNumber' && params[0] === 'latest'),
  )
}

/**
 * One S3 chain behind several mock portals, as one real chain behind several providers: the first
 * request of any portal that reaches a step raises the floor or forks the chain on all of them. A
 * stream request reaches the fork past the head. A JSON-RPC reader never asks past the head, so it
 * reaches the fork at its first head poll after it asked for the head block: between two fetch
 * rounds. Inside a round, the blocks and their logs would come from two branches, and the reader
 * would deliver the old branch's blocks without their logs, which S3's oracle, expecting every
 * block, cannot follow.
 */
export type S3World = {
  portals: MockChainPortal[]
  /** Every floor the chain has reported, in order. */
  reported: number[]
  /** Opens one more portal over the chain. Its hooks run after the chain has taken the step. */
  portal(hooks?: WorldHooks): Promise<MockChainPortal>
}

export function s3World(): S3World {
  const portals: MockChainPortal[] = []
  const reported: number[] = [S3.finalized]
  let raised = false
  let forked = false
  let headAsked = false

  const reach = (block: number) => {
    if (!raised && block >= S3.raiseAt) {
      raised = true
      reported.push(S3.raisedTo)
      for (const portal of portals) {
        portal.setHead({ finalized: S3.raisedTo })
      }
    }

    if (!forked && block >= S3.forkAt) {
      forked = true
      for (const portal of portals) {
        portal.fork(
          S3.ancestor,
          numbers(S3.ancestor + 1, S3.to).map((number) => blockAt(number, 1)),
        )
      }
    }
  }

  return {
    portals,
    reported,
    async portal({ delay, rpcDelay }: WorldHooks = {}) {
      const portal: MockChainPortal = await mockPortal({
        blocks: numbers(S3.from, S3.latest).map((number) => blockAt(number, 0)),
        latest: S3.latest,
        finalized: S3.finalized,
        blocksPerResponse: S3.blocksPerResponse,
        delay: async (request) => {
          reach(request.query.fromBlock)

          await delay?.(request, portal)
        },
        rpcDelay: async (calls) => {
          const asked = highestAsked(calls)
          reach(asked)
          if (headAsked && isHeadPoll(calls)) {
            reach(S3.forkAt)
          }

          headAsked ||= asked >= S3.latest

          await rpcDelay?.(calls, portal)
        },
      })
      portals.push(portal)

      return portal
    },
  }
}
