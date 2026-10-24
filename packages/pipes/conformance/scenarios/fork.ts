import { mockBlock } from '~/testing/evm/index.js'
import { type MockChainPortal, mockPortal } from '~/testing/index.js'

import {
  type Cursor,
  type DurabilityClass,
  type OracleRow,
  type OracleState,
  ReferenceModel,
} from '../oracle/reference-model.js'
import { erc20Logs, inStoreOrder, numbers } from './erc20.js'

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

export const S3_RANGES = [{ from: S3.from, to: S3.to }]

/** Branch 0 is the chain the run starts on; branch 1 replaces its blocks above the ancestor. */
type Branch = 0 | 1

/** Branch 0 hashes as `mockBlock` makes them, branch 1 as the mock chain makes up its first fork. */
function hashAt(number: number, branch: Branch): `0x${string}` {
  const digits = number.toString(16)

  return branch === 0 ? `0x${digits.padStart(64, '0')}` : `0x00000001${digits.padStart(56, '0')}`
}

/** A transfer in every block, an approval in every even one; branch 1 shifts both values. */
function valuesAt(number: number, branch: Branch) {
  const shift = 500 * branch
  const approval = number % 2 === 0 ? number + 1000 + shift : undefined

  return { transfer: number + shift, approval }
}

function blockAt(number: number, branch: Branch) {
  const { transfer, approval } = valuesAt(number, branch)
  const logs = erc20Logs({
    transfer: BigInt(transfer),
    approval: approval === undefined ? undefined : BigInt(approval),
  })
  const parentBranch: Branch = branch === 1 && number - 1 > S3.ancestor ? 1 : 0

  return mockBlock({
    number,
    hash: hashAt(number, branch),
    parentHash: hashAt(Math.max(number - 1, 0), parentBranch),
    transactions: [{ logs }],
  })
}

function branchOf(block: Cursor): Branch {
  if (block.hash === hashAt(block.number, 0)) {
    return 0
  }

  if (block.number > S3.ancestor && block.hash === hashAt(block.number, 1)) {
    return 1
  }

  throw new Error(`S3 has no block ${block.number} with the hash ${block.hash}`)
}

/** A block's cursor, as a batch carries it. */
function cursorAt(number: number, branch: Branch): Cursor {
  return { number, hash: hashAt(number, branch), timestamp: number * 1000 }
}

/** The block the chain ends with at `number`. */
function canonicalAt(number: number): Cursor {
  return cursorAt(number, number > S3.ancestor ? 1 : 0)
}

/** The rows S3 stores for a block of either branch, as the store reads them back: INT64 as bigint. */
export function s3RowsAt(block: Cursor): OracleRow[] {
  const { transfer, approval } = valuesAt(block.number, branchOf(block))
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
export const S3_ROWS: OracleRow[] = inStoreOrder(numbers(S3.from, S3.to).map(canonicalAt).flatMap(s3RowsAt))

/**
 * The rows a store whose cursor is `cursor` holds: the blocks up to it, on the branch the cursor is
 * on above the ancestor, so a store a crash left before the fork still holds the forked rows.
 */
export function s3RowsUpTo(cursor: Cursor | undefined): OracleRow[] {
  if (cursor === undefined) {
    return []
  }

  const branch = branchOf(cursor)
  const blocks = numbers(S3.from, cursor.number).map((number) => cursorAt(number, number > S3.ancestor ? branch : 0))

  return inStoreOrder(blocks.flatMap(s3RowsAt))
}

/** The state S3 must leave in the store: the range end, the raised floor, every block above it. */
export function s3EndState(): OracleState {
  return {
    current: canonicalAt(S3.to),
    finalized: { number: S3.raisedTo, hash: hashAt(S3.raisedTo, 0) },
    rollbackChain: numbers(S3.raisedTo + 1, S3.to).map(canonicalAt),
  }
}

/** The oracle for S3, as a full-stream class. */
export function s3Oracle(durability: DurabilityClass): ReferenceModel {
  return new ReferenceModel({ durability, mode: 'full', ranges: S3_RANGES, transform: s3RowsAt })
}

/** The portal of S3, which forks once (`s3Portal`). */
export type S3Simulator = { chain: MockChainPortal }

/**
 * The portal of S3. It raises the floor and forks the chain at the first request that reaches each
 * step, whichever process sends it, so a crash and a restart meet the same history.
 */
export async function s3Portal(): Promise<MockChainPortal> {
  let raised = false
  let forked = false

  const portal: MockChainPortal = await mockPortal({
    blocks: numbers(S3.from, S3.latest).map((number) => blockAt(number, 0)),
    latest: S3.latest,
    finalized: S3.finalized,
    blocksPerResponse: S3.blocksPerResponse,
    delay: ({ query }) => {
      if (!raised && query.fromBlock >= S3.raiseAt) {
        raised = true
        portal.setHead({ finalized: S3.raisedTo })
      }

      if (!forked && query.fromBlock >= S3.forkAt) {
        forked = true
        portal.fork(
          S3.ancestor,
          numbers(S3.ancestor + 1, S3.to).map((number) => blockAt(number, 1)),
        )
      }
    },
  })

  return portal
}
