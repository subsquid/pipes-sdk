import { MOCK_GENESIS_TIMESTAMP, mockBlock } from '~/testing/evm/index.js'
import { type MockChainPortal, mockPortal } from '~/testing/index.js'

import { type Cursor, type OracleRow, ReferenceModel } from '../oracle/reference-model.js'
import { Replay } from '../oracle/replay.js'
import { erc20Logs, inStoreOrder, numbers } from './erc20.js'
import type { FullStreamClass, FullStreamScenario } from './full-stream.js'

/** Three replacements, each deeper than the previous one, ending at the finalized floor. */
export const STORM = { from: 0, to: 21, floor: 3, heads: [9, 13, 17], ancestors: [7, 5, 3] } as const
export type StormSimulator = { chain: MockChainPortal }

function hash(number: number, branch: number): `0x${string}` {
  return `0x${branch.toString(16).padStart(8, '0')}${number.toString(16).padStart(56, '0')}`
}

function cursor(number: number, branch: number): Cursor {
  return { number, hash: hash(number, branch), timestamp: MOCK_GENESIS_TIMESTAMP + number }
}

function branchOf(block: Cursor): number {
  return Number.parseInt(block.hash.slice(2, 10), 16)
}

function rowsAt(block: Cursor): OracleRow[] {
  const value = block.number + 100 * branchOf(block)

  return [
    { table: 'transfers', block: block.number, value: { blockNumber: BigInt(block.number), value: String(value) } },
  ]
}

function block(number: number, branch: number) {
  return mockBlock({
    ...cursor(number, branch),
    transactions: [{ logs: erc20Logs({ transfer: BigInt(number + 100 * branch) }) }],
  })
}

export function forkStorm(durability: FullStreamClass): FullStreamScenario<StormSimulator> {
  const ranges = [{ from: STORM.from, to: STORM.to }]
  const rowsUpTo = (at: Cursor | undefined) => {
    if (at === undefined) return []
    const branch = branchOf(at)
    // Each new fork replaces the whole previous fork, leaving the original prefix.
    const ancestor = branch === 0 ? STORM.to : STORM.ancestors[branch - 1]

    return inStoreOrder(
      numbers(0, at.number).flatMap((number) => rowsAt(cursor(number, number > ancestor ? branch : 0))),
    )
  }

  return {
    range: ranges[0],
    ranges,
    oracle: () => new ReferenceModel({ durability, mode: 'full', ranges, transform: rowsAt }),
    rowsUpTo,
    end: () => ({
      rows: rowsUpTo(cursor(STORM.to, 3)),
      state: {
        current: cursor(STORM.to, 3),
        finalized: { number: STORM.floor, hash: hash(STORM.floor, 0) },
        rollbackChain: numbers(STORM.floor + 1, STORM.to).map((number) => cursor(number, 3)),
      },
    }),
    follower: (model, simulator, from) => new Replay(model, simulator.chain.requests, from),
  }
}

export async function forkStormPortal(): Promise<StormSimulator> {
  let branch = 0
  const chain: MockChainPortal = await mockPortal({
    blocks: numbers(0, STORM.heads[0]).map((number) => block(number, 0)),
    latest: STORM.heads[0],
    finalized: STORM.floor,
    blocksPerResponse: 3,
    previousBlocks: 32,
    delay: ({ query }) => {
      if (branch < STORM.heads.length && query.fromBlock > STORM.heads[branch]) {
        const ancestor = STORM.ancestors[branch]
        branch++
        const end = branch < STORM.heads.length ? STORM.heads[branch] : STORM.to
        chain.fork(
          ancestor,
          numbers(ancestor + 1, end).map((number) => block(number, branch)),
        )
      }
    },
  })

  return { chain }
}
