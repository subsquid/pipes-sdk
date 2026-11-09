import { MOCK_GENESIS_TIMESTAMP } from '~/testing/evm/index.js'
import { type MockPortal, type MockPortalRequest, type MockResponse, mockPortal } from '~/testing/index.js'

import { type Cursor, ReferenceModel } from '../oracle/reference-model.js'
import { Replay } from '../oracle/replay.js'
import { inStoreOrder, numbers } from './erc20.js'
import { blockAt, hashAt, s3RowsAt } from './fork.js'
import type { FullStreamClass, FullStreamScenario } from './full-stream.js'

/** Both histories discover the common ancestor through two partial canonical windows. */
export const FORK_HISTORIES = ['missing-finalized', 'deep'] as const
export type ForkHistory = (typeof FORK_HISTORIES)[number]
export type ForkHistorySimulator = { chain: MockPortal & { requests: MockPortalRequest[] } }

const ANCESTOR = 1

function anchor(number: number, branch: 0 | 1) {
  return { number, hash: hashAt(number, branch) }
}

function cursor(number: number, branch: 0 | 1): Cursor {
  return { ...anchor(number, branch), timestamp: MOCK_GENESIS_TIMESTAMP + number }
}

/** The exact history, independent of which target consumes it. */
export function forkHistory(
  durability: FullStreamClass,
  history: ForkHistory,
): FullStreamScenario<ForkHistorySimulator> {
  const from = history === 'missing-finalized' ? 2 : 1
  const floor = history === 'missing-finalized' ? anchor(4, 1) : anchor(1, 0)
  const ranges = [{ from: 0, to: 7 }]
  const rowsAt = (block: Cursor) => s3RowsAt(block, ANCESTOR)
  const rowsUpTo = (at: Cursor | undefined) => {
    if (at === undefined) return []
    const branch = at.hash === hashAt(at.number, 0) ? 0 : 1

    return inStoreOrder(
      numbers(from, at.number).flatMap((number) => rowsAt(cursor(number, number > ANCESTOR ? branch : 0))),
    )
  }

  return {
    range: ranges[0],
    ranges,
    // The request starts at zero, but the filtered response has no rows before `from`.
    // Replay models the delivered block domain; the fixed end state checks the complete result.
    oracle: () => new ReferenceModel({ durability, mode: 'full', ranges: [{ from, to: 7 }], transform: rowsAt }),
    rowsUpTo,
    end: () => ({
      rows: rowsUpTo(cursor(7, 1)),
      state: {
        current: cursor(7, 1),
        finalized: floor,
        rollbackChain: numbers(floor.number + 1, 7).map((number) => cursor(number, 1)),
      },
    }),
    follower: (model, simulator, from) => new Replay(model, simulator.chain.requests, from),
  }
}

/**
 * Script only these fixed partial windows. The shared mock portal serves the wire protocol;
 * its response callbacks journal the answers for the existing replay oracle.
 */
export async function forkHistoryPortal(history: ForkHistory): Promise<ForkHistorySimulator> {
  const requests: MockPortalRequest[] = []
  const responses: (Extract<MockResponse, { statusCode: 200 }> | Extract<MockResponse, { statusCode: 409 }>)[] = [
    {
      statusCode: 200,
      data: numbers(history === 'missing-finalized' ? 2 : 1, 5).map((number) => blockAt(number, 0, ANCESTOR)),
      head: { finalized: anchor(1, 0) },
    },
    { statusCode: 409, data: { previousBlocks: [anchor(4, 1), anchor(5, 1)] } },
    { statusCode: 409, data: { previousBlocks: [anchor(1, 0), anchor(2, 1), anchor(3, 1)] } },
    {
      statusCode: 200,
      data: numbers(2, 7).map((number) => blockAt(number, 1, ANCESTOR)),
      head: history === 'missing-finalized' ? { finalized: anchor(4, 1) } : undefined,
    },
  ]
  const portal = await mockPortal(
    responses.map((response) => ({
      ...response,
      validateRequest: (query: MockPortalRequest['query']) => {
        requests.push({
          endpoint: 'stream',
          query,
          status: response.statusCode,
          head: response.statusCode === 200 ? response.head : undefined,
          answer:
            response.statusCode === 200
              ? { kind: 'blocks', blocks: response.data }
              : { kind: 'fork', previousBlocks: response.data.previousBlocks ?? [] },
        })
      },
    })),
  )

  return { chain: { ...portal, requests } }
}

/** The observable boundaries each rollback must reach, including their original hashes. */
export const HISTORY_ROLLBACKS = [anchor(3, 0), anchor(1, 0)]
export const HISTORY_REQUESTS = [
  { fromBlock: 0, toBlock: 7 },
  { fromBlock: 6, toBlock: 7, parentBlockHash: hashAt(5, 0) },
  { fromBlock: 4, toBlock: 7, parentBlockHash: hashAt(3, 0) },
  { fromBlock: 2, toBlock: 7, parentBlockHash: hashAt(1, 0) },
]

/** Wire fields required by the ERC-20 decoder on initial and rollback requests. */
export const HISTORY_QUERY_FIELDS = {
  block: { number: true, hash: true, timestamp: true },
  transaction: { from: true, to: true, hash: true, sighash: true },
  log: { address: true, topics: true, data: true, transactionHash: true, logIndex: true, transactionIndex: true },
}
