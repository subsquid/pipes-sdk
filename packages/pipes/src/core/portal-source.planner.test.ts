import { afterEach, describe, expect, it } from 'vitest'

import { type PlanRequestArgs, type RequestPlanner } from '~/core/query-builder.js'
import { evmPortalStream, evmQuery } from '~/evm/index.js'
import { type MockEvmChainPortal, mockEvmChainPortal } from '~/testing/evm/index.js'
import { readAll } from '~/testing/index.js'

function hashOf(number: number) {
  return `0x${number.toString(16).padStart(64, '0')}`
}

/** Answers with every block of a fixed-length stretch, and records what it was asked. */
function stretches(length: number, calls: PlanRequestArgs[] = []): RequestPlanner<any> {
  return async (args) => {
    calls.push(args)

    return { request: { includeAllBlocks: true }, to: args.from + length - 1 }
  }
}

function blockNumbers(range: { from: number; to: number }, ...planners: RequestPlanner<any>[]) {
  let query = evmQuery().addFields({ block: { number: true, hash: true } })
  for (const planner of planners) {
    query = query.addPlannedRequest({ range, planner })
  }

  return query.build().pipe((blocks) => blocks.map((b) => b.header.number))
}

describe('Portal stream with request planners', () => {
  let portal: MockEvmChainPortal

  afterEach(async () => {
    await portal?.close()
  })

  it('fetches a planned range as consecutive segments, each anchored to the one before', async () => {
    portal = await mockEvmChainPortal({ blocks: [], latest: 9 })
    const calls: PlanRequestArgs[] = []

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockNumbers({ from: 0, to: 9 }, stretches(4, calls)),
    })

    const blocks = await readAll(stream)

    expect(blocks).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    expect(calls.map(({ from, to }) => ({ from, to }))).toEqual([
      { from: 0, to: 9 },
      { from: 4, to: 9 },
      { from: 8, to: 9 },
    ])
    expect(
      portal.requests.map(({ query }) => ({
        fromBlock: query.fromBlock,
        toBlock: query.toBlock,
        parentBlockHash: query.parentBlockHash,
      })),
    ).toEqual([
      { fromBlock: 0, toBlock: 3, parentBlockHash: undefined },
      { fromBlock: 4, toBlock: 7, parentBlockHash: hashOf(3) },
      { fromBlock: 8, toBlock: 9, parentBlockHash: hashOf(7) },
    ])
  })

  it('ends a segment where the first of several planners stops holding', async () => {
    portal = await mockEvmChainPortal({ blocks: [], latest: 9 })

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockNumbers({ from: 0, to: 9 }, stretches(3), stretches(5)),
    })

    const blocks = await readAll(stream)

    expect(blocks).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    expect(portal.requests.map(({ query }) => [query.fromBlock, query.toBlock])).toEqual([
      [0, 2],
      [3, 5],
      [6, 8],
      [9, 9],
    ])
  })

  it('merges the planned request into the static one of the range', async () => {
    portal = await mockEvmChainPortal({ blocks: [], latest: 3 })
    const planner: RequestPlanner<any> = async () => ({ request: { logs: [{ topic0: ['0xbb'] }] } })

    const outputs = evmQuery()
      .addFields({ block: { number: true, hash: true } })
      .addLogRequest({ range: { from: 0, to: 3 }, request: { topic0: ['0xaa'] } })
      .addPlannedRequest({ range: { from: 0, to: 3 }, planner })
      .build()

    await readAll(evmPortalStream({ id: 'test', portal: portal.url, outputs }))

    expect(portal.requests.map(({ query }) => query.logs)).toEqual([[{ topic0: ['0xaa'] }, { topic0: ['0xbb'] }]])
  })

  it('fails on a planner answer that ends before the block it was asked for', async () => {
    portal = await mockEvmChainPortal({ blocks: [], latest: 9 })
    const planner: RequestPlanner<any> = async ({ from }) => ({ request: {}, to: from - 1 })

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockNumbers({ from: 5, to: 9 }, planner),
    })

    await expect(readAll(stream)).rejects.toThrow(
      'A request planner asked for block 5 answered with a request that ends at 4',
    )
    expect(portal.requests).toEqual([])
  })

  it('fails when a planner fails', async () => {
    portal = await mockEvmChainPortal({ blocks: [], latest: 9 })
    const planner: RequestPlanner<any> = async () => {
      throw new Error('planner is broken')
    }

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockNumbers({ from: 0, to: 9 }, planner),
    })

    await expect(readAll(stream)).rejects.toThrow('planner is broken')
  })
})
