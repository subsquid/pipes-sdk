import { setTimeout as sleep } from 'node:timers/promises'

import { afterEach, describe, expect, it, vi } from 'vitest'

import { resolveForkCursor } from '~/core/fork.js'
import { TargetState, createTarget } from '~/core/target.js'
import { BlockCursor } from '~/core/types.js'
import { evmPortalStream } from '~/evm/index.js'
import { HttpError } from '~/http-client/index.js'
import { PortalClient } from '~/portal-client/client.js'

import { mockBlock } from './evm/mock-block.js'
import { withPortal } from './portal-fixture.js'
import { blockDecoder } from './test-block-stream.js'
import { type MockChainPortal, type MockPortal, finalizedMockPortal, mockPortal } from './test-portal.js'

const A = '0x00000000000000000000000000000000000000aa'
const B = '0x00000000000000000000000000000000000000bb'
const X = '0x000000000000000000000000000000000000000000000000000000000000000a'
const Y = '0x000000000000000000000000000000000000000000000000000000000000000b'

function log(address: `0x${string}`, topic: `0x${string}`) {
  return { address, topics: [topic], data: '0x' as const }
}

const blocks = [
  mockBlock({ number: 3, transactions: [{ logs: [log(A, X)] }, { from: B, logs: [log(B, X)] }] }),
  mockBlock({ number: 5, transactions: [{ logs: [log(B, X)] }] }),
  mockBlock({ number: 7, transactions: [{ logs: [log(A, Y)] }] }),
]

const FIELDS = {
  block: { number: true, hash: true },
  log: { address: true, topics: true, transactionIndex: true },
  transaction: { transactionIndex: true, from: true },
} as const

async function streamAll(portal: MockPortal, query: Record<string, unknown>) {
  const client = new PortalClient({ url: portal.url })
  const stream = client.getStream({ type: 'evm', fromBlock: 0, fields: FIELDS, ...query } as any)

  const result: any[] = []
  for await (const batch of stream) {
    result.push(...batch.blocks)
  }

  return result
}

async function post(portal: MockPortal, path: string, query: Record<string, unknown>) {
  return fetch(`${portal.url}/${path}`, { method: 'POST', body: JSON.stringify({ type: 'evm', ...query }) })
}

async function blockNumbers(res: Response): Promise<number[]> {
  const lines = (await res.text()).trim().split('\n')

  return lines.map((line) => JSON.parse(line).header.number)
}

/** Two transactions; the first has two logs, a call with one subcall, and a balance change. */
const related = {
  header: { number: 3, hash: '0x3' },
  transactions: [
    { transactionIndex: 0, from: A, to: B },
    { transactionIndex: 1, from: B },
  ],
  logs: [
    { logIndex: 0, transactionIndex: 0, address: A, topics: [X], data: '0x' },
    { logIndex: 1, transactionIndex: 0, address: B, topics: [Y], data: '0x' },
    { logIndex: 2, transactionIndex: 1, address: B, topics: [X], data: '0x' },
  ],
  traces: [
    { transactionIndex: 0, traceAddress: [], type: 'call', action: { from: A, to: B } },
    { transactionIndex: 0, traceAddress: [0], type: 'call', action: { from: B, to: A } },
    { transactionIndex: 1, traceAddress: [], type: 'create', action: { from: B } },
  ],
  stateDiffs: [
    { transactionIndex: 0, address: A, key: 'balance', kind: '*' },
    { transactionIndex: 1, address: B, key: 'nonce', kind: '+' },
  ],
}

async function selectRelated(portal: MockPortal, request: Record<string, unknown>) {
  const res = await post(portal, 'stream', { fromBlock: 3, toBlock: 3, ...request })
  expect(res.status).toBe(200)

  const block = JSON.parse(await res.text())

  return {
    logs: (block.logs ?? []).map((l: any) => l.logIndex),
    transactions: (block.transactions ?? []).map((tx: any) => tx.transactionIndex),
    traces: (block.traces ?? []).map((t: any) => `${t.transactionIndex}:${t.traceAddress.join('.')}`),
    stateDiffs: (block.stateDiffs ?? []).map((d: any) => `${d.transactionIndex}:${d.key}`),
  }
}

describe('mockPortal over a chain', () => {
  let portal: MockChainPortal

  afterEach(async () => {
    await portal?.close()
  })

  it('sends the selected logs and the first and last block of the covered range', async () => {
    portal = await mockPortal({ blocks, latest: 10 })

    const result = await streamAll(portal, { fromBlock: 1, toBlock: 10, logs: [{ address: [A], topic0: [X] }] })

    expect(result.map((b) => [b.header.number, (b.logs ?? []).map((l: any) => l.address)])).toEqual([
      [1, []],
      [3, [A]],
      [10, []],
    ])
  })

  it('matches addresses and topics in any letter case', async () => {
    portal = await mockPortal({ blocks, latest: 10 })

    const result = await streamAll(portal, { fromBlock: 3, toBlock: 3, logs: [{ address: [B.toUpperCase()] }] })

    expect(result.map((b) => b.logs.map((l: any) => l.address))).toEqual([[B]])
  })

  it('adds the transaction of a log only when a request that matches it asks for it', async () => {
    portal = await mockPortal({ blocks, latest: 10 })

    const result = await streamAll(portal, {
      fromBlock: 3,
      toBlock: 3,
      logs: [{ address: [A], transaction: true }, { address: [B] }],
    })

    expect(result[0].logs).toHaveLength(2)
    expect(result[0].transactions.map((tx: any) => tx.transactionIndex)).toEqual([0])
  })

  it('selects transactions by their own filters, and their logs on request', async () => {
    portal = await mockPortal({ blocks, latest: 10 })

    const result = await streamAll(portal, { fromBlock: 3, toBlock: 3, transactions: [{ from: [B], logs: true }] })

    expect(result[0].transactions.map((tx: any) => tx.transactionIndex)).toEqual([1])
    expect(result[0].logs.map((l: any) => l.address)).toEqual([B])
  })

  it.each([
    [
      'the traces and state diffs of a transaction',
      { transactions: [{ from: [A], traces: true, stateDiffs: true }] },
      { logs: [], transactions: [0], traces: ['0:', '0:0'], stateDiffs: ['0:balance'] },
    ],
    [
      'the sibling logs of a log',
      { logs: [{ address: [A], transactionLogs: true }] },
      { logs: [0, 1], transactions: [], traces: [], stateDiffs: [] },
    ],
    [
      'the traces and state diffs of a log',
      { logs: [{ address: [A], transactionTraces: true, transactionStateDiffs: true }] },
      { logs: [0], transactions: [], traces: ['0:', '0:0'], stateDiffs: ['0:balance'] },
    ],
    [
      'the parents and transaction of a trace',
      { traces: [{ callTo: [A], parents: true, transaction: true }] },
      { logs: [], transactions: [0], traces: ['0:', '0:0'], stateDiffs: [] },
    ],
    [
      'the subtraces of a trace',
      { traces: [{ callFrom: [A], subtraces: true }] },
      { logs: [], transactions: [], traces: ['0:', '0:0'], stateDiffs: [] },
    ],
    [
      'only traces of the type a trace filter names',
      { traces: [{ createFrom: [B] }] },
      { logs: [], transactions: [], traces: ['1:'], stateDiffs: [] },
    ],
    [
      'the transaction of a state diff',
      { stateDiffs: [{ address: [B], kind: ['+'], transaction: true }] },
      { logs: [], transactions: [1], traces: [], stateDiffs: ['1:nonce'] },
    ],
  ])('selects %s', async (_, request, expected) => {
    portal = await mockPortal({ blocks: [related], latest: 3 })

    expect(await selectRelated(portal, request)).toEqual(expected)
  })

  it('skips an item whose filtered field is null', async () => {
    const block = {
      header: { number: 3, hash: '0x3' },
      transactions: [{ transactionIndex: 0, from: A, to: null }],
      traces: [{ transactionIndex: 0, traceAddress: [], type: 'suicide', action: { address: A, refundAddress: null } }],
    }
    portal = await mockPortal({ blocks: [block], latest: 3 })

    const selected = await selectRelated(portal, {
      transactions: [{ to: [A] }],
      traces: [{ suicideRefundAddress: [A] }],
    })

    expect(selected).toEqual({ logs: [], transactions: [], traces: [], stateDiffs: [] })
  })

  it('covers at most `blocksPerResponse` blocks per response', async () => {
    portal = await mockPortal({ blocks, latest: 10, blocksPerResponse: 4 })

    const result = await streamAll(portal, { fromBlock: 1, toBlock: 10, logs: [{ topic0: [X, Y] }] })

    expect(portal.requests.map((r) => r.query.fromBlock)).toEqual([1, 5, 9])
    expect(result.map((b) => b.header.number)).toEqual([1, 3, 4, 5, 7, 8, 9, 10])
  })

  it('stops the finalized stream at the finalized head', async () => {
    portal = await mockPortal({ blocks, finalized: 5, latest: 10 })

    const below = await post(portal, 'finalized-stream', { fromBlock: 4, toBlock: 10 })
    const above = await post(portal, 'finalized-stream', { fromBlock: 6 })

    expect(await blockNumbers(below)).toEqual([4, 5])
    expect(above.status).toBe(204)
  })

  it('streams up to the latest head on the finalized route of a dataset without finality', async () => {
    portal = await mockPortal({ blocks, finalized: null, latest: 10 })

    const res = await post(portal, 'finalized-stream', { fromBlock: 9 })

    expect(await blockNumbers(res)).toEqual([9, 10])
  })

  it('answers 409 to a parent hash that is not on the chain, with the canonical blocks before it', async () => {
    portal = await mockPortal({ blocks, latest: 10, previousBlocks: 2 })

    const res = await post(portal, 'stream', { fromBlock: 4, parentBlockHash: '0xdead' })

    expect(res.status).toBe(409)
    expect(((await res.json()) as any).previousBlocks).toEqual([
      { number: 2, hash: mockBlock({ number: 2 }).header.hash },
      { number: 3, hash: blocks[0].header.hash },
    ])
  })

  it('refuses a query with more addresses than allowed', async () => {
    portal = await mockPortal({ blocks, latest: 10, maxAddresses: 2 })

    const res = await post(portal, 'stream', { fromBlock: 1, logs: [{ address: [A, B] }, { address: [A] }] })

    expect(res.status).toBe(400)
  })

  it('reports heads, and no finalized head when it has none', async () => {
    portal = await mockPortal({ blocks, finalized: null, latest: 10 })
    const client = new PortalClient({ url: portal.url })

    expect(await client.getHead({ finalized: true })).toBeUndefined()

    portal.setHead({ finalized: 8, latest: 12 })

    expect(await client.getHead({ finalized: true })).toMatchObject({ number: 8 })
    expect(await client.getHead({ finalized: false })).toMatchObject({ number: 12 })
  })

  it('sends the items of other networks unfiltered', async () => {
    const instruction = { programId: 'p', d8: '0x01' }
    portal = await mockPortal({ blocks: [{ header: { number: 1, hash: '0x1' }, instructions: [instruction] }] })

    const res = await post(portal, 'stream', {
      type: 'solana',
      fromBlock: 1,
      instructions: [{ programId: ['other'] }],
    })

    expect(JSON.parse(await res.text())).toEqual({ header: { number: 1, hash: '0x1' }, instructions: [instruction] })
  })

  it('answers 409 to a request anchored on a block that a fork replaced', async () => {
    portal = await mockPortal({ latest: 10, finalized: 5 })
    const before = await streamAll(portal, { fromBlock: 1, toBlock: 10, includeAllBlocks: true })
    const hashOf = (number: number) => before.find((b) => b.header.number === number).header.hash

    portal.fork(7)

    const res = await post(portal, 'stream', { fromBlock: 11, parentBlockHash: hashOf(10) })
    const { previousBlocks } = (await res.json()) as any

    expect(res.status).toBe(409)
    expect(previousBlocks.find((b: any) => b.number === 7).hash).toBe(hashOf(7))
    expect(previousBlocks.find((b: any) => b.number === 8).hash).not.toBe(hashOf(8))
    expect(previousBlocks.at(-1).number).toBe(10)
  })

  it('refuses a fork branch whose block keeps the hash of the block it replaces', async () => {
    portal = await mockPortal({ latest: 10 })

    expect(() => portal.fork(7, [mockBlock({ number: 8 })])).toThrow('keeps the hash')
    expect(() => portal.fork(7, [mockBlock({ number: 7, hash: '0xfe' })])).toThrow('not above the fork point')
  })

  it('extends the chain above the head', async () => {
    portal = await mockPortal({ latest: 2 })

    portal.append(mockBlock({ number: 3, transactions: [{ logs: [log(A, X)] }] }))
    const result = await streamAll(portal, { fromBlock: 3, toBlock: 3, logs: [{ address: [A] }] })

    expect(result.map((b) => [b.header.number, b.logs.length])).toEqual([[3, 1]])
    expect(() => portal.append(mockBlock({ number: 3 }))).toThrow('not above the head')
  })

  it('records a request when it arrives, also while `delay` holds it', async () => {
    let release!: () => void
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    portal = await mockPortal({ latest: 2, delay: ({ query }) => (query.fromBlock === 1 ? held : undefined) })

    const first = post(portal, 'stream', { fromBlock: 1 })
    await vi.waitFor(() => expect(portal.requests).toHaveLength(1))
    expect(portal.requests[0].status).toBeUndefined()

    await post(portal, 'stream', { fromBlock: 2 })
    release()
    await first

    expect(portal.requests.map((r) => [r.query.fromBlock, r.status])).toEqual([
      [1, 200],
      [2, 200],
    ])
  })

  it('serves faults to the next requests, then answers from the chain again', async () => {
    portal = await mockPortal({ latest: 2 })
    portal.fail({ status: 503, times: 2, retryAfter: 1 })

    const first = await post(portal, 'stream', { fromBlock: 1 })
    await post(portal, 'stream', { fromBlock: 1 })
    await post(portal, 'stream', { fromBlock: 1 })

    expect(first.headers.get('retry-after')).toBe('1')
    expect(portal.requests.map((r) => r.status)).toEqual([503, 503, 200])
  })

  it('resumes a restarted pipe and resolves a fork with no scripted response', async () => {
    portal = await mockPortal({ latest: 10, finalized: 5, blocksPerResponse: 1 })

    let state: TargetState | undefined
    const rollbackChain: BlockCursor[] = []
    const target = createTarget<unknown>({
      write: async ({ read }) => {
        for await (const { ctx } of read(state)) {
          rollbackChain.push(...ctx.stream.state.rollbackChain)
          state = { latest: ctx.stream.state.current, finalized: ctx.stream.head.finalized ?? null }
        }
      },
      resolveFork: (canonical) =>
        resolveForkCursor([{ rollbackChain, finalized: state?.finalized ?? undefined }], canonical),
    })
    const run = (to: number) =>
      evmPortalStream({ id: 'test', portal: portal.url, logger: 'silent', outputs: blockDecoder({ from: 1, to }) })
        // createTarget's input type cannot name the decoder's output here
        .pipeTo(target as any)

    await run(10)
    const before = state?.latest

    portal.fork(7)
    portal.setHead({ latest: 12 })
    const first = portal.requests.length
    await run(12)

    expect(portal.requests.slice(first).map((r) => [r.query.fromBlock, r.status])).toEqual([
      [11, 409],
      [8, 200],
      [9, 200],
      [10, 200],
      [11, 200],
      [12, 200],
    ])
    expect(before?.number).toBe(10)
    expect(state?.latest.number).toBe(12)
  })
})

describe('mockPortal request log', () => {
  withPortal('records on each request what it answered and the head it reported', async ({ portal: open }) => {
    const portal = await open({ latest: 10, finalized: 5 })
    const before = await streamAll(portal, { fromBlock: 9, toBlock: 10, includeAllBlocks: true })
    const parentBlockHash = before.at(-1).header.hash

    portal.fork(7)
    portal.setHead({ finalized: 6 })
    await post(portal, 'stream', { fromBlock: 11, parentBlockHash })

    const [served, forked] = portal.requests
    const blocks = served.answer?.kind === 'blocks' ? served.answer.blocks.map((block) => block.header.number) : []

    expect(blocks).toEqual([9, 10])
    expect(served.head?.finalized?.number).toBe(5)
    expect(forked.answer?.kind).toBe('fork')
    expect(forked.head?.finalized?.number).toBe(6)
  })
})

describe('mockPortal stall', () => {
  withPortal('serves a stall as the first blocks of the answer, then no more bytes', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    portal.fail({ stallAfter: 2 })

    const res = await post(portal, 'stream', { fromBlock: 1, includeAllBlocks: true })
    const reader = res.body!.getReader()
    const decoder = new TextDecoder()

    let received = ''
    while ((received.match(/\n/g) ?? []).length < 2) {
      const { value } = await reader.read()
      received += decoder.decode(value, { stream: true })
    }

    const next = await Promise.race([reader.read().then(() => 'data'), sleep(100).then(() => 'silence')])
    await reader.cancel()

    const numbers = received
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line).header.number)

    expect(res.status).toBe(200)
    expect(numbers).toEqual([1, 2])
    expect(next).toBe('silence')
    expect(portal.requests[0].answer?.kind).toBe('stalled')
  })
})

describe('mockPortal timestamp route', () => {
  /** The block the route answers for `seconds`, or the status of its refusal. */
  async function blockAt(portal: MockPortal, seconds: number) {
    try {
      return await new PortalClient({ url: portal.url }).resolveTimestamp(seconds)
    } catch (error) {
      return `HTTP ${(error as HttpError).response.status}`
    }
  }

  // Made-up blocks are `number * 1000` seconds apart from genesis.
  withPortal('finds the first block at or after a time in seconds', async ({ portal: open }) => {
    const portal = await open({ latest: 3 })

    expect(await blockAt(portal, 0)).toBe(0)
    expect(await blockAt(portal, 1_000)).toBe(1)
    expect(await blockAt(portal, 1_500)).toBe(2)
    expect(await blockAt(portal, 3_001)).toBe('HTTP 404')
  })

  withPortal('reads block timestamps in milliseconds on a tron dataset', async ({ portal: open }) => {
    const second = 1_700_000_000
    const blocks = [0, 1, 2].map((number) => ({
      header: { number, hash: `0x${number}`, timestamp: (second + number * 3) * 1000 },
    }))
    const portal = await open({ kind: 'tron', blocks })

    expect(await blockAt(portal, second + 3)).toBe(1)
    expect(await blockAt(portal, second + 4)).toBe(2)
    expect(await blockAt(portal, second + 7)).toBe('HTTP 404')
  })
})

describe('mockPortal over a response list', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  it('answers stream requests with the list in order, then with 500', async () => {
    const header = { number: 1, hash: '0x1' }
    portal = await mockPortal([{ statusCode: 204 }, { statusCode: 200, data: [{ header }] }])

    const statuses: number[] = []
    let body = ''
    for (let i = 0; i < 3; i++) {
      const res = await post(portal, 'stream', { fromBlock: 1 })
      statuses.push(res.status)
      body += await res.text()
    }

    expect(statuses).toEqual([204, 200, 500])
    expect(body).toBe(JSON.stringify({ header }) + '\n')
  })

  it('answers only the finalized stream through the deprecated finalizedMockPortal', async () => {
    portal = await finalizedMockPortal([{ statusCode: 204 }])

    const hot = await post(portal, 'stream', { fromBlock: 1 })
    const finalized = await post(portal, 'finalized-stream', { fromBlock: 1 })

    expect([hot.status, finalized.status]).toEqual([404, 204])
  })
})
