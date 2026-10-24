import { event, indexed } from '@subsquid/evm-abi'
import * as p from '@subsquid/evm-codec'
import { pino } from 'pino'
import { toEventSelector } from 'viem'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { createTarget } from '~/core/target.js'
import { PortalClient } from '~/portal-client/client.js'
import {
  type ChainPortalRequest,
  type MockEvmChainPortal,
  type PortalBlock,
  encodeEvent,
  mockBlock,
  mockEvmChainPortal,
} from '~/testing/evm/index.js'

import { evmEventDecoder } from './evm-decoder.js'
import { evmPortalStream } from './evm-portal-source.js'
import { FactoryLookaheadOptions, contractFactory } from './factory.js'
import { contractFactorySqliteStore } from './factory-adapters/sqlite.js'
import { FactoryDiscovery, FactoryLookahead, decideSegment } from './factory-lookahead.js'

// ── ABIs ──

const POOL_CREATED_SIGNATURE = 'PoolCreated(address,address,uint24,int24,address)'
const SWAP_SIGNATURE = 'Swap(address,address,int256,int256,uint160,uint128,int24)'
const FLASH_SIGNATURE = 'Flash(address,address,uint256,uint256,uint256,uint256)'

const POOL_CREATED_ABI = [
  {
    type: 'event' as const,
    name: 'PoolCreated',
    inputs: [
      { name: 'token0', type: 'address', indexed: true },
      { name: 'token1', type: 'address', indexed: true },
      { name: 'fee', type: 'uint24', indexed: true },
      { name: 'tickSpacing', type: 'int24', indexed: false },
      { name: 'pool', type: 'address', indexed: false },
    ],
  },
] as const

const POOL_ABI = [
  {
    type: 'event' as const,
    name: 'Swap',
    inputs: [
      { name: 'sender', type: 'address', indexed: true },
      { name: 'recipient', type: 'address', indexed: true },
      { name: 'amount0', type: 'int256', indexed: false },
      { name: 'amount1', type: 'int256', indexed: false },
      { name: 'sqrtPriceX96', type: 'uint160', indexed: false },
      { name: 'liquidity', type: 'uint128', indexed: false },
      { name: 'tick', type: 'int24', indexed: false },
    ],
  },
  {
    type: 'event' as const,
    name: 'Flash',
    inputs: [
      { name: 'sender', type: 'address', indexed: true },
      { name: 'recipient', type: 'address', indexed: true },
      { name: 'amount0', type: 'uint256', indexed: false },
      { name: 'amount1', type: 'uint256', indexed: false },
      { name: 'paid0', type: 'uint256', indexed: false },
      { name: 'paid1', type: 'uint256', indexed: false },
    ],
  },
] as const

const factoryAbi = {
  PoolCreated: event(toEventSelector(POOL_CREATED_SIGNATURE), POOL_CREATED_SIGNATURE, {
    token0: indexed(p.address),
    token1: indexed(p.address),
    fee: indexed(p.uint24),
    tickSpacing: p.int24,
    pool: p.address,
  }),
}

const poolAbi = {
  Swap: event(toEventSelector(SWAP_SIGNATURE), SWAP_SIGNATURE, {
    sender: indexed(p.address),
    recipient: indexed(p.address),
    amount0: p.int256,
    amount1: p.int256,
    sqrtPriceX96: p.uint160,
    liquidity: p.uint128,
    tick: p.int24,
  }),
  Flash: event(toEventSelector(FLASH_SIGNATURE), FLASH_SIGNATURE, {
    sender: indexed(p.address),
    recipient: indexed(p.address),
    amount0: p.uint256,
    amount1: p.uint256,
    paid0: p.uint256,
    paid1: p.uint256,
  }),
}

// ── Addresses ──

type Hex = `0x${string}`

const FACTORY = '0x1f98431c8ad98523631ae4a59f267346ea31f984' as Hex
const IMPOSTOR_FACTORY = '0x00000000000000000000000000000000000fac70' as Hex
const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as Hex
const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' as Hex
const USDT = '0xdac17f958d2ee523a2206206994597c13d831ec7' as Hex
const SENDER = '0xdef1cafe0000000000000000000000000000dead' as Hex
const OTHER_SENDER = '0x0000000000000000000000000000000000005e4d' as Hex
const RECIPIENT = '0xbeef0000000000000000000000000000deadbeef' as Hex

function pool(n: number): Hex {
  return `0x${n.toString(16).padStart(40, 'a')}` as Hex
}

// ── Events ──

function poolCreated(address: Hex, token0: Hex = WETH, factory: Hex = FACTORY) {
  return encodeEvent({
    abi: POOL_CREATED_ABI,
    eventName: 'PoolCreated',
    address: factory,
    args: { token0, token1: USDC, fee: 3000, tickSpacing: 10, pool: address },
  })
}

function swap(address: Hex, amount = 1n) {
  return encodeEvent({
    abi: POOL_ABI,
    eventName: 'Swap',
    address,
    args: {
      sender: SENDER,
      recipient: RECIPIENT,
      amount0: amount,
      amount1: -amount,
      sqrtPriceX96: 3n,
      liquidity: 4n,
      tick: 5,
    },
  })
}

function flash(address: Hex, sender: Hex = SENDER) {
  return encodeEvent({
    abi: POOL_ABI,
    eventName: 'Flash',
    address,
    args: { sender, recipient: RECIPIENT, amount0: 1n, amount1: 2n, paid0: 3n, paid1: 4n },
  })
}

// ── Random chains ──

/** mulberry32: a small seeded generator, so a failing chain can be rebuilt from its seed */
function random(seed: number) {
  let a = seed >>> 0

  return () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)

    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

type Pool = { address: Hex; block: number; token0: Hex }

type Log = ReturnType<typeof swap>

/**
 * A chain where the factory creates pools now and then, often several in one block, and the pools
 * emit events from their creation block on. Unrelated contracts emit the same events, and another
 * contract emits the factory event, so a query that filters too little or too much shows.
 */
function randomChain(seed: number, length: number) {
  const next = random(seed)
  const pools: Pool[] = []
  const blocks: PortalBlock[] = []

  for (let number = 1; number <= length; number++) {
    const transactions: { logs: Log[] }[] = []

    if (next() < 0.08) {
      const count = 1 + Math.floor(next() * 3)

      for (let i = 0; i < count; i++) {
        const created: Pool = { address: pool(pools.length + 1), block: number, token0: next() < 0.6 ? WETH : USDT }
        pools.push(created)

        const logs = [poolCreated(created.address, created.token0)]
        if (next() < 0.5) logs.push(swap(created.address, BigInt(number)))
        transactions.push({ logs })
      }
    }

    for (const existing of pools) {
      if (existing.block >= number || next() > 0.05) continue

      const log =
        next() < 0.7
          ? swap(existing.address, BigInt(number))
          : flash(existing.address, next() < 0.5 ? SENDER : OTHER_SENDER)
      transactions.push({ logs: [log] })
    }

    if (next() < 0.3) transactions.push({ logs: [swap(pool(10_000 + number))] })
    if (next() < 0.1) transactions.push({ logs: [flash(pool(20_000 + number))] })
    if (next() < 0.05) transactions.push({ logs: [poolCreated(pool(30_000 + number), WETH, IMPOSTOR_FACTORY)] })

    if (transactions.length > 0) {
      blocks.push(mockBlock({ number, transactions }))
    }
  }

  return { blocks, pools }
}

// ── Pipe ──

type Row = { kind: string; block: number; logIndex: number; contract: string; createdAt?: number; event: object }

type RunOptions = {
  range: { from: number; to: number }
  lookahead: false | FactoryLookaheadOptions
  withParams?: boolean
  /** Shared between runs to resume with the children an earlier run stored */
  database?: Awaited<ReturnType<typeof contractFactorySqliteStore>>
  resumeAt?: number
  /** Hands every response over at once instead of buffering them for up to 300 ms */
  unbuffered?: boolean
}

function captureLogger() {
  const messages: string[] = []
  const logger = pino(
    { level: 'debug', messageKey: 'message' },
    { write: (line: string) => messages.push(JSON.parse(line).message) },
  )

  return { logger, messages }
}

async function runPipe(portal: MockEvmChainPortal, options: RunOptions) {
  const { logger, messages } = captureLogger()
  const database = options.database ?? (await contractFactorySqliteStore({ path: ':memory:' }))

  const factory = contractFactory({
    address: FACTORY,
    event: options.withParams ? { event: factoryAbi.PoolCreated, params: { token0: WETH } } : factoryAbi.PoolCreated,
    childAddressField: 'pool',
    database,
    lookahead: options.lookahead === false ? false : options.lookahead,
  })

  const decoder = evmEventDecoder({
    range: options.range,
    contracts: factory,
    events: {
      swaps: poolAbi.Swap,
      flashes: { event: poolAbi.Flash, params: { sender: SENDER } },
    },
  }).pipe(({ swaps, flashes }) =>
    [...swaps.map((e) => ({ kind: 'swap', e })), ...flashes.map((e) => ({ kind: 'flash', e }))].map(
      ({ kind, e }): Row => ({
        kind,
        block: e.block.number,
        logIndex: e.rawEvent.logIndex,
        contract: e.contract,
        createdAt: e.factory?.blockNumber,
        event: e.event,
      }),
    ),
  )

  const rows: Row[] = []
  const resumeAt = options.resumeAt
  const state =
    resumeAt === undefined
      ? undefined
      : { latest: { number: resumeAt, hash: `0x${resumeAt.toString(16).padStart(64, '0')}` }, finalized: null }

  const client = options.unbuffered ? new PortalClient({ url: portal.url, maxIdleTimeMs: 1 }) : portal.url

  await evmPortalStream({ id: 'test', portal: client, logger, outputs: decoder }).pipeTo(
    createTarget({
      write: async ({ read }) => {
        for await (const { data } of read(state)) {
          rows.push(...data)
        }
      },
    }),
  )

  const sorted = rows.sort((a, b) => a.block - b.block || a.logIndex - b.logIndex)

  return { rows: sorted, messages, database }
}

const SWAP_TOPIC = poolAbi.Swap.topic
const FLASH_TOPIC = poolAbi.Flash.topic

function childRequestsOf(request: ChainPortalRequest) {
  return (request.query.logs ?? []).filter((l) => l.topic0?.includes(SWAP_TOPIC) || l.topic0?.includes(FLASH_TOPIC))
}

function isFiltered(request: ChainPortalRequest) {
  return childRequestsOf(request).some((l) => l.address !== undefined)
}

function mainRequests(portal: MockEvmChainPortal) {
  return portal.requests.filter((r) => r.endpoint === 'stream')
}

/**
 * Checks every main-stream query against the chain: a filtered query must hold every accepted
 * child created up to its last block, end at or below the finalized head, and fit the budget.
 */
function assertQueriesCover({
  portal,
  pools,
  range,
  finalized,
  maxAddresses,
}: {
  portal: MockEvmChainPortal
  pools: Pool[]
  range: { from: number }
  finalized: number
  maxAddresses: number
}) {
  for (const request of mainRequests(portal)) {
    const factoryRequests = (request.query.logs ?? []).filter((l) => l.address?.includes(FACTORY))
    expect(factoryRequests).toHaveLength(1)

    const children = childRequestsOf(request)
    expect(children).toHaveLength(2)

    if (!isFiltered(request)) continue

    const toBlock = request.query.toBlock
    expect(toBlock).toBeDefined()
    expect(toBlock!).toBeLessThanOrEqual(finalized)

    const addresses = children[0].address!
    for (const child of children) {
      expect(child.address).toEqual(addresses)
    }

    expect(addresses.length * children.length).toBeLessThanOrEqual(maxAddresses)

    const required = pools.filter((p) => p.block >= range.from && p.block <= toBlock!).map((p) => p.address)
    for (const address of required) {
      expect(addresses).toContain(address)
    }
  }
}

// ── Tests ──

describe('decideSegment', () => {
  const base = {
    from: 100,
    to: undefined,
    frontier: 99,
    finalizedHead: 10_000,
    stored: [] as string[],
    discovered: [] as { address: string; block: number }[],
    requestsPerQuery: 1,
    maxAddresses: 10,
    minSegmentBlocks: 100,
    previousSegment: 0,
  }

  it('falls back to topic-only queries near the finalized head', () => {
    expect(decideSegment({ ...base, finalizedHead: 150 })).toEqual({
      kind: 'full-scan',
      reason: 'it is close to the finalized head',
    })
  })

  it('falls back to topic-only queries above the finalized head', () => {
    expect(decideSegment({ ...base, finalizedHead: 50 })).toMatchObject({ kind: 'full-scan' })
  })

  it('filters a short range that ends below the finalized head', () => {
    expect(decideSegment({ ...base, to: 110, frontier: 110 })).toEqual({ kind: 'filter', to: 110, addresses: [] })
  })

  it('waits until the scan is the minimum segment ahead', () => {
    expect(decideSegment({ ...base, frontier: 150 })).toEqual({ kind: 'wait' })
    expect(decideSegment({ ...base, frontier: 199 })).toMatchObject({ kind: 'filter', to: 199 })
  })

  it('takes every block the scan has covered', () => {
    expect(decideSegment({ ...base, frontier: 5_000 })).toMatchObject({ kind: 'filter', to: 5_000 })
  })

  it('filters by the stored children and the ones created up to the end, sorted', () => {
    const decision = decideSegment({
      ...base,
      frontier: 300,
      stored: ['0xc'],
      discovered: [
        { address: '0xb', block: 120 },
        { address: '0xa', block: 300 },
      ],
    })

    expect(decision).toEqual({ kind: 'filter', to: 300, addresses: ['0xa', '0xb', '0xc'] })
  })

  it('does not count a stored child twice', () => {
    const decision = decideSegment({
      ...base,
      frontier: 300,
      maxAddresses: 2,
      stored: ['0xa', '0xb'],
      discovered: [{ address: '0xa', block: 150 }],
    })

    expect(decision).toEqual({ kind: 'filter', to: 300, addresses: ['0xa', '0xb'] })
  })

  it('ends the filter before the block of the first child over the budget', () => {
    const decision = decideSegment({
      ...base,
      frontier: 1_000,
      maxAddresses: 2,
      discovered: [
        { address: '0xa', block: 100 },
        { address: '0xb', block: 150 },
        { address: '0xc', block: 400 },
        { address: '0xd', block: 400 },
      ],
    })

    expect(decision).toEqual({ kind: 'filter', to: 399, addresses: ['0xa', '0xb'] })
  })

  it('counts the address list once per child-event request', () => {
    const decision = decideSegment({
      ...base,
      frontier: 1_000,
      maxAddresses: 4,
      requestsPerQuery: 2,
      discovered: [
        { address: '0xa', block: 100 },
        { address: '0xb', block: 150 },
        { address: '0xc', block: 400 },
      ],
    })

    expect(decision).toMatchObject({ kind: 'filter', to: 399 })
  })

  it('falls back to topic-only queries when the budget ends the filter too soon', () => {
    const discovered = [
      { address: '0xa', block: 100 },
      { address: '0xb', block: 150 },
    ]

    expect(decideSegment({ ...base, frontier: 1_000, maxAddresses: 1, discovered })).toEqual({
      kind: 'full-scan',
      reason: 'its children no longer fit into 1 addresses',
    })
    expect(decideSegment({ ...base, from: 150, frontier: 1_000, maxAddresses: 1, discovered })).toMatchObject({
      kind: 'full-scan',
    })
  })

  it('falls back to topic-only queries when the stored children alone are over the budget', () => {
    expect(decideSegment({ ...base, frontier: 1_000, maxAddresses: 1, stored: ['0xa', '0xb'] })).toMatchObject({
      kind: 'full-scan',
    })
  })

  it('takes what the scan has once it caught up with the finalized head', () => {
    expect(decideSegment({ ...base, frontier: 250, finalizedHead: 250 })).toMatchObject({ kind: 'filter', to: 250 })
  })

  it('wants each segment twice as long as the previous one while the scan runs', () => {
    expect(decideSegment({ ...base, frontier: 499, previousSegment: 300 })).toEqual({ kind: 'wait' })
    expect(decideSegment({ ...base, frontier: 699, previousSegment: 300 })).toMatchObject({ kind: 'filter', to: 699 })
  })

  it('waits until the scan reaches the first block', () => {
    expect(decideSegment({ ...base, frontier: 50 })).toEqual({ kind: 'wait' })
  })
})

describe('Factory lookahead', () => {
  const portals: MockEvmChainPortal[] = []

  async function chainPortal(options: Parameters<typeof mockEvmChainPortal>[0]) {
    const portal = await mockEvmChainPortal(options)
    portals.push(portal)

    return portal
  }

  afterEach(async () => {
    vi.restoreAllMocks()
    await Promise.all(portals.splice(0).map((p) => p.close()))
  })

  it('filters the finalized blocks by the children created up to them, then falls back near the head', async () => {
    const blocks = [
      mockBlock({ number: 5, transactions: [{ logs: [poolCreated(pool(1))] }, { logs: [swap(pool(1))] }] }),
      mockBlock({ number: 30, transactions: [{ logs: [poolCreated(pool(2)), poolCreated(pool(3))] }] }),
      mockBlock({ number: 40, transactions: [{ logs: [swap(pool(2)), swap(pool(99))] }] }),
      mockBlock({ number: 85, transactions: [{ logs: [poolCreated(pool(4))] }, { logs: [swap(pool(4))] }] }),
      mockBlock({ number: 95, transactions: [{ logs: [swap(pool(1)), swap(pool(4))] }] }),
    ]
    const options = { blocks, finalized: 80, latest: 100 }

    const portal = await chainPortal(options)
    const lookahead = await runPipe(portal, { range: { from: 0, to: 100 }, lookahead: { minSegmentBlocks: 10 } })

    const fullScan = await runPipe(await chainPortal(options), { range: { from: 0, to: 100 }, lookahead: false })

    expect(lookahead.rows).toEqual(fullScan.rows)
    expect(lookahead.rows.map((r) => [r.block, r.contract])).toEqual([
      [5, pool(1)],
      [40, pool(2)],
      [85, pool(4)],
      [95, pool(1)],
      [95, pool(4)],
    ])

    expect(
      mainRequests(portal).map((r) => ({
        fromBlock: r.query.fromBlock,
        toBlock: r.query.toBlock,
        parentBlockHash: r.query.parentBlockHash,
        address: childRequestsOf(r)[0].address,
      })),
    ).toEqual([
      { fromBlock: 0, toBlock: 80, parentBlockHash: undefined, address: [pool(1), pool(2), pool(3)] },
      {
        fromBlock: 81,
        toBlock: 100,
        parentBlockHash: `0x${(80).toString(16).padStart(64, '0')}`,
        address: undefined,
      },
    ])
    expect(lookahead.messages).toContain(
      'factory lookahead: fetching child events by topic from block 81 on, as it is close to the finalized head',
    )
  })

  it('is off unless the factory asks for it', async () => {
    const blocks = [mockBlock({ number: 5, transactions: [{ logs: [poolCreated(pool(1)), swap(pool(1))] }] })]
    const portal = await chainPortal({ blocks, finalized: 100, latest: 100 })

    const factory = contractFactory({
      address: FACTORY,
      event: factoryAbi.PoolCreated,
      childAddressField: 'pool',
      database: await contractFactorySqliteStore({ path: ':memory:' }),
    })

    const rows: unknown[] = []
    await evmPortalStream({
      id: 'test',
      portal: portal.url,
      logger: captureLogger().logger,
      outputs: evmEventDecoder({ range: { from: 0, to: 100 }, contracts: factory, events: { swaps: poolAbi.Swap } }),
    }).pipeTo(
      createTarget({
        write: async ({ read }) => {
          for await (const { data } of read()) {
            rows.push(...data.swaps)
          }
        },
      }),
    )

    expect(rows).toHaveLength(1)
    expect(factory.lookaheadOptions()).toBeNull()
    expect(portal.requests.filter((r) => r.endpoint === 'finalized-stream')).toEqual([])
    expect(mainRequests(portal).map((r) => [r.query.fromBlock, r.query.toBlock, isFiltered(r)])).toEqual([
      [0, 100, false],
    ])
  })

  it('takes the defaults for `lookahead: true` and refuses limits below one', async () => {
    const options = { address: FACTORY, event: factoryAbi.PoolCreated, childAddressField: 'pool' as const }
    const database = await contractFactorySqliteStore({ path: ':memory:' })

    expect(contractFactory({ ...options, database, lookahead: true }).lookaheadOptions()).toEqual({
      maxAddresses: 5_000,
      minSegmentBlocks: 10_000,
    })
    expect(() =>
      evmEventDecoder({
        range: { from: 0 },
        contracts: contractFactory({ ...options, database, lookahead: { minSegmentBlocks: 0 } }),
        events: { swaps: poolAbi.Swap },
      }),
    ).toThrow('lookahead.minSegmentBlocks must be a positive integer, got 0')
    expect(() =>
      contractFactory({ ...options, database, lookahead: { maxAddresses: 1.5 } }).lookaheadOptions(),
    ).toThrow('lookahead.maxAddresses must be a positive integer, got 1.5')
  })

  it('fetches by topic alone when the dataset reports no finalized head', async () => {
    const blocks = [mockBlock({ number: 5, transactions: [{ logs: [poolCreated(pool(1)), swap(pool(1))] }] })]
    const portal = await chainPortal({ blocks, finalized: null, latest: 50 })

    const { rows, messages } = await runPipe(portal, { range: { from: 0, to: 50 }, lookahead: { minSegmentBlocks: 1 } })

    expect(rows.map((r) => r.block)).toEqual([5])
    expect(portal.requests.filter((r) => r.endpoint === 'finalized-stream')).toEqual([])
    expect(mainRequests(portal).every((r) => !isFiltered(r))).toBe(true)
    expect(messages).toContain(
      'factory lookahead: fetching child events by topic from block 0 on, as the dataset reports no finalized head',
    )
  })

  it('does not scan when the stream starts close to the finalized head', async () => {
    const portal = await chainPortal({ blocks: [], finalized: 95, latest: 100 })

    await runPipe(portal, { range: { from: 90, to: 100 }, lookahead: { minSegmentBlocks: 10 } })

    expect(portal.requests.filter((r) => r.endpoint === 'finalized-stream')).toEqual([])
    expect(mainRequests(portal).every((r) => !isFiltered(r))).toBe(true)
  })

  it('falls back to topic-only queries from the block where the children stop fitting', async () => {
    const blocks = [
      mockBlock({ number: 5, transactions: [{ logs: [poolCreated(pool(1)), poolCreated(pool(2))] }] }),
      mockBlock({ number: 40, transactions: [{ logs: [poolCreated(pool(3))] }] }),
      mockBlock({ number: 60, transactions: [{ logs: [swap(pool(1)), swap(pool(3))] }] }),
    ]
    const portal = await chainPortal({ blocks, finalized: 100, latest: 100 })

    const { rows } = await runPipe(portal, {
      range: { from: 0, to: 100 },
      // Two child-event requests carry the list twice: two children fill four addresses
      lookahead: { minSegmentBlocks: 10, maxAddresses: 4 },
    })

    expect(rows.map((r) => [r.block, r.contract])).toEqual([
      [60, pool(1)],
      [60, pool(3)],
    ])
    expect(
      mainRequests(portal).map((r) => [r.query.fromBlock, r.query.toBlock, childRequestsOf(r)[0].address]),
    ).toEqual([
      [0, 39, [pool(1), pool(2)]],
      [40, 100, undefined],
    ])
  })

  it('filters a resumed run by the children an earlier run stored', async () => {
    const blocks = [
      mockBlock({ number: 5, transactions: [{ logs: [poolCreated(pool(1))] }] }),
      mockBlock({ number: 60, transactions: [{ logs: [poolCreated(pool(2))] }] }),
      mockBlock({ number: 70, transactions: [{ logs: [swap(pool(1)), swap(pool(2))] }] }),
    ]
    const options = { blocks, finalized: 100, latest: 100 }

    const first = await runPipe(await chainPortal(options), { range: { from: 0, to: 50 }, lookahead: false })

    const portal = await chainPortal(options)
    const { rows } = await runPipe(portal, {
      range: { from: 0, to: 100 },
      lookahead: { minSegmentBlocks: 10 },
      database: first.database,
      resumeAt: 50,
    })

    expect(rows.map((r) => [r.block, r.contract])).toEqual([
      [70, pool(1)],
      [70, pool(2)],
    ])
    expect(
      mainRequests(portal).map((r) => [r.query.fromBlock, r.query.toBlock, childRequestsOf(r)[0].address]),
    ).toEqual([[51, 100, [pool(1), pool(2)]]])
  })

  it('fetches by topic alone when the scan fails', async () => {
    const blocks = [mockBlock({ number: 5, transactions: [{ logs: [poolCreated(pool(1)), swap(pool(1))] }] })]
    const portal = await chainPortal({
      blocks,
      finalized: 100,
      latest: 100,
      delay: ({ endpoint }) => {
        if (endpoint === 'finalized-stream') throw new Error('scan is broken')
      },
    })

    const { rows, messages } = await runPipe(portal, {
      range: { from: 0, to: 100 },
      lookahead: { minSegmentBlocks: 1 },
    })

    expect(rows.map((r) => r.block)).toEqual([5])
    expect(mainRequests(portal).every((r) => !isFiltered(r))).toBe(true)
    expect(messages.some((m) => m.includes('as the factory scan failed'))).toBe(true)
  })

  it('filters by every stored child the decoder accepts, numeric params included', async () => {
    const database = await contractFactorySqliteStore({ path: ':memory:' })
    const factory = contractFactory({
      address: FACTORY,
      event: { event: factoryAbi.PoolCreated, params: { fee: 3000 } },
      childAddressField: 'pool',
      database,
    })
    await factory.migrate()

    const child = (address: Hex, fee: number, factoryAddress: string = FACTORY) => ({
      childAddress: address,
      factoryAddress,
      blockNumber: 1,
      transactionIndex: 0,
      logIndex: 0,
      event: { token0: WETH, token1: USDC, fee, tickSpacing: 10, pool: address },
    })
    await database.save([child(pool(1), 3000), child(pool(2), 500), child(pool(3), 3000, IMPOSTOR_FACTORY)])

    expect(await factory.getContract(pool(1))).not.toBeNull()
    expect(await factory.storedChildAddresses()).toEqual([pool(1)])
  })

  it('leaves values that are not addresses out of the filter', async () => {
    const blocks = [
      mockBlock({ number: 5, transactions: [{ logs: [poolCreated(pool(1)), poolCreated(pool(2), USDT)] }] }),
      mockBlock({ number: 20, transactions: [{ logs: [swap(pool(1))] }] }),
    ]
    const portal = await chainPortal({ blocks, finalized: 100, latest: 100 })
    const database = await contractFactorySqliteStore({ path: ':memory:' })

    const factory = contractFactory({
      address: FACTORY,
      event: factoryAbi.PoolCreated,
      childAddressField: (event) => (event.token0 === WETH ? event.pool.toUpperCase().replace('0X', '0x') : 'unknown'),
      database,
      lookahead: { minSegmentBlocks: 10 },
    })

    const rows: unknown[] = []
    await evmPortalStream({
      id: 'test',
      portal: portal.url,
      logger: captureLogger().logger,
      outputs: evmEventDecoder({ range: { from: 0, to: 100 }, contracts: factory, events: { swaps: poolAbi.Swap } }),
    }).pipeTo(
      createTarget({
        write: async ({ read }) => {
          for await (const { data } of read()) {
            rows.push(...data.swaps)
          }
        },
      }),
    )

    // A checksummed child never equals a log address, so the decoder keeps nothing either way
    expect(rows).toEqual([])
    expect(mainRequests(portal).map((r) => childRequestsOf(r)[0].address)).toEqual([[pool(1)]])
  })

  it('stops the scan when the pipe ends', async () => {
    const stop = vi.spyOn(FactoryDiscovery.prototype, 'stop')
    const portal = await chainPortal({ blocks: [], finalized: 1_000, latest: 1_000 })

    await runPipe(portal, { range: { from: 0, to: 500 }, lookahead: { minSegmentBlocks: 10 } })

    expect(stop).toHaveBeenCalled()
  })

  it('restarts the scan after a rollback', async () => {
    const portal = await chainPortal({ blocks: [], finalized: 1_000, latest: 1_000 })
    const factory = contractFactory({
      address: FACTORY,
      event: factoryAbi.PoolCreated,
      childAddressField: 'pool',
      database: await contractFactorySqliteStore({ path: ':memory:' }),
    })
    await factory.migrate()

    const lookahead = new FactoryLookahead({
      factory,
      childRequests: (address) => [{ address, topic0: [SWAP_TOPIC] }],
      maxAddresses: 100,
      minSegmentBlocks: 10,
    })
    const args = { from: 0, to: 500, portal: new PortalClient({ url: portal.url }), logger: captureLogger().logger }

    await expect(lookahead.plan(args)).resolves.toMatchObject({ to: 500 })
    lookahead.rollback()
    await expect(lookahead.plan(args)).resolves.toMatchObject({ to: 500 })
    lookahead.stop()

    expect(portal.requests.filter((r) => r.endpoint === 'finalized-stream').map((r) => r.query.fromBlock)).toEqual([
      0, 0,
    ])
  })

  describe('gives the same events as topic-only queries on random chains', () => {
    const scenarios = [
      { minSegmentBlocks: 1, maxAddresses: 10_000, blocksPerResponse: 13, finalized: 300, withParams: false },
      { minSegmentBlocks: 25, maxAddresses: 10_000, blocksPerResponse: Infinity, finalized: 300, withParams: true },
      { minSegmentBlocks: 5, maxAddresses: 12, blocksPerResponse: 7, finalized: 300, withParams: false },
      { minSegmentBlocks: 5, maxAddresses: 12, blocksPerResponse: 7, finalized: 300, withParams: true },
      { minSegmentBlocks: 20, maxAddresses: 10_000, blocksPerResponse: 50, finalized: 180, withParams: false },
      {
        minSegmentBlocks: 3,
        maxAddresses: 40,
        blocksPerResponse: 11,
        finalized: 260,
        withParams: true,
        slowScan: true,
      },
      {
        minSegmentBlocks: 2,
        maxAddresses: 10_000,
        blocksPerResponse: 5,
        finalized: 299,
        withParams: false,
        slowScan: true,
      },
    ]

    let filteredQueries = 0

    for (const [index, scenario] of scenarios.entries()) {
      for (const seed of [1, 2, 3]) {
        it(`scenario ${index}, seed ${seed}`, async () => {
          const { blocks, pools } = randomChain(seed * 100 + index, 300)
          const from = seed * 10
          const range = { from, to: 300 }
          const accepted = pools.filter((p) => !scenario.withParams || p.token0 === WETH)

          const chain = {
            blocks,
            finalized: scenario.finalized,
            latest: 300,
            blocksPerResponse: scenario.blocksPerResponse,
          }

          const portal = await chainPortal({
            ...chain,
            delay: scenario.slowScan
              ? async ({ endpoint }) => {
                  if (endpoint === 'finalized-stream') await new Promise((resolve) => setTimeout(resolve, 2))
                }
              : undefined,
          })

          const lookahead = await runPipe(portal, {
            range,
            withParams: scenario.withParams,
            lookahead: { minSegmentBlocks: scenario.minSegmentBlocks, maxAddresses: scenario.maxAddresses },
            unbuffered: scenario.slowScan,
          })
          const fullScan = await runPipe(await chainPortal(chain), {
            range,
            withParams: scenario.withParams,
            lookahead: false,
          })

          // A failed scan falls back to topic-only queries and would make this comparison vacuous
          expect(lookahead.messages.filter((m) => m.includes('scan failed'))).toEqual([])

          expect(fullScan.rows.length).toBeGreaterThan(0)
          expect(lookahead.rows).toEqual(fullScan.rows)

          assertQueriesCover({
            portal,
            pools: accepted,
            range,
            finalized: scenario.finalized,
            maxAddresses: scenario.maxAddresses,
          })

          filteredQueries += mainRequests(portal).filter(isFiltered).length
        })
      }
    }

    it('filtered at least some queries in these scenarios', () => {
      expect(filteredQueries).toBeGreaterThan(scenarios.length)
    })
  })
})
