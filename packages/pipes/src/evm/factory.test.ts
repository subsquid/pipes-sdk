import { event, indexed } from '@subsquid/evm-abi'
import * as p from '@subsquid/evm-codec'
import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from 'vitest'

import { createTarget } from '~/core/target.js'
import { createMemoryTarget } from '~/targets/memory/memory-target.js'
import { encodeEvent, mockBlock, mockEvmPortalStream, resetMockBlockCounter } from '~/testing/evm/index.js'
import { MockPortal, mockPortal, readAll } from '~/testing/index.js'

import { FactoryEvent, evmEventDecoder } from './evm-decoder.js'
import { evmPortalStream } from './evm-portal-source.js'
import { Factory, InternalFactoryEvent, contractFactory } from './factory.js'
import { contractFactorySqliteStore } from './factory-adapters/sqlite.js'

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

const SWAP_ABI = [
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
] as const

const factoryAbi = {
  PoolCreated: event(
    '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118',
    'PoolCreated(address,address,uint24,int24,address)',
    {
      token0: indexed(p.address),
      token1: indexed(p.address),
      fee: indexed(p.uint24),
      tickSpacing: p.int24,
      pool: p.address,
    },
  ),
}

const poolAbi = {
  Swap: event(
    '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
    'Swap(address,address,int256,int256,uint160,uint128,int24)',
    {
      sender: indexed(p.address),
      recipient: indexed(p.address),
      amount0: p.int256,
      amount1: p.int256,
      sqrtPriceX96: p.uint160,
      liquidity: p.uint128,
      tick: p.int24,
    },
  ),
}

// ── Addresses ──

const UNISWAP_FACTORY = '0x1f98431c8ad98523631ae4a59f267346ea31f984' as `0x${string}`
const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as `0x${string}`
const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' as `0x${string}`
const USDT = '0xdac17f958d2ee523a2206206994597c13d831ec7' as `0x${string}`
const WETH_USDC_POOL = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8' as `0x${string}`
const USDT_USDC_POOL = '0x9db9e0e53058c89e5b94e29621a205198648425b' as `0x${string}`
const UNKNOWN_POOL = '0xaaaaaac3a0ff1de082011efddc58f1908eb6e6d8' as `0x${string}`
const SENDER = '0xdef1cafe0000000000000000000000000000dead' as `0x${string}`
const RECIPIENT = '0xbeef0000000000000000000000000000deadbeef' as `0x${string}`

// ── Helpers ──

function encodePoolCreated(pool: `0x${string}`, token0 = WETH, token1 = USDC, fee = 3000, tickSpacing = 10) {
  return encodeEvent({
    abi: POOL_CREATED_ABI,
    eventName: 'PoolCreated',
    address: UNISWAP_FACTORY,
    args: { token0, token1, fee, tickSpacing, pool },
  })
}

function encodeSwap(address: `0x${string}`) {
  return encodeEvent({
    abi: SWAP_ABI,
    eventName: 'Swap',
    address,
    args: { sender: SENDER, recipient: RECIPIENT, amount0: 1n, amount1: 2n, sqrtPriceX96: 3n, liquidity: 4n, tick: 5 },
  })
}

/** Block 1: PoolCreated for WETH/USDC pool. Block 2: swap from unknown pool (skipped) + swap from known pool (decoded). */
async function createSimpleChildPortal() {
  return mockEvmPortalStream({
    blocks: [
      mockBlock({
        number: 1,
        transactions: [{ logs: [encodePoolCreated(WETH_USDC_POOL)] }],
      }),
      mockBlock({
        number: 2,
        transactions: [{ logs: [encodeSwap(UNKNOWN_POOL)] }, { logs: [encodeSwap(WETH_USDC_POOL)] }],
      }),
    ],
  })
}

/** Block 1: two PoolCreated (WETH + USDT). Block 2: swap from WETH pool. Block 3: swap from USDT pool. */
async function createFilteredFactoryPortal() {
  return mockEvmPortalStream({
    blocks: [
      mockBlock({
        number: 1,
        transactions: [{ logs: [encodePoolCreated(WETH_USDC_POOL, WETH), encodePoolCreated(USDT_USDC_POOL, USDT)] }],
      }),
      mockBlock({
        number: 2,
        transactions: [{ logs: [encodeSwap(WETH_USDC_POOL)] }],
      }),
      mockBlock({
        number: 3,
        transactions: [{ logs: [encodeSwap(USDT_USDC_POOL)] }],
      }),
    ],
  })
}

describe('Factory', () => {
  let portal: MockPortal

  beforeEach(() => {
    resetMockBlockCounter()
  })

  afterEach(async () => {
    await portal?.close()
  })

  it('should support bigint in parent event ', async () => {
    const db = await contractFactorySqliteStore({ path: ':memory:' })
    await db.migrate()

    const entity = {
      childAddress: '0xchild',
      factoryAddress: '0xfactory',
      blockNumber: 1,
      transactionIndex: 0,
      logIndex: 0,
      event: {
        someBigint: 123n,
        nested: { value: 456n },
      },
    }

    await db.save([entity])

    const contracts: any[] = await db.all()

    expect(contracts).toHaveLength(1)
    expect(contracts[0].event['someBigint']).toBe(123n)
    expect(contracts[0].event['nested'].value).toBe(456n)
  })

  it('should decode child event', async () => {
    portal = await createSimpleChildPortal()

    const db = await contractFactorySqliteStore({ path: ':memory:' })

    const poolFactory = contractFactory({
      address: UNISWAP_FACTORY,
      event: factoryAbi.PoolCreated,
      childAddressField: 'pool',
      database: db,
    })

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmEventDecoder({
        range: { from: 1, to: 2 },
        contracts: poolFactory,
        events: {
          swaps: poolAbi.Swap,
        },
      }).pipe((d) => d.swaps),
    })

    const res = await readAll(stream)
    expect(res).toMatchInlineSnapshot(`
      [
        {
          "block": {
            "hash": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "number": 2,
          },
          "contract": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
          "event": {
            "amount0": 1n,
            "amount1": 2n,
            "liquidity": 4n,
            "recipient": "0xbeef0000000000000000000000000000deadbeef",
            "sender": "0xdef1cafe0000000000000000000000000000dead",
            "sqrtPriceX96": 3n,
            "tick": 5,
          },
          "factory": {
            "blockNumber": 1,
            "contract": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
            "event": {
              "fee": 3000,
              "pool": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
              "tickSpacing": 10,
              "token0": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "token1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            },
          },
          "rawEvent": {
            "address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
            "data": "0x00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000005",
            "logIndex": 1,
            "topics": [
              "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
              "0x000000000000000000000000def1cafe0000000000000000000000000000dead",
              "0x000000000000000000000000beef0000000000000000000000000000deadbeef",
            ],
            "transactionHash": "0x934c7927ff44855bb2839a79a5bcd7f5b8241403acd5bebca71470d282b34712",
            "transactionIndex": 1,
          },
          "timestamp": 1970-01-01T00:33:20.000Z,
        },
      ]
    `)

    const contracts = await db.all()
    expect(contracts).toMatchInlineSnapshot(`
      [
        {
          "blockNumber": 1,
          "childAddress": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
          "event": {
            "fee": 3000,
            "pool": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
            "tickSpacing": 10,
            "token0": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "token1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
          },
          "factoryAddress": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
          "logIndex": 0,
          "transactionIndex": 0,
        },
      ]
    `)
  })

  it('should skip null parameter', async () => {
    portal = await createSimpleChildPortal()

    const db = await contractFactorySqliteStore({ path: ':memory:' })
    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmEventDecoder({
        range: { from: 1, to: 2 },
        contracts: contractFactory({
          address: UNISWAP_FACTORY,
          event: factoryAbi.PoolCreated,
          childAddressField: () => null,
          database: db,
        }),
        events: {
          swaps: poolAbi.Swap,
        },
      }).pipe((d) => d.swaps),
    })

    const res = await readAll(stream)
    expect(res).toHaveLength(0)

    const contracts = await db.all()
    expect(contracts).toHaveLength(0)
  })

  it('should set event with same topic to correct factory', async () => {
    portal = await createSimpleChildPortal()

    const db = await contractFactorySqliteStore({ path: ':memory:' })
    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: {
        v1: evmEventDecoder({
          range: { from: 1, to: 2 },
          contracts: contractFactory({
            address: '0x00000000000000000000000000000000000000000',
            event: factoryAbi.PoolCreated,
            childAddressField: 'pool',
            database: db,
          }),
          events: {
            swaps: poolAbi.Swap,
          },
        }),
        v2: evmEventDecoder({
          range: { from: 1, to: 2 },
          contracts: contractFactory({
            address: UNISWAP_FACTORY,
            event: factoryAbi.PoolCreated,
            childAddressField: 'pool',
            database: db,
          }),
          events: {
            swaps: poolAbi.Swap,
          },
        }),
      },
    })

    let v1: any[] = []
    let v2: any[] = []
    for await (const chunk of stream) {
      v1 = [...v1, ...chunk.data.v1.swaps]
      v2 = [...v2, ...chunk.data.v2.swaps]
    }

    expect(v1).toHaveLength(0)
    expect(v2).toHaveLength(1)
  })

  it('should handle fork', async () => {
    const FORKED_POOL = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d7' as const

    const poolCreatedLog = encodePoolCreated(FORKED_POOL)
    const swapLog = encodeSwap(WETH_USDC_POOL)

    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          mockBlock({ number: 1, hash: '0x1' }),
          // this block will be forked
          mockBlock({ number: 2, hash: '0x2', transactions: [{ logs: [poolCreatedLog] }] }),
        ],
        head: { finalized: { number: 1, hash: '0x1' } },
      },
      {
        statusCode: 409,
        data: {
          previousBlocks: [{ number: 1, hash: '0x1' }],
        },
      },
      {
        statusCode: 200,
        data: [
          // swap from a different pool address than the forked PoolCreated registered
          mockBlock({ number: 2, hash: '0x2a', transactions: [{ logs: [swapLog] }] }),
          mockBlock({ number: 3, hash: '0x3a' }),
        ],
        head: { finalized: { number: 3, hash: '0x3a' } },
      },
    ])

    const res: any[] = []

    const db = await contractFactorySqliteStore({ path: ':memory:' })

    await evmPortalStream({
      id: 'test-factory',
      portal: {
        url: portal.url,
      },
      outputs: evmEventDecoder({
        range: { from: 1, to: 3 },
        contracts: contractFactory({
          address: UNISWAP_FACTORY,
          event: factoryAbi.PoolCreated,
          childAddressField: 'pool',
          database: db,
        }),
        events: {
          swaps: poolAbi.Swap,
        },
      }).pipe((d) =>
        d.swaps.map((s) => {
          return {
            ...s,
            blockNumber: s.block.number,
          }
        }),
      ),
    }).pipeTo(
      // A hot sink with a trivial rollback: the subject is the factory's own fork handling, and a
      // finalized-only target (like the memory one) would never be shown the 409 at all.
      createTarget<any>({
        write: async ({ read }) => {
          for await (const batch of read()) {
            if (batch.data.length > 0) {
              res.push(batch.data)
            }
          }
        },
        resolveFork: async (canonicalBlocks) => canonicalBlocks.at(-1) ?? null,
      }),
    )

    expect(res).toHaveLength(0)

    const contracts = await db.all()
    expect(contracts).toHaveLength(0)
  })

  it('should filter factory events by indexed parameters', async () => {
    portal = await createFilteredFactoryPortal()

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmEventDecoder({
        range: { from: 1, to: 2 },
        contracts: contractFactory({
          address: UNISWAP_FACTORY,
          event: {
            event: factoryAbi.PoolCreated,
            params: {
              token0: WETH,
            },
          },
          childAddressField: 'pool',
          database: contractFactorySqliteStore({ path: ':memory:' }),
        }),
        events: {
          swaps: poolAbi.Swap,
        },
      }).pipe((d) => d.swaps),
    })

    const res = await readAll(stream)

    expect(res).toHaveLength(1)
    expect(res[0].contract).toBe(WETH_USDC_POOL)
    expect(res[0].factory?.event.token0).toBe(WETH)
  })

  /**
   * This test addresses a bug that occurred when starting the indexer with one set of factory
   * event parameters, stopping, then restarting it with a different set for the same factory.
   *
   * On the first run, it returned the expected events. After a restart with different params,
   * it returned both events matching the updated params and extra events from the previous run.
   * This was because Factory.getContract only checked if the contract address was in the database,
   * not whether the correct set of parameters had been used.
   */
  it('should only return events matching new factory parameters after second run with different params', async () => {
    const db = await contractFactorySqliteStore({ path: ':memory:' })

    const getPipeline = async (token0: string) => {
      portal = await createFilteredFactoryPortal()

      return evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: evmEventDecoder({
          range: { from: 1, to: 2 },
          contracts: contractFactory({
            address: UNISWAP_FACTORY,
            event: {
              event: factoryAbi.PoolCreated,
              params: {
                token0,
              },
            },
            childAddressField: 'pool',
            database: db,
          }),
          events: {
            swaps: poolAbi.Swap,
          },
        }).pipe((d) => d.swaps),
      })
    }

    const firstRun = await getPipeline(WETH)
    const firstRunRes = await readAll(firstRun)
    expect(firstRunRes).toHaveLength(1)
    expect(firstRunRes[0].contract).toBe(WETH_USDC_POOL)
    expect(firstRunRes[0].factory?.event.token0).toBe(WETH)

    const secondRun = await getPipeline(USDT)
    const secondRunRes = await readAll(secondRun)
    expect(secondRunRes).toHaveLength(1)
    expect(secondRunRes[0].contract).toBe(USDT_USDC_POOL)
    expect(secondRunRes[0].factory?.event.token0).toBe(USDT)
  })

  it('normalizes params when reading all contracts from database', async () => {
    portal = await createSimpleChildPortal()

    const contractsFactory = contractFactory({
      address: UNISWAP_FACTORY,
      event: {
        event: factoryAbi.PoolCreated,
        params: {
          // Parameter in different case than emitted event
          token0: WETH.toUpperCase() as `0x${string}`,
          token1: USDC.toUpperCase() as `0x${string}`,
        },
      },
      childAddressField: 'pool',
      database: await contractFactorySqliteStore({ path: ':memory:' }),
    })

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmEventDecoder({
        range: { from: 1, to: 2 },
        contracts: contractsFactory,
        events: {
          swaps: poolAbi.Swap,
        },
      }).pipe((d) => d.swaps),
    })

    await readAll(stream)

    const contracts = await contractsFactory.getAllContracts()
    expect(contracts).toMatchInlineSnapshot(`
      [
        {
          "blockNumber": 1,
          "childAddress": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
          "event": {
            "fee": 3000,
            "pool": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
            "tickSpacing": 10,
            "token0": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "token1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
          },
          "factoryAddress": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
          "logIndex": 0,
          "transactionIndex": 0,
        },
      ]
    `)
  })
})

describe('Factory types', () => {
  const args = {
    token0: indexed(p.address),
    token1: indexed(p.address),
    fee: indexed(p.uint24),
    tickSpacing: p.int24,
    pool: p.address,
  }
  type Args = typeof args

  it('InternalFactoryEvent generates the return types properly', () => {
    type Result = InternalFactoryEvent<Args>
    expectTypeOf<Result>().toEqualTypeOf<{
      childAddress: string
      factoryAddress: string
      blockNumber: number
      transactionIndex: number
      logIndex: number
      event: {
        token0: string
        token1: string
        fee: number
        tickSpacing: number
        pool: string
      }
    }>()
  })

  it('getAllContracts returns a typed response according to event params', () => {
    type Result = Awaited<ReturnType<Factory<Args>['getAllContracts']>>
    expectTypeOf<Result>().toEqualTypeOf<InternalFactoryEvent<Args>[]>
  })

  it('getContract returns a typed response according to event params', () => {
    type Result = Awaited<ReturnType<Factory<Args>['getContract']>>
    expectTypeOf<Result>().toEqualTypeOf<FactoryEvent<{
      token0: string
      token1: string
      fee: number
      tickSpacing: number
      pool: string
    }> | null>()
  })

  it('getContract returns a typed response according to event params', () => {
    type Result = Awaited<ReturnType<Factory<Args>['getContract']>>
    expectTypeOf<Result>().toEqualTypeOf<FactoryEvent<{
      token0: string
      token1: string
      fee: number
      tickSpacing: number
      pool: string
    }> | null>()
  })

  it('should type factory returns according to params passed', async () => {
    const poolFactory = contractFactory({
      address: '',
      event: {
        event: factoryAbi.PoolCreated,
        params: {
          token0: '',
        },
      },
      childAddressField: 'pool',
      database: contractFactorySqliteStore({ path: ':memory:' }),
    })

    type Result = Awaited<ReturnType<(typeof poolFactory)['getContract']>>
    type Expected = FactoryEvent<{
      readonly token0: string
      readonly token1: string
      readonly fee: number
      readonly tickSpacing: number
      readonly pool: string
    }> | null
    expectTypeOf<Result>().toEqualTypeOf<Expected>()
  })
})
