import { event, indexed } from '@subsquid/evm-abi'
import * as p from '@subsquid/evm-codec'
import { afterEach, beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest'

import { PortalRange, QueryAwareTransformer } from '~/core/index.js'
import { encodeEvent, mockBlock, mockEvmPortalStream, resetMockBlockCounter } from '~/testing/evm/index.js'
import { MockPortal, MockResponse, mockMetricsServer, mockPortal, readAll, testLogger } from '~/testing/index.js'

import { commonAbis } from './abi/common.js'
import {
  DecodedEventPipeArgs,
  EventFilter,
  EventWithArgs,
  EventWithArgsInput,
  EventsMap,
  IndexedKeys,
  IndexedParams,
  IndexedParamsInput,
  evmEventDecoder,
} from './evm-decoder.js'
import { EvmQueryBuilder } from './evm-query-builder.js'
import { evmPortalStream } from './evm-stream.js'
import { contractFactory } from './factory.js'
import { contractFactorySqliteStore } from './factory-adapters/sqlite.js'

async function captureQueryBuilder(
  decoder: QueryAwareTransformer<any, any, EvmQueryBuilder<any>>,
  logger = testLogger(),
) {
  const query = new EvmQueryBuilder()
  await decoder.setupQuery({
    query,
    logger,
    // portal: {} as any,
  })
  return query
}

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

const swapAbi = {
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

const FACTORY_ADDRESS = '0x1f98431c8ad98523631ae4a59f267346ea31f984'

describe('evmEventDecoder types', () => {
  it('type IndexedKeys picks indexed params from ERC20 Transfer', async () => {
    type Result = IndexedKeys<(typeof commonAbis.erc20.events.Transfer)['params']>
    expectTypeOf<Result>().toEqualTypeOf<'from' | 'to'>()
  })

  it('type IndexedParams picks indexed params from ERC20 Transfer', () => {
    type Result = IndexedParams<typeof commonAbis.erc20.events.Transfer>
    expectTypeOf<Result>().toEqualTypeOf<{
      from?: string[]
      to?: string[]
    }>()
  })

  it('type IndexedParamsInput picks indexed params from ERC20 Transfer', () => {
    type Result = IndexedParamsInput<typeof commonAbis.erc20.events.Transfer>
    expectTypeOf<Result>().toEqualTypeOf<{
      from?: string | string[]
      to?: string | string[]
    }>()
  })

  it("type IndexedParams doesn't pick not indexed params from ERC20 Transfer", () => {
    type Result = IndexedParams<typeof commonAbis.erc20.events.Transfer>
    expectTypeOf<Result>().not.toEqualTypeOf<{
      from?: string
      to?: string
      value?: number
    }>()
  })

  it('type EventWithArgs only allows for indexed params', () => {
    type Result = EventWithArgs<typeof commonAbis.erc20.events.Transfer>

    expectTypeOf<Result>().toEqualTypeOf<{
      event: typeof commonAbis.erc20.events.Transfer
      params: {
        from?: string[]
        to?: string[]
      }
    }>()

    expectTypeOf<Result>().not.toEqualTypeOf<{
      event: typeof commonAbis.erc20.events.Transfer
      params: {
        from?: string
        to?: string
        value?: bigint
      }
    }>()
  })

  it('type EventWithArgsInput only allows for indexed params', () => {
    type Result = EventWithArgsInput<typeof commonAbis.erc20.events.Transfer>
    expectTypeOf<Result>().toEqualTypeOf<{
      event: typeof commonAbis.erc20.events.Transfer
      params: {
        from?: string | string[]
        to?: string | string[]
      }
    }>()
  })

  it('type EventMap accepts only event ABI', () => {
    type Result = EventsMap<{
      Approval: typeof commonAbis.erc20.events.Approval
      Transfer: typeof commonAbis.erc20.events.Transfer
    }>

    expectTypeOf<Result>().toEqualTypeOf<{
      readonly Approval: typeof commonAbis.erc20.events.Approval
      readonly Transfer: typeof commonAbis.erc20.events.Transfer
    }>()
  })

  it('type EventMapWithArgs accepts both forms of events', () => {
    type Result = EventsMap<typeof commonAbis.erc20.events>
    expectTypeOf<Result>().toExtend<{
      Transfer:
        | typeof commonAbis.erc20.events.Transfer
        | {
            event: typeof commonAbis.erc20.events.Transfer
            params: {
              to?: string | string[]
              from?: string | string[]
            }
          }
      Approval:
        | typeof commonAbis.erc20.events.Approval
        | {
            event: typeof commonAbis.erc20.events.Approval
            params: {
              owner?: string | string[]
              spender?: string | string[]
            }
          }
    }>()
  })

  it('type EventsMap can receive mixed keys of EventMap and EventsMap', () => {
    type Result = EventsMap<typeof commonAbis.erc20.events>
    expectTypeOf<Result>().toExtend<{
      Transfer:
        | typeof commonAbis.erc20.events.Transfer
        | {
            event: typeof commonAbis.erc20.events.Transfer
            params: {
              from?: string | string[]
              to?: string | string[]
            }
          }
      Approval:
        | typeof commonAbis.erc20.events.Approval
        | {
            event: typeof commonAbis.erc20.events.Approval
            params: {
              owner?: string | string[]
              spender?: string | string[]
            }
          }
    }>()
  })

  it('type EventsMap should not receive not defined indexed params', () => {
    type Result = EventsMap<{
      Approval: {
        event: typeof commonAbis.erc20.events.Approval
        params: { spender: string; owner: string }
      }
    }>

    expectTypeOf<Result['Approval']>().not.toEqualTypeOf<{
      event: typeof commonAbis.erc20.events.Approval
      params: {
        owner?: string | string[]
        spender?: string | string
        // Values isn't indexed
        value?: bigint | bigint[]
      }
    }>()
  })

  it('type DecodedEventPipeArgs should receive both types of event definition', () => {
    type Result = DecodedEventPipeArgs<typeof commonAbis.erc20.events, string[]>

    expectTypeOf<Result>().toExtend<{
      range: PortalRange
      events: {
        Transfer:
          | typeof commonAbis.erc20.events.Transfer
          | {
              event: typeof commonAbis.erc20.events.Transfer
              params: {
                from?: string | string[]
                to?: string | string[]
              }
            }
        Approval:
          | typeof commonAbis.erc20.events.Approval
          | {
              event: typeof commonAbis.erc20.events.Approval
              params: {
                owner?: string | string[]
                spender?: string | string[]
              }
            }
      }
    }>()
  })
})

describe('EventFilter type', () => {
  it('accepts a simple AbiEvent', () => {
    const filter: EventFilter<typeof commonAbis.erc20.events.Transfer> = commonAbis.erc20.events.Transfer
    expectTypeOf(filter).toMatchTypeOf<EventFilter<typeof commonAbis.erc20.events.Transfer>>()
  })

  it('accepts the filtered form with event and params', () => {
    const filter: EventFilter<typeof commonAbis.erc20.events.Transfer> = {
      event: commonAbis.erc20.events.Transfer,
      params: { from: '0x1' },
    }
    expectTypeOf(filter).toMatchTypeOf<EventFilter<typeof commonAbis.erc20.events.Transfer>>()
  })

  it('EventFilter is a union of AbiEvent and EventWithArgsInput', () => {
    type Result = EventFilter<typeof commonAbis.erc20.events.Transfer>
    expectTypeOf<typeof commonAbis.erc20.events.Transfer>().toMatchTypeOf<Result>()
    expectTypeOf<EventWithArgsInput<typeof commonAbis.erc20.events.Transfer>>().toMatchTypeOf<Result>()
  })
})

describe('evmEventDecoder queries', () => {
  it('should build query for events without params', async () => {
    const range = { from: 0, to: 100 }
    const contracts = ['0x123']

    const decoder = evmEventDecoder({
      range,
      contracts: contracts,
      events: {
        Transfer: commonAbis.erc20.events.Transfer,
      },
    })

    const capturedQueryBuilder = await captureQueryBuilder(decoder)

    const requests = capturedQueryBuilder.getRequests()
    const fields = capturedQueryBuilder.getFields()

    expect(requests).toHaveLength(1)
    expect(requests[0].request?.logs).toBeDefined()
    expect(requests[0].request?.logs?.[0]?.topic0).toEqual([commonAbis.erc20.events.Transfer.topic])
    expect(requests[0].request?.logs?.[0]?.topic1).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.topic2).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.topic3).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.address).toEqual(contracts)
    expect(requests[0].request?.logs?.[0]?.transaction).toBe(true)
    expect(fields).toMatchObject({
      block: {
        number: true,
        hash: true,
        timestamp: true,
      },
      transaction: {
        from: true,
        to: true,
        hash: true,
        sighash: true,
      },
      log: {
        address: true,
        topics: true,
        data: true,
        transactionHash: true,
        logIndex: true,
        transactionIndex: true,
      },
    })
  })

  it('should build query batching events without params together', async () => {
    const range = { from: 0, to: 100 }
    const contracts = ['0x123']

    const decoder = evmEventDecoder({
      range,
      contracts: contracts,
      events: {
        Transfer: commonAbis.erc20.events.Transfer,
        Approval: commonAbis.erc20.events.Approval,
      },
    })

    const capturedQueryBuilder = await captureQueryBuilder(decoder)

    const requests = capturedQueryBuilder.getRequests()
    const fields = capturedQueryBuilder.getFields()

    expect(requests).toHaveLength(1)
    expect(requests[0].request?.logs).toBeDefined()
    expect(requests[0].request?.logs?.[0]?.topic0).toEqual([
      commonAbis.erc20.events.Transfer.topic,
      commonAbis.erc20.events.Approval.topic,
    ])
    expect(requests[0].request?.logs?.[0]?.topic1).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.topic2).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.topic3).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.address).toEqual(contracts)
    expect(requests[0].request?.logs?.[0]?.transaction).toBe(true)
    expect(fields).toMatchObject({
      block: {
        number: true,
        hash: true,
        timestamp: true,
      },
      transaction: {
        from: true,
        to: true,
        hash: true,
        sighash: true,
      },
      log: {
        address: true,
        topics: true,
        data: true,
        transactionHash: true,
        logIndex: true,
        transactionIndex: true,
      },
    })
  })

  it('should build query with corresponding topics for each param', async () => {
    const range = { from: 0, to: 100 }
    const contracts = ['0x123']

    // `from` is topic1 on ERC20 Transfer event
    const fromParamDecoder = evmEventDecoder({
      range,
      contracts: contracts,
      events: {
        Transfer: {
          event: commonAbis.erc20.events.Transfer,
          params: {
            from: '0x1',
          },
        },
      },
    })
    const fromDecoder = await captureQueryBuilder(fromParamDecoder)
    const fromRequests = fromDecoder.getRequests()
    expect(fromRequests[0].request?.logs?.[0]?.topic0).toEqual([commonAbis.erc20.events.Transfer.topic])
    expect(fromRequests[0].request?.logs?.[0]?.topic1).toEqual([
      '0x0000000000000000000000000000000000000000000000000000000000000001',
    ])
    expect(fromRequests[0].request?.logs?.[0]?.topic2).toEqual(undefined)
    expect(fromRequests[0].request?.logs?.[0]?.topic3).toEqual(undefined)

    // `from` is topic2 on ERC20 Transfer event
    const toParamDecoder = evmEventDecoder({
      range,
      contracts: contracts,
      events: {
        Transfer: {
          event: commonAbis.erc20.events.Transfer,
          params: {
            to: '0x2',
          },
        },
      },
    })
    const toDecoder = await captureQueryBuilder(toParamDecoder)
    const toRequests = toDecoder.getRequests()
    expect(toRequests[0].request?.logs?.[0]?.topic0).toEqual([commonAbis.erc20.events.Transfer.topic])
    expect(toRequests[0].request?.logs?.[0]?.topic1).toEqual(undefined)
    expect(toRequests[0].request?.logs?.[0]?.topic2).toEqual([
      '0x0000000000000000000000000000000000000000000000000000000000000002',
    ])
    expect(fromRequests[0].request?.logs?.[0]?.topic3).toEqual(undefined)
  })

  it('should build query for events with params', async () => {
    const range = { from: 0, to: 100 }
    const contracts = ['0x123']

    const decoder = evmEventDecoder({
      range,
      contracts: contracts,
      events: {
        Transfer: {
          event: commonAbis.erc20.events.Transfer,
          params: {
            from: '0x1',
            to: '0x2',
          },
        },
      },
    })

    const capturedQueryBuilder = await captureQueryBuilder(decoder)

    const requests = capturedQueryBuilder.getRequests()
    const fields = capturedQueryBuilder.getFields()

    expect(requests).toHaveLength(1)
    expect(requests[0].request?.logs).toBeDefined()
    expect(requests[0].request?.logs?.[0]?.topic0).toEqual([commonAbis.erc20.events.Transfer.topic])
    expect(requests[0].request?.logs?.[0]?.topic1).toEqual([
      '0x0000000000000000000000000000000000000000000000000000000000000001',
    ])
    expect(requests[0].request?.logs?.[0]?.topic2).toEqual([
      '0x0000000000000000000000000000000000000000000000000000000000000002',
    ])
    expect(requests[0].request?.logs?.[0]?.topic3).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.address).toEqual(contracts)
    expect(requests[0].request?.logs?.[0]?.transaction).toBe(true)
    expect(fields).toMatchObject({
      block: {
        number: true,
        hash: true,
        timestamp: true,
      },
      transaction: {
        from: true,
        to: true,
        hash: true,
        sighash: true,
      },
      log: {
        address: true,
        topics: true,
        data: true,
        transactionHash: true,
        logIndex: true,
        transactionIndex: true,
      },
    })
  })

  it('event params should accept value or array of values', async () => {
    const range = { from: 0, to: 100 }
    const contracts = ['0x123']

    const decoder = evmEventDecoder({
      range,
      contracts: contracts,
      events: {
        Transfer: {
          event: commonAbis.erc20.events.Transfer,
          params: {
            from: ['0x1', '0x2'],
            to: '0x3',
          },
        },
      },
    })

    const capturedQueryBuilder = await captureQueryBuilder(decoder)

    const requests = capturedQueryBuilder.getRequests()

    expect(requests[0].request?.logs?.[0]?.topic0).toEqual([commonAbis.erc20.events.Transfer.topic])
    expect(requests[0].request?.logs?.[0]?.topic1).toEqual([
      '0x0000000000000000000000000000000000000000000000000000000000000001',
      '0x0000000000000000000000000000000000000000000000000000000000000002',
    ])
    expect(requests[0].request?.logs?.[0]?.topic2).toEqual([
      '0x0000000000000000000000000000000000000000000000000000000000000003',
    ])
    expect(requests[0].request?.logs?.[0]?.topic3).toEqual(undefined)
  })

  it('should build query with mixed events (with and without params)', async () => {
    const range = { from: 0, to: 100 }
    const contracts = ['0x123']

    const decoder = evmEventDecoder({
      range,
      contracts: contracts,
      events: {
        Transfer: commonAbis.erc20.events.Transfer,
        Approval: {
          event: commonAbis.erc20.events.Approval,
          params: {
            owner: '0x1',
            spender: '0x2',
          },
        },
      },
    })

    const capturedQueryBuilder = await captureQueryBuilder(decoder)

    const requests = capturedQueryBuilder.getRequests()
    const fields = capturedQueryBuilder.getFields()

    expect(requests).toHaveLength(2)

    expect(requests[0].request?.logs).toBeDefined()
    expect(requests[0].request?.logs?.[0]?.topic0).toEqual([commonAbis.erc20.events.Transfer.topic])
    expect(requests[0].request?.logs?.[0]?.topic1).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.topic2).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.topic3).toEqual(undefined)
    expect(requests[0].request?.logs?.[0]?.address).toEqual(contracts)
    expect(requests[0].request?.logs?.[0]?.transaction).toBe(true)

    expect(requests[1].request?.logs).toBeDefined()
    expect(requests[1].request?.logs?.[0]?.topic0).toEqual([commonAbis.erc20.events.Approval.topic])
    expect(requests[1].request?.logs?.[0]?.topic1).toEqual([
      '0x0000000000000000000000000000000000000000000000000000000000000001',
    ])
    expect(requests[1].request?.logs?.[0]?.topic2).toEqual([
      '0x0000000000000000000000000000000000000000000000000000000000000002',
    ])
    expect(requests[1].request?.logs?.[0]?.topic3).toEqual(undefined)
    expect(requests[1].request?.logs?.[0]?.address).toEqual(contracts)
    expect(requests[1].request?.logs?.[0]?.transaction).toBe(true)

    expect(fields).toMatchObject({
      block: {
        number: true,
        hash: true,
        timestamp: true,
      },
      transaction: {
        from: true,
        to: true,
        hash: true,
        sighash: true,
      },
      log: {
        address: true,
        topics: true,
        data: true,
        transactionHash: true,
        logIndex: true,
        transactionIndex: true,
      },
    })
  })

  it('should build multiple requests when more than one event with params are provided', async () => {
    const range = { from: 0, to: 100 }
    const contracts = ['0x123']

    const decoder = evmEventDecoder({
      range,
      contracts: contracts,
      events: {
        Transfer: {
          event: commonAbis.erc20.events.Transfer,
          params: {
            from: '0x1',
            to: '0x2',
          },
        },
        Approval: {
          event: commonAbis.erc20.events.Approval,
          params: {
            owner: '0x3',
            spender: '0x4',
          },
        },
      },
    })

    const capturedQueryBuilder = await captureQueryBuilder(decoder)

    const requests = capturedQueryBuilder.getRequests()
    const fields = capturedQueryBuilder.getFields()

    expect(requests).toHaveLength(2)
    expect(requests[0].request).toEqual({
      logs: [
        {
          topic0: [commonAbis.erc20.events.Transfer.topic],
          topic1: ['0x0000000000000000000000000000000000000000000000000000000000000001'],
          topic2: ['0x0000000000000000000000000000000000000000000000000000000000000002'],
          topic3: undefined,
          address: contracts,
          transaction: true,
        },
      ],
    })
    expect(requests[1].request).toEqual({
      logs: [
        {
          topic0: [commonAbis.erc20.events.Approval.topic],
          topic1: ['0x0000000000000000000000000000000000000000000000000000000000000003'],
          topic2: ['0x0000000000000000000000000000000000000000000000000000000000000004'],
          topic3: undefined,
          address: contracts,
          transaction: true,
        },
      ],
    })

    expect(fields).toMatchObject({
      block: {
        number: true,
        hash: true,
        timestamp: true,
      },
      transaction: {
        from: true,
        to: true,
        hash: true,
        sighash: true,
      },
      log: {
        address: true,
        topics: true,
        data: true,
        transactionHash: true,
        logIndex: true,
        transactionIndex: true,
      },
    })
  })

  it('should build query for an event with indexed parameters around a non-indexed value', async () => {
    const abi = {
      CustomTransfer: event(
        '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
        'Transfer(address,uint256,address)',
        { from: indexed(p.address), value: p.uint256, to: indexed(p.address) },
      ),
    }

    const decoder = evmEventDecoder({
      range: { from: 0, to: 100 },
      contracts: ['0x123'],
      events: {
        Transfer: {
          event: abi.CustomTransfer,
          params: {
            from: '0x1',
            to: '0x2',
          },
        },
      },
    })
    const capturedQueryBuilder = await captureQueryBuilder(decoder)
    const requests = capturedQueryBuilder.getRequests()

    expect(requests).toHaveLength(1)
    expect(requests[0].request).toEqual({
      logs: [
        {
          topic0: [abi.CustomTransfer.topic],
          topic1: ['0x0000000000000000000000000000000000000000000000000000000000000001'],
          topic2: ['0x0000000000000000000000000000000000000000000000000000000000000002'],
          topic3: undefined,
          address: ['0x123'],
          transaction: true,
        },
      ],
    })
  })

  describe('evmEventDecoder duplicate events', () => {
    it('should log error when duplicate event topics are detected', async () => {
      const logger = testLogger()
      const errorSpy = vi.spyOn(logger, 'error')
      const duplicateEvent = commonAbis.erc20.events.Transfer
      const decoder = evmEventDecoder({
        range: { from: 0, to: 100 },
        contracts: ['0x123'],
        events: {
          transfers1: duplicateEvent,
          transfers2: duplicateEvent,
        },
      })
      await captureQueryBuilder(decoder, logger)

      expect(errorSpy).toHaveBeenCalledTimes(1)

      const errorCall = errorSpy.mock.calls[0][0]
      expect(errorCall).toContain('Duplicate event topics detected')
      expect(errorCall).toContain('transfers1')
      expect(errorCall).toContain('transfers2')
      expect(errorCall).toContain(duplicateEvent.topic)

      errorSpy.mockRestore()
    })

    it('should log error when duplicate event topics are detected across AbiEvent and EventWithArgs', async () => {
      const logger = testLogger()
      const errorSpy = vi.spyOn(logger, 'error')
      const duplicateEvent = commonAbis.erc20.events.Transfer
      const decoder = evmEventDecoder({
        range: { from: 0, to: 100 },
        contracts: ['0x123'],
        events: {
          transfers1: duplicateEvent,
          transfers2: {
            event: duplicateEvent,
            params: {
              from: '0x1',
              to: '0x2',
            },
          },
        },
      })

      await captureQueryBuilder(decoder, logger)

      expect(errorSpy).toHaveBeenCalledTimes(1)

      const errorCall = errorSpy.mock.calls[0][0]
      expect(errorCall).toContain('Duplicate event topics detected')
      expect(errorCall).toContain('transfers1')
      expect(errorCall).toContain('transfers2')
      expect(errorCall).toContain(duplicateEvent.topic)

      errorSpy.mockRestore()
    })

    it('should not log error when all events have unique topics', async () => {
      const logger = testLogger()
      const errorSpy = vi.spyOn(logger, 'error')

      const decoder = evmEventDecoder({
        range: { from: 0, to: 100 },
        contracts: ['0x123'],
        events: {
          transfers: commonAbis.erc20.events.Transfer,
          approvals: commonAbis.erc20.events.Approval,
        },
      })

      await captureQueryBuilder(decoder, logger)

      expect(errorSpy).not.toHaveBeenCalled()

      errorSpy.mockRestore()
    })
  })

  it('should build query for Factory with params', async () => {
    const db = await contractFactorySqliteStore({ path: ':memory:' })
    const range = { from: 0, to: 100 }

    const decoder = evmEventDecoder({
      range,
      contracts: contractFactory({
        address: FACTORY_ADDRESS,
        event: {
          event: factoryAbi.PoolCreated,
          params: {
            token0: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            token1: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
          },
        },
        childAddressField: 'pool',
        database: db,
      }),
      events: {
        swaps: swapAbi.Swap,
      },
    })

    const capturedQueryBuilder = await captureQueryBuilder(decoder)

    const requests = capturedQueryBuilder.getRequests()
    const factoryRequest = requests.find((r) => r.request?.logs?.[0]?.address?.includes(FACTORY_ADDRESS))

    expect(factoryRequest).toBeDefined()
    expect(factoryRequest?.request?.logs?.[0]?.topic0).toEqual([factoryAbi.PoolCreated.topic])
    expect(factoryRequest?.request?.logs?.[0]?.topic1).toEqual([
      '0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ])
    expect(factoryRequest?.request?.logs?.[0]?.topic2).toEqual([
      '0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    ])
    expect(factoryRequest?.request?.logs?.[0]?.topic3).toBeUndefined()
  })

  it('should build query for Factory without params', async () => {
    const db = await contractFactorySqliteStore({ path: ':memory:' })
    const range = { from: 0, to: 100 }

    const decoder = evmEventDecoder({
      range,
      contracts: contractFactory({
        address: FACTORY_ADDRESS,
        event: factoryAbi.PoolCreated,
        childAddressField: 'pool',
        database: db,
      }),
      events: {
        swaps: swapAbi.Swap,
      },
    })

    const capturedQueryBuilder = await captureQueryBuilder(decoder)

    const requests = capturedQueryBuilder.getRequests()
    const factoryRequest = requests.find((r) => r.request?.logs?.[0]?.address?.includes(FACTORY_ADDRESS))

    expect(factoryRequest).toBeDefined()
    expect(factoryRequest?.request?.logs?.[0]?.topic0).toEqual([factoryAbi.PoolCreated.topic])
    expect(factoryRequest?.request?.logs?.[0]?.topic1).toBeUndefined()
    expect(factoryRequest?.request?.logs?.[0]?.topic2).toBeUndefined()
    expect(factoryRequest?.request?.logs?.[0]?.topic3).toBeUndefined()
  })
})

describe('evmEventDecoder transform', () => {
  let portal: MockPortal

  beforeEach(async () => {
    portal = await mockPortal(PORTAL_MOCK_RESPONSE)
  })

  afterEach(async () => {
    await portal?.close()
  })

  const PORTAL_MOCK_RESPONSE: MockResponse[] = [
    {
      statusCode: 200,
      data: [
        {
          header: { number: 1, hash: '0x1', timestamp: 2000 },
          logs: [
            {
              address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
              topics: [
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
                '0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f',
              ],
              logIndex: 0,
              transactionIndex: 0,
              transactionHash: '0xdeadbeef',
              data: '0x000000000000000000000000000000000000000000000000013737bc62530000',
            },
            {
              address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
              topics: [
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
                '0x0000000000000000000000003611b82c7b13e72b26eb0e9be0613bee7a45ac7c',
              ],
              logIndex: 1,
              transactionIndex: 1,
              transactionHash: '0xdeadbeef',
              data: '0x0000000000000000000000000000000000000000000000000100000000000000',
            },
            {
              address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
              topics: [
                '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925',
                '0x000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                '0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff',
              ],
              logIndex: 2,
              transactionIndex: 2,
              transactionHash: '0xdeadbeef',
              data: '0x0000000000000000000000000000000000000000000000000100000000000000',
            },
          ],
        },
      ],
    },
  ]

  it('should decode the events when passed AbiEvent', async () => {
    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmEventDecoder({
        range: { from: 0, to: 1 },
        events: {
          transfers: commonAbis.erc20.events.Transfer,
        },
      }),
    }).pipe((e) => e.transfers)

    const res = await readAll(stream)

    expect(res).toMatchInlineSnapshot(`
      [
        {
          "block": {
            "hash": "0x1",
            "number": 1,
          },
          "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
          "event": {
            "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
            "to": "0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
            "value": 87600000000000000n,
          },
          "factory": null,
          "rawEvent": {
            "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "data": "0x000000000000000000000000000000000000000000000000013737bc62530000",
            "logIndex": 0,
            "topics": [
              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
              "0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
            ],
            "transactionHash": "0xdeadbeef",
            "transactionIndex": 0,
          },
          "timestamp": 1970-01-01T00:33:20.000Z,
        },
        {
          "block": {
            "hash": "0x1",
            "number": 1,
          },
          "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
          "event": {
            "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
            "to": "0x3611b82c7b13e72b26eb0e9be0613bee7a45ac7c",
            "value": 72057594037927936n,
          },
          "factory": null,
          "rawEvent": {
            "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "data": "0x0000000000000000000000000000000000000000000000000100000000000000",
            "logIndex": 1,
            "topics": [
              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
              "0x0000000000000000000000003611b82c7b13e72b26eb0e9be0613bee7a45ac7c",
            ],
            "transactionHash": "0xdeadbeef",
            "transactionIndex": 1,
          },
          "timestamp": 1970-01-01T00:33:20.000Z,
        },
      ]
    `)
  })

  it.each([
    //
    { contracts: [], expected: 0 },
    { contracts: ['0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef'], expected: 0 },
    { contracts: ['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'], expected: 2 },
    { contracts: ['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'.toUpperCase()], expected: 2 },
  ])(`should filter events by specified contracts $contracts -> $expected`, async ({ contracts, expected }) => {
    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmEventDecoder({
        range: { from: 0, to: 1 },
        contracts: contracts, // No contracts should filter out all events
        events: {
          transfers: commonAbis.erc20.events.Transfer,
        },
      }).pipe((e) => e.transfers),
    })

    const res = await readAll(stream)

    expect(res).toHaveLength(expected)
  })

  it('should decode the events when passed an EventWithArgs', async () => {
    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmEventDecoder({
        range: { from: 0, to: 1 },
        events: {
          transfers: {
            event: commonAbis.erc20.events.Transfer,
            params: {
              from: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',
            },
          },
        },
      }),
    }).pipe((e) => e.transfers)

    const res = await readAll(stream)
    expect(res).toMatchInlineSnapshot(`
      [
        {
          "block": {
            "hash": "0x1",
            "number": 1,
          },
          "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
          "event": {
            "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
            "to": "0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
            "value": 87600000000000000n,
          },
          "factory": null,
          "rawEvent": {
            "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "data": "0x000000000000000000000000000000000000000000000000013737bc62530000",
            "logIndex": 0,
            "topics": [
              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
              "0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
            ],
            "transactionHash": "0xdeadbeef",
            "transactionIndex": 0,
          },
          "timestamp": 1970-01-01T00:33:20.000Z,
        },
        {
          "block": {
            "hash": "0x1",
            "number": 1,
          },
          "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
          "event": {
            "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
            "to": "0x3611b82c7b13e72b26eb0e9be0613bee7a45ac7c",
            "value": 72057594037927936n,
          },
          "factory": null,
          "rawEvent": {
            "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "data": "0x0000000000000000000000000000000000000000000000000100000000000000",
            "logIndex": 1,
            "topics": [
              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
              "0x0000000000000000000000003611b82c7b13e72b26eb0e9be0613bee7a45ac7c",
            ],
            "transactionHash": "0xdeadbeef",
            "transactionIndex": 1,
          },
          "timestamp": 1970-01-01T00:33:20.000Z,
        },
      ]
    `)
  })

  it('should decode the events when mixed EventWithArgs and AbiEvent', async () => {
    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmEventDecoder({
        range: { from: 0, to: 1 },
        events: {
          approvals: commonAbis.erc20.events.Approval,
          transfers: {
            event: commonAbis.erc20.events.Transfer,
            params: {
              from: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',
            },
          },
        },
      }),
    }).pipe((e) => [...e['transfers'], ...e['approvals']])

    const res = await readAll(stream)

    expect(res).toMatchInlineSnapshot(`
      [
        {
          "block": {
            "hash": "0x1",
            "number": 1,
          },
          "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
          "event": {
            "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
            "to": "0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
            "value": 87600000000000000n,
          },
          "factory": null,
          "rawEvent": {
            "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "data": "0x000000000000000000000000000000000000000000000000013737bc62530000",
            "logIndex": 0,
            "topics": [
              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
              "0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
            ],
            "transactionHash": "0xdeadbeef",
            "transactionIndex": 0,
          },
          "timestamp": 1970-01-01T00:33:20.000Z,
        },
        {
          "block": {
            "hash": "0x1",
            "number": 1,
          },
          "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
          "event": {
            "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
            "to": "0x3611b82c7b13e72b26eb0e9be0613bee7a45ac7c",
            "value": 72057594037927936n,
          },
          "factory": null,
          "rawEvent": {
            "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "data": "0x0000000000000000000000000000000000000000000000000100000000000000",
            "logIndex": 1,
            "topics": [
              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
              "0x0000000000000000000000003611b82c7b13e72b26eb0e9be0613bee7a45ac7c",
            ],
            "transactionHash": "0xdeadbeef",
            "transactionIndex": 1,
          },
          "timestamp": 1970-01-01T00:33:20.000Z,
        },
        {
          "block": {
            "hash": "0x1",
            "number": 1,
          },
          "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
          "event": {
            "owner": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "spender": "0xffffffffffffffffffffffffffffffffffffffff",
            "value": 72057594037927936n,
          },
          "factory": null,
          "rawEvent": {
            "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "data": "0x0000000000000000000000000000000000000000000000000100000000000000",
            "logIndex": 2,
            "topics": [
              "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
              "0x000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
              "0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff",
            ],
            "transactionHash": "0xdeadbeef",
            "transactionIndex": 2,
          },
          "timestamp": 1970-01-01T00:33:20.000Z,
        },
      ]
    `)
  })
})

describe('evmEventDecoder multi-output isolation', () => {
  const CONTRACT_A = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as const
  const CONTRACT_B = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' as const

  let portal: MockPortal

  beforeEach(async () => {
    resetMockBlockCounter()

    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          mockBlock({
            transactions: [
              {
                logs: [
                  encodeEvent({
                    abi: commonAbis.erc20.abi,
                    eventName: 'Transfer',
                    address: CONTRACT_A,
                    args: {
                      from: '0x0000000000000000000000000000000000000001',
                      to: '0x0000000000000000000000000000000000000002',
                      value: 100n,
                    },
                  }),
                ],
              },
              {
                logs: [
                  encodeEvent({
                    abi: commonAbis.erc20.abi,
                    eventName: 'Transfer',
                    address: CONTRACT_B,
                    args: {
                      from: '0x0000000000000000000000000000000000000003',
                      to: '0x0000000000000000000000000000000000000004',
                      value: 200n,
                    },
                  }),
                ],
              },
            ],
          }),
        ],
      },
    ])
  })

  afterEach(async () => {
    await portal?.close()
  })

  it('should isolate events between two evmEventDecoders with different contracts', async () => {
    const results: { decoderA: { transfers: any[] }; decoderB: { transfers: any[] } }[] = []

    for await (const batch of evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: {
        decoderA: evmEventDecoder({
          range: { from: 0, to: 1 },
          contracts: [CONTRACT_A],
          events: { transfers: commonAbis.erc20.events.Transfer },
        }),
        decoderB: evmEventDecoder({
          range: { from: 0, to: 1 },
          contracts: [CONTRACT_B],
          events: { transfers: commonAbis.erc20.events.Transfer },
        }),
      },
    })) {
      results.push(batch.data)
    }

    const decoderA = results.flatMap((r) => r.decoderA.transfers)
    const decoderB = results.flatMap((r) => r.decoderB.transfers)

    expect(decoderA).toHaveLength(1)
    expect(decoderA[0].contract).toBe(CONTRACT_A)
    expect(decoderA[0].event.value).toBe(100n)

    expect(decoderB).toHaveLength(1)
    expect(decoderB[0].contract).toBe(CONTRACT_B)
    expect(decoderB[0].event.value).toBe(200n)
  })
})

describe('evmEventDecoder decode errors', () => {
  const CONTRACT = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  const FROM = '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d'
  const TO = '0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f'

  let portal: MockPortal

  beforeEach(async () => {
    // A decodable Transfer followed by one that passes topic routing but has empty
    // data — `value` (uint256) cannot be read, so `decode` throws.
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          {
            header: { number: 1, hash: '0x1', timestamp: 2000 },
            logs: [
              {
                address: CONTRACT,
                topics: [TRANSFER_TOPIC, FROM, TO],
                logIndex: 0,
                transactionIndex: 0,
                transactionHash: '0xdeadbeef',
                data: '0x000000000000000000000000000000000000000000000000013737bc62530000',
              },
              {
                address: CONTRACT,
                topics: [TRANSFER_TOPIC, FROM, TO],
                logIndex: 1,
                transactionIndex: 1,
                transactionHash: '0xdeadbeef',
                data: '0x',
              },
            ],
          },
        ],
      },
    ])
  })

  afterEach(async () => {
    await portal?.close()
  })

  function stream(metrics: ReturnType<typeof mockMetricsServer>, onError?: (ctx: any, error: any) => unknown) {
    return evmPortalStream({
      id: 'test',
      portal: portal.url,
      logger: false,
      metrics: metrics.server,
      outputs: evmEventDecoder({
        range: { from: 0, to: 1 },
        events: { transfers: commonAbis.erc20.events.Transfer },
        onError,
      }).pipe((e) => e.transfers),
    })
  }

  it('is fatal by default — no hook re-throws the decode error', async () => {
    const metrics = mockMetricsServer()

    await expect(readAll(stream(metrics))).rejects.toThrow(/decod/i)
    expect(metrics.counter('sqd_decode_errors_skipped_total')).toBeUndefined()
  })

  it('a returning hook suppresses the record and counts the skip', async () => {
    const metrics = mockMetricsServer()
    const seen: any[] = []

    const res = await readAll(stream(metrics, (_ctx, error) => seen.push(error)))

    expect(res).toHaveLength(1)
    expect(res[0].event.value).toBe(87600000000000000n)
    expect(seen).toHaveLength(1)

    const skipped = metrics.counter('sqd_decode_errors_skipped_total')
    expect(skipped.total).toBe(1)
    expect(skipped.calls[0].labels).toEqual({ id: 'test' })
  })

  it('a re-throwing hook stays fatal and records no skip', async () => {
    const metrics = mockMetricsServer()

    await expect(
      readAll(
        stream(metrics, (_ctx, error) => {
          throw error
        }),
      ),
    ).rejects.toThrow(/decod/i)
    expect(metrics.counter('sqd_decode_errors_skipped_total')).toBeUndefined()
  })
})

describe('evmEventDecoder factory decode errors', () => {
  const UNISWAP_FACTORY = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
  const POOL = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
  const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
  const SENDER = '0xdef1cafe0000000000000000000000000000dead'
  const RECIPIENT = '0xbeef0000000000000000000000000000deadbeef'

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

  const factoryPoolCreated = event(
    '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118',
    'PoolCreated(address,address,uint24,int24,address)',
    {
      token0: indexed(p.address),
      token1: indexed(p.address),
      fee: indexed(p.uint24),
      tickSpacing: p.int24,
      pool: p.address,
    },
  )

  const poolSwap = event(
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
  )

  function encodePoolCreated(pool: string) {
    return encodeEvent({
      abi: POOL_CREATED_ABI,
      eventName: 'PoolCreated',
      address: UNISWAP_FACTORY,
      args: { token0: WETH, token1: USDC, fee: 3000, tickSpacing: 10, pool: pool as `0x${string}` },
    })
  }

  function encodeSwap(address: string) {
    return encodeEvent({
      abi: SWAP_ABI,
      eventName: 'Swap',
      address: address as `0x${string}`,
      args: {
        sender: SENDER,
        recipient: RECIPIENT,
        amount0: 1n,
        amount1: 2n,
        sqrtPriceX96: 3n,
        liquidity: 4n,
        tick: 5,
      },
    })
  }

  let portal: MockPortal

  beforeEach(async () => {
    resetMockBlockCounter()

    // A decodable PoolCreated (registers the child pool) followed by one that keeps the
    // PoolCreated topics — so `isFactoryEvent` routes it — but carries empty data, so the
    // factory `decode` throws. A sibling Swap from the registered pool must still survive.
    portal = await mockEvmPortalStream({
      blocks: [
        mockBlock({
          number: 1,
          transactions: [
            { logs: [encodePoolCreated(POOL), { ...encodePoolCreated(POOL), data: '0x' }, encodeSwap(POOL)] },
          ],
        }),
      ],
    })
  })

  afterEach(async () => {
    await portal?.close()
  })

  function stream(metrics: ReturnType<typeof mockMetricsServer>, onError?: (ctx: any, error: any) => unknown) {
    const poolFactory = contractFactory({
      address: UNISWAP_FACTORY,
      event: factoryPoolCreated,
      childAddressField: 'pool',
      database: contractFactorySqliteStore({ path: ':memory:' }),
    })

    return evmPortalStream({
      id: 'test',
      portal: portal.url,
      logger: false,
      metrics: metrics.server,
      outputs: evmEventDecoder({
        range: { from: 0, to: 1 },
        contracts: poolFactory,
        events: { swaps: poolSwap },
        onError,
      }).pipe((e) => e.swaps),
    })
  }

  it('is fatal by default — a malformed factory event re-throws', async () => {
    const metrics = mockMetricsServer()

    await expect(readAll(stream(metrics))).rejects.toThrow(/decod/i)
    expect(metrics.counter('sqd_decode_errors_skipped_total')).toBeUndefined()
  })

  it('a returning hook suppresses the factory event and keeps decoding siblings', async () => {
    const metrics = mockMetricsServer()
    const seen: any[] = []

    const res = await readAll(stream(metrics, (_ctx, error) => seen.push(error)))

    expect(res).toHaveLength(1)
    expect(res[0].contract).toBe(POOL)
    expect(seen).toHaveLength(1)

    const skipped = metrics.counter('sqd_decode_errors_skipped_total')
    expect(skipped.total).toBe(1)
    expect(skipped.calls[0].labels).toEqual({ id: 'test' })
  })

  it('a re-throwing hook stays fatal and records no skip', async () => {
    const metrics = mockMetricsServer()

    await expect(
      readAll(
        stream(metrics, (_ctx, error) => {
          throw error
        }),
      ),
    ).rejects.toThrow(/decod/i)
    expect(metrics.counter('sqd_decode_errors_skipped_total')).toBeUndefined()
  })
})
