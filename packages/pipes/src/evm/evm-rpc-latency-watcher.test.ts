import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { BatchContext } from '~/core/portal-source.js'
import { MockWebSocket, mockWebSocket } from '~/testing/mock-websocket.js'
import { testLogger } from '~/testing/test-logger.js'

import { EvmQueryBuilder } from './evm-query-builder.js'
import { evmRpcLatencyWatcher } from './evm-rpc-latency-watcher.js'

const profilerStub: Record<string, unknown> = {
  start: () => profilerStub,
  measure: async (_: unknown, fn: () => unknown) => fn(),
  end: () => {},
  data: undefined,
}

function makeBatchContext(receivedAt: Date): BatchContext {
  return {
    profiler: profilerStub,
    batch: { blocksCount: 1, bytesSize: 0, requests: {}, lastBlockReceivedAt: receivedAt },
  } as unknown as BatchContext
}

function newHeads(head: unknown) {
  return { method: 'eth_subscription', params: { result: head } }
}

describe('evmRpcLatencyWatcher factory', () => {
  let transformer: ReturnType<typeof evmRpcLatencyWatcher> | undefined

  afterEach(async () => {
    await transformer?.stop({ logger: testLogger() })
    transformer = undefined
  })

  it('seeds the query with block.number and block.timestamp', async () => {
    transformer = evmRpcLatencyWatcher({ rpcUrl: ['ws://127.0.0.1:1'] })

    const builder = new EvmQueryBuilder<{ block: { number: true; timestamp: true } }>()
    await transformer.setupQuery({ query: builder, logger: testLogger() })

    expect(builder.getFields()).toEqual({
      block: { number: true, timestamp: true },
    })
  })

  it('sets the range to start from latest', async () => {
    transformer = evmRpcLatencyWatcher({ rpcUrl: ['ws://127.0.0.1:1'] })

    const builder = new EvmQueryBuilder<{ block: { number: true; timestamp: true } }>()
    await transformer.setupQuery({ query: builder, logger: testLogger() })

    const requests = builder.getRequests()
    expect(requests).toContainEqual(expect.objectContaining({ range: { from: 'latest' } }))
  })
})

describe('evmRpcLatencyWatcher subscription', () => {
  let transformer: ReturnType<typeof evmRpcLatencyWatcher> | undefined
  let restoreWebSocket: () => void

  beforeEach(() => {
    restoreWebSocket = mockWebSocket()
  })

  afterEach(async () => {
    await transformer?.stop({ logger: testLogger() })
    transformer = undefined
    restoreWebSocket()
  })

  it('subscribes to newHeads and matches an observed head against the portal block', async () => {
    transformer = evmRpcLatencyWatcher({ rpcUrl: ['ws://rpc'] })
    await transformer.start({} as never)

    MockWebSocket.last.open()
    expect(JSON.parse(MockWebSocket.last.sent[0])).toMatchObject({
      method: 'eth_subscribe',
      params: ['newHeads'],
    })

    MockWebSocket.last.message(newHeads({ number: '0x64', hash: '0xabc', timestamp: '0x66' }))

    const result = await transformer.run(
      [{ header: { number: 100, timestamp: 0x66 } }] as never,
      makeBatchContext(new Date('2026-05-09T00:00:10Z')),
    )

    expect(result[0]?.number).toBe(100)
    expect(result[0]?.rpc[0]).toMatchObject({ url: 'ws://rpc', hash: '0xabc' })
  })

  it('ignores a subscription frame with no head instead of crashing', async () => {
    // `params.result` is absent on the subscription ack and on some providers' keepalives;
    // reading `head.number` off it threw and took the whole process down.
    transformer = evmRpcLatencyWatcher({ rpcUrl: ['ws://rpc'] })
    await transformer.start({} as never)
    MockWebSocket.last.open()

    expect(() => MockWebSocket.last.message({ method: 'eth_subscription', params: {} })).not.toThrow()
    expect(() => MockWebSocket.last.message({ jsonrpc: '2.0', id: 1, result: '0xsub' })).not.toThrow()

    const result = await transformer.run(
      [{ header: { number: 1, timestamp: 0 } }] as never,
      makeBatchContext(new Date()),
    )
    expect(result[0]?.rpc).toEqual([{ url: 'ws://rpc', unresolved: 'rpc-missing' }])
  })

  it('closes the socket on stop', async () => {
    transformer = evmRpcLatencyWatcher({ rpcUrl: ['ws://rpc'] })
    await transformer.start({} as never)
    MockWebSocket.last.open()

    await transformer.stop({ logger: testLogger() })
    transformer = undefined

    expect(MockWebSocket.instances[0].closed).toBe(true)
  })
})
