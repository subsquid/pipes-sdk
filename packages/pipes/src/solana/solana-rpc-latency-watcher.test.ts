import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { BatchContext } from '~/core/portal-source.js'
import { MockWebSocket, mockWebSocket } from '~/testing/mock-websocket.js'
import { testLogger } from '~/testing/test-logger.js'

import { SolanaQueryBuilder } from './solana-query-builder.js'
import { solanaRpcLatencyWatcher } from './solana-rpc-latency-watcher.js'

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

function slotUpdate(result: unknown) {
  return { method: 'slotsUpdatesNotification', params: { result } }
}

describe('solanaRpcLatencyWatcher factory', () => {
  let transformer: ReturnType<typeof solanaRpcLatencyWatcher> | undefined

  afterEach(async () => {
    await transformer?.stop({ logger: testLogger() })
    transformer = undefined
  })

  it('seeds the query with block.number and block.timestamp', async () => {
    transformer = solanaRpcLatencyWatcher({ rpcUrl: ['ws://127.0.0.1:1'] })

    const builder = new SolanaQueryBuilder<{ block: { number: true; timestamp: true } }>()
    await transformer.setupQuery({ query: builder, logger: testLogger() })

    expect(builder.getFields()).toEqual({
      block: { number: true, timestamp: true },
    })
  })

  it('sets the range to start from latest', async () => {
    transformer = solanaRpcLatencyWatcher({ rpcUrl: ['ws://127.0.0.1:1'] })

    const builder = new SolanaQueryBuilder<{ block: { number: true; timestamp: true } }>()
    await transformer.setupQuery({ query: builder, logger: testLogger() })

    const requests = builder.getRequests()
    expect(requests).toContainEqual(expect.objectContaining({ range: { from: 'latest' } }))
  })
})

describe('solanaRpcLatencyWatcher subscription', () => {
  let transformer: ReturnType<typeof solanaRpcLatencyWatcher> | undefined
  let restoreWebSocket: () => void

  beforeEach(() => {
    restoreWebSocket = mockWebSocket()
  })

  afterEach(async () => {
    await transformer?.stop({ logger: testLogger() })
    transformer = undefined
    restoreWebSocket()
  })

  it('records a slot on optimisticConfirmation and leaves hash undefined', async () => {
    transformer = solanaRpcLatencyWatcher({ rpcUrl: ['ws://rpc'] })
    await transformer.start({} as never)

    MockWebSocket.last.open()
    expect(JSON.parse(MockWebSocket.last.sent[0])).toMatchObject({ method: 'slotsUpdatesSubscribe' })

    const timestamp = new Date('2026-05-09T00:00:00Z')
    MockWebSocket.last.message(
      slotUpdate({ type: 'optimisticConfirmation', slot: 500, timestamp: timestamp.getTime() }),
    )

    const result = await transformer.run(
      [{ header: { number: 500, timestamp: timestamp.getTime() / 1000 } }] as never,
      makeBatchContext(new Date('2026-05-09T00:00:01Z')),
    )

    expect(result[0]?.number).toBe(500)
    expect(result[0]?.rpc[0]?.url).toBe('ws://rpc')
    expect(result[0]?.rpc[0]?.hash).toBeUndefined()
  })

  it('ignores slot updates that have not reached optimisticConfirmation', async () => {
    transformer = solanaRpcLatencyWatcher({ rpcUrl: ['ws://rpc'] })
    await transformer.start({} as never)
    MockWebSocket.last.open()

    MockWebSocket.last.message(slotUpdate({ type: 'firstShredReceived', slot: 501, timestamp: Date.now() }))
    expect(() => MockWebSocket.last.message({ method: 'slotsUpdatesNotification', params: {} })).not.toThrow()

    const result = await transformer.run(
      [{ header: { number: 501, timestamp: 0 } }] as never,
      makeBatchContext(new Date()),
    )
    expect(result[0]?.rpc).toEqual([{ url: 'ws://rpc', unresolved: 'rpc-missing' }])
  })
})
