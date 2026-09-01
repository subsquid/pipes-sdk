import { describe, expect, it } from 'vitest'

import type { BatchContext } from '~/core/portal-source.js'

import { type RpcLatencyListener, RpcLatencyWatcher, rpcLatencyWatcher } from './rpc-latency-watcher.js'

class StubWatcher extends RpcLatencyWatcher {
  watched: string[] = []
  stopped = 0

  watch(url: string): RpcLatencyListener {
    this.watched.push(url)

    return {
      stop: () => {
        this.stopped++
      },
    }
  }
}

const profilerStub: Record<string, unknown> = {
  start: () => profilerStub,
  measure: async (_: unknown, fn: () => unknown) => fn(),
  end: () => {},
  data: undefined,
}

function makeBatchContext(receivedAt: Date): BatchContext {
  return {
    profiler: profilerStub,
    batch: {
      blocksCount: 1,
      bytesSize: 0,
      requests: {},
      lastBlockReceivedAt: receivedAt,
    },
  } as unknown as BatchContext
}

function header(number: number, timestamp = 0) {
  return { header: { number, timestamp } }
}

describe('rpcLatencyWatcher transformer', () => {
  it('propagates the RPC-observed hash from lookup() into LatencySample.rpc[].hash', async () => {
    // Reorg-safe joining downstream (BigQuery freshness probe) requires the hash to flow
    // end-to-end: addBlock → lookup → LatencySample. A regression dropping the field would
    // silently break (number, hash) matching, leaving the probe pairing observations across
    // chain forks. This pins the wire-up.
    const watcher = new StubWatcher(['ws://rpc-1', 'ws://rpc-2'])
    watcher.start()

    const blockTimestamp = new Date('2026-05-09T00:00:00Z')
    const rpc1ReceivedAt = new Date('2026-05-09T00:00:01Z')
    const rpc2ReceivedAt = new Date('2026-05-09T00:00:02Z')
    watcher.addBlock('ws://rpc-1', {
      number: 100,
      hash: '0xCANONICAL',
      timestamp: blockTimestamp,
      receivedAt: rpc1ReceivedAt,
    })
    watcher.addBlock('ws://rpc-2', {
      number: 100,
      hash: '0xCANONICAL',
      timestamp: blockTimestamp,
      receivedAt: rpc2ReceivedAt,
    })

    const transformer = rpcLatencyWatcher({ watcher })
    const portalReceivedAt = new Date('2026-05-09T00:00:03Z')
    const result = await transformer.run(
      [{ header: { number: 100, timestamp: blockTimestamp.getTime() / 1000 } }],
      makeBatchContext(portalReceivedAt),
    )

    expect(result).toHaveLength(1)
    expect(result[0]?.number).toBe(100)
    expect(result[0]?.rpc).toEqual([
      { url: 'ws://rpc-1', hash: '0xCANONICAL', receivedAt: rpc1ReceivedAt, portalDelayMs: 2000 },
      { url: 'ws://rpc-2', hash: '0xCANONICAL', receivedAt: rpc2ReceivedAt, portalDelayMs: 1000 },
    ])
  })

  it('emits hash=undefined for sources that do not carry one (e.g. Solana)', async () => {
    // Solana's slot updates carry no hash; the field stays undefined end-to-end. Downstream
    // consumers degrade to number-only matching — this test pins that the pipeline doesn't
    // accidentally fabricate a value (e.g. empty string) on the way through.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()

    watcher.addBlock('ws://rpc', {
      number: 42,
      timestamp: new Date('2026-05-09T00:00:00Z'),
      receivedAt: new Date('2026-05-09T00:00:01Z'),
    })

    const transformer = rpcLatencyWatcher({ watcher })
    const result = await transformer.run(
      [{ header: { number: 42, timestamp: new Date('2026-05-09T00:00:00Z').getTime() / 1000 } }],
      makeBatchContext(new Date('2026-05-09T00:00:02Z')),
    )

    expect(result[0]?.rpc[0]?.hash).toBeUndefined()
  })

  it('reports a negative delay for a head the portal delivered before the reference RPC', async () => {
    // The bug this pins: the sample used to be taken at delivery time and dropped whenever the
    // reference had not reported the head yet — i.e. exactly when the portal won. Every "portal
    // is ahead" observation vanished, the distribution was truncated at zero, and its tail
    // described the reference node's hiccups rather than portal freshness.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()
    watcher.addBlock('ws://rpc', { number: 99, timestamp: new Date(), receivedAt: new Date() })

    const transformer = rpcLatencyWatcher({ watcher })

    const portalReceivedAt = new Date()
    const pendingBatch = await transformer.run([header(100)], makeBatchContext(portalReceivedAt))
    expect(pendingBatch).toEqual([])

    watcher.addBlock('ws://rpc', {
      number: 100,
      timestamp: new Date(),
      receivedAt: new Date(portalReceivedAt.getTime() + 500),
    })

    const result = await transformer.run([header(101)], makeBatchContext(new Date()))

    expect(result).toHaveLength(1)
    expect(result[0]?.number).toBe(100)
    expect(result[0]?.rpc[0]?.portalDelayMs).toBe(-500)
  })

  it('keeps the first portal receipt when a pending head is delivered again', async () => {
    // Freshness is "when did the portal first have a block at this height"; a re-delivery must
    // not re-stamp the sample (and must not push the resolve deadline out forever).
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()
    watcher.addBlock('ws://rpc', { number: 99, timestamp: new Date(), receivedAt: new Date() })

    const transformer = rpcLatencyWatcher({ watcher })

    const firstReceipt = new Date()
    await transformer.run([header(100)], makeBatchContext(firstReceipt))
    await transformer.run([header(100)], makeBatchContext(new Date(firstReceipt.getTime() + 2_000)))

    watcher.addBlock('ws://rpc', {
      number: 100,
      timestamp: new Date(),
      receivedAt: new Date(firstReceipt.getTime() + 500),
    })

    const result = await transformer.run([header(101)], makeBatchContext(new Date()))

    expect(result[0]?.rpc[0]?.portalDelayMs).toBe(-500)
  })

  it('closes a head as rpc-behind once the resolve window expires', async () => {
    // A reference that is stuck below the head is a real, one-sided observation: the portal is
    // ahead by at least the window. Emitting it without a delay keeps it out of the histogram
    // while still making it countable.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()
    watcher.addBlock('ws://rpc', { number: 99, timestamp: new Date(), receivedAt: new Date() })

    const transformer = rpcLatencyWatcher({ watcher, resolveTimeoutMs: 0 })
    const result = await transformer.run([header(100)], makeBatchContext(new Date()))

    expect(result).toHaveLength(1)
    expect(result[0]?.rpc).toEqual([{ url: 'ws://rpc', unresolved: 'rpc-behind' }])
  })

  it('closes a head as rpc-missing when the reference is already past it', async () => {
    // Backfill: the portal replays old blocks the reference observed long ago and has since
    // evicted. Waiting for them would time out as `rpc-behind` and fake a portal lead.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()
    watcher.addBlock('ws://rpc', { number: 100_000, timestamp: new Date(), receivedAt: new Date() })

    const transformer = rpcLatencyWatcher({ watcher })
    const result = await transformer.run([header(100)], makeBatchContext(new Date()))

    expect(result).toHaveLength(1)
    expect(result[0]?.rpc).toEqual([{ url: 'ws://rpc', unresolved: 'rpc-missing' }])
  })

  it('reports rpc-missing for a node that has observed nothing at all', async () => {
    // A silent reference proves nothing about the portal — it must not read as a portal lead.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()

    const transformer = rpcLatencyWatcher({ watcher })
    const result = await transformer.run([header(999)], makeBatchContext(new Date()))

    expect(result[0]?.rpc).toEqual([{ url: 'ws://rpc', unresolved: 'rpc-missing' }])
  })

  it('does not let a head the reference skipped block later ones', async () => {
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()
    watcher.addBlock('ws://rpc', { number: 99, timestamp: new Date(), receivedAt: new Date() })

    const transformer = rpcLatencyWatcher({ watcher })
    await transformer.run([header(100)], makeBatchContext(new Date()))

    // The reference jumps straight to 101 — 100 will never arrive.
    const rpcReceivedAt = new Date()
    watcher.addBlock('ws://rpc', { number: 101, timestamp: new Date(), receivedAt: rpcReceivedAt })

    const result = await transformer.run([header(101)], makeBatchContext(new Date(rpcReceivedAt.getTime() + 300)))

    expect(result.map((s) => [s.number, s.rpc[0]?.unresolved])).toEqual([
      [100, 'rpc-missing'],
      [101, undefined],
    ])
    expect(result[1]?.rpc[0]?.portalDelayMs).toBe(300)
  })

  it('drops pending heads above a rollback cursor', async () => {
    // Heads on the abandoned fork would sit out the resolve window and surface as `rpc-behind`,
    // i.e. a portal lead that never happened.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()
    watcher.addBlock('ws://rpc', { number: 99, timestamp: new Date(), receivedAt: new Date() })

    const transformer = rpcLatencyWatcher({ watcher })
    await transformer.run([header(100)], makeBatchContext(new Date()))

    await transformer.rollback({ number: 99 }, {} as never)

    watcher.addBlock('ws://rpc', { number: 100, timestamp: new Date(), receivedAt: new Date() })
    const result = await transformer.run([header(101)], makeBatchContext(new Date()))

    expect(result).toEqual([])
  })

  it('emits nothing while the watcher has no endpoints', async () => {
    const watcher = new StubWatcher([])

    const transformer = rpcLatencyWatcher({ watcher })
    const result = await transformer.run([header(1)], makeBatchContext(new Date()))

    expect(result).toEqual([])
  })
})

describe('RpcLatencyWatcher lifecycle', () => {
  it('re-subscribes after stop(), so a stream restart does not blind it permanently', () => {
    // stop() used to be a one-way latch: after the first restart the sockets stayed
    // shut and lookup() returned [] forever, freezing the gauges on their last value.
    const watcher = new StubWatcher(['ws://rpc-1', 'ws://rpc-2'])

    watcher.start()
    expect(watcher.watched).toEqual(['ws://rpc-1', 'ws://rpc-2'])

    watcher.stop()
    expect(watcher.stopped).toBe(2)

    watcher.start()
    expect(watcher.watched).toEqual(['ws://rpc-1', 'ws://rpc-2', 'ws://rpc-1', 'ws://rpc-2'])
  })

  it('retains observed heads across a restart', () => {
    // Dropping the buffer would stall matching until each RPC re-observed a head.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()
    watcher.addBlock('ws://rpc', { number: 7, timestamp: new Date(), receivedAt: new Date() })

    watcher.stop()
    watcher.start()

    expect(watcher.lookup(7)).toHaveLength(1)
  })

  it('ignores a repeated start()', () => {
    const watcher = new StubWatcher(['ws://rpc'])

    watcher.start()
    watcher.start()

    expect(watcher.watched).toEqual(['ws://rpc'])
  })

  it('ignores a repeated stop() instead of stopping listeners twice', () => {
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()

    watcher.stop()
    watcher.stop()

    expect(watcher.stopped).toBe(1)
  })

  it('subscribes from the transformer start hook', async () => {
    const watcher = new StubWatcher(['ws://rpc'])
    const transformer = rpcLatencyWatcher({ watcher })

    await transformer.start({} as never)

    expect(watcher.watched).toEqual(['ws://rpc'])
  })

  it('evicts the oldest heads once a node exceeds the retention cap', () => {
    // Nothing pruned `nodes`, so entries accumulated for the process's lifetime.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()

    for (let i = 1; i <= 600; i++) {
      watcher.addBlock('ws://rpc', { number: i, timestamp: new Date(), receivedAt: new Date() })
    }

    expect(watcher.nodes.get('ws://rpc')?.size).toBe(512)
    expect(watcher.lookup(88)).toEqual([])
    expect(watcher.lookup(89)).toHaveLength(1)
    expect(watcher.lookup(600)).toHaveLength(1)
  })

  it('tracks the highest head seen, so an evicted one is not mistaken for a pending one', () => {
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()

    for (let i = 1; i <= 600; i++) {
      watcher.addBlock('ws://rpc', { number: i, timestamp: new Date(), receivedAt: new Date() })
    }

    expect(watcher.mayObserve('ws://rpc', 88)).toBe(false)
    expect(watcher.mayObserve('ws://rpc', 601)).toBe(true)
  })

  it('keeps the high-water mark when an older head arrives late', () => {
    // A reorg can replay a lower head. Letting it lower the mark would make every head above it
    // look pending again, and evicted ones would sit out the resolve window as `rpc-behind`.
    const watcher = new StubWatcher(['ws://rpc'])
    watcher.start()

    watcher.addBlock('ws://rpc', { number: 200, timestamp: new Date(), receivedAt: new Date() })
    watcher.addBlock('ws://rpc', { number: 199, timestamp: new Date(), receivedAt: new Date() })

    expect(watcher.mayObserve('ws://rpc', 200)).toBe(false)
  })
})
