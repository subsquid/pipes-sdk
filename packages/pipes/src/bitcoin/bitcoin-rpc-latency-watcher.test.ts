import { afterEach, describe, expect, it, vi } from 'vitest'

import { type MockBitcoinRpc, mockBitcoinRpc } from '~/testing/bitcoin/index.js'
import { testLogger } from '~/testing/test-logger.js'

import { BitcoinQueryBuilder } from './bitcoin-query-builder.js'
import { BitcoinRpcLatencyWatcher, bitcoinRpcLatencyWatcher } from './bitcoin-rpc-latency-watcher.js'

describe('BitcoinRpcLatencyWatcher', () => {
  let mock: MockBitcoinRpc | undefined
  let watcher: BitcoinRpcLatencyWatcher | undefined

  afterEach(async () => {
    watcher?.stop()
    watcher = undefined
    await mock?.close()
    mock = undefined
  })

  it('records new heads from getbestblockhash + getblockheader', async () => {
    let height = 800_000
    const time = 1_700_000_000

    mock = await mockBitcoinRpc((method) => {
      if (method === 'getbestblockhash') return `hash-${height}`
      if (method === 'getblockheader') return { hash: `hash-${height}`, height, time }
      throw new Error(`unexpected method ${method}`)
    })

    watcher = new BitcoinRpcLatencyWatcher([mock.url], 25)
    watcher.start()

    await vi.waitFor(
      () => {
        const lookup = watcher!.lookup(800_000)
        expect(lookup).toHaveLength(1)
        expect(lookup[0].url).toBe(mock!.url)
        expect(lookup[0].timestamp).toEqual(new Date(time * 1000))
        // Hash must round-trip through addBlock → lookup() so reorg-aware downstream
        // consumers (e.g. the BigQuery freshness probe) can match (number, hash).
        expect(lookup[0].hash).toBe(`hash-${800_000}`)
      },
      { timeout: 1_000, interval: 20 },
    )

    height = 800_001

    await vi.waitFor(
      () => {
        expect(watcher!.lookup(800_001)).toHaveLength(1)
      },
      { timeout: 1_000, interval: 20 },
    )

    // Watcher must avoid re-fetching the header for an unchanged tip — exactly one
    // getblockheader call per observed height.
    expect(mock!.calls.filter((c) => c.method === 'getblockheader').length).toBe(2)
  })

  it('keeps polling after a transient RPC failure', async () => {
    let failNext = true

    mock = await mockBitcoinRpc((method) => {
      if (failNext) {
        failNext = false
        throw new Error('boom')
      }
      if (method === 'getbestblockhash') return 'hash-1'
      if (method === 'getblockheader') return { hash: 'hash-1', height: 1, time: 1 }
      throw new Error(`unexpected ${method}`)
    })

    watcher = new BitcoinRpcLatencyWatcher([mock.url], 25)
    watcher.start()

    await vi.waitFor(
      () => {
        expect(watcher!.lookup(1)).toHaveLength(1)
      },
      { timeout: 1_000, interval: 20 },
    )
  })

  it('extracts user:pass@ from the URL into a Basic Authorization header without leaking credentials downstream', async () => {
    mock = await mockBitcoinRpc((method) => {
      if (method === 'getbestblockhash') return 'hash-1'
      if (method === 'getblockheader') return { hash: 'hash-1', height: 1, time: 1 }
      throw new Error(`unexpected ${method}`)
    })

    const credentials = 'rpcuser:rpcpass'
    const expected = `Basic ${Buffer.from(credentials).toString('base64')}`
    const authedUrl = mock.url.replace('http://', `http://${credentials}@`)

    watcher = new BitcoinRpcLatencyWatcher([authedUrl], 25)
    watcher.start()

    await vi.waitFor(
      () => {
        expect(watcher!.lookup(1)).toHaveLength(1)
      },
      { timeout: 1_000, interval: 20 },
    )

    // Every received call carried the expected Authorization header.
    expect(mock.calls.length).toBeGreaterThan(0)
    for (const call of mock.calls) {
      expect(call.auth).toBe(expected)
    }

    // Critically: `lookup()` (and therefore the `LatencySample.rpc[].url` surfaced
    // to the pipeline / metrics / logs) must never echo back the credentials.
    const [entry] = watcher!.lookup(1)
    // URL is normalized through WHATWG `URL` (which appends a trailing slash);
    // what matters is that credentials are stripped.
    expect(entry.url.startsWith(mock.url)).toBe(true)
    expect(entry.url).not.toContain('rpcuser')
    expect(entry.url).not.toContain('rpcpass')
    expect(entry.url).not.toContain('@')
    // The base class also indexes `nodes` by URL — verify there too.
    for (const key of watcher!.nodes.keys()) {
      expect(key).not.toContain('rpcuser')
      expect(key).not.toContain('@')
    }
  })

  it('aborts requests that exceed the per-call timeout and keeps polling', async () => {
    mock = await mockBitcoinRpc(
      () =>
        new Promise(() => {
          // never resolves — simulates a hung RPC
        }),
    )

    // Short interval + short timeout ⇒ the loop must abort the in-flight
    // request after `requestTimeoutMs` and start a new tick after `intervalMs`.
    watcher = new BitcoinRpcLatencyWatcher([mock.url], 100, 30)
    watcher.start()

    await vi.waitFor(
      () => {
        // ≥ 2 ticks is the proof that timeouts are firing — without abort, a
        // hung server would pin the loop on the very first request forever.
        expect(mock!.calls.length).toBeGreaterThanOrEqual(2)
      },
      { timeout: 2_000, interval: 30 },
    )
  })
})

describe('bitcoinRpcLatencyWatcher factory', () => {
  it('seeds the query with block.number and block.timestamp', async () => {
    const transformer = bitcoinRpcLatencyWatcher({
      rpcUrl: ['http://127.0.0.1:1'],
      intervalMs: 60_000,
      requestTimeoutMs: 25,
    })

    const builder = new BitcoinQueryBuilder<{ block: { number: true; timestamp: true } }>()
    await transformer.setupQuery({ query: builder, logger: testLogger() })

    expect(builder.getFields()).toEqual({
      block: { number: true, timestamp: true },
    })

    // Tear down the underlying polling loop (sits on a child transformer).
    await transformer.stop({ logger: testLogger() })
  })
})
