import { describe, expect, vi } from 'vitest'

import { PortalClient, StreamData } from '~/portal-client/index.js'
import { MockPortal } from '~/testing/index.js'
import { withPortal as it } from '~/testing/portal-fixture.js'
import { StubPortalClient } from '~/testing/stub-portal.js'

import { defaultLogger } from '../logger.js'
import { FallbackClient } from './client.js'

/**
 * Mixed finality — a finalized-only source doing the cheap bulk backfill, a hot source taking over
 * at the finality frontier.
 *
 * The finalized-only source is the **real `PortalClient`** over a mock HTTP portal on purpose. A
 * finalized-only portal at its frontier does not hang: it answers 204, which the client turns into a
 * stream of empty batches. A stub that hangs instead would let broken behaviour pass.
 */

const silent = defaultLogger({ level: process.env['FB_LOG'] ? 'debug' : 'silent' })
// Every block, so the chain-mode portal does not drop the header-only blocks between the range ends.
const QUERY = { type: 'evm', fromBlock: 1, includeAllBlocks: true } as any

const block = (number: number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })

/**
 * The finalized frontier at block 3: every request past it gets 204, and its head is still reported,
 * which is why an exhausted source keeps looking healthy.
 */
const BULK = { blocks: [1, 2, 3].map(block), latest: 3, finalized: 3 }

function client(p: MockPortal, finalized: boolean) {
  return new PortalClient({ url: p.url, finalized, headPollIntervalMs: 5 })
}

/** A hot source over a stub client that reports `head` and serves from the requested block up to it. */
function tip(head: number, { pauseMs = 0, onBlock = () => {} } = {}) {
  const stub = new StubPortalClient(async function* (query) {
    for (let n = query.fromBlock; n <= head; n++) {
      onBlock()
      yield {
        blocks: [block(n)],
        head: {},
        meta: { bytes: 0, requestedFromBlock: n, lastBlockReceivedAt: new Date(), requests: {} },
      }
      if (pauseMs > 0) {
        await new Promise((r) => setTimeout(r, pauseMs))
      }
    }
  })
  vi.spyOn(stub, 'getHead').mockResolvedValue({ number: head, hash: `0x${head}` })

  return stub
}

async function readBlocks(fb: FallbackClient, options?: { finalized: boolean }, limit = Infinity) {
  const got: number[] = []
  for await (const b of fb.getStream(QUERY, options) as AsyncIterable<StreamData<any>>) {
    got.push(...b.blocks.map((x: any) => x.header.number))
    if (got.length >= limit) {
      break
    }
  }

  return got
}

describe('mixed finality — the finalized-only source keeps its commitment', () => {
  it('a consumer-forwarded `finalized: false` does not down-force a finalized-only source', async ({ portal }) => {
    // `PortalStream` forwards its own effective commitment as a plain boolean, and for a mixed set
    // that is `false`. A source client lets the option win over its own config, so forwarding it
    // verbatim would silently put a finalized-only source on the hot stream.
    const bulk = await portal({ blocks: [block(1), block(2)] })
    const fb = new FallbackClient({
      sources: [{ name: 'bulk', client: client(bulk, true) }],
      detection: { capabilityProbe: false },
      logger: silent,
    })

    expect(await readBlocks(fb, { finalized: false }, 2)).toEqual([1, 2])
    expect(bulk.requests[0].endpoint).toBe('finalized-stream')
  }, 20_000)

  it('a consumer CAN still raise finality for a hot source', async ({ portal }) => {
    // The opposite direction must keep working: a finalized-requiring target forces every source
    // onto its finalized stream, although this source is configured hot.
    const hot = await portal({ blocks: [block(1)] })
    const fb = new FallbackClient({
      sources: [{ name: 'hot', client: client(hot, false) }],
      detection: { capabilityProbe: false },
      logger: silent,
    })

    expect(await readBlocks(fb, { finalized: true }, 1)).toEqual([1])
    expect(hot.requests[0].endpoint).toBe('finalized-stream')
  }, 20_000)
})

describe('mixed finality — handoff at the frontier', () => {
  it('hands off to the hot source when the finalized one stops making progress', async ({ portal }) => {
    // The bulk portal serves 1..3 and then answers 204 in a loop — it keeps ANSWERING, so the
    // handoff cannot rely on a hanging request; it is the lack of *progress* that must trip it.
    const bulk = await portal(BULK)
    const hot = tip(6)

    const fb = new FallbackClient({
      sources: [
        { name: 'bulk', client: client(bulk, true) },
        { name: 'tip', client: hot },
      ],
      detection: { capabilityProbe: false, maxStalenessMs: 60, freshnessTickMs: 15, headTtlMs: 0, maxLagBlocks: null },
      logger: silent,
    })

    const got: number[] = []
    let servedLast: number | undefined
    for await (const b of fb.getStream(QUERY) as AsyncIterable<StreamData<any>>) {
      got.push(...b.blocks.map((x: any) => x.header.number))
      servedLast = fb.activeIndex
    }

    expect(got).toEqual([1, 2, 3, 4, 5, 6]) // no gap across the handoff
    expect(fb.switchCount).toBe(1)
    expect(servedLast).toBe(1)
    // The hot source was asked to continue from just past the finalized frontier.
    expect(hot.reads[0]).toMatchObject({ fromBlock: 4, parentBlockHash: '0x3' })
  }, 20_000)

  it('holds, and reports a chain stall, when nothing is ahead of the frontier', async ({ portal }) => {
    // Same frontier, but the standby is no further along. Handing off would gain nothing, so the
    // pipe must hold rather than churn — and say so through `chainStalled`.
    const bulk = await portal(BULK)
    const standby = tip(3)

    const fb = new FallbackClient({
      sources: [
        { name: 'bulk', client: client(bulk, true) },
        { name: 'standby', client: standby },
      ],
      detection: { capabilityProbe: false, maxStalenessMs: 60, freshnessTickMs: 15, headTtlMs: 0, maxLagBlocks: null },
      logger: silent,
    })

    const got: number[] = []
    const drain = (async () => {
      for await (const b of fb.getStream(QUERY) as AsyncIterable<StreamData<any>>) {
        got.push(...b.blocks.map((x: any) => x.header.number))
      }
    })()
    drain.catch(() => {})
    await new Promise((r) => setTimeout(r, 400))

    expect(got).toEqual([1, 2, 3])
    expect(fb.switchCount).toBe(0)
    expect(fb.activeIndex).toBe(0)
    expect(fb.chainStalled).toBe(true) // the state is visible, not silent
    expect(standby.reads).toHaveLength(0)
  }, 20_000)

  it('does not crawl back into the exhausted source once past its frontier', async ({ portal }) => {
    // After the handoff the pipe is above the bulk portal's finalized head. It stays reachable and
    // recovers to `healthy`, so only the "can it serve where we are" guard keeps eager switch-up
    // from stalling the pipe again.
    const bulk = await portal(BULK)
    let served = 0
    const hot = tip(40, { pauseMs: 5, onBlock: () => served++ })

    const fb = new FallbackClient({
      sources: [
        { name: 'bulk', client: client(bulk, true) },
        { name: 'tip', client: hot },
      ],
      detection: {
        capabilityProbe: false, // the guard must hold on its own, without a probe to lean on
        maxStalenessMs: 60,
        freshnessTickMs: 15,
        headTtlMs: 0,
        maxLagBlocks: null,
        cooldownMs: 20, // recovers fast, so eager gets every chance to reclaim it
        livenessRecoverThreshold: 1,
      },
      logger: silent,
    })

    const got: number[] = []
    let servedLast: number | undefined
    for await (const b of fb.getStream(QUERY) as AsyncIterable<StreamData<any>>) {
      got.push(...b.blocks.map((x: any) => x.header.number))
      servedLast = fb.activeIndex
    }

    expect(got).toEqual(Array.from({ length: 40 }, (_, i) => i + 1))
    expect(served).toBe(37) // the hot source served 4..40 uninterrupted
    expect(fb.switchCount).toBe(1) // handed off exactly once — no thrash back into the frontier
    expect(servedLast).toBe(1)
  }, 20_000)
})

describe('mixed finality — one stream at a time', () => {
  it('refuses a second concurrent stream instead of interleaving its freshness state', async ({ portal }) => {
    // The per-stream state (forced commitment, probes, head cache, tip latch) describes the stream
    // in flight; a second concurrent stream would silently reinterpret the first one's commitment.
    const bulk = await portal(BULK)
    const fb = new FallbackClient({
      sources: [{ name: 'bulk', client: client(bulk, true) }],
      detection: { capabilityProbe: false },
      logger: silent,
    })

    const first = fb.getStream(QUERY)[Symbol.asyncIterator]()
    await first.next() // start it

    const second = fb.getStream(QUERY)[Symbol.asyncIterator]()
    await expect(second.next()).rejects.toThrowError(/one stream at a time/)

    // Once the first stream is released, a new one is fine.
    await first.return?.(undefined)
    const third = fb.getStream(QUERY)[Symbol.asyncIterator]()
    await expect(third.next()).resolves.toBeDefined()
    await third.return?.(undefined)
  }, 20_000)
})
