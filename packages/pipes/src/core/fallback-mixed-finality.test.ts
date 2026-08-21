import { afterEach, describe, expect, it } from 'vitest'

import { PortalClient, StreamData } from '~/portal-client/index.js'
import { MockPortal, finalizedMockPortal, mockBlockStreamClient, mockPortal } from '~/testing/index.js'

import { FallbackClient } from './fallback-client.js'
import { defaultLogger } from './logger.js'

/**
 * Mixed finality — a finalized-only source doing the cheap bulk backfill, a hot source taking over
 * at the finality frontier.
 *
 * These run against the **real `PortalClient`** and a mock HTTP portal on purpose. A finalized-only
 * portal at its frontier does not hang: it answers 204, which the client turns into a stream of
 * empty batches. A hand-written mock that hangs instead would let broken behaviour pass.
 */

const silent = defaultLogger({ level: process.env['FB_LOG'] ? 'debug' : 'silent' })
const QUERY = { type: 'evm', fromBlock: 0 } as any

let portal: MockPortal | undefined
let hotPortal: MockPortal | undefined

afterEach(async () => {
  await portal?.close()
  await hotPortal?.close()
  portal = undefined
  hotPortal = undefined
})

const block = (number: number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })

/**
 * A finalized-only portal that serves `blocks`, then answers 204 for the rest of the test — the
 * count is far beyond what a test can consume, so a handoff can only come from the stall verdict.
 * (With a short script the portal eventually 500s, which would fail the source over as an ordinary
 * stream error and let a broken stall path pass.)
 */
async function finalizedBulkPortal(blocks: number[], trailing204s = 20_000) {
  const frontier = blocks[blocks.length - 1]
  return finalizedMockPortal(
    [
      { statusCode: 200 as const, data: blocks.map(block) },
      ...Array.from({ length: trailing204s }, () => ({ statusCode: 204 as const })),
    ],
    // It answers heads perfectly well at its frontier — that is exactly why an exhausted source
    // keeps looking healthy, and why the "can it serve the cursor" guard has to carry the weight.
    { head: { number: frontier, hash: `0x${frontier}` } },
  )
}

function client(p: MockPortal, finalized: boolean) {
  return new PortalClient({ url: p.url, finalized, headPollIntervalMs: 5 })
}

describe('mixed finality — the finalized-only source keeps its commitment', () => {
  it('a consumer-forwarded `finalized: false` does not down-force a finalized-only source', async () => {
    // `PortalStream` forwards its own effective commitment as a plain boolean, and for a mixed set
    // that is `false`. A source client lets the option win over its own config, so forwarding it
    // verbatim would silently put a finalized-only source on the hot stream. The mock portal here
    // serves ONLY /finalized-stream and 404s /stream, so a down-force fails loudly.
    portal = await finalizedMockPortal([{ statusCode: 200, data: [block(1), block(2)] }])
    const fb = new FallbackClient({
      sources: [{ name: 'bulk', client: client(portal, true) }],
      detection: { capabilityProbe: false },
      logger: silent,
    })

    const got: number[] = []
    for await (const b of fb.getStream(QUERY, { finalized: false }) as AsyncIterable<StreamData<any>>) {
      got.push(...b.blocks.map((x: any) => x.header.number))
      if (got.length >= 2) break
    }

    expect(got).toEqual([1, 2])
  }, 20_000)

  it('a consumer CAN still raise finality for a hot source', async () => {
    // The opposite direction must keep working: a finalized-requiring target forces every source
    // onto its finalized stream. Here the mock serves only /finalized-stream, and the source is
    // configured hot — so the blocks can only arrive if the forced `true` reached it.
    portal = await finalizedMockPortal([{ statusCode: 200, data: [block(1)] }])
    const fb = new FallbackClient({
      sources: [{ name: 'hot', client: client(portal, false) }],
      detection: { capabilityProbe: false },
      logger: silent,
    })

    const got: number[] = []
    for await (const b of fb.getStream(QUERY, { finalized: true }) as AsyncIterable<StreamData<any>>) {
      got.push(...b.blocks.map((x: any) => x.header.number))
      break
    }

    expect(got).toEqual([1])
  }, 20_000)
})

describe('mixed finality — handoff at the frontier', () => {
  it('hands off to the hot source when the finalized one stops making progress', async () => {
    // The bulk portal serves 1..3 and then answers 204 in a loop — it keeps ANSWERING, so the
    // handoff cannot rely on a hanging request; it is the lack of *progress* that must trip it.
    portal = await finalizedBulkPortal([1, 2, 3])
    const tip = mockBlockStreamClient({
      name: 'tip',
      getHead: async () => ({ number: 6, hash: '0x6' }),
      stream: async function* (q: any) {
        for (let n = q.fromBlock; n <= 6; n++) {
          yield {
            blocks: [block(n)],
            head: {},
            meta: { bytes: 0, requestedFromBlock: n, lastBlockReceivedAt: new Date(), requests: {} },
          }
        }
      },
    })

    const fb = new FallbackClient({
      sources: [
        { name: 'bulk', client: client(portal, true) },
        { name: 'tip', client: tip },
      ],
      detection: { capabilityProbe: false, maxStalenessMs: 60, freshnessTickMs: 15, headTtlMs: 0, maxLagBlocks: null },
      logger: silent,
    })

    const got: number[] = []
    for await (const b of fb.getStream(QUERY) as AsyncIterable<StreamData<any>>) {
      got.push(...b.blocks.map((x: any) => x.header.number))
    }

    expect(got).toEqual([1, 2, 3, 4, 5, 6]) // no gap across the handoff
    expect(fb.switchCount).toBe(1)
    expect(fb.activeIndex).toBe(1)
    // The hot source was asked to continue from just past the finalized frontier.
    expect(tip.reads[0]).toMatchObject({ fromBlock: 4, parentBlockHash: '0x3' })
  }, 20_000)

  it('holds, and reports a chain stall, when nothing is ahead of the frontier', async () => {
    // Same frontier, but the standby is no further along. Handing off would gain nothing, so the
    // pipe must hold rather than churn — and say so through `chainStalled`.
    portal = await finalizedBulkPortal([1, 2, 3])
    const standby = mockBlockStreamClient({
      name: 'standby',
      getHead: async () => ({ number: 3, hash: '0x3' }),
      stream: async function* () {},
    })

    const fb = new FallbackClient({
      sources: [
        { name: 'bulk', client: client(portal, true) },
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

  it('does not crawl back into the exhausted source once past its frontier', async () => {
    // After the handoff the pipe is above the bulk portal's finalized head. It stays reachable and
    // recovers to `healthy`, so only the "can it serve where we are" guard keeps eager switch-up
    // from stalling the pipe again.
    portal = await finalizedBulkPortal([1, 2, 3])
    let served = 0
    const tip = mockBlockStreamClient({
      name: 'tip',
      getHead: async () => ({ number: 40, hash: '0x40' }),
      stream: async function* (q: any) {
        for (let n = q.fromBlock; n <= 40; n++) {
          served++
          yield {
            blocks: [block(n)],
            head: {},
            meta: { bytes: 0, requestedFromBlock: n, lastBlockReceivedAt: new Date(), requests: {} },
          }
          await new Promise((r) => setTimeout(r, 5))
        }
      },
    })

    const fb = new FallbackClient({
      sources: [
        { name: 'bulk', client: client(portal, true) },
        { name: 'tip', client: tip },
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
    for await (const b of fb.getStream(QUERY) as AsyncIterable<StreamData<any>>) {
      got.push(...b.blocks.map((x: any) => x.header.number))
    }

    expect(got).toEqual(Array.from({ length: 40 }, (_, i) => i + 1))
    expect(served).toBe(37) // the hot source served 4..40 uninterrupted
    expect(fb.switchCount).toBe(1) // handed off exactly once — no thrash back into the frontier
    expect(fb.activeIndex).toBe(1)
  }, 20_000)
})

describe('mixed finality — reported finality', () => {
  it('reports hot unless every source is finalized-only', async () => {
    portal = await finalizedMockPortal([])
    hotPortal = await mockPortal([])
    const finalizedOnly = { name: 'bulk', client: client(portal, true) }
    const hot = { name: 'tip', client: client(hotPortal, false) }
    const build = (sources: any[]) => new FallbackClient({ sources, logger: silent })

    expect(build([finalizedOnly, hot]).finalized).toBe(false)
    expect(build([hot, finalizedOnly]).finalized).toBe(false)
    expect(build([finalizedOnly, finalizedOnly]).finalized).toBe(true)
    expect(build([hot, hot]).finalized).toBe(false)
  }, 20_000)
})

describe('mixed finality — one stream at a time', () => {
  it('refuses a second concurrent stream instead of interleaving its freshness state', async () => {
    // The per-stream state (forced commitment, probes, head cache, tip latch) describes the stream
    // in flight; a second concurrent stream would silently reinterpret the first one's commitment.
    portal = await finalizedBulkPortal([1, 2, 3])
    const fb = new FallbackClient({
      sources: [{ name: 'bulk', client: client(portal, true) }],
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
