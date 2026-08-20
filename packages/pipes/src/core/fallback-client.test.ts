import { describe, expect, it } from 'vitest'

import { BlockStreamClient, ForkException, StreamData } from '~/portal-client/index.js'

import { FallbackClient, FallbackClientOptions, FallbackClientSource } from './fallback-client.js'
import { FallbackStrategy } from './fallback-strategy.js'
import { defaultLogger } from './logger.js'
import { BlockCursor } from './types.js'

/**
 * Unit tests for the fallback meta-client, using mock `BlockStreamClient`s (async generators
 * yielding raw `StreamData` batches). No HTTP is mocked here; the Portal/RPC clients are exercised
 * separately.
 */

/** Keep the default cause-logging (warn) out of the test output. */
const silent = defaultLogger({ level: 'silent' })

function cursor(n: number, hash = `0x${n}`): BlockCursor {
  return { number: n, hash }
}

type WireBlock = { header: { number: number; hash: string } }

function batch(n: number, opts: { hash?: string; finalized?: number } = {}): StreamData<WireBlock> {
  return {
    blocks: [{ header: { number: n, hash: opts.hash ?? `0x${n}` } }],
    head: opts.finalized != null ? { finalized: { number: opts.finalized, hash: `0x${opts.finalized}` } } : {},
    meta: { bytes: 0, requestedFromBlock: n, lastBlockReceivedAt: new Date(), requests: {} },
  }
}

type StreamFn = (query: any) => AsyncGenerator<StreamData<WireBlock>>
type HeadFn = () => Promise<BlockCursor | undefined>
type ProbeFn = () => Promise<{ ok: boolean; cause?: any }>

type MockSource = FallbackClientSource & { reads: any[] }

/** A mock underlying source; `reads` records every `getStream` query it received. */
function source(name: string, stream: StreamFn, getHead?: HeadFn, probeCapability?: ProbeFn): MockSource {
  const reads: any[] = []
  const client = {
    finalized: false,
    getUrl: () => `mock://${name}`,
    getMetadata: async () => ({ dataset: name, aliases: [], real_time: true, start_block: 0 }),
    getHead: async () => (getHead ? await getHead() : undefined),
    resolveTimestamp: async () => {
      throw new Error(`${name}: resolveTimestamp unsupported`)
    },
    getStream: (query: any) => {
      reads.push(query)
      return { [Symbol.asyncIterator]: () => stream(query)[Symbol.asyncIterator]() }
    },
  } as unknown as BlockStreamClient

  return { name, client, reads, ...(probeCapability ? { probeCapability: probeCapability as any } : {}) }
}

const QUERY: any = { type: 'evm', fromBlock: 0 }

function fallback(
  sources: FallbackClientSource[],
  policy?: FallbackClientOptions['policy'],
  extra?: Partial<FallbackClientOptions>,
): FallbackClient {
  // Probes default off in these tests — the generic probe issues real `getStream` slices, which
  // would pollute the mocks' read records. Probe behavior is exercised via per-source overrides.
  return new FallbackClient({ sources, policy, capabilityProbe: false, logger: silent, ...extra })
}

async function collect(fb: FallbackClient, query: any = QUERY): Promise<number[]> {
  const out: number[] = []
  for await (const b of fb.getStream(query)) out.push(...b.blocks.map((x: any) => x.header.number))
  return out
}

/** Never resolves — models a source whose request hangs forever. */
const hang = (): Promise<never> => new Promise<never>(() => {})
/** Resolves after `ms` — models a slow request / a bounded delay before an error. */
const wait = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

describe('FallbackClient — supervisor', () => {
  it('drives the lowest-index source; standbys are untouched', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      yield batch(2)
    })
    const s1 = source('s1', async function* () {
      yield batch(99)
    })
    const fb = fallback([s0, s1])

    expect(await collect(fb)).toEqual([1, 2])
    expect(s1.reads).toHaveLength(0)
  })

  it('resumes the next source just past the last delivered block on a non-fork error', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      yield batch(2)
      throw new Error('boom')
    })
    const s1 = source('s1', async function* () {
      yield batch(3)
    })
    const fb = fallback([s0, s1])

    expect(await collect(fb)).toEqual([1, 2, 3])
    expect(s1.reads).toHaveLength(1)
    expect(s1.reads[0]).toMatchObject({ fromBlock: 3, parentBlockHash: '0x2' })
  })

  it('cascades through multiple failing sources', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      throw new Error('e0')
    })
    const s1 = source('s1', async function* () {
      throw new Error('e1')
    })
    const s2 = source('s2', async function* () {
      yield batch(2)
    })
    const fb = fallback([s0, s1, s2])

    expect(await collect(fb)).toEqual([1, 2])
    expect(s1.reads[0]).toMatchObject({ fromBlock: 2, parentBlockHash: '0x1' })
    expect(s2.reads[0]).toMatchObject({ fromBlock: 2, parentBlockHash: '0x1' })
  })

  it('propagates ForkException instead of switching', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      throw new ForkException([cursor(1)], { fromBlock: 2, parentBlockHash: '0x1' })
    })
    const s1 = source('s1', async function* () {
      yield batch(99)
    })
    const fb = fallback([s0, s1])

    const seen: number[] = []
    await expect(
      (async () => {
        for await (const b of fb.getStream(QUERY)) seen.push(...b.blocks.map((x: any) => x.header.number))
      })(),
    ).rejects.toBeInstanceOf(ForkException)
    expect(seen).toEqual([1])
    expect(s1.reads).toHaveLength(0)
  })

  it('passes batch heads through untouched — the finalized watermark is the PortalStream’s job', async () => {
    // Unlike the pre-facade supervisor, the client does NOT own the monotonic finalized watermark:
    // the consuming PortalStream clamps every batch (source switches included). This guards that the
    // client stays a pure pass-through, so the clamp happens exactly once, in one place.
    const s0 = source('s0', async function* () {
      yield batch(1, { finalized: 2 })
      throw new Error('s0 down')
    })
    const s1 = source('s1', async function* () {
      yield batch(3, { finalized: 1 }) // shallower head right after the switch — passed through as-is
    })
    const fb = fallback([s0, s1])

    const finalizedSeen: (number | undefined)[] = []
    for await (const b of fb.getStream(QUERY)) finalizedSeen.push(b.head.finalized?.number)

    expect(finalizedSeen).toEqual([2, 1])
  })

  it('propagates a ForkException thrown by a source reached AFTER a switch (fork straddles the switch)', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      throw new Error('s0 down') // non-fork error → fail over to s1
    })
    // s1 resumes at the last delivered block and detects a reorg right there. A fork thrown by the
    // switched-in source must propagate untouched (the target/consumer rewinds), never trigger
    // another source switch.
    const s1 = source('s1', async function* () {
      throw new ForkException([cursor(1)], { fromBlock: 2, parentBlockHash: '0x1' })
    })
    const fb = fallback([s0, s1])

    const seen: number[] = []
    await expect(
      (async () => {
        for await (const b of fb.getStream(QUERY)) seen.push(...b.blocks.map((x: any) => x.header.number))
      })(),
    ).rejects.toBeInstanceOf(ForkException)
    expect(seen).toEqual([1]) // s0's block delivered; then s1's fork propagated
  })

  it('reclaims a recovered higher-preference source once its capability probe confirms it', async () => {
    let now = 0
    let s0reads = 0
    let probes = 0
    const s0 = source(
      's0',
      async function* () {
        s0reads++
        if (s0reads === 1) throw new Error('s0 down') // initial failure → fail over to s1
        yield batch(50) // reclaimed after the probe confirms capability
      },
      undefined,
      async () => {
        probes++
        return { ok: true }
      },
    )
    const s1 = source('s1', async function* () {
      for (let n = 1; n <= 6; n++) {
        yield batch(n)
        now += 100 // advance the (injected) clock so s0's cooldown elapses between batches
      }
    })

    const fb = fallback([s0, s1], {
      clock: () => now,
      cooldownMs: 50,
      capabilityProbeIntervalMs: 0,
      headTtlMs: 0,
      livenessRecoverThreshold: 1, // one successful probe is enough to confirm + recover
    })

    const out = await collect(fb)

    expect(probes).toBeGreaterThan(0) // the standby s0 was probed
    expect(out).toContain(50) // and reclaimed once healthy
    expect(fb.metrics().switchCount).toBe(2) // s0 → s1 (failover) → s0 (switch-up)
  })

  it('eager: reclaims a recovered probe-less higher-preference source via head-poll liveness alone', async () => {
    let now = 0
    let s0reads = 0
    // s0 has NO capability probe — its recovery must come from head-poll liveness only (the head
    // fetch doubles as the liveness signal that promotes a probe-less source back to healthy).
    const s0 = source(
      's0',
      async function* () {
        s0reads++
        if (s0reads === 1) throw new Error('s0 down') // initial failure → fail over to s1
        yield batch(50) // reclaimed once head-poll liveness promotes it
      },
      async () => cursor(50),
    )
    const s1 = source('s1', async function* () {
      for (let n = 1; n <= 6; n++) {
        yield batch(n)
        now += 100 // advance clock so s0's cooldown elapses and it gets head-polled between batches
      }
    })

    const fb = fallback([s0, s1], {
      clock: () => now,
      cooldownMs: 50,
      headTtlMs: 0,
      livenessRecoverThreshold: 1, // one head-poll liveness pass is enough for a probe-less source
      preferPrimary: 'eager',
      maxLagBlocks: null,
      maxStalenessMs: null,
    })

    const out = await collect(fb)

    expect(out).toContain(50) // s0 reclaimed without ever running a capability probe
    expect(fb.metrics().switchCount).toBe(2) // s0 → s1 (failover) → s0 (switch-up)
  })

  it('onFailureOnly: does not switch up to a recovered higher-preference source', async () => {
    let now = 0
    let s0reads = 0
    const s0 = source(
      's0',
      async function* () {
        s0reads++
        if (s0reads === 1) throw new Error('s0 down')
        yield batch(50) // would be reclaimed under 'eager' — must NOT be under 'onFailureOnly'
      },
      undefined,
      async () => ({ ok: true }),
    )
    const s1 = source('s1', async function* () {
      for (let n = 1; n <= 4; n++) {
        yield batch(n)
        now += 100
      }
    })

    const fb = fallback([s0, s1], {
      clock: () => now,
      cooldownMs: 50,
      capabilityProbeIntervalMs: 0,
      headTtlMs: 0,
      livenessRecoverThreshold: 1,
      preferPrimary: 'onFailureOnly', // sticky: only switch on failure, never reclaim
    })

    const out = await collect(fb)

    expect(out).toEqual([1, 2, 3, 4]) // stayed on s1 the whole time
    expect(out).not.toContain(50) // s0 never reclaimed
    expect(fb.metrics().switchCount).toBe(1) // only the initial failover — no switch-up
  })

  it('does not switch up to a standby whose capability probe keeps failing', async () => {
    let now = 0
    let probes = 0
    const s0 = source(
      's0',
      async function* () {
        throw new Error('s0 down') // always fails the real query
      },
      undefined,
      async () => {
        probes++
        // reachable-but-incapable: never confirms, and reports a classified cause
        return { ok: false, cause: { check: 'capability' as const, reason: 'http' as const, code: 400, detail: 'x' } }
      },
    )
    const s1 = source('s1', async function* () {
      for (let n = 1; n <= 4; n++) {
        yield batch(n)
        now += 100
      }
    })

    const fb = fallback([s0, s1], {
      clock: () => now,
      cooldownMs: 50,
      capabilityProbeIntervalMs: 0,
      headTtlMs: 0,
      livenessRecoverThreshold: 1,
    })

    const out = await collect(fb)

    expect(probes).toBeGreaterThan(0) // s0 was probed but never confirmed
    expect(out).toEqual([1, 2, 3, 4]) // stayed on s1 the whole time
    expect(fb.metrics().switchCount).toBe(1) // only the initial failover — no churn back to s0
  })

  it('survives a capability probe that throws synchronously (fails as capability, flag not stranded)', async () => {
    let now = 0
    let probes = 0
    const s0 = source(
      's0',
      async function* () {
        throw new Error('s0 down') // always fails the real query
      },
      undefined,
      // A misbehaving custom probe that throws *synchronously*, before returning a Promise.
      (() => {
        probes++
        throw new Error('sync boom')
      }) as any,
    )
    const s1 = source('s1', async function* () {
      for (let n = 1; n <= 4; n++) {
        yield batch(n)
        now += 100
      }
    })

    const fb = fallback([s0, s1], {
      clock: () => now,
      cooldownMs: 50,
      capabilityProbeIntervalMs: 0,
      headTtlMs: 0,
      livenessRecoverThreshold: 1,
    })

    const out = await collect(fb)

    // A synchronous throw escaping #maybeProbeCapability would stall the read loop (and strand
    // #capabilityProbing at true). Normalizing it into a rejection keeps the supervisor streaming
    // s1 to completion, and the probe having run proves the fire-and-forget path executed.
    expect(probes).toBeGreaterThan(0)
    expect(out).toEqual([1, 2, 3, 4])
    expect(fb.metrics().switchCount).toBe(1) // never churned back to the incapable s0
  })

  it('throws AllSourcesDown after a finite timeout', async () => {
    const down: StreamFn = async function* () {
      throw new Error('down')
    }
    const fb = fallback([source('s0', down), source('s1', down)], { allDownTimeoutMs: 0, allDownPollMs: 1 })

    await expect(collect(fb)).rejects.toThrowError(/all fallback data sources/)
  })

  it('clears the active source and freshness gauges during an all-down gap', async () => {
    // One source that global-stalls (no other source ⇒ nothing fresher) then errors. On the error
    // there is no eligible source left, so the all-down path — not a switch — is the only thing that
    // can clear the freshness gauges it left set.
    const s0 = source('s0', async function* () {
      yield batch(50)
      await wait(60)
      throw new Error('s0 down')
    })
    const fb = fallback([s0], {
      maxStalenessMs: 30,
      freshnessTickMs: 5,
      allDownTimeoutMs: 0,
      allDownPollMs: 1,
      cooldownMs: 60_000,
    })

    const it = fb.getStream(QUERY)[Symbol.asyncIterator]()
    expect((await it.next()).value.blocks[0].header.number).toBe(50)

    // Pull the next batch so the staleness clock runs: s0 stalls past maxStalenessMs with no fresher
    // alternative → global stall flagged on the active.
    const pending = it.next()
    await wait(45)
    expect(fb.chainStalled).toBe(true)
    expect(fb.activeIndex).toBe(0)

    // s0 then errors; nothing eligible remains → all-down. The gauges must not keep reporting s0.
    await expect(pending).rejects.toThrowError(/all fallback data sources/)
    expect(fb.activeIndex).toBeUndefined()
    expect(fb.metrics().sources.every((s) => !s.active)).toBe(true)
    expect(fb.chainStalled).toBe(false)
    expect(fb.staleness).toBe(0)
    expect(fb.lag).toBe(0)
    expect(fb.chainHead).toBeUndefined()
  }, 5000)
})

describe('FallbackClient — metrics', () => {
  it('reports the active source, switch count, and per-source health', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      throw new Error('boom')
    })
    const s1 = source('s1', async function* () {
      yield batch(2)
    })
    const fb = fallback([s0, s1])

    await collect(fb)
    const m = fb.metrics()

    expect(m.activeIndex).toBe(1)
    expect(m.switchCount).toBe(1)
    expect(m.sources).toMatchObject([
      { name: 's0', health: 'unhealthy', active: false },
      { name: 's1', health: 'unknown', active: true },
    ])
    // The unhealthy source carries its classified cause; the healthy/unknown one does not.
    expect(m.sources[0].cause).toMatchObject({ check: 'stream', reason: 'unknown' })
    expect(m.sources[0].cause?.detail).toContain('boom')
    expect(m.sources[1].cause).toBeUndefined()
  })
})

describe('FallbackClient — freshness', () => {
  it('(a) lag: fails over once it falls behind the independent head (after arming at the tip)', async () => {
    const s1heads = [95, 110] // first boundary arms (lag 5), second trips (lag 19)
    const s0 = source('s0', async function* () {
      yield batch(90)
      yield batch(91)
      yield batch(92) // not reached
    })
    const s1 = source(
      's1',
      async function* () {
        yield batch(92)
        yield batch(93)
      },
      async () => cursor(s1heads.shift() ?? 110),
    )
    const fb = fallback([s0, s1], { maxLagBlocks: 10, maxStalenessMs: null, headTtlMs: 0 })

    expect(await collect(fb)).toEqual([90, 91, 92, 93])
    expect(fb.activeIndex).toBe(1)
    expect(s1.reads[0]).toMatchObject({ fromBlock: 92, parentBlockHash: '0x91' }) // resumed after last delivered
  })

  it('(b) historical sync: a huge lag during backfill never fails over (never armed)', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      yield batch(2)
      yield batch(3)
    })
    const s1 = source(
      's1',
      async function* () {},
      async () => cursor(1_000_000),
    )
    const fb = fallback([s0, s1], { maxLagBlocks: 10, maxStalenessMs: null, headTtlMs: 0 })

    expect(await collect(fb)).toEqual([1, 2, 3])
    expect(fb.activeIndex).toBe(0)
  })

  it('(c) staleness: fails over a stalled source when a fresher source is ahead', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      await hang()
    })
    const s1 = source(
      's1',
      async function* () {
        yield batch(2)
      },
      async () => cursor(100),
    )
    const fb = fallback([s0, s1], { maxStalenessMs: 30, freshnessTickMs: 5, headTtlMs: 0, maxLagBlocks: null })

    expect(await collect(fb)).toEqual([1, 2])
    expect(fb.activeIndex).toBe(1)
  }, 5000)

  it('(e) slow-handler immunity: a slow downstream consumer between yields does not mark the source stale', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      yield batch(2)
      yield batch(3)
    })
    // A fresher standby exists, so staleness *could* fire — but the staleness clock spans a single
    // source `next()`, not wall-clock across yields, so time spent in a slow consumer must not count.
    const s1 = source(
      's1',
      async function* () {},
      async () => cursor(100),
    )
    const fb = fallback([s0, s1], { maxStalenessMs: 30, freshnessTickMs: 5, headTtlMs: 0, maxLagBlocks: null })

    const got: number[] = []
    for await (const b of fb.getStream(QUERY)) {
      got.push(...b.blocks.map((x: any) => x.header.number))
      await wait(50) // slow handler (> maxStalenessMs), but parked at yield — clock not running on s0
    }
    expect(got).toEqual([1, 2, 3]) // all served by s0
    expect(fb.activeIndex).toBe(0) // never failed over
  }, 5000)

  it('(d) global stall: no fresher source → holds + flags chainStalled, no churn', async () => {
    const s0 = source('s0', async function* () {
      yield batch(50)
      await wait(120)
      throw new Error('client timeout') // eventually errors, like a real client
    })
    const s1 = source(
      's1',
      async function* () {
        yield batch(51)
      },
      async () => cursor(50), // same head → global stall
    )
    const fb = fallback([s0, s1], { maxStalenessMs: 30, freshnessTickMs: 5, headTtlMs: 0, maxLagBlocks: null })

    const it = fb.getStream(QUERY)[Symbol.asyncIterator]()
    expect((await it.next()).value.blocks[0].header.number).toBe(50)

    const pending = it.next() // hangs; staleness climbs but no fresher source exists
    await wait(80)
    expect(fb.chainStalled).toBe(true)
    expect(fb.activeIndex).toBe(0) // held — did NOT churn

    // s0 finally errors → ordinary failover to s1
    expect((await pending).value.blocks[0].header.number).toBe(51)
    expect(fb.activeIndex).toBe(1)
    expect(fb.chainStalled).toBe(false) // cleared once progress resumed on the new source
  }, 5000)

  it('(d2) global stall: keeps probing the held source, and recovers when one becomes fresher', async () => {
    // The active hangs forever; recovery must come from continued probing of the other source.
    const s0 = source('s0', async function* () {
      yield batch(50)
      await hang()
    })

    // Global stall (same head) for a while... Head polls run once per tick past the staleness
    // threshold (~35ms in), so this script keeps the stall going past the 60ms checkpoint below.
    const s1heads = [50, 50, 50, 50, 50, 50, 50, 50]
    let probedCapability = 0
    const s1 = source(
      's1',
      async function* () {
        yield batch(51)
      },
      async () => cursor(s1heads.shift() ?? 51), // ...then it advances to 51
      async () => (probedCapability++, { ok: true }),
    )
    const fb = fallback([s0, s1], { maxStalenessMs: 30, freshnessTickMs: 5, headTtlMs: 0, maxLagBlocks: null })

    const it = fb.getStream(QUERY)[Symbol.asyncIterator]()
    expect((await it.next()).value.blocks[0].header.number).toBe(50)

    // While held, the supervisor keeps polling the other source — liveness *and* capability —
    // so it is positioned to notice recovery.
    const next = it.next()
    await wait(60)
    expect(fb.chainStalled).toBe(true)
    expect(s1heads.length).toBeLessThan(3) // s1's head was (re)polled during the hold (liveness)
    expect(probedCapability).toBeGreaterThan(0) // capability probe fired during the hold

    // s1's head advances past us → fail over to it, recovering without the active ever resolving.
    expect((await next).value.blocks[0].header.number).toBe(51)
    expect(fb.activeIndex).toBe(1)
    expect(fb.chainStalled).toBe(false)
  }, 5000)

  it('(f) thresholds disabled: neither lag nor staleness fires', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      yield batch(2)
    })
    const s1 = source(
      's1',
      async function* () {},
      async () => cursor(1_000_000),
    )
    const fb = fallback([s0, s1], { maxLagBlocks: null, maxStalenessMs: null, headTtlMs: 0 })

    expect(await collect(fb)).toEqual([1, 2])
    expect(fb.activeIndex).toBe(0)
  })

  it('(g) resets freshness gauges on a switch (no stale lag from the old source)', async () => {
    const s1heads = [95, 110] // arm at lag 5, then trip at lag 19
    const s0 = source('s0', async function* () {
      yield batch(90)
      yield batch(91)
    })
    // Empty standby: after failover the stream ends immediately, so no boundary recomputes
    // freshness — exposing whether the switch itself cleared the old source's lag.
    const s1 = source(
      's1',
      async function* () {},
      async () => cursor(s1heads.shift() ?? 110),
    )
    const fb = fallback([s0, s1], { maxLagBlocks: 10, maxStalenessMs: null, headTtlMs: 0 })

    expect(await collect(fb)).toEqual([90, 91])
    expect(fb.activeIndex).toBe(1)
    expect(fb.lag).toBe(0) // not the stale 19 the lag trigger recorded against s0
  })

  it('(h) re-arms lag per stream: a reused instance does not inherit "at tip" for a backfill', async () => {
    let phase = 1
    const s0 = source('s0', async function* () {
      if (phase === 1) {
        yield batch(100)
      } else {
        yield batch(1)
        yield batch(2)
      }
    })
    // Phase 1: head sits at s0 (arms the lag trigger). Phase 2: head is far ahead (backfill).
    const s1 = source(
      's1',
      async function* () {},
      async () => cursor(phase === 1 ? 100 : 1_000_000),
    )
    const fb = fallback([s0, s1], { maxLagBlocks: 10, maxStalenessMs: null, headTtlMs: 0 })

    // Stream 1 reaches the tip → arms the lag trigger on the instance.
    expect(await collect(fb, { ...QUERY, fromBlock: 100 })).toEqual([100])

    // Stream 2 on the SAME instance backfills far behind head. If the armed state leaked, the first
    // boundary would trip (lag ~1e6 > 10) and fail over; a per-stream reset prevents that.
    phase = 2
    expect(await collect(fb)).toEqual([1, 2])
    expect(fb.activeIndex).toBe(0) // stayed on s0 — no spurious failover
  })

  it('(i) does not arm lag while the reference is behind us (stale standby) — no spurious failover', async () => {
    // The standby is first *behind* the active (negative lag), then jumps to the real tip while the
    // active is still backfilling. Arming on the negative lag would let that jump trip a spurious
    // failover; gating arming on `lag >= 0` keeps us on the active.
    const s1heads = [40, 1_000] // behind us at 50 (lag -10), then far ahead
    const s0 = source('s0', async function* () {
      yield batch(50)
      yield batch(51)
      yield batch(52)
    })
    const s1 = source(
      's1',
      async function* () {},
      async () => cursor(s1heads.shift() ?? 1_000),
    )
    const fb = fallback([s0, s1], { maxLagBlocks: 10, maxStalenessMs: null, headTtlMs: 0 })

    expect(await collect(fb, { ...QUERY, fromBlock: 50 })).toEqual([50, 51, 52])
    expect(fb.activeIndex).toBe(0) // never armed (was ahead of the reference) ⇒ no lag failover
  })
})

describe('FallbackClient — head-poll timeout (robustness)', () => {
  // A head poll that never resolves — models a sick standby: TCP up, no response.
  const hangHead = (): Promise<BlockCursor | undefined> => new Promise<BlockCursor | undefined>(() => {})

  it('a sick standby whose getHead hangs does not stall the healthy active source', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      yield batch(2)
      yield batch(3)
    })
    // Its head poll hangs; without the timeout, the per-batch lag check would block s0 forever.
    const s1 = source('s1', async function* () {}, hangHead)
    const fb = fallback([s0, s1], {
      maxLagBlocks: 10,
      maxStalenessMs: null,
      headTtlMs: 0,
      headPollTimeoutMs: 20,
      livenessFailThreshold: 1, // one timed-out poll condemns the sick standby
      cooldownMs: 60_000,
    })

    expect(await collect(fb)).toEqual([1, 2, 3]) // the healthy primary streamed to completion
    expect(fb.activeIndex).toBe(0)
    const s1health = fb.metrics().sources[1]
    expect(s1health.health).toBe('unhealthy')
    expect(s1health.cause).toMatchObject({ check: 'liveness', reason: 'timeout' })
  }, 5000)
})

describe('FallbackClient — active capability confirmation', () => {
  it('reaches healthy by serving batches, without the standby capability probe ever running', async () => {
    let probed = 0
    const s0 = source(
      's0',
      async function* () {
        yield batch(1)
        yield batch(2)
        yield batch(3)
        yield batch(4)
      },
      undefined,
      async () => {
        probed++
        return { ok: true }
      },
    )
    const fb = fallback([s0], { livenessRecoverThreshold: 3, maxLagBlocks: null, maxStalenessMs: null })

    expect(await collect(fb)).toEqual([1, 2, 3, 4])
    // The active source proved capability by serving the query — the standby probe never ran for it,
    // yet it still left `unknown` for `healthy`.
    expect(probed).toBe(0)
    expect(fb.metrics().sources[0].health).toBe('healthy')
  })
})

describe('FallbackClient — BlockStreamClient surface', () => {
  it('getHead returns the highest head any source reports; getMetadata the first that answers', async () => {
    const s0 = source(
      's0',
      async function* () {},
      async () => cursor(10),
    )
    const s1 = source(
      's1',
      async function* () {},
      async () => cursor(42),
    )
    const fb = fallback([s0, s1])

    expect((await fb.getHead())?.number).toBe(42)
    expect((await fb.getMetadata()).dataset).toBe('s0')
  })

  it('rejects sources that disagree on `finalized`', () => {
    const s0 = source('s0', async function* () {})
    const s1 = source('s1', async function* () {})
    ;(s1.client as any).finalized = true

    expect(() => fallback([s0, s1])).toThrowError(/agree on `finalized`/)
  })

  it('getUrl names the active source', async () => {
    const s0 = source('s0', async function* () {
      throw new Error('down')
    })
    const s1 = source('s1', async function* () {
      yield batch(1)
    })
    const fb = fallback([s0, s1])

    expect(fb.getUrl()).toBe('mock://s0') // nothing driven yet → primary
    await collect(fb)
    expect(fb.getUrl()).toBe('mock://s1')
  })
})

describe('FallbackClient — custom strategy (code as config)', () => {
  it('pins a source: a `select` handler routes every selection, ignoring preference order', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1) // healthy and preferred — but the strategy pins s1
    })
    const s1 = source('s1', async function* () {
      yield batch(7)
      yield batch(8)
    })
    const pinToS1: FallbackStrategy = (ctx) => {
      if (ctx.event.type === 'select') return { action: 'use', index: 1 }

      return undefined
    }
    const fb = fallback([s0, s1], { maxLagBlocks: null, maxStalenessMs: null }, { strategy: pinToS1 })

    expect(await collect(fb)).toEqual([7, 8])
    expect(s0.reads).toHaveLength(0)
  })

  it('events the strategy leaves unanswered fall back to the default (failover still works)', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      throw new Error('boom')
    })
    const s1 = source('s1', async function* () {
      yield batch(2)
    })
    // Handles nothing — every event returns undefined, so behavior must equal the default.
    const noop: FallbackStrategy = () => undefined
    const fb = fallback([s0, s1], undefined, { strategy: noop })

    expect(await collect(fb)).toEqual([1, 2])
    expect(fb.activeIndex).toBe(1)
  })

  it('a `batch` handler can jump sources voluntarily — the abandoned source stays healthy', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
      yield batch(2) // never reached: the strategy jumps after the first batch
    })
    const s1 = source('s1', async function* () {
      yield batch(2)
    })
    const jumpOnce: FallbackStrategy = (ctx) => {
      if (ctx.event.type === 'batch' && ctx.activeIndex === 0) return { action: 'use', index: 1 }

      return undefined
    }
    const fb = fallback([s0, s1], { maxLagBlocks: null, maxStalenessMs: null }, { strategy: jumpOnce })

    expect(await collect(fb)).toEqual([1, 2])
    expect(fb.metrics().switchCount).toBe(1)
    expect(fb.metrics().sources[0].health).not.toBe('unhealthy') // a voluntary jump is not a failure
    expect(s1.reads[0]).toMatchObject({ fromBlock: 2, parentBlockHash: '0x1' }) // still cursor-continuous
  })

  it('a `stall` handler can hold a source the default would have abandoned', async () => {
    let s0resume: (() => void) | undefined
    const s0 = source('s0', async function* () {
      yield batch(1)
      await new Promise<void>((r) => {
        s0resume = r
      })
      yield batch(2)
    })
    // A fresher standby exists, so the DEFAULT stall policy would fail s0 over; this strategy holds.
    const s1 = source(
      's1',
      async function* () {
        yield batch(99)
      },
      async () => cursor(100),
    )
    const holdAlways: FallbackStrategy = (ctx) => {
      if (ctx.event.type === 'stall') return { action: 'hold' }

      return undefined
    }
    const fb = fallback(
      [s0, s1],
      { maxStalenessMs: 10, freshnessTickMs: 5, headTtlMs: 0, maxLagBlocks: null },
      { strategy: holdAlways },
    )

    const it = fb.getStream(QUERY)[Symbol.asyncIterator]()
    expect((await it.next()).value.blocks[0].header.number).toBe(1)

    const pending = it.next()
    await wait(50) // well past maxStalenessMs, with a fresher standby — default would have switched
    expect(fb.activeIndex).toBe(0)
    expect(s1.reads).toHaveLength(0)

    s0resume?.()
    expect((await pending).value.blocks[0].header.number).toBe(2) // held source resumed
  }, 5000)

  it('a `select` handler can abort with a custom error', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
    })
    const giveUp: FallbackStrategy = (ctx) => {
      if (ctx.event.type === 'select') return { action: 'abort', error: new Error('strategy says no') }

      return undefined
    }
    const fb = fallback([s0], undefined, { strategy: giveUp })

    await expect(collect(fb)).rejects.toThrowError('strategy says no')
  })

  it('rejects an out-of-range source index from the strategy', async () => {
    const s0 = source('s0', async function* () {
      yield batch(1)
    })
    const offByOne: FallbackStrategy = (ctx) => {
      if (ctx.event.type === 'select') return { action: 'use', index: 5 }

      return undefined
    }
    const fb = fallback([s0], undefined, { strategy: offByOne })

    await expect(collect(fb)).rejects.toThrowError(/selected source index 5/)
  })

  it('sees classified errors and health snapshots in the `select` context', async () => {
    const contexts: any[] = []
    const s0 = source('s0', async function* () {
      yield batch(1)
      throw new Error('connection refused')
    })
    const s1 = source('s1', async function* () {
      yield batch(2)
    })
    const observe: FallbackStrategy = (ctx) => {
      if (ctx.event.type === 'select') contexts.push(structuredClone({ event: ctx.event, sources: ctx.sources }))
      return undefined // observe only; defer every decision to the default
    }
    const fb = fallback([s0, s1], undefined, { strategy: observe })

    expect(await collect(fb)).toEqual([1, 2])

    // First selection: no prior error, both sources unknown.
    expect(contexts[0].event.error).toBeUndefined()
    expect(contexts[0].sources.map((s: any) => s.health)).toEqual(['unknown', 'unknown'])
    // Re-selection after s0's failure: the classified cause is on the event and in s0's snapshot.
    expect(contexts[1].event.error?.detail).toContain('connection refused')
    expect(contexts[1].sources[0].health).toBe('unhealthy')
    expect(contexts[1].sources[0].cause?.detail).toContain('connection refused')
  })
})
