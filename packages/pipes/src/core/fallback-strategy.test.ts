import { describe, expect, it } from 'vitest'

import { AllSourcesDownError } from './fallback-health.js'
import { FallbackSourceSnapshot, FallbackStrategyContext, defaultFallbackStrategy } from './fallback-strategy.js'

/**
 * The stock strategy is a pure function of the context, so it is testable (and composable by
 * custom strategies) without any engine: these pin down the decision it derives from each event.
 */

function snap(
  index: number,
  health: FallbackSourceSnapshot['health'],
  extra: Partial<FallbackSourceSnapshot> = {},
): FallbackSourceSnapshot {
  return { index, name: `s${index}`, health, active: false, ...extra }
}

function ctx(
  partial: Partial<FallbackStrategyContext> & Pick<FallbackStrategyContext, 'event'>,
): FallbackStrategyContext {
  return { sources: [], atTip: false, ...partial }
}

describe('defaultFallbackStrategy', () => {
  it('select: drives the lowest-index healthy or unknown source', () => {
    const strategy = defaultFallbackStrategy()
    const command = strategy(
      ctx({ event: { type: 'select' }, sources: [snap(0, 'unhealthy'), snap(1, 'unknown'), snap(2, 'healthy')] }),
    )

    expect(command).toEqual({ action: 'use', index: 1 })
  })

  it('select: holds while all sources are down and the timeout has not elapsed', () => {
    const strategy = defaultFallbackStrategy({ allDownTimeoutMs: 1000 })
    const command = strategy(ctx({ event: { type: 'select' }, sources: [snap(0, 'unhealthy')], allDownMs: 500 }))

    expect(command).toEqual({ action: 'hold' })
  })

  it('select: aborts with AllSourcesDownError once the all-down timeout elapses', () => {
    const strategy = defaultFallbackStrategy({ allDownTimeoutMs: 1000 })
    const command = strategy(ctx({ event: { type: 'select' }, sources: [snap(0, 'unhealthy')], allDownMs: 1000 }))

    expect(command).toMatchObject({ action: 'abort' })
    expect((command as { error?: Error }).error).toBeInstanceOf(AllSourcesDownError)
  })

  it('batch: fails over on the lagging verdict', () => {
    // The verdict is the detection's (thresholds + tip-arming live there); the strategy just acts.
    const strategy = defaultFallbackStrategy()
    const base = { activeIndex: 0, sources: [snap(0, 'healthy', { active: true })] }

    expect(strategy(ctx({ ...base, event: { type: 'batch', lagging: false, stale: false } }))).toEqual({
      action: 'hold',
    })
    expect(strategy(ctx({ ...base, event: { type: 'batch', lagging: true, stale: false } }))).toEqual({
      action: 'failover',
    })
  })

  it('batch: eagerly reclaims a recovered higher-preference source; onFailureOnly does not', () => {
    const sources = [snap(0, 'healthy'), snap(1, 'healthy', { active: true })]
    const batch = { event: { type: 'batch', lagging: false, stale: false } as const, activeIndex: 1, sources }

    expect(defaultFallbackStrategy()(ctx(batch))).toEqual({ action: 'use', index: 0 })
    expect(defaultFallbackStrategy({ preferPrimary: 'onFailureOnly' })(ctx(batch))).toEqual({ action: 'hold' })
  })

  it('batch: a source that answers without progressing is stale too — hand off if anything is ahead', () => {
    // A finalized-only source parked at its frontier keeps answering (empty batches), so the stall
    // ticker never fires; the boundary verdict is what catches it.
    const strategy = defaultFallbackStrategy()
    const stalled = {
      event: { type: 'batch', lagging: false, stale: true } as const,
      activeIndex: 0,
      cursor: { number: 50, hash: '0x50' },
    }

    expect(
      strategy(ctx({ ...stalled, sources: [snap(0, 'healthy', { active: true }), snap(1, 'healthy', { head: 60 })] })),
    ).toEqual({ action: 'failover' })
    // Nothing ahead ⇒ everyone is equally stuck; churning would not help.
    expect(
      strategy(ctx({ ...stalled, sources: [snap(0, 'healthy', { active: true }), snap(1, 'healthy', { head: 50 })] })),
    ).toEqual({ action: 'hold' })
  })

  it('batch: never reclaims a preferred source that cannot serve the cursor', () => {
    // The exhausted finalized-only source stays reachable and healthy-looking; switching back into
    // it would only stall the pipe again.
    const strategy = defaultFallbackStrategy()
    const batch = {
      event: { type: 'batch', lagging: false, stale: false } as const,
      activeIndex: 1,
      cursor: { number: 105, hash: '0x105' },
    }

    expect(
      strategy(
        ctx({
          ...batch,
          sources: [snap(0, 'healthy', { head: 100 }), snap(1, 'healthy', { active: true, head: 130 })],
        }),
      ),
    ).toEqual({ action: 'hold' })
    // Once it can serve where we are, it is reclaimed as usual.
    expect(
      strategy(
        ctx({
          ...batch,
          sources: [snap(0, 'healthy', { head: 105 }), snap(1, 'healthy', { active: true, head: 130 })],
        }),
      ),
    ).toEqual({ action: 'use', index: 0 })
  })

  it('stall: fails over on the stale verdict only when a fresher source is ahead', () => {
    const strategy = defaultFallbackStrategy()
    const stalled = {
      event: { type: 'stall', pendingMs: 200, stale: true } as const,
      activeIndex: 0,
      cursor: { number: 50, hash: '0x50' },
    }

    // Everyone stuck at our own height — chain stall, churning would not help.
    expect(
      strategy(ctx({ ...stalled, sources: [snap(0, 'healthy', { active: true }), snap(1, 'healthy', { head: 50 })] })),
    ).toEqual({ action: 'hold' })
    // A standby is ahead — the active source is the stale one.
    expect(
      strategy(ctx({ ...stalled, sources: [snap(0, 'healthy', { active: true }), snap(1, 'healthy', { head: 60 })] })),
    ).toEqual({ action: 'failover' })
  })

  it('is configurable standalone — a custom strategy can delegate to its own instance', () => {
    // The documented composition path: different decision options, same algorithm.
    const sticky = defaultFallbackStrategy({ preferPrimary: 'onFailureOnly' })
    const batch = {
      event: { type: 'batch', lagging: false, stale: false } as const,
      activeIndex: 1,
      sources: [snap(0, 'healthy'), snap(1, 'healthy', { active: true })],
    }

    expect(sticky(ctx(batch))).toEqual({ action: 'hold' })
    expect(defaultFallbackStrategy()(ctx(batch))).toEqual({ action: 'use', index: 0 })
  })
})
