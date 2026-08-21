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

  it('batch: fails over on excessive lag, but only once armed at the tip', () => {
    const strategy = defaultFallbackStrategy({ maxLagBlocks: 10 })
    const base = {
      event: { type: 'batch' } as const,
      activeIndex: 0,
      sources: [snap(0, 'healthy', { active: true })],
      lagBlocks: 50,
    }

    expect(strategy(ctx({ ...base, atTip: false }))).toEqual({ action: 'hold' }) // backfill: never
    expect(strategy(ctx({ ...base, atTip: true }))).toEqual({ action: 'failover' })
  })

  it('batch: eagerly reclaims a recovered higher-preference source; onFailureOnly does not', () => {
    const sources = [snap(0, 'healthy'), snap(1, 'healthy', { active: true })]
    const batch = { event: { type: 'batch' } as const, activeIndex: 1, sources }

    expect(defaultFallbackStrategy()(ctx(batch))).toEqual({ action: 'use', index: 0 })
    expect(defaultFallbackStrategy({ preferPrimary: 'onFailureOnly' })(ctx(batch))).toEqual({ action: 'hold' })
  })

  it('stall: fails over past the threshold only when a fresher source is ahead', () => {
    const strategy = defaultFallbackStrategy({ maxStalenessMs: 100 })
    const stalled = {
      event: { type: 'stall', pendingMs: 200 } as const,
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
    // The documented composition path: different thresholds, same algorithm.
    const lenient = defaultFallbackStrategy({ maxLagBlocks: 100 })
    const base = {
      event: { type: 'batch' } as const,
      activeIndex: 0,
      sources: [snap(0, 'healthy', { active: true })],
      atTip: true,
    }

    expect(lenient(ctx({ ...base, lagBlocks: 50 }))).toEqual({ action: 'hold' })
    expect(lenient(ctx({ ...base, lagBlocks: 150 }))).toEqual({ action: 'failover' })
  })
})
