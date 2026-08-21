import { describe, expect, it } from 'vitest'

import { MockGauge } from '~/testing/index.js'

import { FallbackMetrics } from './fallback-client.js'
import { registerFallbackMetrics } from './fallback-metrics.js'

/** A metrics surface that mimics a real server: gauges are cached by name on re-registration. */
function mockMetrics() {
  const captured = new Map<string, { collect: (this: MockGauge) => void; gauge: MockGauge }>()
  const metrics: any = {
    gauge(config: any) {
      const existing = captured.get(config.name)
      // The node metrics server returns the cached gauge and DROPS the new collect callback.
      if (existing) return existing.gauge

      const gauge = new MockGauge()
      captured.set(config.name, { collect: config.collect, gauge })
      return gauge
    },
  }

  return {
    metrics,
    /** Run every gauge's scrape-time collect callback. */
    scrape() {
      for (const { collect, gauge } of captured.values()) collect.call(gauge)
    },
    calls: (name: string) => captured.get(name)!.gauge.calls,
  }
}

function snapshot(overrides: Partial<FallbackMetrics> = {}): FallbackMetrics {
  return {
    activeIndex: 1,
    switchCount: 2,
    lag: 7,
    staleness: 1500,
    chainHead: 100,
    chainStalled: true,
    sources: [
      {
        name: 'portal',
        health: 'unhealthy',
        active: false,
        cause: { check: 'capability', reason: 'http', code: 400, detail: 'capability check failed: http 400, …' },
      },
      { name: 'rpc', health: 'unknown', active: true },
    ],
    ...overrides,
  }
}

describe('registerFallbackMetrics', () => {
  it('exports active source, per-source health, and switch count via collect', () => {
    const m = mockMetrics()
    registerFallbackMetrics(m.metrics, { metrics: () => snapshot() }, 'my-pipe')
    m.scrape()

    expect(m.calls('sqd_fallback_active')).toEqual([
      { labels: { id: 'my-pipe', source: 'portal' }, value: 0 },
      { labels: { id: 'my-pipe', source: 'rpc' }, value: 1 },
    ])

    const health = m.calls('sqd_fallback_source_health')
    // The unhealthy row carries the cause as bounded labels; the request detail is never a label.
    expect(health).toContainEqual({
      labels: { id: 'my-pipe', source: 'portal', state: 'unhealthy', check: 'capability', reason: 'http', code: '400' },
      value: 1,
    })
    expect(health).toContainEqual({
      labels: { id: 'my-pipe', source: 'portal', state: 'healthy', check: '', reason: '', code: '' },
      value: 0,
    })
    expect(health).toContainEqual({
      labels: { id: 'my-pipe', source: 'rpc', state: 'unknown', check: '', reason: '', code: '' },
      value: 1,
    })
    expect(health.every((c) => !JSON.stringify(c).includes('capability check failed'))).toBe(true)

    expect(m.calls('sqd_fallback_switches')).toEqual([{ labels: { id: 'my-pipe' }, value: 2 }])
    expect(m.calls('sqd_fallback_lag_blocks')).toEqual([{ labels: { id: 'my-pipe' }, value: 7 }])
    expect(m.calls('sqd_fallback_staleness_ms')).toEqual([{ labels: { id: 'my-pipe' }, value: 1500 }])
    expect(m.calls('sqd_fallback_chain_stalled')).toEqual([{ labels: { id: 'my-pipe' }, value: 1 }])
  })

  it('exports every pipe sharing one metrics surface, not just the first', () => {
    // A metrics server caches gauges by name and drops the collect callback of a re-registration,
    // so a second pipe registered the naive way would be invisible — its state silently replaced by
    // the first pipe's on every scrape.
    const m = mockMetrics()
    registerFallbackMetrics(m.metrics, { metrics: () => snapshot({ switchCount: 2 }) }, 'pipe-a')
    registerFallbackMetrics(m.metrics, { metrics: () => snapshot({ switchCount: 9, lag: 42 }) }, 'pipe-b')
    m.scrape()

    expect(m.calls('sqd_fallback_switches')).toEqual([
      { labels: { id: 'pipe-a' }, value: 2 },
      { labels: { id: 'pipe-b' }, value: 9 },
    ])
    expect(m.calls('sqd_fallback_lag_blocks')).toEqual([
      { labels: { id: 'pipe-a' }, value: 7 },
      { labels: { id: 'pipe-b' }, value: 42 },
    ])
    // Per-source series stay separable by pipe too.
    expect(m.calls('sqd_fallback_active')).toEqual([
      { labels: { id: 'pipe-a', source: 'portal' }, value: 0 },
      { labels: { id: 'pipe-a', source: 'rpc' }, value: 1 },
      { labels: { id: 'pipe-b', source: 'portal' }, value: 0 },
      { labels: { id: 'pipe-b', source: 'rpc' }, value: 1 },
    ])
  })

  it('keeps registries of different metrics surfaces independent', () => {
    const a = mockMetrics()
    const b = mockMetrics()
    registerFallbackMetrics(a.metrics, { metrics: () => snapshot({ switchCount: 1 }) }, 'pipe-a')
    registerFallbackMetrics(b.metrics, { metrics: () => snapshot({ switchCount: 5 }) }, 'pipe-b')
    a.scrape()
    b.scrape()

    expect(a.calls('sqd_fallback_switches')).toEqual([{ labels: { id: 'pipe-a' }, value: 1 }])
    expect(b.calls('sqd_fallback_switches')).toEqual([{ labels: { id: 'pipe-b' }, value: 5 }])
  })
})
