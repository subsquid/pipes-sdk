import { describe, expect, it } from 'vitest'

import { MockGauge } from '~/testing/index.js'

import { registerFallbackMetrics } from './fallback-metrics.js'
import { FallbackMetrics } from './fallback-source.js'

describe('registerFallbackMetrics', () => {
  it('exports active source, per-source health, and switch count via collect', () => {
    const captured = new Map<string, { collect: (this: MockGauge) => void; gauge: MockGauge }>()
    const metrics: any = {
      gauge(config: any) {
        const gauge = new MockGauge()
        captured.set(config.name, { collect: config.collect, gauge })
        return gauge
      },
    }

    const snapshot: FallbackMetrics = {
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
    }
    registerFallbackMetrics(metrics, { metrics: () => snapshot })

    // Drive each gauge's scrape-time collect callback.
    for (const { collect, gauge } of captured.values()) collect.call(gauge)

    expect(captured.get('sqd_fallback_active')!.gauge.calls).toEqual([
      { labels: { source: 'portal' }, value: 0 },
      { labels: { source: 'rpc' }, value: 1 },
    ])

    const health = captured.get('sqd_fallback_source_health')!.gauge
    // The unhealthy row carries the cause as bounded labels; the request detail is never a label.
    expect(health.calls).toContainEqual({
      labels: { source: 'portal', state: 'unhealthy', check: 'capability', reason: 'http', code: '400' },
      value: 1,
    })
    expect(health.calls).toContainEqual({
      labels: { source: 'portal', state: 'healthy', check: '', reason: '', code: '' },
      value: 0,
    })
    expect(health.calls).toContainEqual({
      labels: { source: 'rpc', state: 'unknown', check: '', reason: '', code: '' },
      value: 1,
    })
    expect(health.calls.every((c) => !JSON.stringify(c).includes('capability check failed'))).toBe(true)

    expect(captured.get('sqd_fallback_switches')!.gauge.calls).toEqual([{ value: 2 }])
    expect(captured.get('sqd_fallback_lag_blocks')!.gauge.calls).toEqual([{ value: 7 }])
    expect(captured.get('sqd_fallback_staleness_ms')!.gauge.calls).toEqual([{ value: 1500 }])
    expect(captured.get('sqd_fallback_chain_stalled')!.gauge.calls).toEqual([{ value: 1 }])
  })
})
