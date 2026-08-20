import { FallbackMetrics } from './fallback-source.js'
import { Metrics } from './metrics-server.js'

export interface FallbackMetricsSource {
  metrics(): FallbackMetrics
}

const HEALTH_STATES = ['healthy', 'unhealthy', 'unknown'] as const

/**
 * Register pull-based gauges that export a {@link FallbackSource}'s observable state on a metrics
 * surface (§4 — "unhealthiness reflected in metrics"): which source is active, each source's
 * trinary health, and the cumulative switch count. The gauges read `source.metrics()` on every
 * scrape via the prom-style `collect` callback, so there is nothing to push.
 */
export function registerFallbackMetrics(
  metrics: Metrics,
  source: FallbackMetricsSource,
  prefix = 'sqd_fallback',
): void {
  metrics.gauge<'source'>({
    name: `${prefix}_active`,
    help: 'Currently active fallback source (1 = active, 0 = standby)',
    labelNames: ['source'],
    collect() {
      for (const s of source.metrics().sources) {
        this.set({ source: s.name }, s.active ? 1 : 0)
      }
    },
  })

  metrics.gauge<'source' | 'state' | 'check' | 'reason' | 'code'>({
    name: `${prefix}_source_health`,
    help:
      'Per-source trinary health (1 for the current state, 0 otherwise). The unhealthy row carries ' +
      'the cause as `check`/`reason`/`code` labels (empty otherwise); the full detail incl. the ' +
      'request is in logs, never a label.',
    labelNames: ['source', 'state', 'check', 'reason', 'code'],
    collect() {
      // Reset so a previous scrape's cause labels (e.g. an old `code`) don't linger as stale series
      // once the source recovers or fails for a different reason. Optional-chained: a custom
      // MetricsServer may not implement reset() (then stale series just aren't pruned).
      this.reset?.()
      for (const s of source.metrics().sources) {
        for (const state of HEALTH_STATES) {
          // Only the current, unhealthy state row gets cause labels.
          const c = state === 'unhealthy' ? s.cause : undefined
          this.set(
            {
              source: s.name,
              state,
              check: c?.check ?? '',
              reason: c?.reason ?? '',
              code: c?.code != null ? String(c.code) : '',
            },
            s.health === state ? 1 : 0,
          )
        }
      }
    },
  })

  metrics.gauge({
    // No `_total` suffix — that suffix is reserved for Counters by Prometheus convention, and this is
    // a pull-based gauge set to the current cumulative value (matches the Squid metric name).
    name: `${prefix}_switches`,
    help: 'Cumulative number of fallback source switches',
    collect() {
      this.set(source.metrics().switchCount)
    },
  })

  metrics.gauge({
    name: `${prefix}_lag_blocks`,
    help: 'Blocks the active source is behind the independent chain-head reference',
    collect() {
      this.set(source.metrics().lag)
    },
  })

  metrics.gauge({
    name: `${prefix}_staleness_ms`,
    help: 'Duration the active source has had a batch request outstanding (ms)',
    collect() {
      this.set(source.metrics().staleness)
    },
  })

  metrics.gauge({
    name: `${prefix}_chain_stalled`,
    help: 'Whether every source is stuck at the same head (1 = stalled)',
    collect() {
      this.set(source.metrics().chainStalled ? 1 : 0)
    },
  })
}
