import { Metrics } from '../metrics-server.js'
import { FallbackMetrics } from './client.js'

export interface FallbackMetricsSource {
  metrics(): FallbackMetrics
}

const HEALTH_STATES = ['healthy', 'unhealthy', 'unknown'] as const

const PREFIX = 'sqd_fallback'

/**
 * Every fallback on one metrics surface, by pipe id. A metrics server returns the existing gauge on
 * re-registration and drops the new `collect`, so the gauges are created once and read this map.
 */
const REGISTRIES = new WeakMap<Metrics, Map<string, FallbackMetricsSource>>()

/** Register gauges that read a fallback's `metrics()` on every scrape, labelled with the pipe `id`. */
export function registerFallbackMetrics(metrics: Metrics, source: FallbackMetricsSource, id: string): void {
  const registered = REGISTRIES.get(metrics)
  if (registered) {
    registered.set(id, source)

    return
  }

  const sources = new Map<string, FallbackMetricsSource>([[id, source]])
  REGISTRIES.set(metrics, sources)

  metrics.gauge<'id' | 'source'>({
    name: `${PREFIX}_active`,
    help: 'Currently active fallback source (1 = active, 0 = standby)',
    labelNames: ['id', 'source'],
    collect() {
      this.reset?.()
      for (const [id, fallback] of sources) {
        for (const s of fallback.metrics().sources) {
          this.set({ id, source: s.name }, s.active ? 1 : 0)
        }
      }
    },
  })

  metrics.gauge<'id' | 'source' | 'state' | 'check' | 'reason' | 'code'>({
    name: `${PREFIX}_source_health`,
    help:
      'Per-source trinary health (1 for the current state, 0 otherwise). The unhealthy row carries ' +
      'the cause as `check`/`reason`/`code` labels (empty otherwise); the full detail incl. the ' +
      'request is in logs, never a label.',
    labelNames: ['id', 'source', 'state', 'check', 'reason', 'code'],
    collect() {
      // Reset so a previous scrape's cause labels (e.g. an old `code`) don't linger as stale series
      // once the source recovers or fails for a different reason. Optional-chained: a custom
      // MetricsServer may not implement reset() (then stale series just aren't pruned).
      this.reset?.()
      for (const [id, fallback] of sources) {
        for (const s of fallback.metrics().sources) {
          for (const state of HEALTH_STATES) {
            // Only the current, unhealthy state row gets cause labels.
            const c = state === 'unhealthy' ? s.cause : undefined
            this.set(
              {
                id,
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
      }
    },
  })

  /**
   * One scalar gauge per pipe, read straight off the snapshot. A reading of `undefined` publishes
   * no series for that pipe at all: a gauge that cannot be computed must be absent, not zero.
   */
  const scalar = (name: string, help: string, read: (m: FallbackMetrics) => number | undefined) => {
    metrics.gauge<'id'>({
      name: `${PREFIX}_${name}`,
      help,
      labelNames: ['id'],
      collect() {
        this.reset?.()
        for (const [id, fallback] of sources) {
          const value = read(fallback.metrics())
          if (value != null) this.set({ id }, value)
        }
      },
    })
  }

  // No `_total` suffix on the switch count — that suffix is reserved for Counters by Prometheus
  // convention, and this is a pull-based gauge set to the current cumulative value.
  scalar('switches', 'Cumulative number of fallback source switches', (m) => m.switchCount)
  scalar(
    'lag_blocks',
    'Blocks the active source is behind the independent chain-head reference; absent while not computable',
    (m) => m.lag,
  )
  scalar(
    'staleness_ms',
    "Active source's unproductive wait: accumulated time spent answering without delivering a block, excluding time the consumer holds the stream (ms)",
    (m) => m.staleness,
  )
  scalar('chain_stalled', 'Whether every source is stuck at the same head (1 = stalled)', (m) =>
    m.chainStalled ? 1 : 0,
  )
}
