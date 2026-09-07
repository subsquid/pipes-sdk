import { FallbackMetrics } from './fallback-client.js'
import { Metrics } from './metrics-server.js'

export interface FallbackMetricsSource {
  metrics(): FallbackMetrics
}

const HEALTH_STATES = ['healthy', 'unhealthy', 'unknown'] as const

/**
 * Every fallback registered against one metrics surface, keyed by metric prefix and then by pipe
 * id. The gauges are created once per prefix and read this registry on each scrape, so a second
 * pipe sharing the surface is exported too.
 *
 * This indirection is load-bearing: a metrics server caches gauges by name and returns the existing
 * one on re-registration, *dropping the new `collect` callback*. Registering a second fallback the
 * naive way would therefore silently keep exporting only the first one's state.
 */
const REGISTRIES = new WeakMap<Metrics, Map<string, Map<string, FallbackMetricsSource>>>()

/**
 * Register pull-based gauges that export a {@link FallbackClient}'s observable state: which source
 * is active, each source's trinary health, the cumulative switch count, and the freshness gauges.
 * The gauges read `source.metrics()` on every scrape via the prom-style `collect` callback, so
 * there is nothing to push.
 *
 * Every series carries the pipe `id`, matching how the rest of the SDK labels per-pipe metrics, so
 * several pipes can share one metrics server and stay individually observable.
 */
export function registerFallbackMetrics(
  metrics: Metrics,
  source: FallbackMetricsSource,
  id: string,
  prefix = 'sqd_fallback',
): void {
  let byPrefix = REGISTRIES.get(metrics)
  if (!byPrefix) {
    byPrefix = new Map()
    REGISTRIES.set(metrics, byPrefix)
  }

  const registered = byPrefix.get(prefix)
  if (registered) {
    // The gauges exist and already read this map — just make this pipe visible to them.
    registered.set(id, source)
    return
  }

  const sources = new Map<string, FallbackMetricsSource>([[id, source]])
  byPrefix.set(prefix, sources)

  metrics.gauge<'id' | 'source'>({
    name: `${prefix}_active`,
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
    name: `${prefix}_source_health`,
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
      name: `${prefix}_${name}`,
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
