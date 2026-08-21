/**
 * Failure/recovery detection for the {@link FallbackClient}: the detection knobs, and the
 * per-source trinary health state machine they configure. This mirrors the Squid SDK's fallback
 * health model (the two SDKs deliberately share no code — only test scenarios), ported onto the
 * Pipes cursor model.
 */
import { CapabilityProbeOptions } from './fallback-capability.js'
import { SourceErrorInfo } from './fallback-diagnostics.js'

/** Trinary health (§4): `unknown` lets the first batch ship before any probe completes. */
export type FallbackHealth = 'healthy' | 'unhealthy' | 'unknown'

/**
 * How the fallback engine *detects* failure and recovery — and nothing else. These knobs
 * configure the sensors (capability probes, head polls, liveness bookkeeping, cooldowns) and
 * define the freshness conditions whose verdicts are delivered to the strategy as events
 * (`stall.stale`, `batch.lagging`). What to *do* about a verdict is the strategy's job — the
 * stock decisions are tuned via `strategy: {…}` ({@link DefaultFallbackStrategyOptions}) or
 * replaced with a function.
 */
export interface FallbackDetectionOptions {
  /**
   * Probe every source's actual capability (default `true`): a source counts as `healthy` only
   * once a one-block slice of the configured query succeeds at the indexing frontier — catching a
   * reachable-but-incapable source (trace/`debug_` disabled, pruned state, a Portal answering
   * HTTP 400 to a type-valid query) before a switch-up promotes it. Pass `false` to detect by
   * liveness alone, or `{timeoutMs}` to tune the probe.
   */
  capabilityProbe?: boolean | CapabilityProbeOptions
  /** Backoff between re-checks while every source is down. */
  allDownPollMs?: number
  /** Cooldown an `unhealthy` source waits before returning to `unknown`. */
  cooldownMs?: number
  /** `K` — consecutive failed liveness probes that flip a source `unhealthy`. */
  livenessFailThreshold?: number
  /** `M` — consecutive liveness passes (capability confirmed) required to become `healthy`. */
  livenessRecoverThreshold?: number
  /**
   * Minimum gap between capability probes of the same standby source. The probe is a full query
   * slice, so it is throttled to keep recovery from re-running it on every batch boundary. For a
   * source with `getHead`, a cheap head poll carries liveness and the probe only confirms capability;
   * for a source without it, the probe doubles as the liveness signal. Default 5s.
   */
  capabilityProbeIntervalMs?: number
  /**
   * Defines the *stalled* condition: a request outstanding longer than this makes the active
   * source count as stale (`stall` events carry the verdict; the stock strategy fails a stale
   * source over when a fresher one is ahead). Default 3min; `null` disables the condition.
   */
  maxStalenessMs?: number | null
  /**
   * Defines the *lagging* condition: falling more than this many blocks behind an independent
   * head makes the active source count as lagging — armed only once the stream first reaches the
   * tip, so a backfill never trips it (`batch` events carry the verdict; the stock strategy fails
   * a lagging source over). Default 10; `null` disables the condition.
   */
  maxLagBlocks?: number | null
  /** How often, while a request is outstanding, to re-check staleness. Default 1s. */
  freshnessTickMs?: number
  /** Cache an independent head poll this long, to bound the head-query rate. Default 5s. */
  headTtlMs?: number
  /**
   * Time-box each independent head poll. A head poll is `await`ed on the batch-critical path, so an
   * unbounded one lets a sick standby — TCP up but not responding — stall an otherwise-healthy active
   * source. A poll exceeding this counts as a liveness failure and returns no head. Default 500ms;
   * `null` disables the guard and relies on the underlying client's own request timeout.
   */
  headPollTimeoutMs?: number | null
  /** Injectable clock (ms) for deterministic tests. Defaults to `Date.now`. */
  clock?: () => number
}

export interface ResolvedFallbackDetection {
  capabilityProbe: boolean | CapabilityProbeOptions
  allDownPollMs: number
  cooldownMs: number
  livenessFailThreshold: number
  livenessRecoverThreshold: number
  capabilityProbeIntervalMs: number
  maxStalenessMs: number | null
  maxLagBlocks: number | null
  freshnessTickMs: number
  headTtlMs: number
  headPollTimeoutMs: number | null
  clock: () => number
}

const DEFAULTS: ResolvedFallbackDetection = {
  capabilityProbe: true,
  allDownPollMs: 1000,
  cooldownMs: 30_000,
  livenessFailThreshold: 2,
  livenessRecoverThreshold: 3,
  capabilityProbeIntervalMs: 5000,
  maxStalenessMs: 180_000,
  maxLagBlocks: 10,
  freshnessTickMs: 1000,
  headTtlMs: 5000,
  headPollTimeoutMs: 500,
  clock: () => Date.now(),
}

function orDefault<T>(value: T | undefined, fallback: T): T {
  return value === undefined ? fallback : value
}

export function resolveFallbackDetection(p?: FallbackDetectionOptions): ResolvedFallbackDetection {
  return {
    capabilityProbe: p?.capabilityProbe ?? DEFAULTS.capabilityProbe,
    allDownPollMs: p?.allDownPollMs ?? DEFAULTS.allDownPollMs,
    cooldownMs: p?.cooldownMs ?? DEFAULTS.cooldownMs,
    livenessFailThreshold: p?.livenessFailThreshold ?? DEFAULTS.livenessFailThreshold,
    livenessRecoverThreshold: p?.livenessRecoverThreshold ?? DEFAULTS.livenessRecoverThreshold,
    capabilityProbeIntervalMs: p?.capabilityProbeIntervalMs ?? DEFAULTS.capabilityProbeIntervalMs,
    maxStalenessMs: orDefault(p?.maxStalenessMs, DEFAULTS.maxStalenessMs),
    maxLagBlocks: orDefault(p?.maxLagBlocks, DEFAULTS.maxLagBlocks),
    freshnessTickMs: p?.freshnessTickMs ?? DEFAULTS.freshnessTickMs,
    headTtlMs: p?.headTtlMs ?? DEFAULTS.headTtlMs,
    headPollTimeoutMs: orDefault(p?.headPollTimeoutMs, DEFAULTS.headPollTimeoutMs),
    clock: p?.clock ?? DEFAULTS.clock,
  }
}

export class AllSourcesDownError extends Error {
  override readonly name = 'AllSourcesDownError'

  constructor() {
    super('all fallback data sources are unavailable')
  }
}

/**
 * Per-source trinary health state machine. Pure and timer-free: fed signals (`onStreamError`,
 * `onBatch`, liveness/capability probe results); cooldown expiry resolves lazily on `state` read.
 *
 * A source without a capability probe treats capability as always-confirmed, so liveness alone
 * promotes it. A source *with* a probe drops its confirmation whenever it goes unhealthy, so it can
 * never return to `healthy` until a fresh probe succeeds — liveness alone cannot resurrect a node
 * that keeps failing the real query (e.g. a Portal answering HTTP 400 to a query that passed
 * type-level validation), which would otherwise recover, get re-promoted, and fail again (churn).
 */
export class SourceHealth {
  #state: FallbackHealth = 'unknown'
  #livenessPass = 0
  #livenessFail = 0
  #hasCapabilityProbe: boolean
  #capabilityOk: boolean
  #cooldownUntil = 0
  #cause: SourceErrorInfo | undefined

  constructor(
    private detection: ResolvedFallbackDetection,
    hasCapabilityProbe: boolean,
  ) {
    this.#hasCapabilityProbe = hasCapabilityProbe
    this.#capabilityOk = !hasCapabilityProbe
  }

  get state(): FallbackHealth {
    if (this.#state === 'unhealthy' && this.detection.clock() >= this.#cooldownUntil) {
      this.#toUnknown()
    }

    return this.#state
  }

  /** True once capability has been confirmed — or always, for a source with no capability probe. */
  get capabilityConfirmed(): boolean {
    return this.#capabilityOk
  }

  /** Why the source is currently unhealthy (`undefined` unless `state === 'unhealthy'`). */
  get cause(): SourceErrorInfo | undefined {
    return this.state === 'unhealthy' ? this.#cause : undefined
  }

  onStreamError(cause?: SourceErrorInfo): void {
    this.#toUnhealthy(cause)
  }

  onBatch(): void {
    this.onLivenessPass()
  }

  onLivenessPass(): void {
    if (this.state === 'unhealthy') return

    this.#livenessFail = 0
    this.#livenessPass++
    this.#maybeHealthy()
  }

  onLivenessFail(cause?: SourceErrorInfo): void {
    if (this.state === 'unhealthy') return

    this.#livenessPass = 0
    this.#livenessFail++
    if (this.#livenessFail >= this.detection.livenessFailThreshold) {
      this.#toUnhealthy(cause)
    }
  }

  onCapability(ok: boolean, cause?: SourceErrorInfo): void {
    if (this.state === 'unhealthy') return

    if (ok) {
      this.#capabilityOk = true
      this.#maybeHealthy()
    } else {
      this.#toUnhealthy(cause)
    }
  }

  #maybeHealthy(): void {
    if (this.#capabilityOk && this.#livenessPass >= this.detection.livenessRecoverThreshold) {
      this.#state = 'healthy'
      this.#cause = undefined
    }
  }

  #toUnhealthy(cause?: SourceErrorInfo): void {
    this.#state = 'unhealthy'
    this.#cooldownUntil = this.detection.clock() + this.detection.cooldownMs
    this.#livenessPass = 0
    this.#livenessFail = 0
    this.#cause = cause
    // A probed source must re-prove it can serve the query before it can recover; otherwise a node
    // that stays reachable but keeps failing the real query would flap back to healthy on liveness
    // alone, get re-promoted, and fail again — the churn loop.
    this.#capabilityOk = !this.#hasCapabilityProbe
  }

  #toUnknown(): void {
    this.#state = 'unknown'
    this.#livenessPass = 0
    this.#livenessFail = 0
    this.#cause = undefined
  }
}
