/**
 * Health + selection for the {@link FallbackSource}. This mirrors the Squid SDK's fallback
 * health model (the two SDKs deliberately share no code — only test scenarios), ported onto the
 * Pipes cursor model.
 */
import { SourceErrorInfo } from './fallback-diagnostics.js'

/** Trinary health (§4): `unknown` lets the first batch ship before any probe completes. */
export type FallbackHealth = 'healthy' | 'unhealthy' | 'unknown'

export interface FallbackPolicy {
  /** `eager` (default) reclaims a recovered higher-preference source at a batch boundary. */
  preferPrimary?: 'eager' | 'onFailureOnly'
  /** All sources down: `null` (default) polls forever; a finite value throws after waiting. */
  allDownTimeoutMs?: number | null
  /** Backoff between all-down poll attempts. */
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
   * Staleness/lag detection needs an independent chain-head reference, so it only runs for sources
   * that implement `getHead`. `null` disables each check.
   */
  /** Fail a stalled source over (to a fresher one) if a batch stays outstanding this long. Default 3min. */
  maxStalenessMs?: number | null
  /** Fail the active over once it falls this many blocks behind the independent head. Default 10. */
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

export interface ResolvedFallbackPolicy {
  preferPrimary: 'eager' | 'onFailureOnly'
  allDownTimeoutMs: number | null
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

const DEFAULTS: ResolvedFallbackPolicy = {
  preferPrimary: 'eager',
  allDownTimeoutMs: null,
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

export function resolveFallbackPolicy(p?: FallbackPolicy): ResolvedFallbackPolicy {
  return {
    preferPrimary: p?.preferPrimary ?? DEFAULTS.preferPrimary,
    allDownTimeoutMs: orDefault(p?.allDownTimeoutMs, DEFAULTS.allDownTimeoutMs),
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
    private policy: ResolvedFallbackPolicy,
    hasCapabilityProbe: boolean,
  ) {
    this.#hasCapabilityProbe = hasCapabilityProbe
    this.#capabilityOk = !hasCapabilityProbe
  }

  get state(): FallbackHealth {
    if (this.#state === 'unhealthy' && this.policy.clock() >= this.#cooldownUntil) {
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
    if (this.#livenessFail >= this.policy.livenessFailThreshold) {
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
    if (this.#capabilityOk && this.#livenessPass >= this.policy.livenessRecoverThreshold) {
      this.#state = 'healthy'
      this.#cause = undefined
    }
  }

  #toUnhealthy(cause?: SourceErrorInfo): void {
    this.#state = 'unhealthy'
    this.#cooldownUntil = this.policy.clock() + this.policy.cooldownMs
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

/**
 * Picks the active source: failover tries the lowest-index `healthy` or `unknown` source
 * (optimistically — the stream is the fastest health test); switch-up only ever promotes to a
 * `healthy` source of higher preference (lower index) than the active one.
 */
export class Selector {
  constructor(private health: SourceHealth[]) {}

  pickForFailover(): number | undefined {
    for (let i = 0; i < this.health.length; i++) {
      const s = this.health[i].state
      if (s === 'healthy' || s === 'unknown') return i
    }

    return undefined
  }

  pickSwitchUp(active: number): number | undefined {
    for (let i = 0; i < active; i++) {
      if (this.health[i].state === 'healthy') return i
    }

    return undefined
  }
}
