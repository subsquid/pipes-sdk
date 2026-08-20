/**
 * Code-as-config for the fallback's *decisions*. The engine ({@link FallbackClient}) keeps the
 * machinery nobody should have to rewrite — health bookkeeping, liveness/capability probes,
 * head polling, error classification, staleness clocks — and consults a {@link FallbackStrategy}
 * at every decision point: which source to drive, whether to abandon the active one, whether to
 * reclaim a recovered one. The default strategy reproduces the documented policy exactly; a custom
 * one can replace any subset of the decisions (return `undefined` to fall back to the default for
 * that event), so "not our algorithm" is a function away without giving up the machinery.
 *
 * Safety invariants are NOT delegated: fork propagation, resume-from-cursor and never switching
 * mid-batch stay in the engine regardless of the strategy.
 */
import { SourceErrorInfo } from './fallback-diagnostics.js'
import { AllSourcesDownError, FallbackHealth, ResolvedFallbackPolicy } from './fallback-health.js'
import { BlockCursor } from './types.js'

/** Why the strategy is being consulted. */
export type FallbackEvent =
  /**
   * No source is being driven — at startup, or right after the active one failed (`error` carries
   * the classified failure). Decide which source to drive next: `use` picks one (any health — an
   * optimistic pick is legal, the stream itself is the fastest health test), `hold` waits
   * `allDownPollMs` and asks again, `abort` gives up and fails the stream.
   */
  | { type: 'select'; error?: SourceErrorInfo }
  /**
   * A batch was just delivered — the only point where a *voluntary* switch is safe. Decide whether
   * to stay (`hold`), reclaim/jump to another source (`use` — the active source stays healthy), or
   * abandon the active one (`failover` — marks it unhealthy first).
   */
  | { type: 'batch' }
  /**
   * The active source has had a request outstanding for `pendingMs` (consulted every
   * `freshnessTickMs` while waiting). `failover` abandons it; `hold` keeps waiting.
   */
  | { type: 'stall'; pendingMs: number }

export interface FallbackSourceSnapshot {
  index: number
  name: string
  health: FallbackHealth
  active: boolean
  /** Why the source is currently unhealthy (`undefined` otherwise). */
  cause?: SourceErrorInfo
  /** Latest independently-polled head block number (TTL-cached); `undefined` when unknown. */
  head?: number
}

export interface FallbackStrategyContext {
  event: FallbackEvent
  /** Index of the source currently being driven; `undefined` while none is. */
  activeIndex?: number
  /** One snapshot per configured source, in preference order. */
  sources: FallbackSourceSnapshot[]
  /** The last block delivered to the pipe — the resume point of any switch. */
  cursor?: BlockCursor
  /**
   * Blocks the pipe is behind the highest head among the *other* sources (an independent
   * reference — never the active source's own head). `undefined` when not computable.
   */
  lagBlocks?: number
  /** Latched true once the stream first reaches the chain tip (lag failover arms only then). */
  atTip: boolean
  /** ms since every source went unhealthy; only set on `select` when no source is eligible. */
  allDownMs?: number
}

export type FallbackCommand =
  /** Drive this source (on `select`), or switch to it at the boundary (on `batch`). */
  | { action: 'use'; index: number }
  /** Abandon the active source — it is marked unhealthy (with cooldown) — and re-select. */
  | { action: 'failover' }
  /** Keep the current source / keep waiting. */
  | { action: 'hold' }
  /** Fail the stream with `error` (defaults to {@link AllSourcesDownError}). */
  | { action: 'abort'; error?: Error }

/**
 * Decides what the fallback does at each {@link FallbackEvent}. Returning `undefined` (or nothing)
 * delegates that event to the default strategy, so a custom strategy only has to express what it
 * wants to change — e.g. handle `select` and ignore the rest.
 */
export type FallbackStrategy = (ctx: FallbackStrategyContext) => FallbackCommand | undefined | void

/**
 * The stock algorithm, expressed as a {@link FallbackStrategy} over the policy's decision knobs:
 *
 * - `select`: drive the lowest-index `healthy` or `unknown` source; with none eligible, poll until
 *   `allDownTimeoutMs` elapses (`null` ⇒ forever), then abort.
 * - `batch`: fail the active source over once it lags more than `maxLagBlocks` behind an
 *   independent head (armed only after first reaching the tip); otherwise, under `eager`
 *   preference, reclaim the lowest-index recovered (`healthy`) source above the active one.
 * - `stall`: fail the active source over once its request has been outstanding longer than
 *   `maxStalenessMs` **and** a fresher source is ahead — if everyone is equally stuck it is a
 *   chain stall, and churning sources would not help, so hold.
 */
export function defaultFallbackStrategy(policy: ResolvedFallbackPolicy): FallbackStrategy {
  return (ctx: FallbackStrategyContext): FallbackCommand => {
    switch (ctx.event.type) {
      case 'select': {
        for (const s of ctx.sources) {
          if (s.health === 'healthy' || s.health === 'unknown') return { action: 'use', index: s.index }
        }
        if (policy.allDownTimeoutMs != null && (ctx.allDownMs ?? 0) >= policy.allDownTimeoutMs) {
          return { action: 'abort', error: new AllSourcesDownError() }
        }

        return { action: 'hold' }
      }

      case 'batch': {
        if (policy.maxLagBlocks != null && ctx.atTip && (ctx.lagBlocks ?? 0) > policy.maxLagBlocks) {
          return { action: 'failover' }
        }
        if (policy.preferPrimary === 'eager' && ctx.activeIndex !== undefined) {
          for (const s of ctx.sources) {
            if (s.index >= ctx.activeIndex) break
            if (s.health === 'healthy') return { action: 'use', index: s.index }
          }
        }

        return { action: 'hold' }
      }

      case 'stall': {
        if (policy.maxStalenessMs != null && ctx.event.pendingMs > policy.maxStalenessMs) {
          const cursorNumber = ctx.cursor?.number ?? -1
          const fresherAhead = ctx.sources.some((s) => !s.active && s.head != null && s.head > cursorNumber)
          if (fresherAhead) return { action: 'failover' }
        }

        return { action: 'hold' }
      }
    }
  }
}
