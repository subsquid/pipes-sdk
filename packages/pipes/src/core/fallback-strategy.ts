/**
 * The deciding half of the fallback. Responsibilities are split in two:
 *
 * - **`detection` is sensing** ({@link FallbackDetectionOptions}): it configures how failure and
 *   recovery are *detected* — capability probes, head polls, liveness thresholds, cooldowns — and
 *   defines the freshness conditions whose verdicts arrive on the events here (`stall.stale`,
 *   `batch.lagging`).
 * - **`strategy` is deciding**: what to *do* about the detected state — which source to drive,
 *   whether to abandon the active one, whether to reclaim a recovered one. Configure the stock
 *   decisions with plain options ({@link DefaultFallbackStrategyOptions}), or replace them with a
 *   {@link FallbackStrategy} function. A custom function receives the measurements *and* the stock
 *   decision ({@link FallbackStrategyContext.defaultCommand}), so it can inspect, veto, or amend
 *   it — returning `undefined` lets it stand.
 *
 * The engine ({@link FallbackClient}) keeps the machinery nobody should have to rewrite — health
 * bookkeeping, probes, head polling, error classification, staleness clocks — and its safety
 * invariants are NOT delegated: fork propagation, resume-from-cursor and never switching mid-batch
 * hold regardless of the strategy.
 */
import { SourceErrorInfo } from './fallback-diagnostics.js'
import { AllSourcesDownError, FallbackHealth } from './fallback-health.js'
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
   * A batch was just delivered — the only point where a *voluntary* switch is safe. Two detection
   * verdicts ride along: `lagging` (behind an independent head by more than `maxLagBlocks`, armed
   * only once the tip was first reached) and `stale` (no block delivered for `maxStalenessMs` — a
   * source can keep answering *without progressing*, e.g. a portal parked at its finality frontier
   * returning empty batches). Decide whether to stay (`hold`), reclaim/jump to another source
   * (`use` — the active source stays healthy), or abandon the active one (`failover` — marks it
   * unhealthy first).
   */
  | { type: 'batch'; lagging: boolean; stale: boolean }
  /**
   * The active source has had a request outstanding for `pendingMs` (consulted every
   * `freshnessTickMs` while waiting). `stale` is the detection verdict: the request has been
   * pending longer than `maxStalenessMs`. `failover` abandons the source; `hold` keeps waiting.
   */
  | { type: 'stall'; pendingMs: number; stale: boolean }

export interface FallbackSourceSnapshot {
  index: number
  name: string
  health: FallbackHealth
  active: boolean
  /** Why the source is currently unhealthy (`undefined` otherwise). */
  cause?: SourceErrorInfo
  /** Latest independently-polled head block number (TTL-cached); `undefined` when unknown. */
  head?: number
  /**
   * Detection's verdict that this source's reach has fallen structurally under the pipe, so taking
   * over would only stall it. Ordinary ingestion jitter does not count — see the allowance in
   * `FallbackDetectionOptions.maxLagBlocks`.
   */
  behind?: boolean
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
  /** Latched true once the stream first reaches the chain tip (the lagging verdict arms then). */
  atTip: boolean
  /** ms since every source went unhealthy; only set on `select` when no source is eligible. */
  allDownMs?: number
  /**
   * What the stock strategy decides for this event — always set when a custom strategy is
   * consulted. Inspect it to veto or amend the stock behavior instead of re-deriving it;
   * returning `undefined` (or `defaultCommand` itself) lets it stand.
   */
  defaultCommand?: FallbackCommand
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
 * lets the stock decision (`ctx.defaultCommand`) stand, so a custom strategy only has to express
 * what it wants to change — handle one event and ignore the rest, or veto a specific stock
 * decision:
 *
 * ```ts
 * // never fail over to the expensive RPC standby while still backfilling
 * strategy: (ctx) => {
 *   const d = ctx.defaultCommand
 *   if (d?.action === 'use' && ctx.sources[d.index].name === 'rpc' && !ctx.atTip) {
 *     return { action: 'hold' }
 *   }
 *   return undefined // everything else: stock behavior
 * }
 * ```
 */
export type FallbackStrategy = (ctx: FallbackStrategyContext) => FallbackCommand | undefined | void

/** Is a source other than the active one known to be ahead of the pipe's cursor? */
function fresherSourceAhead(ctx: FallbackStrategyContext): boolean {
  const cursorNumber = ctx.cursor?.number ?? -1
  // An unhealthy source cannot be switched to, so its (frozen) head is no reason to abandon the
  // active one — that only walks the pipe into an all-down gap.
  return ctx.sources.some((s) => !s.active && s.health !== 'unhealthy' && s.head != null && s.head > cursorNumber)
}

/** Tuning for the stock strategy — the plain-data alternative to a custom function. */
export interface DefaultFallbackStrategyOptions {
  /** `eager` (default) reclaims a recovered higher-preference source at a batch boundary. */
  preferPrimary?: 'eager' | 'onFailureOnly'
  /** All sources down: `null` (default) keeps re-selecting forever; a finite value aborts after waiting this long. */
  allDownTimeoutMs?: number | null
}

/**
 * The stock algorithm, expressed as a {@link FallbackStrategy} over the detection verdicts:
 *
 * - `select`: drive the lowest-index `healthy` or `unknown` source; with none eligible, keep
 *   re-selecting until `allDownTimeoutMs` elapses (`null` ⇒ forever), then abort.
 * - `batch`: fail the active source over when the detection says it is `lagging`, or `stale` while
 *   another source is ahead (a source that answers without progressing — a portal parked at its
 *   finality frontier — is stalled too); otherwise, under `eager` preference, reclaim the
 *   lowest-index recovered (`healthy`) source that can actually serve the cursor.
 * - `stall`: fail the active source over when the detection says it is `stale` **and** a fresher
 *   source is ahead — if everyone is equally stuck it is a chain stall, and churning sources
 *   would not help, so hold.
 *
 * This is what runs when no custom strategy is configured, and what produces
 * {@link FallbackStrategyContext.defaultCommand} when one is. It is a pure function of the
 * context, so a custom strategy can also instantiate its own (e.g. with a different preference
 * mode) and delegate to it: `defaultFallbackStrategy({ preferPrimary: 'onFailureOnly' })(ctx)`.
 */
export function defaultFallbackStrategy(options?: DefaultFallbackStrategyOptions): FallbackStrategy {
  const preferPrimary = options?.preferPrimary ?? 'eager'
  const allDownTimeoutMs = options?.allDownTimeoutMs === undefined ? null : options.allDownTimeoutMs

  return (ctx: FallbackStrategyContext): FallbackCommand => {
    switch (ctx.event.type) {
      case 'select': {
        for (const s of ctx.sources) {
          if (s.health === 'healthy' || s.health === 'unknown') return { action: 'use', index: s.index }
        }
        if (allDownTimeoutMs != null && (ctx.allDownMs ?? 0) >= allDownTimeoutMs) {
          return { action: 'abort', error: new AllSourcesDownError() }
        }

        return { action: 'hold' }
      }

      case 'batch': {
        if (ctx.event.lagging) {
          return { action: 'failover' }
        }
        // Answering without progressing is a stall too — hand off, but only to somewhere better.
        // If nothing is ahead of us, everyone is equally stuck and churning would not help.
        if (ctx.event.stale && fresherSourceAhead(ctx)) {
          return { action: 'failover' }
        }
        if (preferPrimary === 'eager' && ctx.activeIndex !== undefined) {
          for (const s of ctx.sources) {
            if (s.index >= ctx.activeIndex) break
            // Never reclaim a source that has fallen structurally behind: an exhausted
            // finalized-only source stays reachable and healthy-looking, and switching back into it
            // would only stall the pipe again. A source merely trailing by ingestion jitter is
            // still a good source to hand back to, so only detection's `behind` verdict disqualifies.
            if (s.health === 'healthy' && !s.behind) {
              return { action: 'use', index: s.index }
            }
          }
        }

        return { action: 'hold' }
      }

      case 'stall': {
        if (ctx.event.stale && fresherSourceAhead(ctx)) {
          return { action: 'failover' }
        }

        return { action: 'hold' }
      }
    }
  }
}
