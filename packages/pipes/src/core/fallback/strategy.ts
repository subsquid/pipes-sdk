/**
 * The deciding half of the fallback: a pure function from a detection event and a snapshot of the
 * sources to a command. The engine ({@link FallbackClient}) senses; this decides. Keeping it pure
 * keeps the decision table testable without the engine.
 */

import { BlockCursor } from '../types.js'
import { SourceErrorInfo } from './diagnostics.js'
import { FallbackHealth } from './health.js'

/** Tuning for the fallback decisions. */
export interface FallbackStrategyOptions {
  /** `eager` (default) reclaims a recovered higher-preference source at a batch boundary. */
  preferPrimary?: 'eager' | 'onFailureOnly'
  /** All sources down: `null` (default) keeps re-selecting forever; a finite value aborts after waiting this long. */
  allDownTimeoutMs?: number | null
}

/** Why a decision is needed. */
export type FallbackEvent =
  /** No source is driven: at startup, or right after the active one failed. */
  | { type: 'select'; error?: SourceErrorInfo }
  /** A batch was delivered: the only point where a voluntary switch is safe. */
  | { type: 'batch'; lagging: boolean; stale: boolean }
  /** The active source has had a request outstanding past a stall tick. */
  | { type: 'stall'; stale: boolean }

export type FallbackCommand =
  /** Drive this source (on `select`), or switch to it at the boundary (on `batch`). */
  | { action: 'use'; index: number }
  /** Mark the active source unhealthy and select again. */
  | { action: 'failover' }
  /** Keep the current source, or keep waiting. */
  | { action: 'hold' }
  /** Every source stayed down past `allDownTimeoutMs`. */
  | { action: 'abort' }

export interface SourceSnapshot {
  index: number
  health: FallbackHealth
  active: boolean
  /** The last polled head; `undefined` when unknown. */
  head?: number
  /** Taking this source over would only stall the pipe again (see `FallbackClient#isBehind`). */
  behind?: boolean
}

export interface StrategyContext {
  event: FallbackEvent
  activeIndex?: number
  /** One snapshot per source, in preference order. */
  sources: SourceSnapshot[]
  /** The last block delivered to the pipe. */
  cursor?: BlockCursor
  /** ms since every source went unhealthy; set on `select` only. */
  allDownMs?: number
}

export type FallbackStrategy = (ctx: StrategyContext) => FallbackCommand

/** Is a source other than the active one known to be ahead of `cursor`? */
export function fresherSourceAhead(
  sources: readonly SourceSnapshot[],
  cursor: { number: number } | undefined,
): boolean {
  const cursorNumber = cursor?.number ?? -1

  // The frozen head of an unhealthy source is no reason to leave: it cannot be switched to.
  return sources.some((s) => !s.active && s.health !== 'unhealthy' && s.head != null && s.head > cursorNumber)
}

/**
 * - `select`: drive the lowest-index `healthy` or `unknown` source; with none, hold until
 *   `allDownTimeoutMs` elapses, then abort.
 * - `batch`: fail over when `lagging`, or when `stale` while another source is ahead. Otherwise,
 *   under `eager`, reclaim the lowest-index `healthy` source that is not behind.
 * - `stall`: fail over when `stale` and another source is ahead. If nothing is ahead, it is a chain
 *   stall, and a switch would not help.
 */
export function fallbackStrategy(options?: FallbackStrategyOptions): FallbackStrategy {
  const preferPrimary = options?.preferPrimary ?? 'eager'
  const allDownTimeoutMs = options?.allDownTimeoutMs ?? null

  return (ctx) => {
    switch (ctx.event.type) {
      case 'select': {
        const eligible = ctx.sources.find((s) => s.health === 'healthy' || s.health === 'unknown')
        if (eligible) {
          return { action: 'use', index: eligible.index }
        }

        const timedOut = allDownTimeoutMs != null && (ctx.allDownMs ?? 0) >= allDownTimeoutMs

        return timedOut ? { action: 'abort' } : { action: 'hold' }
      }

      case 'batch': {
        if (ctx.event.lagging) {
          return { action: 'failover' }
        }
        if (ctx.event.stale && fresherSourceAhead(ctx.sources, ctx.cursor)) {
          return { action: 'failover' }
        }

        if (preferPrimary === 'eager' && ctx.activeIndex !== undefined) {
          // An exhausted finalized-only source stays reachable and healthy-looking; only `behind`
          // keeps the pipe from switching back into it.
          const reclaim = ctx.sources.find((s) => s.index < ctx.activeIndex! && s.health === 'healthy' && !s.behind)
          if (reclaim) {
            return { action: 'use', index: reclaim.index }
          }
        }

        return { action: 'hold' }
      }

      case 'stall': {
        if (ctx.event.stale && fresherSourceAhead(ctx.sources, ctx.cursor)) {
          return { action: 'failover' }
        }

        return { action: 'hold' }
      }
    }
  }
}
