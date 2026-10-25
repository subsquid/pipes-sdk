import { BlockCursor } from './types.js'

/**
 * Returns the finalized cursor with the higher block number — the monotonic
 * "high-watermark". `undefined` is treated as the lowest possible head, so any
 * concrete cursor wins over it; two `undefined`s stay `undefined`.
 */
export function maxFinalized<T extends BlockCursor>(a: T | undefined, b: T | undefined): T | undefined {
  if (!a) return b
  if (!b) return a

  return b.number > a.number ? b : a
}

/**
 * Normalises a persisted finalized value (which may be `{}` / partial JSON for
 * "no finalized head yet") into a usable cursor or `undefined`.
 */
export function normalizeFinalized(value: unknown): BlockCursor | undefined {
  if (value && typeof value === 'object' && typeof (value as BlockCursor).number === 'number') {
    return value as BlockCursor
  }

  return undefined
}

/**
 * Stateful monotonic finalized-head tracker for a pipe.
 *
 * Seeded from the target's own PERSISTED finalized head at startup so the
 * watermark survives an unclean restart mid-fork, then advanced as batches
 * commit. The floor only ever moves forward.
 *
 * Different data sources can disagree on how DEEP finality starts (a Portal
 * replica swap, or a switch to an RPC source with a fixed confirmation depth),
 * so an incoming finalized head may be LOWER than one already persisted. Acting
 * on the lower value would "un-finalize" blocks already committed as final and
 * corrupt fork resolution, so the effective finalized head never regresses below
 * the floor. Clamping up is safe because finalized *history* is agreed across
 * sources — blocks in `(incoming, floor]` are identical, just not yet re-marked
 * final by the new source.
 */
export class FinalizedWatermark {
  #floor: BlockCursor | undefined

  constructor(floor?: BlockCursor) {
    this.#floor = floor
  }

  /** Highest finalized head ever seen or persisted. */
  get floor(): BlockCursor | undefined {
    return this.#floor
  }

  /**
   * Seed the floor from persisted state. Safe to call repeatedly — it keeps the
   * higher of the existing and provided cursors.
   */
  seed(floor: BlockCursor | undefined): void {
    this.#floor = maxFinalized(this.#floor, floor)
  }

  /**
   * Clamp an incoming batch's finalized head against the floor, advancing the
   * floor to (and returning) the clamped value — the finalized head the target
   * should act on / persist. It can only move forward, never regress below the
   * floor; `undefined` in with no floor stays `undefined` (no-finality passthrough).
   */
  clamp(finalized: BlockCursor | undefined): BlockCursor | undefined {
    this.#floor = maxFinalized(this.#floor, finalized)

    return this.#floor
  }
}
