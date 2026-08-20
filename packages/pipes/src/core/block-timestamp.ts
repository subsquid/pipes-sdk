/** Epoch seconds at 2001-09-09. Below this a chain timestamp is absent, a devnet stub, or garbage. */
const MIN_PLAUSIBLE_SECONDS = 1_000_000_000

/** Epoch seconds at 2096-10-02. Above this the value cannot be a real block time. */
const MAX_PLAUSIBLE_SECONDS = 4_000_000_000

/** Anything at or above this is milliseconds: as seconds it would land in the year 33658. */
const MILLISECOND_THRESHOLD = MIN_PLAUSIBLE_SECONDS * 1000

/**
 * Normalize a portal block timestamp to epoch seconds, or `undefined` when it cannot be trusted.
 *
 * Portal block times are network-dependent (GAP-24): EVM and Solana report seconds, tron and
 * substrate milliseconds. Consumers that subtract them from a wall clock need one unit.
 *
 * Out-of-range values are dropped rather than clamped. Tron's portal is known to emit integers
 * above 2^53 (`query/tron.ts` tolerates them deliberately), and a single such value absorbed into
 * a Prometheus histogram moves `_sum` to a magnitude where the float64 ULP exceeds every later
 * sample — the series then freezes for the life of the process instead of merely spiking.
 */
export function blockTimestampSeconds(timestamp: number | undefined): number | undefined {
  if (typeof timestamp !== 'number' || !Number.isFinite(timestamp)) return undefined

  const seconds = timestamp >= MILLISECOND_THRESHOLD ? timestamp / 1000 : timestamp

  if (seconds < MIN_PLAUSIBLE_SECONDS || seconds > MAX_PLAUSIBLE_SECONDS) return undefined

  return seconds
}
