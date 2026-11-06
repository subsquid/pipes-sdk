// A longer delay overflows setTimeout, which then fires at once.
const MAX_TIMER_MS = 2 ** 31 - 1

const UNIT_MS: Record<string, number> = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000 }
const DURATION_PART = /(\d+(?:\.\d+)?)\s*(ms|s|m|h)/g
const DURATION = /^\d+(?:\.\d+)?\s*(?:ms|s|m|h)(?:\s*\d+(?:\.\d+)?\s*(?:ms|s|m|h))*$/

/** The milliseconds in a duration such as `'2m 30s'`, or undefined when it is not a positive one. */
export function parseDuration(value: unknown): number | undefined {
  if (typeof value !== 'string' || !DURATION.test(value.trim())) {
    return undefined
  }

  let total = 0
  for (const [, amount, unit] of value.matchAll(DURATION_PART)) {
    total += Number(amount) * UNIT_MS[unit]
  }

  const usable = total > 0 && total <= MAX_TIMER_MS

  return usable ? total : undefined
}
