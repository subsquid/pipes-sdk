import type { Logger } from '~/core/logger.js'

/** Sleeps for the given number of milliseconds. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Retries the given async function up to `retries` times.
 *
 * - `delayMs`: base wait between attempts. Linear by default.
 * - `backoff`: `'exp'` doubles `delayMs` per attempt with ±50% jitter (recommended for
 *   transport-level transient errors like RESOURCE_EXHAUSTED, where a flat short delay
 *   re-triggers the same throttle the previous attempt hit). Default `'linear'`.
 * - `shouldRetry`: predicate gating retry on the error. Default retries every error.
 *   Pass a classifier (e.g. `isTransientError`) to fail fast on definitive errors and
 *   stop wasting the budget on them.
 * - `logger`: warns ONCE, after a call that needed retries finally succeeds — a line per
 *   attempt would bury the log on every transient hiccup. A call that exhausts the budget
 *   logs nothing here: it throws, and the caller reports it.
 */
export async function doWithRetry<T>(
  fn: () => Promise<T>,
  {
    retries = 3,
    delayMs = 50,
    backoff = 'linear',
    shouldRetry = () => true,
    title,
    logger,
  }: {
    retries?: number
    delayMs?: number
    backoff?: 'linear' | 'exp'
    shouldRetry?: (error: unknown) => boolean
    title?: string
    logger?: Logger
  } = {},
): Promise<T> {
  let lastError: unknown

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const result = await fn()

      if (attempt > 0) {
        logger?.warn({
          message: `${title ?? 'call'} succeeded after ${attempt} ${attempt === 1 ? 'retry' : 'retries'}`,
          error: lastError,
          attempts: attempt + 1,
        })
      }

      return result
    } catch (error) {
      if (attempt === retries || !shouldRetry(error)) {
        throw error
      }

      lastError = error

      if (delayMs > 0) {
        const wait =
          backoff === 'exp'
            ? // Exponential: delayMs * 2^attempt, then ±50% jitter to de-correlate
              // concurrent retries hitting the same throttled resource.
              delayMs * 2 ** attempt * (0.5 + Math.random())
            : delayMs
        await sleep(wait)
      }
    }
  }

  throw new Error(`Maximum number of ${title ? `${title} ` : ''}retries (${retries}) exceeded`)
}
