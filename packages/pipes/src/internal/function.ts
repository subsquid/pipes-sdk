/**
 * Runs `fn` at most once, sharing the in-flight promise with every concurrent caller. Guarding on
 * the *result* instead — `if (value) return value` — leaves a window where callers that arrive
 * before the first one resolves each start their own run, which for a lazily constructed client
 * means several clients (and several connection pools) for one configured endpoint.
 *
 * A failed run is not cached: the next caller starts a fresh attempt, so a transient failure
 * cannot poison the helper for the life of the process.
 */
export function once<T>(fn: () => Promise<T>): () => Promise<T> {
  let pending: Promise<T> | undefined

  return () => {
    pending ??= fn().catch((e) => {
      pending = undefined
      throw e
    })

    return pending
  }
}

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
 */
export async function doWithRetry<T>(
  fn: () => Promise<T>,
  {
    retries = 3,
    delayMs = 50,
    backoff = 'linear',
    shouldRetry = () => true,
    title,
  }: {
    retries?: number
    delayMs?: number
    backoff?: 'linear' | 'exp'
    shouldRetry?: (error: unknown) => boolean
    title?: string
  } = {},
): Promise<T> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fn()
    } catch (error) {
      if (attempt === retries || !shouldRetry(error)) {
        throw error
      }

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
