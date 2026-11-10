/** Race `promise` against a `ms` timeout that rejects with `makeError()`. `ms == null` disables it. */
export function withTimeout<T>(
  promise: Promise<T>,
  ms: number | null | undefined,
  makeError: () => unknown,
): Promise<T> {
  if (ms == null) {
    return promise
  }

  promise.catch(() => {}) // an abandoned (timed-out) promise must not surface as unhandled
  const timer = delay(ms)
  const timeout = timer.promise.then(() => Promise.reject(makeError()))

  return Promise.race([promise, timeout]).finally(() => timer.cancel())
}

/** Close an iterator without waiting: `return()` of a stalled stream can hang on the same fetch. */
export function safeReturn(it: AsyncIterator<unknown>): void {
  try {
    it.return?.()?.then(
      () => {},
      () => {},
    )
  } catch {
    /* ignore */
  }
}

/** A cancellable timer: `promise` resolves after `ms` unless `cancel()` clears it first. */
export function delay(ms: number): { promise: Promise<void>; cancel: () => void } {
  let cancel = () => {}
  const promise = new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms)
    cancel = () => clearTimeout(timer)
  })

  return { promise, cancel: () => cancel() }
}

/** Resolves after `ms`, or as soon as `signal` aborts. */
export function abortableSleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve()
      return
    }

    const done = () => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    signal?.addEventListener('abort', done, { once: true })
  })
}

/**
 * `iterator.next()`, or `undefined` as soon as `signal` aborts. For a source that cannot cancel its
 * own request: the abandoned `next()` settles on its own.
 */
export async function nextUnlessAborted<T>(
  iterator: AsyncIterator<T>,
  signal: AbortSignal | undefined,
): Promise<IteratorResult<T> | undefined> {
  if (signal?.aborted) {
    return undefined
  }

  const next = iterator.next()
  if (!signal) {
    return next
  }

  next.catch(() => {}) // an abandoned request must not surface as unhandled

  let onAbort = () => {}
  const aborted = new Promise<undefined>((resolve) => {
    onAbort = () => resolve(undefined)
    signal.addEventListener('abort', onAbort, { once: true })
  })

  try {
    return await Promise.race([next, aborted])
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
}
