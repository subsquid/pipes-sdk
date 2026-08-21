/**
 * Race `promise` against a `ms` timeout. On timeout the result rejects with `makeError()`; the
 * abandoned `promise` is silenced so its late settlement can't surface as an unhandled rejection,
 * and the timer is always cleared. `ms == null` disables the guard and returns `promise` unchanged,
 * deferring to the underlying operation's own timeout.
 *
 * `makeError` is a thunk so the (possibly side-effecting) error value is built only if the timeout
 * actually fires — and so callers can attach a classified cause (see the capability probe) rather
 * than a bare `Error`.
 */
export function withTimeout<T>(
  promise: Promise<T>,
  ms: number | null | undefined,
  makeError: () => unknown,
): Promise<T> {
  if (ms == null) return promise

  promise.catch(() => {}) // an abandoned (timed-out) promise must not surface as unhandled
  // The timer handle stays inside the executor and is reached only through `cancelTimer`, which
  // starts as a no-op — so the cleanup can never run against an unassigned handle, whatever this
  // grows into later.
  let cancelTimer = () => {}
  const timeout = new Promise<never>((_resolve, reject) => {
    const timer = setTimeout(() => reject(makeError()), ms)
    cancelTimer = () => clearTimeout(timer)
  })

  return Promise.race([promise, timeout]).finally(() => cancelTimer())
}

/**
 * Fire-and-forget close of an async iterator. Closing a *stalled* source's stream must neither block
 * failover nor surface a late rejection: `return()` can itself hang on the same unresolved fetch, and
 * its rejection (if any) is swallowed. Never awaited by design.
 */
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

/**
 * A cancellable timer: `promise` resolves after `ms` unless `cancel()` clears it first. The stall
 * ticker races it against the pending batch, and must clear the timer on every race winner so
 * abandoned ticks don't accumulate.
 */
export function delay(ms: number): { promise: Promise<void>; cancel: () => void } {
  let cancel = () => {}
  const promise = new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms)
    cancel = () => clearTimeout(timer)
  })

  return { promise, cancel: () => cancel() }
}
