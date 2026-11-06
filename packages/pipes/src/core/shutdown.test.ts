import { getEventListeners } from 'node:events'

import { it as base, describe, expect, vi } from 'vitest'

import { parseDuration } from '~/internal/validation/parse-duration.js'

import { PipeError, PipelineShutdownTimeoutError, PipelineStoppedError, ShutdownConfigurationError } from './errors.js'
import type { Logger } from './logger.js'
import { type ResolvedShutdown, resolveShutdown, withShutdown } from './shutdown.js'

function deferred<T = void>() {
  let resolve: (value: T) => void = () => {}
  let reject: (error: unknown) => void = () => {}
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })

  return { promise, resolve, reject }
}

const fakeLogger = () =>
  ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), child: vi.fn() }) as unknown as Logger

const listeners = () => process.listeners('SIGTERM').length + process.listeners('SIGINT').length

const shutdown = (timeout: string, onShutdown?: () => unknown): ResolvedShutdown => {
  return { timeout, timeoutMs: parseDuration(timeout) as number, onShutdown }
}

/** Fake timers, so a deadline is reached without waiting for it. Promises still run. */
const it = base.extend<{ clock: void }>({
  clock: [
    async ({}, use) => {
      vi.useFakeTimers()
      await use()
      vi.useRealTimers()
    },
    { auto: true },
  ],
})

describe('resolveShutdown', () => {
  it('uses the defaults when the option is omitted or empty', () => {
    const defaults = { timeout: '30s', timeoutMs: 30_000, onShutdown: undefined }

    expect(resolveShutdown(undefined)).toEqual(defaults)
    expect(resolveShutdown(null as never)).toEqual(defaults)
    expect(resolveShutdown({})).toEqual(defaults)
  })

  it('reads a function as the callback, with the default timeout', () => {
    const onShutdown = () => {}

    expect(resolveShutdown(onShutdown)).toEqual({ timeout: '30s', timeoutMs: 30_000, onShutdown })
  })

  it('reads an options object', () => {
    const onShutdown = () => {}

    expect(resolveShutdown({ timeout: '60s', onShutdown })).toEqual({ timeout: '60s', timeoutMs: 60_000, onShutdown })
  })

  it('reads false as the opt-out', () => {
    expect(resolveShutdown(false)).toBe(false)
  })

  it.each([
    [true],
    ['30s'],
    [30],
    [[]],
    [{ timeout: '0s' }],
    [{ timeout: -1 }],
    [{ timeout: 30_000 }],
    [{ timeout: 'soon' }],
    [{ onShutdown: 'close' }],
  ])('refuses %j with E0005', (setting) => {
    const resolving = () => resolveShutdown(setting as never)

    expect(resolving).toThrow(ShutdownConfigurationError)
    expect(resolving).toThrow(/See: .*\/E0005/)
  })
})

describe('withShutdown', () => {
  it('resolves on natural completion, with no callback and no listener left', async () => {
    const before = listeners()
    const onShutdown = vi.fn()
    let during = 0

    await withShutdown(shutdown('30s', onShutdown), fakeLogger(), () => {
      during = listeners()
    })

    expect(during).toBe(before + 2)
    expect(listeners()).toBe(before)
    expect(onShutdown).not.toHaveBeenCalled()
  })

  it.for([new Error('write failed'), undefined])(
    'preserves a run failure %j without a signal or callback',
    async (failure) => {
      const onShutdown = vi.fn()

      const run = () => {
        throw failure
      }

      await expect(withShutdown(shutdown('30s', onShutdown), fakeLogger(), run)).rejects.toBe(failure)
      expect(onShutdown).not.toHaveBeenCalled()
    },
  )

  it('aborts on a signal, drains, then runs the callback, then rejects with PipelineStoppedError', async () => {
    const order: string[] = []
    const drain = deferred()

    const running = withShutdown(
      shutdown('30s', () => {
        order.push('callback')
      }),
      fakeLogger(),
      async (signal) => {
        signal.addEventListener('abort', () => order.push('abort'))
        await drain.promise
        order.push('drained')
      },
    )

    process.emit('SIGTERM', 'SIGTERM')
    expect(order).toEqual(['abort'])

    drain.resolve()
    const error = await running.catch((e) => e)

    expect(order).toEqual(['abort', 'drained', 'callback'])
    expect(error).toBeInstanceOf(PipelineStoppedError)
    expect(error).toBeInstanceOf(PipeError)
    expect(error.code).toBe('E3001')
    expect(error.signal).toBe('SIGTERM')
  })

  it('stops on SIGINT as well', async () => {
    const running = withShutdown(shutdown('30s'), fakeLogger(), (signal) => aborted(signal))

    process.emit('SIGINT', 'SIGINT')

    await expect(running).rejects.toMatchObject({ code: 'E3001', signal: 'SIGINT' })
  })

  it('waits for an async callback', async () => {
    const callback = deferred()
    let settled = false

    const running = withShutdown(
      shutdown('30s', () => callback.promise),
      fakeLogger(),
      (signal) => aborted(signal),
    ).catch((e) => {
      settled = true

      return e
    })

    process.emit('SIGTERM', 'SIGTERM')
    await vi.advanceTimersByTimeAsync(0)
    expect(settled).toBe(false)

    callback.resolve()
    await expect(running).resolves.toBeInstanceOf(PipelineStoppedError)
  })

  it('runs the callback after a failed drain, and rejects with the drain failure', async () => {
    const failure = new Error('flush failed')
    const onShutdown = vi.fn()

    const running = withShutdown(shutdown('30s', onShutdown), fakeLogger(), async (signal) => {
      await aborted(signal)
      throw failure
    })

    process.emit('SIGTERM', 'SIGTERM')

    await expect(running).rejects.toBe(failure)
    expect(onShutdown).toHaveBeenCalledTimes(1)
  })

  it('rejects with the failure of a callback, thrown or rejected', async () => {
    const failure = new Error('close failed')

    const callbacks = [
      () => {
        throw failure
      },
      () => Promise.reject(failure),
    ]

    for (const callback of callbacks) {
      const running = withShutdown(shutdown('30s', callback), fakeLogger(), (signal) => aborted(signal))

      process.emit('SIGTERM', 'SIGTERM')

      await expect(running).rejects.toBe(failure)
    }
  })

  it.for([new Error('close failed'), undefined])('keeps a drain failure and callback failure %j', async (callback) => {
    const drain = new Error('flush failed')

    const running = withShutdown(
      shutdown('30s', () => {
        throw callback
      }),
      fakeLogger(),
      async (signal) => {
        await aborted(signal)
        throw drain
      },
    )

    process.emit('SIGTERM', 'SIGTERM')

    const error = await running.catch((e) => e)

    expect(error).toBeInstanceOf(AggregateError)
    expect(error.errors).toEqual([drain, callback])
  })

  it('treats a signal while the target finishes a complete range as a stop', async () => {
    const finishing = deferred()
    const onShutdown = vi.fn()

    const running = withShutdown(shutdown('30s', onShutdown), fakeLogger(), () => finishing.promise)

    process.emit('SIGTERM', 'SIGTERM')
    finishing.resolve()

    await expect(running).rejects.toBeInstanceOf(PipelineStoppedError)
    expect(onShutdown).toHaveBeenCalledTimes(1)
  })

  it('expires during the drain, then runs the callback only after the drain settles', async () => {
    const before = listeners()
    const logger = fakeLogger()
    const drain = deferred()
    const onShutdown = vi.fn()

    const running = withShutdown(shutdown('10s', onShutdown), logger, () => drain.promise).catch((e) => e)

    process.emit('SIGTERM', 'SIGTERM')
    await vi.advanceTimersByTimeAsync(10_000)

    const error = await running

    expect(error).toBeInstanceOf(PipelineShutdownTimeoutError)
    expect(error).toMatchObject({ code: 'E3002', signal: 'SIGTERM', timeout: '10s', step: 'drain' })
    expect(onShutdown).not.toHaveBeenCalled()
    expect(listeners()).toBe(before)

    const late = new Error('commit failed late')
    drain.reject(late)
    await vi.advanceTimersByTimeAsync(0)

    expect(onShutdown).toHaveBeenCalledTimes(1)
    expect(logger.error).toHaveBeenCalledWith(expect.objectContaining({ error: late }))
  })

  it('expires during the callback, and keeps a drain failure that came first as the cause', async () => {
    const failure = new Error('flush failed')
    const callback = deferred()

    const running = withShutdown(
      shutdown('10s', () => callback.promise),
      fakeLogger(),
      async (signal) => {
        await aborted(signal)
        throw failure
      },
    ).catch((e) => e)

    process.emit('SIGTERM', 'SIGTERM')
    await vi.advanceTimersByTimeAsync(10_000)

    const error = await running

    expect(error).toBeInstanceOf(PipelineShutdownTimeoutError)
    expect(error.step).toBe('onShutdown')
    expect(error.cause).toBe(failure)
  })

  it('reports a callback that fails after the deadline', async () => {
    const logger = fakeLogger()
    const callback = deferred()

    const running = withShutdown(
      shutdown('10s', () => callback.promise),
      logger,
      (signal) => aborted(signal),
    ).catch((e) => e)

    process.emit('SIGTERM', 'SIGTERM')
    await vi.advanceTimersByTimeAsync(10_000)
    await running

    const late = new Error('close failed late')
    callback.reject(late)
    await vi.advanceTimersByTimeAsync(0)

    expect(logger.error).toHaveBeenCalledWith(expect.objectContaining({ error: late }))
  })

  it('does not restart the budget on a repeated signal, and runs the callback once', async () => {
    const onShutdown = vi.fn()
    const drain = deferred()

    const running = withShutdown(shutdown('10s', onShutdown), fakeLogger(), () => drain.promise).catch((e) => e)

    process.emit('SIGTERM', 'SIGTERM')
    await vi.advanceTimersByTimeAsync(9_000)
    process.emit('SIGTERM', 'SIGTERM')
    process.emit('SIGINT', 'SIGINT')
    await vi.advanceTimersByTimeAsync(1_000)

    const error = await running

    expect(error).toBeInstanceOf(PipelineShutdownTimeoutError)
    expect(error.signal).toBe('SIGTERM')
    expect(onShutdown).not.toHaveBeenCalled()

    drain.resolve()
    await vi.advanceTimersByTimeAsync(0)
    expect(onShutdown).toHaveBeenCalledTimes(1)
  })

  it('clears the deadline once the shutdown ends', async () => {
    const running = withShutdown(shutdown('10s'), fakeLogger(), (signal) => aborted(signal))

    process.emit('SIGTERM', 'SIGTERM')
    process.emit('SIGTERM', 'SIGTERM')
    await expect(running).rejects.toBeInstanceOf(PipelineStoppedError)

    expect(vi.getTimerCount()).toBe(0)
  })

  it('shares one pair of listeners, stops every run, and leaves when the last run ends', async () => {
    const before = listeners()
    const first = vi.fn()
    const second = vi.fn()
    const slowDrain = deferred()

    const fast = withShutdown(shutdown('30s', first), fakeLogger(), (signal) => aborted(signal))
    const slow = withShutdown(shutdown('30s', second), fakeLogger(), async (signal) => {
      await aborted(signal)
      await slowDrain.promise
    })

    expect(listeners()).toBe(before + 2)

    process.emit('SIGTERM', 'SIGTERM')

    await expect(fast).rejects.toBeInstanceOf(PipelineStoppedError)
    expect(first).toHaveBeenCalledTimes(1)
    expect(second).not.toHaveBeenCalled()
    expect(listeners()).toBe(before + 2)

    slowDrain.resolve()

    await expect(slow).rejects.toBeInstanceOf(PipelineStoppedError)
    expect(second).toHaveBeenCalledTimes(1)
    expect(listeners()).toBe(before)
  })

  it('gives each run its own deadline', async () => {
    const hung = deferred()

    const short = withShutdown(shutdown('5s'), fakeLogger(), () => hung.promise).catch((e) => e)
    const long = withShutdown(shutdown('20s'), fakeLogger(), () => hung.promise).catch((e) => e)

    process.emit('SIGTERM', 'SIGTERM')

    await vi.advanceTimersByTimeAsync(5_000)
    await expect(short).resolves.toMatchObject({ timeout: '5s' })
    expect(vi.getTimerCount()).toBe(1)

    await vi.advanceTimersByTimeAsync(15_000)
    await expect(long).resolves.toMatchObject({ timeout: '20s' })
  })

  it('releases listeners of completed runs while another pipe stays active', async () => {
    const pending = deferred()
    const signals: AbortSignal[] = []
    const active = withShutdown(shutdown('30s'), fakeLogger(), (signal) => {
      signals.push(signal)
      return pending.promise
    })
    const signal = signals[0]
    const before = getEventListeners(signal, 'abort').length

    try {
      for (let i = 0; i < 100; i++) {
        await withShutdown(shutdown('30s'), fakeLogger(), () => {})
      }
      expect(getEventListeners(signal, 'abort')).toHaveLength(before)
    } finally {
      pending.resolve()
      await active
    }
    expect(getEventListeners(signal, 'abort')).toHaveLength(0)
  })

  it('stops a run that starts during a shutdown, on the budget of the first signal', async () => {
    const hung = deferred()

    const first = withShutdown(shutdown('10s'), fakeLogger(), () => hung.promise).catch((e) => e)

    process.emit('SIGTERM', 'SIGTERM')
    await vi.advanceTimersByTimeAsync(4_000)

    let startedAborted = false
    const second = withShutdown(shutdown('10s'), fakeLogger(), (signal) => {
      startedAborted = signal.aborted

      return hung.promise
    }).catch((e) => e)

    expect(startedAborted).toBe(true)

    await vi.advanceTimersByTimeAsync(6_000)
    await expect(first).resolves.toBeInstanceOf(PipelineShutdownTimeoutError)
    await expect(second).resolves.toBeInstanceOf(PipelineShutdownTimeoutError)
  })

  it('starts clean after a shutdown has ended', async () => {
    const stopped = withShutdown(shutdown('10s'), fakeLogger(), (signal) => aborted(signal))
    process.emit('SIGTERM', 'SIGTERM')
    await expect(stopped).rejects.toBeInstanceOf(PipelineStoppedError)

    let startedAborted = true
    await withShutdown(shutdown('10s'), fakeLogger(), (signal) => {
      startedAborted = signal.aborted
    })

    expect(startedAborted).toBe(false)
  })
})

function aborted(signal: AbortSignal) {
  return new Promise<void>((resolve) => {
    if (signal.aborted) {
      resolve()

      return
    }

    signal.addEventListener('abort', () => resolve(), { once: true })
  })
}
