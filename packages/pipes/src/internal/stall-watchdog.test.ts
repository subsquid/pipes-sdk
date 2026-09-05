import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { stallWatchdog } from './stall-watchdog.js'

describe('stallWatchdog', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('says nothing about a phase that ends before the threshold', async () => {
    const onStall = vi.fn()
    const onRecover = vi.fn()
    const watchdog = stallWatchdog({ thresholdMs: 1_000, onStall, onRecover })

    watchdog.begin('commit')
    await vi.advanceTimersByTimeAsync(999)
    watchdog.end()

    await vi.advanceTimersByTimeAsync(60_000)

    expect(onStall).not.toHaveBeenCalled()
    expect(onRecover).not.toHaveBeenCalled()
  })

  it('reports at the threshold and then on a doubling interval', async () => {
    const onStall = vi.fn()
    const watchdog = stallWatchdog({ thresholdMs: 1_000, onStall })

    watchdog.begin('commit')

    await vi.advanceTimersByTimeAsync(1_000)
    expect(onStall).toHaveBeenCalledTimes(1)
    expect(onStall).toHaveBeenLastCalledWith({ phase: 'commit', elapsedMs: 1_000, count: 1 })

    // Second report is 2s after the first, not 1s: a wedge that never clears must not
    // produce a line per threshold.
    await vi.advanceTimersByTimeAsync(1_000)
    expect(onStall).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(1_000)
    expect(onStall).toHaveBeenLastCalledWith({ phase: 'commit', elapsedMs: 3_000, count: 2 })

    await vi.advanceTimersByTimeAsync(4_000)
    expect(onStall).toHaveBeenLastCalledWith({ phase: 'commit', elapsedMs: 7_000, count: 3 })

    watchdog.end()
  })

  it('never lets the gap between reports grow past maxIntervalMs', async () => {
    const onStall = vi.fn()
    const watchdog = stallWatchdog({ thresholdMs: 1_000, maxIntervalMs: 2_000, onStall })

    watchdog.begin('commit')

    await vi.advanceTimersByTimeAsync(1_000 + 2_000 + 2_000 + 2_000)
    expect(onStall).toHaveBeenCalledTimes(4)

    watchdog.end()
  })

  it('reports the recovery of a phase that stalled, and its total duration', async () => {
    const onStall = vi.fn()
    const onRecover = vi.fn()
    const watchdog = stallWatchdog({ thresholdMs: 1_000, onStall, onRecover })

    watchdog.begin('commit')
    await vi.advanceTimersByTimeAsync(1_500)
    watchdog.end()

    expect(onRecover).toHaveBeenCalledExactlyOnceWith({ phase: 'commit', elapsedMs: 1_500, count: 1 })
  })

  it('restarts the clock on each phase', async () => {
    const onStall = vi.fn()
    const watchdog = stallWatchdog({ thresholdMs: 1_000, onStall })

    watchdog.begin('fetch')
    await vi.advanceTimersByTimeAsync(900)

    watchdog.begin('commit')
    await vi.advanceTimersByTimeAsync(900)
    expect(onStall).not.toHaveBeenCalled()

    await vi.advanceTimersByTimeAsync(100)
    expect(onStall).toHaveBeenCalledExactlyOnceWith({ phase: 'commit', elapsedMs: 1_000, count: 1 })

    watchdog.end()
  })

  it('goes quiet after end()', async () => {
    const onStall = vi.fn()
    const watchdog = stallWatchdog({ thresholdMs: 1_000, onStall })

    watchdog.begin('commit')
    watchdog.end()

    await vi.advanceTimersByTimeAsync(60_000)

    expect(onStall).not.toHaveBeenCalled()
  })
})
