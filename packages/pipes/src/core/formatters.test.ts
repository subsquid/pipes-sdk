import { describe, expect, it } from 'vitest'

import { formatDuration } from './formatters.js'

describe('formatDuration', () => {
  it('formats sub-minute durations in seconds', () => {
    expect(formatDuration(0)).toBe('0s')
    expect(formatDuration(499)).toBe('0s')
    expect(formatDuration(45_000)).toBe('45s')
    expect(formatDuration(59_400)).toBe('59s')
  })

  it('formats minutes with the remaining seconds', () => {
    // Rounding to the nearest second is what promotes 59.6s to a minute, not a separate rule.
    expect(formatDuration(59_600)).toBe('1m 0s')
    expect(formatDuration(60_000)).toBe('1m 0s')
    expect(formatDuration(150_000)).toBe('2m 30s')
  })

  it('drops seconds once the duration reaches an hour', () => {
    expect(formatDuration(3_600_000)).toBe('1h 0m')
    expect(formatDuration(3_900_000)).toBe('1h 5m')
  })

  it('clamps a negative duration to zero', () => {
    expect(formatDuration(-1_000)).toBe('0s')
  })
})
