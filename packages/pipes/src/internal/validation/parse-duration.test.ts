import { describe, expect, it } from 'vitest'

import { parseDuration } from './parse-duration.js'

describe('parseDuration', () => {
  it.each([
    ['30s', 30_000],
    ['1m', 60_000],
    ['2m 30s', 150_000],
    ['2m30s', 150_000],
    ['1h', 3_600_000],
    ['250ms', 250],
    ['1.5s', 1_500],
    [' 5s ', 5_000],
  ])('reads %j', (value, expected) => {
    expect(parseDuration(value)).toBe(expected)
  })

  it.each([
    ['', 'empty'],
    ['30', 'no unit'],
    ['s', 'no amount'],
    ['0s', 'zero'],
    ['0ms 0s', 'zero parts'],
    ['-5s', 'negative'],
    ['Infinitys', 'not a number'],
    ['1e3s', 'an exponent'],
    ['5 minutes', 'a unit it does not know'],
    ['5S', 'an upper case unit'],
    ['1m and 5s', 'other words'],
    [`${'9'.repeat(400)}s`, 'an infinite amount'],
    ['25d', 'an unknown day unit'],
    ['1000h', 'more than a timer can hold'],
    [30_000, 'a millisecond count'],
    [Number.POSITIVE_INFINITY, 'a non-finite number'],
    [undefined, 'nothing'],
  ])('refuses %j: %s', (value, _why) => {
    expect(parseDuration(value)).toBeUndefined()
  })
})
