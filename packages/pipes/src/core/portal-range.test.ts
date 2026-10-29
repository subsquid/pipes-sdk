import { describe, expect, it } from 'vitest'

import { BlockRangeConfigurationError } from './errors.js'
import { parsePortalRange } from './portal-range.js'

describe('parsePortalRange', () => {
  it('reads formatted block numbers, dates and relative ends', () => {
    expect(parsePortalRange({ from: '1_000_000', to: '+500' })).toEqual({ from: 1_000_000, to: 1_000_500 })
    expect(parsePortalRange({ from: '1,000', to: 2_000 })).toEqual({ from: 1_000, to: 2_000 })
    expect(parsePortalRange({ from: '2024-01-01' })).toEqual({ from: new Date('2024-01-01T00:00:00Z'), to: undefined })
  })

  it('keeps an end of block 0 as a bound', () => {
    expect(parsePortalRange({ from: 0, to: 0 })).toEqual({ from: 0, to: 0 })
  })

  it.each([
    ['a word', { from: 'abc' }],
    ['a fraction', { from: '1.5' }],
    ['a negative number', { from: -1 }],
    ['NaN', { from: Number.NaN }],
    ['a blank string', { from: '  ' }],
    ['a NaN end', { from: 1, to: Number.NaN }],
    ['an unparseable end', { from: 1, to: 'latest' }],
    ['an impossible date', { from: '2024-13-45' }],
    ['an invalid Date object', { from: new Date('garbage') }],
  ])('refuses %s with a coded range error', (_, range) => {
    expect(() => parsePortalRange(range)).toThrow(BlockRangeConfigurationError)
  })
})
