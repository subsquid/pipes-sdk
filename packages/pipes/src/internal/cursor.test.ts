import { describe, expect, it } from 'vitest'

import { isPersistedCursor, isPersistedHeight, parsePersistedHeight } from './cursor.js'

describe('persisted cursor structure', () => {
  it.each([
    { number: 0 },
    { number: 7, timestamp: 1_794_173_943.123 },
    { number: 7, hash: '0x7', timestamp: 0 },
    { number: 7, timestamp: 1_700_000_000_000 },
  ])('accepts %j', (value) => {
    expect(isPersistedCursor(value)).toBe(true)
  })
  it.each(
    [
      null,
      [],
      {},
      { number: -1 },
      { number: 1.5 },
      { number: '5' },
      { number: 1, hash: 3 },
      { number: 1, timestamp: -1 },
      { number: Infinity },
      { number: 1, kind: 'unknown' },
      { number: 1, kind: 'start' },
      { number: 1, timestamp: NaN },
      { number: 1, timestamp: Infinity },
    ].map((value) => ({ value })),
  )('refuses %j', ({ value }) => {
    expect(isPersistedCursor(value)).toBe(false)
  })
})

describe('persisted heights', () => {
  it.each([0, 42, Number.MAX_SAFE_INTEGER])('accepts numeric height %s and its database string forms', (height) => {
    expect(isPersistedHeight(height)).toBe(true)
    expect(parsePersistedHeight(height)).toBe(height)
    expect(parsePersistedHeight(String(height))).toBe(height)
    expect(parsePersistedHeight(`${height}.00`)).toBe(height)
    expect(isPersistedCursor({ number: String(height) })).toBe(false)
  })
  it.each(
    [
      null,
      undefined,
      '',
      ' ',
      '12garbage',
      '1.5',
      -1,
      1.5,
      '1e2',
      '+1',
      '-1',
      NaN,
      Infinity,
      Number.MAX_SAFE_INTEGER + 1,
      true,
      {},
      [],
    ].map((height) => ({ height })),
  )('refuses height %j', ({ height }) => {
    expect(isPersistedHeight(height)).toBe(false)
    expect(parsePersistedHeight(height)).toBeNull()
  })
})
