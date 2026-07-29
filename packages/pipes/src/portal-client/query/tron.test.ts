import { cast } from '@subsquid/util-internal-validation'
import { describe, expect, it } from 'vitest'

import { getBlockSchema } from './tron.js'

// tx `expiration`/`timestamp` share the timestamp decoder.
function castExpiration(value: unknown) {
  const schema = getBlockSchema({ transaction: { expiration: true } })
  const block = { header: {}, transactions: [{ expiration: value }] }

  return cast(schema, block).transactions[0].expiration
}

// block `timestamp`: same decoder, but required (not optional).
function castBlockTimestamp(value: unknown) {
  const schema = getBlockSchema({ block: { timestamp: true } })
  const block = { header: { timestamp: value } }

  return cast(schema, block).header.timestamp
}

// all int64 amounts share the signed decoder; `fee` stands in.
function castFee(value: unknown) {
  const schema = getBlockSchema({ transaction: { fee: true } })
  const block = { header: {}, transactions: [{ fee: value }] }

  return cast(schema, block).transactions[0].fee
}

describe('TRON timestamp validation (int64 milliseconds)', () => {
  it('accepts a normal ms timestamp', () => {
    expect(castExpiration(1782669723000)).toBe(1782669723000)
    expect(castBlockTimestamp(1782669669000)).toBe(1782669669000)
  })

  it('accepts values above the safe-integer range (served as imprecise floats)', () => {
    const huge = Number('639208360527210660')
    expect(Number.isSafeInteger(huge)).toBe(false)
    expect(castExpiration(huge)).toBe(huge)
    expect(castBlockTimestamp(huge)).toBe(huge)
  })

  it('accepts negative int64 values', () => {
    expect(castExpiration(-1)).toBe(-1)
  })

  it('normalizes a selected-but-null field to undefined', () => {
    expect(castExpiration(null)).toBeUndefined()
  })

  it('rejects non-integer and non-number values', () => {
    expect(() => castExpiration(3.14)).toThrow()
    expect(() => castExpiration('1782669723000')).toThrow()
    expect(() => castBlockTimestamp('nope')).toThrow()
  })
})

describe('TRON amount validation (signed int64 BigNum)', () => {
  it('accepts a decimal string and casts to bigint', () => {
    expect(castFee('269000')).toBe(269000n)
  })

  it('accepts zero', () => {
    expect(castFee('0')).toBe(0n)
  })

  it('accepts negative amounts (the portal serves them verbatim)', () => {
    expect(castFee('-269000')).toBe(-269000n)
  })

  it('accepts values beyond 2^53', () => {
    expect(castFee('9007199254740993')).toBe(9007199254740993n)
  })

  it('normalizes a selected-but-null field to undefined', () => {
    expect(castFee(null)).toBeUndefined()
  })

  it('rejects a non-numeric string', () => {
    expect(() => castFee('abc')).toThrow()
  })

  it('rejects a number (amounts arrive as decimal strings)', () => {
    expect(() => castFee(269000)).toThrow()
  })
})
