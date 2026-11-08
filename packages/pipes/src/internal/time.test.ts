import { describe, expect, it } from 'vitest'

import { toEpochSeconds } from './time.js'

describe('toEpochSeconds', () => {
  it('passes epoch-second timestamps through unchanged', () => {
    // Ethereum block 1 and a recent block.
    expect(toEpochSeconds(1_438_269_988)).toBe(1_438_269_988)
    expect(toEpochSeconds(1_700_000_000)).toBe(1_700_000_000)
  })

  it('converts epoch-millisecond timestamps to seconds', () => {
    // `query/tron.ts` and `query/substrate.ts` both declare ms.
    expect(toEpochSeconds(1_700_000_000_000)).toBe(1_700_000_000)
    expect(toEpochSeconds(1_700_000_000_500)).toBe(1_700_000_000.5)
  })

  it('drops the above-2^53 integers tron is known to emit', () => {
    // Written as a parse because that is how it arrives — the literal cannot be spelled at full
    // precision. Pinned by tron-portal-source.test.ts as a value that must survive query
    // validation, so it must not reach a histogram, where it would freeze `_sum` permanently.
    expect(toEpochSeconds(Number('639208360527210660'))).toBeUndefined()
  })

  it('drops absent, zero and non-finite values', () => {
    expect(toEpochSeconds(undefined)).toBeUndefined()
    expect(toEpochSeconds(0)).toBeUndefined()
    expect(toEpochSeconds(Number.NaN)).toBeUndefined()
    expect(toEpochSeconds(Number.POSITIVE_INFINITY)).toBeUndefined()
    expect(toEpochSeconds(-1_700_000_000)).toBeUndefined()
  })

  it('keeps the two units disjoint across the whole plausible range', () => {
    // The unit test is a threshold comparison, so it is only sound while no plausible
    // seconds value reaches the millisecond floor.
    expect(toEpochSeconds(3_999_999_999)).toBe(3_999_999_999)
    expect(toEpochSeconds(4_000_000_001)).toBeUndefined()
    expect(toEpochSeconds(1_000_000_000_000)).toBe(1_000_000_000)
  })
})
