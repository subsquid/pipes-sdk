import { describe, expect, it } from 'vitest'

import { ForkOnFinalizedStreamError, PipeError, SdkErrorName } from './errors.js'

describe('ForkOnFinalizedStreamError (E1005)', () => {
  it('is a fork-handling PipeError carrying code E1005 and a docs link', () => {
    const err = new ForkOnFinalizedStreamError()

    expect(err).toBeInstanceOf(PipeError)
    expect(err.code).toBe('E1005')
    expect(err.name).toBe(SdkErrorName.ForkHandling)
    expect(err.message).toContain('already final')
    expect(err.message).toContain('See: https://docs.sqd.dev/en/sdk/pipes-sdk/errors/E1005')
  })
})
