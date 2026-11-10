import { describe, expect, it } from 'vitest'

import { withRequiredFields } from './decode.js'

describe('withRequiredFields', () => {
  it('forces the structural stateDiff fields, including the `kind` discriminator', () => {
    // `kind` is the stateDiff tagged-union discriminator and must survive projection so a
    // `stateDiffs: [{ kind: [...] }]` where-clause can still match when kind isn't selected for output.
    expect(withRequiredFields({}).stateDiff).toMatchObject({
      transactionIndex: true,
      address: true,
      key: true,
      kind: true,
    })
  })

  it('forces the cursor + filter fields on every item type while preserving the user selection', () => {
    const f = withRequiredFields({ log: { data: true } })
    expect(f.block).toMatchObject({ number: true, hash: true, parentHash: true })
    expect(f.log).toMatchObject({ data: true, logIndex: true, transactionIndex: true })
    expect(f.trace).toMatchObject({ transactionIndex: true, traceAddress: true })
  })
})
