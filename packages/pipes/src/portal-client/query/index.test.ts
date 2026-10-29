import { describe, expect, it } from 'vitest'

import { BlockValidationError } from '~/core/errors.js'

import { getBlockSchema } from './evm.js'
import { castBlocks } from './index.js'

const schema = getBlockSchema({ block: { number: true, timestamp: true } })

describe('castBlocks', () => {
  it('casts blocks that match the schema', () => {
    const [block] = castBlocks(schema, [{ header: { number: 1, timestamp: 1000 } }])

    expect(block.header).toEqual({ number: 1, timestamp: 1000 })
  })

  it('names the block and the field of a value that does not match', () => {
    const cast = () => castBlocks(schema, [{ header: { number: 1_234, timestamp: 'noon' } }])

    expect(cast).toThrow(BlockValidationError)
    expect(cast).toThrow(
      'Block 1,234 does not match the schema of the selected fields: invalid value at /header/timestamp',
    )
  })

  it('says so when the block has no number to name', () => {
    const cast = () => castBlocks(schema, [{ header: { timestamp: 1000 } }])

    expect(cast).toThrow('A block without a number does not match the schema of the selected fields')
  })
})
