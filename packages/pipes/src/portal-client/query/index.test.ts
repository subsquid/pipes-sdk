import { describe, expect, expectTypeOf, it } from 'vitest'

import { BlockValidationError } from '~/core/errors.js'

import { getBlockSchema } from './evm.js'
import type * as hyperliquidFills from './hyperliquid-fills.js'
import type * as hyperliquidReplicaCmds from './hyperliquid-replica-cmds.js'
import { type GetBlock, type Query, castBlocks } from './index.js'

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

describe('GetBlock', () => {
  type F = { block: { number: true } }

  it('maps each Hyperliquid query to its own block', () => {
    expectTypeOf<GetBlock<hyperliquidFills.Query<F>>>().toEqualTypeOf<hyperliquidFills.Block<F>>()
    expectTypeOf<GetBlock<hyperliquidReplicaCmds.Query<F>>>().toEqualTypeOf<hyperliquidReplicaCmds.Block<F>>()
  })

  it('maps every query type to a block', () => {
    type Unmapped<Q extends Query = Query> = Q extends Query
      ? [GetBlock<Q>] extends [never]
        ? Q['type']
        : never
      : never

    expectTypeOf<Unmapped>().toBeNever()
  })
})
