import { unexpectedCase } from '@subsquid/util-internal'
import { ValidationFailure, Validator } from '@subsquid/util-internal-validation'

import { BlockValidationError } from '~/core/errors.js'

import * as bitcoin from './bitcoin.js'
import * as evm from './evm.js'
import * as hyperliquidFills from './hyperliquid-fills.js'
import * as solana from './solana.js'
import * as substrate from './substrate.js'
import * as tron from './tron.js'

export type { Block as BitcoinBlock, FieldSelection as BitcoinFieldSelection } from './bitcoin.js'
export type { PortalBlock, PortalQuery } from './common.js'
export type { Block as EvmBlock, FieldSelection as EvmFieldSelection } from './evm.js'
export type {
  Block as HyperliquidFillsBlock,
  FieldSelection as HyperliquidFillsFieldSelection,
} from './hyperliquid-fills.js'
export type { Block as SolanaBlock, FieldSelection as SolanaFieldSelection } from './solana.js'
export type { Block as SubstrateBlock, FieldSelection as SubstrateFieldSelection } from './substrate.js'
export type { Block as TronBlock, FieldSelection as TronFieldSelection } from './tron.js'

export type SolanaQuery = solana.Query
export type EvmQuery = evm.Query
export type HyperliquidFillsQuery = hyperliquidFills.Query
export type SubstrateQuery = substrate.Query
export type BitcoinQuery = bitcoin.Query
export type TronQuery = tron.Query
export type Query = evm.Query | hyperliquidFills.Query | solana.Query | substrate.Query | bitcoin.Query | tron.Query

export type GetBlock<Q extends Query> = Q extends evm.Query
  ? evm.Block<Q['fields']>
  : Q extends solana.Query
    ? solana.Block<Q['fields']>
    : Q extends substrate.Query
      ? substrate.Block<Q['fields']>
      : Q extends bitcoin.Query
        ? bitcoin.Block<Q['fields']>
        : Q extends tron.Query
          ? tron.Block<Q['fields']>
          : hyperliquidFills.Block<Q['fields']>

const BLOCK_SCHEMAS = new WeakMap<Query, Validator<any, any>>()

/** FIXME split this function */
export function getBlockSchema<Block>(query: Query): Validator<Block, any> {
  let schema = BLOCK_SCHEMAS.get(query)
  if (schema) return schema

  switch (query.type) {
    case 'solana':
      schema = solana.getBlockSchema(query.fields)
      break
    case 'evm':
      schema = evm.getBlockSchema(query.fields)
      break
    case 'hyperliquidFills':
      schema = hyperliquidFills.getBlockSchema(query.fields)
      break
    case 'substrate':
      schema = substrate.getBlockSchema(query.fields)
      break
    case 'bitcoin':
      schema = bitcoin.getBlockSchema(query.fields)
      break
    case 'tron':
      schema = tron.getBlockSchema(query.fields)
      break
    default:
      throw unexpectedCase(query['type'])
  }

  BLOCK_SCHEMAS.set(query, schema)

  return schema
}

/** Casts raw portal blocks to `schema`, naming the block and the field a value of which does not match it. */
export function castBlocks<Block>(schema: Validator<Block, any>, blocks: unknown[]): Block[] {
  return blocks.map((block) => {
    const result = schema.cast(block)
    if (result instanceof ValidationFailure) {
      const number = (block as { header?: { number?: unknown } } | null)?.header?.number

      throw new BlockValidationError({
        block: typeof number === 'number' ? number : undefined,
        failure: result.toString(),
      })
    }

    return result
  })
}
