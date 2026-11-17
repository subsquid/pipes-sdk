import { portalStreamOf } from '~/core/portal-stream-factory.js'
import * as btc from '~/portal-client/query/bitcoin.js'

import { Outputs } from '../core/index.js'
import { BitcoinQueryBuilder, bitcoinQuery } from './bitcoin-query-builder.js'

export type BitcoinFieldSelection = btc.FieldSelection

export * as api from '../portal-client/query/bitcoin.js'

export type BitcoinOutputs = Outputs<btc.FieldSelection, BitcoinQueryBuilder<any>>

export const bitcoinPortalStream = portalStreamOf<BitcoinQueryBuilder<any>>(() =>
  bitcoinQuery().addFields({ block: { hash: true, number: true } }),
)
