import { portalStreamOf } from '~/core/portal-stream-factory.js'
import * as fillsApi from '~/portal-client/query/hyperliquid-fills.js'

import { Outputs } from '../core/index.js'
import { HyperliquidFillsQueryBuilder, hyperliquidFillsQuery } from './hyperliquid-fills-query-builder.js'

export type HyperliquidFillsFieldSelection = fillsApi.FieldSelection

export {
  fillsApi,
  /** @deprecated Use `fillsApi`. */
  fillsApi as api,
}

export type HyperliquidFillsOutputs = Outputs<fillsApi.FieldSelection, HyperliquidFillsQueryBuilder<any>>

export const hyperliquidFillsPortalStream = portalStreamOf<HyperliquidFillsQueryBuilder<any>>(() =>
  hyperliquidFillsQuery().addFields({ block: { hash: true, number: true } }),
)
