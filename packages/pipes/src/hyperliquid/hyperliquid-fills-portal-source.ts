import { portalStreamOf } from '~/core/portal-stream-factory.js'
import * as hl from '~/portal-client/query/hyperliquid-fills.js'

import { Outputs } from '../core/index.js'
import { HyperliquidFillsQueryBuilder, hyperliquidFillsQuery } from './hyperliquid-fills-query-builder.js'

export type HyperliquidFillsFieldSelection = hl.FieldSelection

export * as api from '../portal-client/query/hyperliquid-fills.js'

export type HyperliquidFillsOutputs = Outputs<hl.FieldSelection, HyperliquidFillsQueryBuilder<any>>

export const hyperliquidFillsPortalStream = portalStreamOf<HyperliquidFillsQueryBuilder<any>>(() =>
  hyperliquidFillsQuery().addFields({ block: { hash: true, number: true } }),
)
