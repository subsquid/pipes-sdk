import { portalStreamOf } from '~/core/portal-stream-factory.js'
import * as hl from '~/portal-client/query/hyperliquid-replica-cmds.js'

import { Outputs } from '../core/index.js'
import {
  HyperliquidReplicaCmdsQueryBuilder,
  hyperliquidReplicaCmdsQuery,
} from './hyperliquid-replica-cmds-query-builder.js'

export type HyperliquidReplicaCmdsFieldSelection = hl.FieldSelection

export * as replicaCmdsApi from '../portal-client/query/hyperliquid-replica-cmds.js'

export type HyperliquidReplicaCmdsOutputs = Outputs<hl.FieldSelection, HyperliquidReplicaCmdsQueryBuilder<any>>

export const hyperliquidReplicaCmdsPortalStream = portalStreamOf<HyperliquidReplicaCmdsQueryBuilder<any>>(() =>
  hyperliquidReplicaCmdsQuery().addFields({ block: { hash: true, number: true } }),
)
