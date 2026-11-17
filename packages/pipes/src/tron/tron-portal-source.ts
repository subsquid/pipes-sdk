import { portalStreamOf } from '~/core/portal-stream-factory.js'
import * as tron from '~/portal-client/query/tron.js'

import { Outputs } from '../core/index.js'
import { TronQueryBuilder, tronQuery } from './tron-query-builder.js'

export type TronFieldSelection = tron.FieldSelection

export * as api from '../portal-client/query/tron.js'

export type TronOutputs = Outputs<tron.FieldSelection, TronQueryBuilder<any>>

export const tronPortalStream = portalStreamOf<TronQueryBuilder<any>>(() =>
  tronQuery().addFields({ block: { hash: true, number: true } }),
)
