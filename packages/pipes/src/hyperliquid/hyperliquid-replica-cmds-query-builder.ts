import { QueryAwareTransformer, SetupQueryFn, Subset } from '~/core/index.js'
import { mergeDeep } from '~/internal/object/merge-deep.js'

import { QueryBuilder, RequestOptions } from '../core/query-builder.js'
import * as api from '../portal-client/query/hyperliquid-replica-cmds.js'

export type HyperliquidReplicaCmdsPortalData<F extends api.FieldSelection> = api.Block<F>[]

type HyperliquidReplicaCmdsTransformerOut<F extends api.FieldSelection> = QueryAwareTransformer<
  HyperliquidReplicaCmdsPortalData<F>,
  HyperliquidReplicaCmdsPortalData<F>,
  HyperliquidReplicaCmdsQueryBuilder<F>
>

// biome-ignore lint/complexity/noBannedTypes: <it is a default generic constraint>
export class HyperliquidReplicaCmdsQueryBuilder<F extends api.FieldSelection = {}> extends QueryBuilder<
  F,
  api.DataRequest
> {
  protected override readonly requestOrder: readonly (keyof api.DataRequest)[] = [
    'actions',
    'orderActions',
    'cancelActions',
    'cancelByCloidActions',
    'batchModifyActions',
  ]

  getType() {
    return 'hyperliquidReplicaCmds'
  }

  addFields<T extends api.FieldSelection>(
    fields: Subset<T, api.FieldSelection>,
  ): HyperliquidReplicaCmdsQueryBuilder<F & T> {
    this.fields = mergeDeep(this.fields, fields)

    return this as unknown as HyperliquidReplicaCmdsQueryBuilder<F & T>
  }

  addActionRequest(options: RequestOptions<api.ActionRequest>): this {
    return this.addRequest('actions', options)
  }

  addOrderActionRequest(options: RequestOptions<api.OrderActionRequest>): this {
    return this.addRequest('orderActions', options)
  }

  addCancelActionRequest(options: RequestOptions<api.CancelActionRequest>): this {
    return this.addRequest('cancelActions', options)
  }

  addCancelByCloidActionRequest(options: RequestOptions<api.CancelByCloidActionRequest>): this {
    return this.addRequest('cancelByCloidActions', options)
  }

  addBatchModifyActionRequest(options: RequestOptions<api.BatchModifyActionRequest>): this {
    return this.addRequest('batchModifyActions', options)
  }

  override build(opts?: {
    setupQuery?: SetupQueryFn<HyperliquidReplicaCmdsQueryBuilder<F>>
  }): HyperliquidReplicaCmdsTransformerOut<F> {
    const setupQuery = opts?.setupQuery ?? (({ query }) => query.merge(this))
    return new QueryAwareTransformer(setupQuery, {
      profiler: { name: 'query builder', hidden: true },
      transform: (data) => data,
    })
  }
}

export function hyperliquidReplicaCmdsQuery() {
  return new HyperliquidReplicaCmdsQueryBuilder()
}
