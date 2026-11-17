import { QueryAwareTransformer, SetupQueryFn, Subset, parsePortalRange } from '~/core/index.js'
import { mergeDeep } from '~/internal/object/merge-deep.js'

import { QueryBuilder, Range, RequestOptions, concatQueryLists } from '../core/query-builder.js'
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
  getType() {
    return 'hyperliquidReplicaCmds'
  }

  addFields<T extends api.FieldSelection>(
    fields: Subset<T, api.FieldSelection>,
  ): HyperliquidReplicaCmdsQueryBuilder<F & T> {
    this.fields = mergeDeep(this.fields, fields)

    return this as unknown as HyperliquidReplicaCmdsQueryBuilder<F & T>
  }

  private addRequest(type: keyof api.DataRequest, options: RequestOptions<any>): this {
    this.requests.push({
      range: parsePortalRange(options.range),
      request: {
        [type]: [{ ...options.request }],
      },
    })
    return this
  }

  includeAllBlocks(range?: Range): this {
    this.requests.push({ range: range ?? { from: 0 }, request: { includeAllBlocks: true } })
    return this
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

  mergeDataRequests(...requests: api.DataRequest[]): api.DataRequest {
    let res: api.DataRequest = {}
    for (let req of requests) {
      res.actions = concatQueryLists(res.actions, req.actions)
      res.orderActions = concatQueryLists(res.orderActions, req.orderActions)
      res.cancelActions = concatQueryLists(res.cancelActions, req.cancelActions)
      res.cancelByCloidActions = concatQueryLists(res.cancelByCloidActions, req.cancelByCloidActions)
      res.batchModifyActions = concatQueryLists(res.batchModifyActions, req.batchModifyActions)
      if (res.includeAllBlocks || req.includeAllBlocks) {
        res.includeAllBlocks = true
      }
    }
    return res
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
