import { QueryAwareTransformer, SetupQueryFn, Subset, parsePortalRange } from '~/core/index.js'
import { mergeDeep } from '~/internal/object/merge-deep.js'

import { QueryBuilder, Range, RequestOptions } from '../core/query-builder.js'
import * as api from '../portal-client/query/hyperliquid-fills.js'

export type HyperliquidFillsPortalData<F extends api.FieldSelection> = api.Block<F>[]

type HyperliquidFillsTransformerOut<F extends api.FieldSelection> = QueryAwareTransformer<
  HyperliquidFillsPortalData<F>,
  HyperliquidFillsPortalData<F>,
  HyperliquidFillsQueryBuilder<F>
>

// biome-ignore lint/complexity/noBannedTypes: <it is a default generic constraint>
export class HyperliquidFillsQueryBuilder<F extends api.FieldSelection = {}> extends QueryBuilder<F, api.DataRequest> {
  protected override readonly requestOrder: readonly (keyof api.DataRequest)[] = ['fills']

  getType() {
    return 'hyperliquidFills'
  }

  addFields<T extends api.FieldSelection>(fields: Subset<T, api.FieldSelection>): HyperliquidFillsQueryBuilder<F & T> {
    this.fields = mergeDeep(this.fields, fields)

    return this as unknown as HyperliquidFillsQueryBuilder<F & T>
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

  addFillRequest(options: RequestOptions<api.FillRequest>): this {
    return this.addRequest('fills', options)
  }

  override build(opts?: {
    setupQuery?: SetupQueryFn<HyperliquidFillsQueryBuilder<F>>
  }): HyperliquidFillsTransformerOut<F> {
    const setupQuery = opts?.setupQuery ?? (({ query }) => query.merge(this))
    return new QueryAwareTransformer(setupQuery, {
      profiler: { name: 'query builder', hidden: true },
      transform: (data) => data,
    })
  }
}

export function hyperliquidFillsQuery() {
  return new HyperliquidFillsQueryBuilder()
}
