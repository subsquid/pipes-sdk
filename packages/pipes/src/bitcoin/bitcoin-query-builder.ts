import { QueryAwareTransformer, SetupQueryFn, Subset, parsePortalRange } from '~/core/index.js'
import { mergeDeep } from '~/internal/object/merge-deep.js'

import { QueryBuilder, Range, RequestOptions } from '../core/query-builder.js'
import * as api from '../portal-client/query/bitcoin.js'

export type BitcoinPortalData<F extends api.FieldSelection> = api.Block<F>[]

type BitcoinTransformerOut<F extends api.FieldSelection> = QueryAwareTransformer<
  BitcoinPortalData<F>,
  BitcoinPortalData<F>,
  BitcoinQueryBuilder<F>
>

// biome-ignore lint/complexity/noBannedTypes: <it is a default generic constraint>
export class BitcoinQueryBuilder<F extends api.FieldSelection = {}> extends QueryBuilder<F, api.DataRequest> {
  protected override readonly requestOrder: readonly (keyof api.DataRequest)[] = ['transactions', 'inputs', 'outputs']

  getType() {
    return 'bitcoin'
  }

  addFields<T extends api.FieldSelection>(fields: Subset<T, api.FieldSelection>): BitcoinQueryBuilder<F & T> {
    this.fields = mergeDeep(this.fields, fields)

    return this as unknown as BitcoinQueryBuilder<F & T>
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

  addTransactionRequest(options: RequestOptions<api.TransactionRequest>): this {
    return this.addRequest('transactions', options)
  }

  addInputRequest(options: RequestOptions<api.InputRequest>): this {
    return this.addRequest('inputs', options)
  }

  addOutputRequest(options: RequestOptions<api.OutputRequest>): this {
    return this.addRequest('outputs', options)
  }

  override build(opts?: { setupQuery?: SetupQueryFn<BitcoinQueryBuilder<F>> }): BitcoinTransformerOut<F> {
    const setupQuery = opts?.setupQuery ?? (({ query }) => query.merge(this))
    return new QueryAwareTransformer(setupQuery, {
      profiler: { name: 'query builder', hidden: true },
      transform: (data) => data,
    })
  }
}

export function bitcoinQuery() {
  return new BitcoinQueryBuilder()
}
