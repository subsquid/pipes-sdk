import { QueryAwareTransformer, SetupQueryFn, Subset, parsePortalRange } from '~/core/index.js'
import { mergeDeep } from '~/internal/object/merge-deep.js'

import { QueryBuilder, Range, RequestOptions } from '../core/query-builder.js'
import * as api from '../portal-client/query/tron.js'

export type TronPortalData<F extends api.FieldSelection> = api.Block<F>[]

type TronTransformerOut<F extends api.FieldSelection> = QueryAwareTransformer<
  TronPortalData<F>,
  TronPortalData<F>,
  TronQueryBuilder<F>
>

// biome-ignore lint/complexity/noBannedTypes: <it is a default generic constraint>
export class TronQueryBuilder<F extends api.FieldSelection = {}> extends QueryBuilder<F, api.DataRequest> {
  protected override readonly requestOrder: readonly (keyof api.DataRequest)[] = [
    'transactions',
    'transferTransactions',
    'transferAssetTransactions',
    'triggerSmartContractTransactions',
    'logs',
    'internalTransactions',
  ]

  getType() {
    return 'tron'
  }

  addFields<T extends api.FieldSelection>(fields: Subset<T, api.FieldSelection>): TronQueryBuilder<F & T> {
    this.fields = mergeDeep(this.fields, fields)

    return this as unknown as TronQueryBuilder<F & T>
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

  addTransferTransactionRequest(options: RequestOptions<api.TransferTransactionRequest>): this {
    return this.addRequest('transferTransactions', options)
  }

  addTransferAssetTransactionRequest(options: RequestOptions<api.TransferAssetTransactionRequest>): this {
    return this.addRequest('transferAssetTransactions', options)
  }

  addTriggerSmartContractTransactionRequest(options: RequestOptions<api.TriggerSmartContractTransactionRequest>): this {
    return this.addRequest('triggerSmartContractTransactions', options)
  }

  addLogRequest(options: RequestOptions<api.LogRequest>): this {
    return this.addRequest('logs', options)
  }

  addInternalTransactionRequest(options: RequestOptions<api.InternalTransactionRequest>): this {
    return this.addRequest('internalTransactions', options)
  }

  override build(opts?: { setupQuery?: SetupQueryFn<TronQueryBuilder<F>> }): TronTransformerOut<F> {
    const setupQuery = opts?.setupQuery ?? (({ query }) => query.merge(this))
    return new QueryAwareTransformer(setupQuery, {
      profiler: { name: 'query builder', hidden: true },
      transform: (data) => data,
    })
  }
}

export function tronQuery() {
  return new TronQueryBuilder()
}
