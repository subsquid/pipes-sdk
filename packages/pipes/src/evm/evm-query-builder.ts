import { QueryAwareTransformer, SetupQueryFn, Subset } from '~/core/index.js'
import { mergeDeep } from '~/internal/object/merge-deep.js'
import * as evm from '~/portal-client/query/evm.js'

import { QueryBuilder, RequestOptions } from '../core/query-builder.js'

export type { Subset } from '~/core/index.js'

export type EvmPortalData<F extends evm.FieldSelection> = evm.Block<F>[]

type EvmTransformerOut<F extends evm.FieldSelection> = QueryAwareTransformer<
  EvmPortalData<F>,
  EvmPortalData<F>,
  EvmQueryBuilder<F>
>

// biome-ignore lint/complexity/noBannedTypes: <it is a default generic constraint>
export class EvmQueryBuilder<F extends evm.FieldSelection = {}> extends QueryBuilder<F, evm.DataRequest> {
  protected override readonly requestOrder: readonly (keyof evm.DataRequest)[] = [
    'transactions',
    'logs',
    'traces',
    'stateDiffs',
  ]

  getType() {
    return 'evm'
  }

  addFields<T extends evm.FieldSelection>(fields: Subset<T, evm.FieldSelection>): EvmQueryBuilder<F & T> {
    this.fields = mergeDeep(this.fields, fields as object)

    return this as unknown as EvmQueryBuilder<F & T>
  }

  addLogRequest(options: RequestOptions<evm.LogRequest>): this {
    return this.addRequest('logs', options)
  }

  addTransactionRequest(options: RequestOptions<evm.TransactionRequest>): this {
    return this.addRequest('transactions', options)
  }

  addTraceRequest(options: RequestOptions<evm.TraceRequest>): this {
    return this.addRequest('traces', options)
  }

  addStateDiffRequest(options: RequestOptions<evm.StateDiffRequest>): this {
    return this.addRequest('stateDiffs', options)
  }

  override build(opts?: { setupQuery?: SetupQueryFn<EvmQueryBuilder<F>> }): EvmTransformerOut<F> {
    const setupQuery = opts?.setupQuery ?? (({ query }) => query.merge(this))

    return new QueryAwareTransformer(setupQuery, {
      profiler: {
        name: 'query builder',
        hidden: true,
      },
      transform: (data) => data,
    })
  }
}

export function evmQuery() {
  return new EvmQueryBuilder()
}
