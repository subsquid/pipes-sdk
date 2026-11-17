import { QueryAwareTransformer, SetupQueryFn, Subset, parsePortalRange } from '~/core/index.js'
import { mergeDeep } from '~/internal/object/merge-deep.js'

import { QueryBuilder, Range, RequestOptions } from '../core/query-builder.js'
import * as solana from '../portal-client/query/solana.js'
import { SolanaPortalData } from './solana-portal-source.js'

type SolanaTransformerOut<F extends solana.FieldSelection> = QueryAwareTransformer<
  SolanaPortalData<F>,
  SolanaPortalData<F>,
  SolanaQueryBuilder<F>
>

// biome-ignore lint/complexity/noBannedTypes: <it is a default generic constraint>
export class SolanaQueryBuilder<F extends solana.FieldSelection = {}> extends QueryBuilder<F, solana.DataRequest> {
  protected override readonly requestOrder: readonly (keyof solana.DataRequest)[] = [
    'transactions',
    'logs',
    'balances',
    'tokenBalances',
    'rewards',
    'instructions',
  ]

  getType() {
    return 'solana'
  }

  addFields<T extends solana.FieldSelection>(fields: Subset<T, solana.FieldSelection>): SolanaQueryBuilder<F & T> {
    this.fields = mergeDeep(this.fields, fields)

    return this as unknown as SolanaQueryBuilder<F & T>
  }

  private addRequest(type: keyof solana.DataRequest, options: RequestOptions<any>): this {
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

  addLogRequest(options: RequestOptions<solana.LogRequest>): this {
    return this.addRequest('logs', options)
  }

  addTransactionRequest(options: RequestOptions<solana.TransactionRequest>): this {
    return this.addRequest('transactions', options)
  }

  addRewardRequest(options: RequestOptions<solana.RewardRequest>): this {
    return this.addRequest('rewards', options)
  }

  addBalanceRequest(options: RequestOptions<solana.BalanceRequest>): this {
    return this.addRequest('balances', options)
  }

  addTokenBalanceRequest(options: RequestOptions<solana.TokenBalanceRequest>): this {
    return this.addRequest('tokenBalances', options)
  }

  addInstructionRequest(options: RequestOptions<solana.InstructionRequest>): this {
    return this.addRequest('instructions', options)
  }

  override build(opts?: { setupQuery?: SetupQueryFn<SolanaQueryBuilder<F>> }): SolanaTransformerOut<F> {
    const setupQuery = opts?.setupQuery ?? (({ query }) => query.merge(this))

    return new QueryAwareTransformer(setupQuery, {
      profiler: { name: 'query builder', hidden: true },
      transform: (data) => data,
    })
  }
}

export function solanaQuery() {
  return new SolanaQueryBuilder()
}
