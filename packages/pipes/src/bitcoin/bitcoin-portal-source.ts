import { MetricsServer } from '~/core/metrics-server.js'
import { ProgressTrackerOptions, progressTracker } from '~/core/progress-tracker.js'
import * as btc from '~/portal-client/query/bitcoin.js'

import {
  LogLevel,
  Logger,
  Outputs,
  PortalCache,
  PortalStream,
  SpanHooks,
  Transformer,
  createTransformer,
  mergeOutputs,
} from '../core/index.js'
import { PortalClientOptions, castBlocks, getBlockSchema } from '../portal-client/index.js'
import { BitcoinPortalData, BitcoinQueryBuilder } from './bitcoin-query-builder.js'

export type BitcoinFieldSelection = btc.FieldSelection

export * as api from '../portal-client/query/bitcoin.js'

export type BitcoinOutputs = Outputs<btc.FieldSelection, BitcoinQueryBuilder<any>>

type BitcoinPortalStream<T extends BitcoinOutputs> =
  T extends BitcoinQueryBuilder<infer Q>
    ? BitcoinPortalData<Q>
    : T extends Transformer<any, infer O>
      ? O
      : T extends Record<string, Transformer<any, any> | BitcoinQueryBuilder<any>>
        ? {
            [K in keyof T]: T[K] extends Transformer<any, infer O>
              ? O
              : T[K] extends BitcoinQueryBuilder<infer Q>
                ? BitcoinPortalData<Q>
                : never
          }
        : never

export function bitcoinPortalStream<Out extends BitcoinOutputs>({
  id,
  portal,
  outputs,
  cache,
  logger,
  metrics,
  profiler,
  progress,
}: {
  /**
   * Globally unique, stable identifier for this pipe.
   * Targets use it as a cursor key to persist progress — two pipes with the
   * same `id` will share (and overwrite) each other's cursor.
   */
  id: string
  portal: string | PortalClientOptions
  outputs: Out
  cache?: PortalCache
  metrics?: MetricsServer
  logger?: Logger | LogLevel
  profiler?: boolean | SpanHooks
  progress?: ProgressTrackerOptions
}) {
  type F = { block: { hash: true; number: true } }
  const query = new BitcoinQueryBuilder<F>().addFields({
    block: { hash: true, number: true },
  })

  return new PortalStream<BitcoinQueryBuilder<F>, BitcoinPortalStream<Out>>({
    id,
    portal,
    query,
    cache,
    logger,
    metrics,
    profiler,
    transformers: [
      progressTracker({
        interval: progress?.interval,
        onStart: progress?.onStart,
        onProgress: progress?.onProgress,
      }),
      createTransformer<BitcoinPortalData<F>, BitcoinPortalData<F>>({
        profiler: { name: 'normalize data' },
        transform: (data, ctx) => {
          const schema = getBlockSchema<btc.Block<F>>(ctx.stream.query.raw)

          return castBlocks(schema, data)
        },
      }),
      mergeOutputs(outputs),
    ],
  })
}
