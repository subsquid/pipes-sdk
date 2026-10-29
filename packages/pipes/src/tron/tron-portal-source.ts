import { MetricsServer } from '~/core/metrics-server.js'
import { ProgressTrackerOptions, progressTracker } from '~/core/progress-tracker.js'
import * as tron from '~/portal-client/query/tron.js'

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
import { TronPortalData, TronQueryBuilder } from './tron-query-builder.js'

export type TronFieldSelection = tron.FieldSelection

export * as api from '../portal-client/query/tron.js'

export type TronOutputs = Outputs<tron.FieldSelection, TronQueryBuilder<any>>

type TronPortalStream<T extends TronOutputs> =
  T extends TronQueryBuilder<infer Q>
    ? TronPortalData<Q>
    : T extends Transformer<any, infer O>
      ? O
      : T extends Record<string, Transformer<any, any> | TronQueryBuilder<any>>
        ? {
            [K in keyof T]: T[K] extends Transformer<any, infer O>
              ? O
              : T[K] extends TronQueryBuilder<infer Q>
                ? TronPortalData<Q>
                : never
          }
        : never

export function tronPortalStream<Out extends TronOutputs>({
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
  const query = new TronQueryBuilder<F>().addFields({
    block: { hash: true, number: true },
  })

  return new PortalStream<TronQueryBuilder<F>, TronPortalStream<Out>>({
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
      createTransformer<TronPortalData<F>, TronPortalData<F>>({
        profiler: { name: 'normalize data' },
        transform: (data, ctx) => {
          const schema = getBlockSchema<tron.Block<F>>(ctx.stream.query.raw)

          return castBlocks(schema, data)
        },
      }),
      mergeOutputs(outputs),
    ],
  })
}
