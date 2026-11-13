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
} from '~/core/index.js'
import { MetricsServer } from '~/core/metrics-server.js'
import { ProgressTrackerOptions, progressTracker } from '~/core/progress-tracker.js'

import { BlockStreamClient, PortalClientOptions, castBlocks, getBlockSchema } from '../portal-client/index.js'
import * as evm from '../portal-client/query/evm.js'
import { EvmPortalData, EvmQueryBuilder } from './evm-query-builder.js'

export type EvmFieldSelection = evm.FieldSelection

export * as api from '../portal-client/query/evm.js'

export type EvmOutputs = Outputs<evm.FieldSelection, EvmQueryBuilder<any>>

export type EvmStreamData<T extends EvmOutputs> =
  T extends EvmQueryBuilder<infer Q>
    ? EvmPortalData<Q>
    : T extends Transformer<any, infer O>
      ? O
      : T extends Record<string, Transformer<any, any> | EvmQueryBuilder<any>>
        ? {
            [K in keyof T]: T[K] extends Transformer<any, infer O>
              ? O
              : T[K] extends EvmQueryBuilder<infer Q>
                ? EvmPortalData<Q>
                : never
          }
        : never

export interface EvmPortalStreamOptions<Out extends EvmOutputs> {
  /**
   * Globally unique, stable identifier for this pipe.
   * Targets use it as a cursor key to persist progress — two pipes with the
   * same `id` will share (and overwrite) each other's cursor.
   */
  id: string
  source: string | PortalClientOptions | BlockStreamClient
  outputs: Out
  cache?: PortalCache
  metrics?: MetricsServer
  logger?: Logger | LogLevel
  profiler?: boolean | SpanHooks
  progress?: ProgressTrackerOptions
}

export function evmPortalStream<Out extends EvmOutputs>({
  id,
  source,
  outputs,
  cache,
  logger,
  metrics,
  profiler,
  progress,
}: EvmPortalStreamOptions<Out>) {
  type F = { block: { hash: true; number: true } }
  const query = new EvmQueryBuilder<F>().addFields({
    block: { hash: true, number: true },
  })

  return new PortalStream<EvmQueryBuilder<F>, EvmStreamData<Out>>({
    id,
    source,
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
      createTransformer<EvmPortalData<F>, EvmPortalData<F>>({
        profiler: { name: 'normalize data' },
        transform: (data, ctx) => {
          const schema = getBlockSchema<evm.Block<F>>(ctx.stream.query.raw)

          return castBlocks(schema, data)
        },
      }),
      mergeOutputs(outputs),
    ],
  })
}
