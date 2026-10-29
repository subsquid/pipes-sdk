import { MetricsServer } from '~/core/metrics-server.js'
import { ProgressTrackerOptions, progressTracker } from '~/core/progress-tracker.js'
import { PortalClientOptions, castBlocks, getBlockSchema } from '~/portal-client/index.js'
import * as solana from '~/portal-client/query/solana.js'

import {
  LogLevel,
  Logger,
  Outputs,
  PortalCache,
  PortalStream,
  SpanHooks,
  Transformer,
  createTransformer,
  defaultLogger,
  mergeOutputs,
} from '../core/index.js'
import { SolanaQueryBuilder } from './solana-query-builder.js'

export type SolanaFieldSelection = solana.FieldSelection

export type SolanaPortalData<F extends solana.FieldSelection> = solana.Block<F>[]

type SolanaOutputs = Outputs<solana.FieldSelection, SolanaQueryBuilder<any>>

type SolanaPortalStream<T extends SolanaOutputs> =
  T extends SolanaQueryBuilder<infer Q>
    ? SolanaPortalData<Q>
    : T extends Transformer<any, infer O>
      ? O
      : T extends Record<string, Transformer<any, any> | SolanaQueryBuilder<any>>
        ? {
            [K in keyof T]: T[K] extends Transformer<any, infer O>
              ? O
              : T[K] extends SolanaQueryBuilder<infer Q>
                ? SolanaPortalData<Q>
                : never
          }
        : never

export function solanaPortalStream<Out extends SolanaOutputs>({
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
  const query = new SolanaQueryBuilder<F>().addFields({
    block: { hash: true, number: true },
  })

  return new PortalStream<SolanaQueryBuilder<F>, SolanaPortalStream<Out>>({
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
      createTransformer<SolanaPortalData<F>, SolanaPortalData<F>>({
        profiler: { name: 'normalize data' },
        transform: (data, ctx) => {
          const schema = getBlockSchema<solana.Block<F>>(ctx.stream.query.raw)

          return castBlocks(schema, data)
        },
      }),
      mergeOutputs(outputs),
    ],
  })
}
