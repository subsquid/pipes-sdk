import { MetricsServer } from '~/core/metrics-server.js'
import { ProgressTrackerOptions, progressTracker } from '~/core/progress-tracker.js'
import * as hl from '~/portal-client/query/hyperliquid-fills.js'

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
import { HyperliquidFillsPortalData, HyperliquidFillsQueryBuilder } from './hyperliquid-fills-query-builder.js'

export type HyperliquidFillsFieldSelection = hl.FieldSelection

export * as api from '../portal-client/query/hyperliquid-fills.js'

export type HyperliquidFillsOutputs = Outputs<hl.FieldSelection, HyperliquidFillsQueryBuilder<any>>

type HyperliquidFillsPortalStream<T extends HyperliquidFillsOutputs> =
  T extends HyperliquidFillsQueryBuilder<infer Q>
    ? HyperliquidFillsPortalData<Q>
    : T extends Transformer<any, infer O>
      ? O
      : T extends Record<string, Transformer<any, any> | HyperliquidFillsQueryBuilder<any>>
        ? {
            [K in keyof T]: T[K] extends Transformer<any, infer O>
              ? O
              : T[K] extends HyperliquidFillsQueryBuilder<infer Q>
                ? HyperliquidFillsPortalData<Q>
                : never
          }
        : never

export function hyperliquidFillsPortalStream<Out extends HyperliquidFillsOutputs>({
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
  const query = new HyperliquidFillsQueryBuilder<F>().addFields({
    block: { hash: true, number: true },
  })

  return new PortalStream<HyperliquidFillsQueryBuilder<F>, HyperliquidFillsPortalStream<Out>>({
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
      createTransformer<HyperliquidFillsPortalData<F>, HyperliquidFillsPortalData<F>>({
        profiler: { name: 'normalize data' },
        transform: (data, ctx) => {
          const schema = getBlockSchema<hl.Block<F>>(ctx.stream.query.raw)

          return castBlocks(schema, data)
        },
      }),
      mergeOutputs(outputs),
    ],
  })
}
