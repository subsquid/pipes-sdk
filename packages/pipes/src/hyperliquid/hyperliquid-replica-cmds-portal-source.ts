import { MetricsServer } from '~/core/metrics-server.js'
import { ProgressTrackerOptions, progressTracker } from '~/core/progress-tracker.js'
import * as hl from '~/portal-client/query/hyperliquid-replica-cmds.js'

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
import {
  HyperliquidReplicaCmdsPortalData,
  HyperliquidReplicaCmdsQueryBuilder,
} from './hyperliquid-replica-cmds-query-builder.js'

export type HyperliquidReplicaCmdsFieldSelection = hl.FieldSelection

export * as replicaCmdsApi from '../portal-client/query/hyperliquid-replica-cmds.js'

export type HyperliquidReplicaCmdsOutputs = Outputs<hl.FieldSelection, HyperliquidReplicaCmdsQueryBuilder<any>>

type HyperliquidReplicaCmdsPortalStream<T extends HyperliquidReplicaCmdsOutputs> =
  T extends HyperliquidReplicaCmdsQueryBuilder<infer Q>
    ? HyperliquidReplicaCmdsPortalData<Q>
    : T extends Transformer<any, infer O>
      ? O
      : T extends Record<string, Transformer<any, any> | HyperliquidReplicaCmdsQueryBuilder<any>>
        ? {
            [K in keyof T]: T[K] extends Transformer<any, infer O>
              ? O
              : T[K] extends HyperliquidReplicaCmdsQueryBuilder<infer Q>
                ? HyperliquidReplicaCmdsPortalData<Q>
                : never
          }
        : never

export function hyperliquidReplicaCmdsPortalStream<Out extends HyperliquidReplicaCmdsOutputs>({
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
  const query = new HyperliquidReplicaCmdsQueryBuilder<F>().addFields({
    block: { hash: true, number: true },
  })

  return new PortalStream<HyperliquidReplicaCmdsQueryBuilder<F>, HyperliquidReplicaCmdsPortalStream<Out>>({
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
      createTransformer<HyperliquidReplicaCmdsPortalData<F>, HyperliquidReplicaCmdsPortalData<F>>({
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
