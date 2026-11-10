import {
  LogLevel,
  Logger,
  PortalCache,
  PortalStream,
  SpanHooks,
  createTransformer,
  mergeOutputs,
  pipeLogger,
  registerFallbackMetrics,
} from '~/core/index.js'
import { MetricsServer } from '~/core/metrics-server.js'
import { ProgressTrackerOptions, progressTracker } from '~/core/progress-tracker.js'

import { BlockStreamClient, PortalClientOptions, castBlocks, getBlockSchema } from '../portal-client/index.js'
import * as evm from '../portal-client/query/evm.js'
import { EvmFallbackOptions, EvmSourceSpec, createEvmFallbackClient } from './evm-fallback.js'
import { EvmOutputs, EvmStreamData } from './evm-portal-source.js'
import { EvmPortalData, EvmQueryBuilder } from './evm-query-builder.js'

/**
 * Where an EVM stream reads its blocks from:
 *
 * - a portal dataset URL, portal client options, or a ready client (a `PortalClient` or any
 *   custom {@link BlockStreamClient}) — a single source;
 * - an **array of sources in preference order** — portal URLs/options, `{type: 'rpc', url}`
 *   JSON-RPC endpoints, or custom clients. The stream then drives the first available source and
 *   fails over (and switches back) between them; see {@link EvmSourceSpec} and the `fallback`
 *   option.
 */
export type EvmStreamSource = string | PortalClientOptions | BlockStreamClient | EvmSourceSpec[]

export interface EvmStreamOptions<Out extends EvmOutputs> {
  /**
   * Globally unique, stable identifier for this pipe.
   * Targets use it as a cursor key to persist progress — two pipes with the
   * same `id` will share (and overwrite) each other's cursor.
   */
  id: string
  /** Where to read blocks from — see {@link EvmStreamSource}. */
  source: EvmStreamSource
  outputs: Out
  /**
   * Fallback behavior when `source` is a source list — two halves: `detection` senses failure and
   * recovery (probes, head polls, thresholds — it defines the events), `strategy` decides what to
   * do about it (stock-strategy options, or a custom function). Rejected for a single source.
   */
  fallback?: EvmFallbackOptions
  cache?: PortalCache
  metrics?: MetricsServer
  logger?: Logger | LogLevel
  profiler?: boolean | SpanHooks
  progress?: ProgressTrackerOptions
}

/**
 * Streams EVM data from a portal, a JSON-RPC endpoint, or an ordered fallback list of both.
 *
 * @experimental The source-list and RPC paths are new; `evmPortalStream` stays the stable entry
 * point for a single portal.
 */
export function evmStream<Out extends EvmOutputs>({
  id,
  source,
  outputs,
  fallback,
  cache,
  logger,
  metrics,
  profiler,
  progress,
}: EvmStreamOptions<Out>) {
  if (source == null) {
    throw new Error('`source` is required')
  }
  if (fallback && !Array.isArray(source)) {
    throw new Error(
      '`fallback` requires `source` to be an array of sources — a single source has nothing to fall back to',
    )
  }

  // One logger instance for the whole pipe. Resolving it here rather than letting the stream and
  // the fallback each resolve the same level would leave the pipe logging through two instances.
  const log = pipeLogger(id, logger)

  let blockSource: string | PortalClientOptions | BlockStreamClient
  if (Array.isArray(source)) {
    // The fallback logs source switches and health transitions; it is part of this pipe, so it
    // logs through the pipe's logger (and carries its id) unless the caller overrode it.
    const client = createEvmFallbackClient(source, { ...fallback, logger: fallback?.logger ?? log })
    if (metrics) {
      registerFallbackMetrics(metrics.metrics, client, id)
    }
    blockSource = client
  } else {
    blockSource = source
  }

  type F = { block: { hash: true; number: true } }
  const query = new EvmQueryBuilder<F>().addFields({
    block: { hash: true, number: true },
  })

  return new PortalStream<EvmQueryBuilder<F>, EvmStreamData<Out>>({
    id,
    portal: blockSource,
    query,
    cache,
    logger: log,
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
