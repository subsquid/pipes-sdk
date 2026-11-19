import { PortalClientOptions, castBlocks, getBlockSchema } from '~/portal-client/index.js'

import { LogLevel, Logger } from './logger.js'
import { MetricsServer } from './metrics-server.js'
import { Outputs, mergeOutputs } from './output.js'
import { PortalCache, PortalStream } from './portal-source.js'
import { SpanHooks } from './profiling.js'
import { ProgressTrackerOptions, progressTracker } from './progress-tracker.js'
import { QueryBuilder } from './query-builder.js'
import { Transformer, createTransformer } from './transformer.js'

type BuiltOutput<T> = T extends { build(...args: any[]): Transformer<any, infer O> } ? O : never

/** What a stream yields for `outputs`: a builder's blocks, a transformer's output, or a record of either. */
export type StreamOutput<Out> =
  Out extends QueryBuilder<any>
    ? BuiltOutput<Out>
    : Out extends Transformer<any, infer O>
      ? O
      : { [K in keyof Out]: StreamOutput<Out[K]> }

export type ChainStreamOptions<Out> = {
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
}

/**
 * Makes a dataset's stream function. `QB` is the dataset's builder over any field selection, which
 * every output must be; `newQuery` builds the query the stream starts from.
 */
export function portalStreamOf<QB extends QueryBuilder<any>>(newQuery: () => QB) {
  return <Out extends Outputs<any, QB>>({
    outputs,
    progress,
    ...options
  }: ChainStreamOptions<Out>): PortalStream<QB, StreamOutput<Out>> =>
    new PortalStream<QB, StreamOutput<Out>>({
      ...options,
      query: newQuery(),
      transformers: [
        progressTracker({ interval: progress?.interval, onStart: progress?.onStart, onProgress: progress?.onProgress }),
        createTransformer<unknown[], unknown[]>({
          profiler: { name: 'normalize data' },
          transform: (data, ctx) => castBlocks(getBlockSchema(ctx.stream.query.raw), data),
        }),
        mergeOutputs(outputs),
      ],
    })
}
