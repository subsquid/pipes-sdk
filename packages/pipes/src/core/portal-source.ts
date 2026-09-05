import {
  ApiDataset,
  GetBlock,
  PortalBlockStream,
  PortalBlockStreamOptions,
  PortalClient,
  PortalClientOptions,
  Query,
  isForkException,
} from '~/portal-client/index.js'

import { last } from '../internal/array.js'
import { stallWatchdog } from '../internal/stall-watchdog.js'
import {
  ForkCursorMissingError,
  ForkOnFinalizedStreamError,
  MissingForkAncestorError,
  TargetForkNotSupportedError,
} from './errors.js'
import { FinalizedWatermark } from './finalized-watermark.js'
import { formatBlock, formatDuration } from './formatters.js'
import { LogLevel, Logger, defaultLogger, formatWarning } from './logger.js'
import { Metrics, MetricsServer, noopMetricsServer } from './metrics-server.js'
import { Profiler, Span, SpanHooks } from './profiling.js'
import { ProgressEvent, StartEvent } from './progress-tracker.js'
import { QueryBuilder, type Range, hashQuery } from './query-builder.js'
import { ReadOptions, Target, TargetState } from './target.js'
import { QueryAwareTransformer, Transformer, TransformerArgs, TransformerOptions } from './transformer.js'
import { BlockCursor, HookContext } from './types.js'

const WAITING_FOR_PORTAL = 'waiting for data from the portal'

/**
 * How long the pipe may sit in one place before it is reported as stalled.
 *
 * Two minutes, not seconds: a backfill batch that enriches over RPC can legitimately take a
 * minute, and a warning users learn to ignore is worse than no warning.
 */
const STALL_WARNING_MS = 120_000

/** Names the batch a stall report is about, so the log points at a block range, not "a batch". */
function processingPhase(blocks: { header: { number: number } }[], requestedFromBlock: number) {
  const from = blocks[0]?.header?.number ?? requestedFromBlock
  const to = blocks[blocks.length - 1]?.header?.number ?? from

  if (from === to) {
    return `processing block ${formatBlock(from)}`
  }

  return `processing blocks ${formatBlock(from)} → ${formatBlock(to)}`
}

const NOT_REAL_TIME_WARNING = (name: string) => {
  return formatWarning({
    title: `This dataset (${name}) does not provide real-time (head) block streaming`,
    content: [
      'Portal data for this dataset will lag behind the chain head.',
      'This is expected. Do not rely on this dataset for latency-critical workflows.',
    ],
  })
}

const FORCED_FINALIZED_STREAM_WARNING = formatWarning({
  title: 'This target commits only finalized data, so the pipe was switched to the finalized stream',
  content: [
    'The portal was configured with `finalized: false`, but a hot stream ends as soon as its range',
    'has been delivered — a range ending above the finalized head would leave the target holding an',
    'uncommitted tail and still report success.',
    'Set `finalized: true` on the portal options to silence this.',
  ],
})

/**
 * A view of an existing client whose block stream cannot be switched back to its configured hot
 * default. Keeping the original instance behind a Proxy preserves custom clients and HTTP
 * configuration, while also making `instanceof PortalClient` true for caches.
 *
 * The stream is pinned; head lookups are only defaulted. A caller that explicitly asks for the hot
 * head still gets it — `start` hands this view to transformer hooks, and a transformer measuring
 * head lag must be able to see the block the chain is actually on.
 */
function finalizedPortalView(portal: PortalClient): PortalClient {
  const getHead: PortalClient['getHead'] = async (options) => {
    if (options?.finalized === false) return portal.getHead(options)

    // A dataset that finalizes nothing still streams (its files just aren't reorg-safe), and an
    // absent finalized head would resolve `from: 'latest'` to genesis — a full backfill.
    const head = await portal.getHead({ ...options, finalized: true })

    return head ?? portal.getHead({ ...options, finalized: false })
  }
  const getStream: PortalClient['getStream'] = <Q extends Query>(query: Q, options?: PortalBlockStreamOptions) =>
    portal.getStream(query, { ...options, finalized: true })

  return new Proxy(portal, {
    get(target, property) {
      if (property === 'finalized') return true
      if (property === 'getHead') return getHead
      if (property === 'getStream') return getStream

      // PortalClient uses private fields, so methods/getters must keep the real client as `this`.
      const value = Reflect.get(target, property, target)
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
}

export interface PortalCache {
  getStream<Q extends Query>(options: {
    portal: PortalClient
    query: Q
    logger: Logger
    perBlockUnfinalized?: boolean
    finalized?: boolean
  }): PortalBlockStream<GetBlock<Q>>
}

export type StreamInfo = {
  dataset: ApiDataset
  head: {
    finalized?: BlockCursor
    latest?: BlockCursor
  }
  state: {
    initial: number
    last: number
    current: BlockCursor
    /**
     * The configured query ranges, as resolved at startup and BEFORE the resume bound is applied
     * — `initial` is just `ranges[0].from`, and `last` tracks the end of the final range.
     *
     * Kept unbounded on purpose: a target that names files by the range they cover must tell an
     * un-queried gap between two ranges apart from a block that was queried and simply had no
     * data. After a resume the bounded ranges no longer show the gap, so only this list can.
     */
    ranges: Range[]
    /**
     * List of block cursors representing unfinalized blocks in chronological order.
     * Used for handling blockchain forks by tracking alternative chain versions
     * and enabling rollback to a valid chain state when a fork is detected.
     */
    rollbackChain: BlockCursor[]
  }
  progress?: ProgressEvent['progress']
  query: { url: string; hash: string; raw: any }
}

export type BatchMetadata = {
  blocksCount: number
  bytesSize: number
  requests: Record<number, number>
  lastBlockReceivedAt: Date
}

export type BatchContext = {
  id: string
  profiler: Profiler
  metrics: Metrics
  logger: Logger
  stream: StreamInfo
  batch: BatchMetadata
}

export type PortalBatch<T = any> = { data: T; ctx: BatchContext }

type PartialBlock = { header: { number: number; hash: string; timestamp?: number } }

export function cursorFromHeader(block: PartialBlock): BlockCursor {
  return { number: block.header.number, hash: block.header.hash, timestamp: block.header.timestamp }
}

/** @internal */
export function extractRollbackChain({ blocks, head }: { blocks: PartialBlock[]; head?: BlockCursor }): BlockCursor[] {
  if (!head) return []
  if (!blocks.length) return []

  return blocks
    .filter((b) => {
      return b.header.number > head.number
    })
    .map(cursorFromHeader)
}

export type PortalStreamOptions<Query> = {
  /**
   * Globally unique, stable identifier for this pipe.
   * Targets use it as a cursor key to persist progress — two pipes with the
   * same `id` will share (and overwrite) each other's cursor.
   */
  id: string
  portal: string | PortalClientOptions | PortalClient
  query: Query
  logger?: Logger | LogLevel
  profiler?: boolean | SpanHooks
  cache?: PortalCache
  transformers?: Transformer<any, any>[]
  metrics?: MetricsServer
  progress?: {
    interval?: number
    onStart?: (data: StartEvent) => void
    onProgress?: (progress: ProgressEvent) => void
  }
}

export class PortalStream<Q extends QueryBuilder<any>, T = any> {
  readonly #id: string
  readonly #options: {
    profiler: boolean | SpanHooks
    cache?: PortalCache
  }
  readonly #queryBuilder: Q
  readonly #logger: Logger
  readonly #portal: PortalClient
  readonly #metricServer: MetricsServer
  readonly #transformers: Transformer<any, any>[] = []
  // Single monotonic finalized high-watermark for the whole pipe. Owned here (not
  // per-target) so a source-switch reporting a deeper/transiently-missing finalized
  // head can never un-finalize already-committed data, and every target saveCursor path gets one
  // consistent, never-regressing finalized head. Seeded from the target's persisted floor.
  readonly #watermark = new FinalizedWatermark()
  #started = false

  constructor({ portal, id, query, logger, progress, ...options }: PortalStreamOptions<Q>) {
    // Targets key their persisted cursor by this id, and every state class treats an empty id as
    // "not bound" and falls back to the shared legacy "stream" key — which would silently put
    // this pipe back into the cross-pipe cursor collision the id exists to prevent.
    if (!id?.trim()) {
      throw new Error('PortalStream requires a non-empty "id": targets use it as the cursor key to persist progress')
    }

    this.#id = id
    this.#logger = logger && typeof logger !== 'string' ? logger : defaultLogger({ id: this.#id, level: logger })

    this.#portal =
      portal instanceof PortalClient
        ? portal
        : new PortalClient(
            typeof portal === 'string'
              ? {
                  url: portal,
                  http: {
                    logger: this.#logger,
                    retryAttempts: Number.MAX_SAFE_INTEGER,
                  },
                }
              : {
                  ...portal,
                  http: {
                    logger: this.#logger,
                    retryAttempts: Number.MAX_SAFE_INTEGER,
                    ...portal.http,
                  },
                },
          )

    this.#queryBuilder = query

    this.#options = {
      cache: options.cache,
      profiler: typeof options.profiler === 'undefined' ? process.env.NODE_ENV !== 'production' : options.profiler,
    }

    this.#metricServer = options.metrics ?? noopMetricsServer()
    this.#transformers = options.transformers || []

    this.#metricServer.registerPipe(this.#id)
  }

  /**
   * `forceFinalized` is an argument rather than instance state on purpose: one source can be piped
   * to several targets, and a finalized-only target must not silently pin the endpoint for the hot
   * consumers that follow it.
   */
  private async *read(
    state?: TargetState,
    options?: ReadOptions,
    forceFinalized = false,
  ): AsyncIterable<PortalBatch<T>> {
    // Seed the monotonic finalized watermark from the target's persisted finalized
    // head so it survives an unclean restart mid-fork. Seeding only from the
    // dedicated `finalized` field (never from `latest`) keeps no-finality datasets
    // correct: the floor stays undefined → Infinity-threshold passthrough.
    this.#watermark.seed(state?.finalized ?? undefined)

    const cursor = state?.latest
    const portal = forceFinalized ? finalizedPortalView(this.#portal) : this.#portal

    /*
     Calculates query ranges while excluding blocks that were previously fetched to avoid duplicate processing
     */
    const { bounded, raw } = await this.#queryBuilder.calculateRanges({
      portal,
      bound: cursor ? { from: cursor.number + 1 } : undefined,
    })

    const initial = raw[0]?.range.from || 0
    const ranges = raw.map((r) => r.range)

    this.#logger.debug(`${bounded.length} range(s) configured`)

    await this.start({ initial, current: cursor }, portal)

    const datasetMetadata = await portal.getMetadata()
    if (!datasetMetadata.real_time) {
      this.#logger.warn(NOT_REAL_TIME_WARNING(datasetMetadata.dataset))
    }

    // A wedged target and a silent portal look identical from the outside: the progress line
    // keeps printing the same numbers and nothing else is logged. The watchdog names which of
    // the two is not returning, and repeats on a doubling interval so a stall lasting hours
    // stays visible without flooding the log.
    const watchdog = stallWatchdog({
      thresholdMs: STALL_WARNING_MS,
      onStall: ({ phase, elapsedMs }) => {
        this.#logger.warn({ message: `stalled: ${phase} for ${formatDuration(elapsedMs)}`, phase, elapsedMs })
      },
      onRecover: ({ phase, elapsedMs }) => {
        this.#logger.info({ message: `recovered: ${phase} took ${formatDuration(elapsedMs)}`, phase, elapsedMs })
      },
    })

    for (const { range, request } of bounded) {
      // Anchor the cursor's hash only to the range that continues from it; a later disjoint range
      // doesn't border the cursor and would fault a spurious 409, so it starts unanchored (ADR-20).
      const isResumeContinuation = cursor?.hash != null && range.from === cursor.number + 1

      const query = {
        ...request,
        type: this.#queryBuilder.getType(),
        fields: this.#queryBuilder.getFields(),
        fromBlock: range.from,
        toBlock: range.to,
        parentBlockHash: isResumeContinuation ? cursor.hash : undefined,
      }

      // `portal` already enforces finality when the target requires it. Forwarding the effective
      // value as well keeps the cache contract explicit; a cache that ignores it is still safe.
      // Passed as the boolean it is — `false` means "this pipe is hot", which a cache deciding what
      // is safe to persist must be able to tell apart from "the source said nothing".
      const finalizedStream = portal.finalized

      const source = this.#options.cache
        ? // use cache if available
          this.#options.cache.getStream({
            portal,
            logger: this.#logger,
            query,
            perBlockUnfinalized: options?.perBlockUnfinalized ?? false,
            finalized: finalizedStream,
          })
        : portal.getStream(query, {
            perBlockUnfinalized: options?.perBlockUnfinalized ?? false,
            finalized: finalizedStream,
          })

      let batchSpan = Span.root('batch', this.#options.profiler).addLabels('core')
      let readSpan = batchSpan.start('fetch data').addLabels('core')
      try {
        watchdog.begin(WAITING_FOR_PORTAL)

        for await (const batch of source) {
          readSpan.end()

          const blocks = batch.blocks

          // Held until the consumer comes back for the next batch, so it covers the transformers
          // and everything the target does with this batch, not just the code in this loop.
          watchdog.begin(processingPhase(blocks as { header: { number: number } }[], batch.meta.requestedFromBlock))

          if (blocks.length > 0) {
            // Clamp the portal's finalized head through the monotonic watermark before it
            // reaches any consumer, so a regressed or transiently-missing finalized head can
            // never un-finalize already-committed data. The rollback chain is derived from the
            // CLAMPED head so the two stay consistent (clamp can only raise finalized).
            const finalized = this.#watermark.clamp(batch.head.finalized)

            // TODO WTF with any?
            const lastBatchBlock = last(blocks as { header: { number: number } }[])

            // The last block this run can reach. A configured end bounds it, but never past the
            // chain head: a progress denominator above the head is one the run cannot hit, so the
            // ETA never lands. With no configured end the run just tracks the head. `??` throughout
            // — block 0 is a valid range end and a valid head.
            const chainHead = batch.head.latest?.number
            const runEnd = last(bounded)?.range?.to ?? chainHead ?? finalized?.number ?? Infinity
            const lastBlockNumber = Math.max(
              Math.min(runEnd, chainHead ?? Infinity),
              lastBatchBlock.header?.number ?? -Infinity,
            )

            const ctx: BatchContext = {
              id: this.#id,
              profiler: batchSpan,
              metrics: this.#metricServer.metrics,
              logger: this.#logger,
              stream: {
                dataset: datasetMetadata,
                head: {
                  finalized,
                  latest: batch.head.latest,
                },
                query: {
                  url: portal.getUrl(),
                  hash: await hashQuery(query),
                  raw: query,
                },
                state: {
                  initial,
                  ranges,
                  current: cursorFromHeader(lastBatchBlock as any),
                  last: lastBlockNumber,
                  rollbackChain: extractRollbackChain({
                    blocks: batch.blocks,
                    head: finalized,
                  }),
                },
              },
              batch: {
                blocksCount: batch.blocks.length,
                bytesSize: batch.meta.bytes,
                requests: batch.meta.requests,
                lastBlockReceivedAt: batch.meta.lastBlockReceivedAt,
              },
            }

            const data = await this.applyTransformers(ctx, batch.blocks as T)

            yield { data, ctx }
          } else {
            // Never yielded, so batchEnd won't run.
            batchSpan.end()
          }

          batchSpan = Span.root('batch', this.#options.profiler).addLabels('core')
          readSpan = batchSpan.start('fetch data').addLabels('core')
          watchdog.begin(WAITING_FOR_PORTAL)
        }

        watchdog.end()
      } finally {
        // The last pair is always armed for a batch that never arrives.
        readSpan.end()
        batchSpan.end()
        // A no-op once the loop has run to completion; on a throw or a cancelled consumer it
        // stops the clock without claiming the phase recovered.
        watchdog.abort()
      }
    }

    // Cleanup is owned by the callers (pipeTo/[Symbol.asyncIterator]), which run stop() in a
    // finally on every exit path. Calling it here too is what caused the double stop() on normal
    // completion; the idempotency guard in stop() remains as a safety net.
  }

  pipe<Out>(options: TransformerArgs<T, Out>): PortalStream<Q, Out> {
    if (this.#started) throw new Error('Stream is closed')

    const transformer = options instanceof Transformer ? options : new Transformer(options)

    const id = transformer.id()

    // If there are multiple transformers with the same ID, we append a numeric suffix to make them unique
    // This is important for profiling and logging to avoid confusion between transformers
    // when analyzing performance or debugging issues
    const exists = this.#transformers.filter((t) => t.id() === id)
    if (exists.length) {
      transformer.setId(`${id} ${exists.length + 1}`)
    }

    return new PortalStream<Q, Out>({
      id: this.#id,
      portal: this.#portal,
      query: this.#queryBuilder,
      logger: this.#logger,
      profiler: this.#options.profiler,
      cache: this.#options.cache,
      metrics: this.#metricServer,
      transformers: [...this.#transformers, transformer],
    })
  }

  private async applyTransformers(ctx: BatchContext, data: T) {
    const span = ctx.profiler.start('apply transformers').addLabels('core')

    try {
      for (const transformer of this.#transformers) {
        data = await transformer.run(data, {
          ...ctx,
          profiler: span,
          logger: this.#logger,
        })
      }
    } finally {
      span.end()
    }

    return data
  }

  private context<T extends Record<string, any>>(span: Profiler, rest?: T) {
    return {
      logger: this.#logger,
      profiler: span,
      ...rest,
    } as HookContext & T
  }

  private async rollbackTransformers(profiler: Profiler, cursor: BlockCursor) {
    const span = profiler.start({ name: 'transformers_rollback', labels: 'core' })
    const ctx = this.context(span)
    await Promise.all(this.#transformers.map((t) => t.rollback(cursor, ctx)))
    span.end()
  }

  private async configure() {
    await Promise.all(
      this.#transformers
        .filter((t) => t instanceof QueryAwareTransformer)
        .map((t) =>
          t.setupQuery({
            query: this.#queryBuilder,
            logger: this.#logger,
          }),
        ),
    )
  }

  private async start(state: { initial: number; current?: BlockCursor }, portal: PortalClient) {
    if (this.#started) {
      this.#logger.debug(`stream has been already started, skipping "start" hook...`)
      return
    }

    // Mark the stream as started before invoking user start hooks. If a transformer `start`
    // hook rejects, the outer finally still calls stop(), and the idempotency guard there must
    // let cleanup run for the partially-started stream instead of skipping it.
    this.#started = true

    this.#logger.debug(`invoking <start> hook...`)

    const profiler = Span.root('start', this.#options.profiler).addLabels('core')

    const span = profiler.start({ name: 'transformers', labels: 'core' })
    const ctx = this.context(span, {
      id: this.#id,
      metrics: this.#metricServer.metrics,
      state,
      portal,
    })
    await Promise.all(this.#transformers.map((t) => t.start(ctx)))
    span.end()

    this.#metricServer.start()

    profiler.end()

    this.#logger.debug(`<start> hook invoked`)
  }

  /** @internal */
  async stop() {
    if (!this.#started) {
      this.#logger.debug(`stream is not started, skipping "stop" hook...`)
      return
    }
    this.#started = false

    const profiler = Span.root('stop', this.#options.profiler).addLabels('core')

    const span = profiler.start({ name: 'transformers', labels: 'core' })
    const ctx = this.context(span)
    await Promise.all(this.#transformers.map((t) => t.stop(ctx)))
    span.end()

    profiler.end()

    await this.#metricServer.stop()
  }

  pipeTo(target: Target<T>) {
    const self = this

    // Decided before write(), so the flag handed to the target is the stream it will actually read.
    const forceFinalized = target.requiresFinalizedStream === true && !this.#portal.finalized
    if (forceFinalized) {
      this.#logger.warn(FORCED_FINALIZED_STREAM_WARNING)
    }

    const finalized = this.#portal.finalized || forceFinalized

    return target.write({
      id: this.#id,
      finalized,
      logger: this.#logger,
      read: async function* (state?: TargetState, options?: ReadOptions) {
        await self.configure()

        while (true) {
          try {
            for await (const batch of self.read(state, options, forceFinalized)) {
              yield batch as PortalBatch<T>
              self.batchEnd(batch.ctx)
            }
            return
          } catch (e) {
            if (!isForkException(e)) throw e

            // A fork on `/finalized-stream` contradicts the route's contract. Never dispatch it to
            // a target rollback handler: that could delete data already committed as finalized.
            if (finalized) {
              throw new ForkOnFinalizedStreamError()
            }

            if (!e.canonicalBlocks.length) {
              throw new MissingForkAncestorError()
            }

            if (!target.resolveFork) {
              throw new TargetForkNotSupportedError()
            }

            const forkProfiler = Span.root('fork', self.#options.profiler).addLabels('core')

            const span = forkProfiler.start({ name: 'target_rollback', labels: 'core' })
            const forkedCursor = await target.resolveFork(e.canonicalBlocks)
            span.end()

            if (!forkedCursor) {
              throw new ForkCursorMissingError()
            }

            await self.rollbackTransformers(forkProfiler, forkedCursor)

            // Resume from the forked cursor; the finalized floor persists in the
            // instance #watermark across read() re-invocation, so re-seeding with the
            // same state.finalized is a harmless monotonic no-op.
            state = { latest: forkedCursor, finalized: state?.finalized ?? null }
          } finally {
            await self.stop()
          }
        }
      },
    })
  }

  private batchEnd(ctx: BatchContext) {
    ctx.profiler.end()
    this.#metricServer.batchProcessed(ctx)
  }

  async *[Symbol.asyncIterator](): AsyncIterator<PortalBatch<T>> {
    await this.configure()

    try {
      for await (const batch of this.read()) {
        yield batch
        this.batchEnd(batch.ctx)
      }
    } finally {
      await this.stop()
    }
  }
}
