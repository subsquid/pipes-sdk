import type { BatchPublishOptions, ClientConfig, FlowControlOptions, PubSub } from '@google-cloud/pubsub'

import {
  BatchContext,
  BlockCursor,
  Counter,
  Gauge,
  Histogram,
  Logger,
  Metrics,
  Profiler,
  blockTimestampSeconds,
  createTarget,
  formatBlock,
} from '~/core/index.js'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import {
  CdcEncoder,
  MAX_SEQUENCE_VALUE,
  PROTOCOL_ATTRIBUTES,
  PubsubOp,
  assertPublishRequestSize,
  assertWireLimits,
  buildAttributes,
  canonicalCdcMessageBytes,
  encodeCdcMessage,
  encodeRow,
  readRowId,
  validateUserAttributes,
} from './protocol.js'
import {
  GooglePubsubPublisher,
  PublishMessage,
  Publisher,
  PublisherOptions,
  TopicSetup,
  partitionRows,
  resolvePubsubClient,
} from './publisher.js'
import {
  OutboxRow,
  PendingOperation,
  PubsubState,
  RouteMode,
  RowIdSource,
  SqlitePubsubState,
  stableAttributes,
} from './pubsub-state.js'

export type MessageDraft = {
  /**
   * The table row. It must be a plain object. Set `_id` to a stable string to own the row
   * identity; otherwise the target derives one. The target adds the remaining BigQuery CDC
   * fields before publishing. The canonical codec supports values such as `bigint` and bytes.
   */
  data: object
  /** The block this operation belongs to — drives fork compensation. Required. */
  block: { number: number; hash?: string; timestamp?: number }
  /**
   * `upsert` (default) writes the row; `delete` removes it. Emit `delete` only where the row
   * genuinely disappears (an emptied window) — fork compensation produces its own.
   */
  op?: 'upsert' | 'delete'
  /**
   * Fallback stable row identity when `data._id` is nullish. A `delete` or a materialized-row
   * restore reuses the resolved id verbatim. When both are set, `data._id` takes precedence.
   * Default: `${namespace}:${stream}:${block.number}:${block.hash}:<seq-in-block>`.
   */
  id?: string
  /**
   * User attributes for subscription filtering; copied onto the compensating operation. For a
   * materialized id these must be stable across every revision. Names starting with `_` are
   * reserved by the target, `goog…` by GCP.
   */
  attributes?: Record<string, string>
  /**
   * Available only when `publish.messageOrdering` is enabled. Overrides the topic's default
   * ordering key. A materialized id must never move between keys.
   */
  orderingKey?: string
}

export type RollbackInverse = { op: 'delete' } | { op: 'upsert'; data: object }

export type TopicRoute<Data> = {
  topic: string
  /**
   * `event` (default): every id is write-once on a chain branch; a fork orphans it outright.
   * `materialized`: ids may be updated; a fork restores the surviving revision. Every draft in
   * one materialized route must use the same id source (`data._id`, `MessageDraft.id`,
   * `deriveId`, or the generated fallback).
   */
  mode?: RouteMode
  /**
   * Map one batch of this stream's data to operations. Must be pure and deterministic (no wall
   * clock, no randomness): replays must reproduce identical bytes so duplicates stay
   * recognizable.
   */
  map: (batch: { data: Data; ctx: BatchContext }) => MessageDraft[]
  /**
   * Encoder for the complete CDC row. Defaults to canonical JSON. Must be pure and remain
   * unchanged while this route has pending operations in the state.
   */
  encode?: CdcEncoder
  /**
   * Identity for drafts that leave both `data._id` and `id` unset, when the block-derived default
   * does not fit.
   * A materialized row lives longer than the block that last touched it, so its id has to come
   * from the row itself — that is what `windowTopic` uses. Must be pure and stable across
   * revisions.
   */
  deriveId?: (draft: MessageDraft, context: { namespace: string; stream: string; index: number }) => string
  /**
   * The compensating operation for an id whose every published revision is orphaned by a fork.
   * Default: `{ op: 'delete' }`. Returning an `upsert` is what makes a topic delete-free
   * through forks — the target cannot invent that value, so the route supplies it. Evaluated
   * EAGERLY at the id's first rollbackable publish and stored encoded in the state: at fork
   * time the draft that produced the id is long gone.
   */
  rollbackWhenMissing?: (draft: MessageDraft & { id: string }) => RollbackInverse
}

export type PubsubTargetOptions<T> = {
  /** A constructed client, or `ClientConfig` passed to `new PubSub(...)`. Auth = ADC / emulator. */
  pubsub: PubSub | ClientConfig
  /** Combined local state: cursor + manifest + outbox + sequence counters. */
  state: { path: string } | PubsubState
  /**
   * One route per pipe output stream (keys of the decoder/transformer output).
   *
   * `NoInfer` keeps this off the inference path: the stream's own type has to come from the
   * pipe `pipeTo` hands over, otherwise TypeScript would read the route map itself as the
   * stream shape and every `map` callback would land on `unknown`.
   */
  topics?: { [K in keyof NoInfer<T>]?: TopicRoute<NoInfer<T>[K]> }
  /**
   * Attributes that are constant for this producer — `chain` and `table`, typically. They ride
   * every message. Per-draft `attributes` add to these.
   */
  attributes?: Record<string, string>
  /**
   * The gap-free sequence run, on by default. A subscriber holding a contiguous run of change
   * sequence numbers is then missing no operation the producer committed inside it.
   *
   * Keeping it means one topic per producer: the counter is producer-wide, so a second topic
   * puts a hole in both runs. Set `false` to declare that no consumer of this producer reads
   * the barrier — that, and only that, permits several topics and unordered blocks.
   *
   * It also means one ROUTE per producer in practice. Operations are sequenced in the order
   * they are mapped, and routes are mapped one after another, so a second route feeding the
   * same topic restarts at the batch's first block and is refused (E2429) on any batch
   * spanning more than one block. Merge such streams into one route's `map`.
   */
  sequenceBarrier?: boolean
  /**
   * Permit a run that starts with no state. Off by default, because a cold start under a
   * namespace that has already published restarts the change sequence: BigQuery discards the
   * republished lower numbers as stale and every affected row freezes with no downstream
   * symptom. Recovery from lost state is a fresh namespace and a consumer re-bootstrap, never
   * a restart — so set this only to bootstrap a namespace that has never published.
   *
   * A refused run leaves the state file untouched, so the refusal still holds on the retry.
   */
  allowColdStart?: boolean
  /** Cursor key inside the state file. Defaults to the pipe id (the CursorKey rule, ADR-2). */
  settings?: { id?: string }
  publish?: {
    /**
     * Enable PubSub ordered publishing. Each topic uses its own name as the default ordering key;
     * a route may override it per draft. The subscription must also have message ordering enabled
     * to deliver messages in order. Disabled by default.
     */
    messageOrdering?: boolean
    /**
     * Publish a `_uid` attribute — a globally unique record id for pipelines that demand one
     * (e.g. Dataflow's `idAttribute`). Costs one attribute per message.
     */
    uidAttribute?: boolean
    batching?: BatchPublishOptions
    flowControl?: FlowControlOptions
  }
  /**
   * Opt in to publishing from a dataset that reports no finalized head (RP-44). Without it such a
   * dataset is refused at start: no watermark means no manifest, and this medium cannot retract
   * what it already published. A fork reported anyway stays fatal.
   */
  assumeNoForks?: boolean
  /**
   * Go-live block (frontfill). The pipe may read from far earlier (aggregator warm-up, factory
   * discovery), but nothing below this block is published or recorded. Default: `'latest'` —
   * resolved to the head at first start and persisted, so restarts keep the same go-live block.
   */
  publishFrom?: number | 'latest'
  /**
   * Producer namespace, baked into every GENERATED id. Defaults to the pipe id. Pin it
   * explicitly to decouple feed identity from pipe naming — with the default, renaming the pipe
   * is a breaking change for consumers (a fresh id space).
   */
  namespace?: string
  /**
   * `validate` (default) — fail fast at start if a topic does not exist.
   * `create` — create missing topics (dev convenience; needs admin IAM).
   * `none` — no admin calls at all (least privilege).
   */
  topicSetup?: TopicSetup
  /** @internal Test seam: replaces the Google client wiring wholesale. */
  publisher?: Publisher
}

const CAP_BYTES_PER_SECOND = 1024 * 1024
const META_GO_LIVE = 'go_live_block'
const META_WIRE_CONFIG = 'wire_config'
const STATS_EVERY_BATCHES = 25

/** Chain-scale: block production and chain-to-portal propagation dominate. */
const CHAIN_LAG_BUCKETS = [0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600]

/** Process-scale: buffer dwell, transformers, commit and publish — normally well under a second. */
const PIPELINE_LAG_BUCKETS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60]

/** One drain's measurements, held until `ctx.metrics` exists — see `#recordDrain`. */
type DrainSample = { elapsed: number; bytes: number; saturation: number }

type ResolvedRoute = {
  stream: string
  topic: string
  mode: RouteMode
  encoderKind: EncoderKind
  route: TopicRoute<any>
}

type EncoderKind = 'canonical' | 'custom'

export function pubsubTarget<T>(options: PubsubTargetOptions<T>) {
  const messageOrdering = options.publish?.messageOrdering ?? false
  const topicSetup: TopicSetup = options.topicSetup ?? 'validate'

  const sequenceBarrier = options.sequenceBarrier ?? true

  const routes: ResolvedRoute[] = Object.entries(options.topics ?? {})
    .filter((entry): entry is [string, TopicRoute<any>] => Boolean(entry[1]))
    .map(([stream, route]) => ({
      stream,
      topic: route.topic,
      mode: route.mode ?? 'event',
      encoderKind: route.encode ? 'custom' : 'canonical',
      route,
    }))
  const topics = [...new Set(routes.map((route) => route.topic))]

  // Refused here rather than at first write: with no routes there is nothing the state file can
  // be opened for, and taking its exclusive lock only to fail would strand a concurrent retry.
  if (!routes.length) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.NO_ROUTES, 'Configure at least one topic route.')
  }

  if (sequenceBarrier && topics.length > 1) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.MULTIPLE_TOPICS, [
      `This producer feeds ${topics.length} topics (${topics.join(', ')}), and its change sequence is one ` +
        'counter shared across all of them — so neither topic carries a contiguous run of it.',
      'Run one producer per topic, or set `sequenceBarrier: false` to declare that no consumer of this ' +
        'producer relies on a contiguous sequence run.',
    ])
  }

  validateUserAttributes(options.attributes, {
    topic: topics[0],
    route: 'attributes',
    protocolAttributes: PROTOCOL_ATTRIBUTES.length,
  })

  const state: PubsubState =
    'path' in options.state
      ? new SqlitePubsubState({ path: options.state.path, id: options.settings?.id })
      : options.state

  let publisher: Publisher | undefined = options.publisher
  // The fork hook runs while `write()` is suspended inside its for-await, so the live write
  // loop's context is the one that must drain the compensations.
  let context: WriteContext | undefined

  return createTarget<T>({
    write: async ({ read, logger, id, finalized: finalizedStream }) => {
      const namespace = options.namespace ?? id ?? options.settings?.id ?? 'pipe'
      const uidAttribute = options.publish?.uidAttribute ?? false

      const statePath = 'path' in options.state ? options.state.path : 'custom'
      const allowColdStart = options.allowColdStart ?? false
      const { coldStart } = await state.open({
        cursorKey: options.settings?.id ?? id ?? '',
        logger,
        allowColdStart,
      })

      // Everything past `open` runs under cleanup: the state holds an exclusive lock on its
      // file, so a failure in topic validation or the recovery drain would otherwise leave it
      // locked for the life of the process and make the retry fail as a second producer.
      try {
        if (coldStart && !allowColdStart) {
          throw new PubsubTargetError(PUBSUB_ERROR_CODES.COLD_START_REFUSED, [
            `The PubSub state at "${statePath}" is empty, so this run would restart the change sequence of ` +
              `namespace "${namespace}" at zero.`,
            'If that namespace has already published, BigQuery discards the republished lower numbers as ' +
              'stale and every affected row freezes with no downstream symptom — which is why recovery from ' +
              'lost state is a fresh namespace and a consumer re-bootstrap, not a restart.',
            'Set `allowColdStart: true` to bootstrap a namespace that has never published.',
          ])
        }

        await bindWireConfig(state, {
          namespace,
          uidAttribute,
          messageOrdering,
          attributes: options.attributes ?? {},
          encoders: routes.map(({ stream, encoderKind }) => [stream, encoderKind] as [string, EncoderKind]),
        })
        await run()
      } finally {
        await state.close()
        await publisher?.close()
      }

      async function run() {
        if (!publisher) {
          const client = await resolvePubsubClient(options.pubsub)
          const publisherOptions: PublisherOptions = {
            messageOrdering,
            topicSetup,
            batching: options.publish?.batching,
            flowControl: options.publish?.flowControl,
          }
          publisher = new GooglePubsubPublisher(client, publisherOptions)
        }

        await publisher.setup(topics, logger)

        logger.info({
          message:
            `publishing ${routes.length} route(s) to PubSub — message ordering ${messageOrdering ? 'enabled' : 'disabled'}, ` +
            `namespace "${namespace}", ` +
            `state "${statePath}"`,
          topics: routes.map((r) => `${r.stream} → ${r.topic} (${r.mode})`),
        })

        if (coldStart) {
          logger.warn(
            `PubSub state at "${statePath}" started EMPTY under namespace "${namespace}", and ` +
              '`allowColdStart` permitted it. The change sequence restarts at zero: if this namespace has ' +
              'ever published, destinations now discard its newer changes as stale.',
          )
        }

        if (options.assumeNoForks && !finalizedStream) {
          logger.warn(
            'assumeNoForks is set: nothing is recorded for compensation. If this dataset ever forks, the ' +
              'published operations cannot be retracted and the pipe fails.',
          )
        }

        const cursor = await state.getCursor()

        context = new WriteContext({
          state,
          publisher,
          routes,
          namespace,
          messageOrdering,
          logger,
          uidAttribute,
          constantAttributes: options.attributes ?? {},
          sequenceBarrier,
          assumeNoForks: options.assumeNoForks ?? false,
          finalizedStream: finalizedStream ?? false,
          publishFrom: options.publishFrom ?? 'latest',
          coldStart,
          // So the recovery drain below stamps a floor too, rather than handing a restarted
          // consumer one batch of messages it cannot reason against.
          finalized: cursor?.finalized?.number,
        })

        // Recovery needs no compensation of its own: the cursor only ever advances in the same
        // transaction that enqueues the outbox, so a crash leaves unpublished rows, never
        // unrecorded published ones. Restart = drain, then resume.
        await context.drain()

        for await (const batch of read(cursor)) {
          await context.write(batch)
        }
      }
    },

    resolveFork: async (canonicalBlocks) => {
      if (options.assumeNoForks) {
        throw new PubsubTargetError(PUBSUB_ERROR_CODES.FORK_UNDER_ASSUME_NO_FORKS, [
          'A chain fork was reported on a pipe running with `assumeNoForks: true`.',
          'That flag asserted the dataset cannot fork, so nothing was recorded to compensate with — ' +
            'and the operations have already been published. Remove the flag and re-bootstrap consumers.',
        ])
      }

      const safe = await state.fork(canonicalBlocks)
      context?.rewind()

      // Compensations go out before anything later on their partitions, and before the source
      // re-streams the canonical blocks.
      await context?.drain({ fork: true })

      return safe
    },
  })
}

type WriteContextOptions = {
  state: PubsubState
  publisher: Publisher
  routes: ResolvedRoute[]
  namespace: string
  messageOrdering: boolean
  logger: Logger
  uidAttribute: boolean
  constantAttributes: Record<string, string>
  sequenceBarrier: boolean
  assumeNoForks: boolean
  finalizedStream: boolean
  publishFrom: number | 'latest'
  coldStart: boolean
  /** Persisted finalized head, so the recovery drain has one before the first batch arrives. */
  finalized?: number
}

class WriteContext {
  readonly #options: WriteContextOptions
  readonly #routesByStream: Map<string, ResolvedRoute>
  #metrics?: PubsubMetrics
  #goLive?: number
  #batches = 0
  #finalityChecked = false
  #skippedLogged = false
  #lagWarned = false
  #deferredDrains: DrainSample[] = []
  /** Highest block sequenced so far, for the forward block-order check. Reset by a fork. */
  #lastBlock = -1
  /**
   * The source's finalized head, stamped on every message as `_finalized`. Held in memory
   * only: a consumer reads it by taking the maximum, so a value lost to a restart costs
   * nothing the next batch does not restore.
   */
  #finalized?: number

  constructor(options: WriteContextOptions) {
    this.#options = options
    this.#routesByStream = new Map(options.routes.map((route) => [route.stream, route]))
    this.#finalized = options.finalized
  }

  /** A fork rewinds the cursor, so the next forward batch legitimately steps back in blocks. */
  rewind(): void {
    this.#lastBlock = -1
  }

  async write({ data, ctx }: { data: any; ctx: BatchContext }): Promise<void> {
    const { state, logger } = this.#options
    const span = ctx.profiler.start({ name: 'pubsub', labels: 'queue' })

    try {
      if (!this.#metrics) {
        this.#metrics = registerPubsubMetrics(ctx.metrics)
        this.#metrics.coldStart.set({ id: ctx.id }, this.#options.coldStart ? 1 : 0)

        const deferred = this.#deferredDrains.splice(0)
        for (const sample of deferred) {
          this.#recordDrain(sample)
        }
      }

      this.#assertFinality(ctx)

      // Read at publish time rather than stored per operation: it is a producer-wide reference
      // value, and a batch that reports none leaves the last one standing.
      this.#finalized = ctx.stream.head.finalized?.number ?? this.#finalized

      await this.#resolveGoLive(ctx)
      this.#warnSkippedStreams(data, logger)

      const operations = await span.measure('map', async () => this.#map(data, ctx))
      const forkCapable = this.#rollbackable(ctx)

      await span.measure('state tx', () =>
        state.commit({
          operations,
          forkCapable,
          // Under `assumeNoForks` (or on the finalized stream) no fork can arrive, so the
          // ledger would only be write amplification.
          ledger: forkCapable ? ctx.stream.state.rollbackChain : [],
          cursor: ctx.stream.state.current,
          // The source already clamped this through the pipe's monotonic watermark; persisting
          // it verbatim keeps the stored floor non-regressing (ADR-3).
          finalized: ctx.stream.head.finalized ?? null,
        }),
      )

      for (const operation of operations) {
        this.#metrics.ops.inc({ id: ctx.id, topic: operation.topic, operation: operation.op }, 1)
      }

      let publishAckAt: number | undefined
      try {
        publishAckAt = await this.drain({ span })
      } finally {
        // Observed even when the publish threw. The lag is climbing precisely during an outage,
        // and a series that goes silent then cannot alert on the condition it exists to detect.
        // drain() records publishDuration/publishedBytes before rethrowing for the same reason.
        this.#observeCommitLag(ctx, publishAckAt ?? Date.now())
      }

      this.#batches++
      if (this.#batches % STATS_EVERY_BATCHES === 1) {
        const stats = await state.stats()
        this.#metrics.outboxDepth.set({ id: ctx.id }, stats.outbox)
        this.#metrics.manifestRows.set({ id: ctx.id }, stats.manifest)
      }
    } finally {
      span.end()
    }
  }

  /**
   * Publish everything the state has queued, then delete only the confirmed rows. A publish
   * whose outcome is unknown keeps its row, so the retry resends the SAME seq and bytes.
   *
   * Returns the moment PubSub acked, which is the anchor for the commit-lag histograms. The
   * outbox ack-delete that follows is durability bookkeeping, not publish latency, and the
   * BigQuery metric this one is read beside excludes its own post-commit for the same reason.
   */
  async drain({ span, fork }: { span?: Profiler; fork?: boolean } = {}): Promise<number> {
    const { state, publisher, logger } = this.#options

    const rows = await state.pending()
    if (!rows.length) return Date.now()

    const wire = rows.map((row) => this.#wireMessage(row))

    if (fork) {
      // The outbox does not mark compensations, so a row a failed pre-fork publish left queued
      // counts here too — this is an upper bound on the fork's blast radius.
      this.#metrics?.compensations.observe(rows.length)
      logger.info(`publishing ${rows.length} compensating operation(s) after a fork`)
    }

    const startedAt = Date.now()
    const publish = () => publisher.drain(wire)
    const result = await (span ? span.measure('publish', publish) : publish())
    const publishAckAt = Date.now()
    const elapsed = (publishAckAt - startedAt) / 1000

    if (result.confirmed.length) {
      const ack = () => state.confirm(result.confirmed)
      await (span ? span.measure('ack', ack) : ack())
    }

    this.#recordDrain({ elapsed, bytes: result.bytes, saturation: this.#saturation(wire, elapsed) })

    if (result.error) throw result.error

    return publishAckAt
  }

  /** Seconds spent publishing partitions at or above 80% of PubSub's per-ordering-key cap. */
  #saturation(wire: PublishMessage[], elapsed: number): number {
    if (!this.#options.messageOrdering || elapsed <= 0) return 0

    let saturated = 0
    for (const partition of partitionRows(wire)) {
      const bytes = partition.reduce((sum, row) => sum + row.payload.byteLength, 0)
      if (bytes / elapsed >= 0.8 * CAP_BYTES_PER_SECOND) {
        saturated += elapsed
      }
    }

    return saturated
  }

  #recordDrain(sample: DrainSample): void {
    // The recovery drain runs before any batch has brought `ctx.metrics`. Hold the sample so a
    // large restart backlog still reaches the dashboard once the first batch registers them,
    // instead of republishing the whole outbox with no metric output at all.
    if (!this.#metrics) {
      this.#deferredDrains.push(sample)

      return
    }

    this.#metrics.publishDuration.observe(sample.elapsed)
    this.#metrics.publishedBytes.inc(sample.bytes)
    if (sample.saturation > 0) {
      this.#metrics.saturation.inc(sample.saturation)
    }
  }

  #rollbackable(ctx: BatchContext): boolean {
    return !this.#options.finalizedStream && !this.#options.assumeNoForks && Boolean(ctx.stream.head.finalized)
  }

  /**
   * A dataset that never reports a finalized head is refused, not silently trusted: the absence
   * of a watermark is not evidence that forks cannot happen, and nothing published here can be
   * retracted (RP-44).
   */
  #assertFinality(ctx: BatchContext): void {
    if (this.#finalityChecked) return
    this.#finalityChecked = true

    if (this.#options.finalizedStream || this.#options.assumeNoForks) return
    if (ctx.stream.head.finalized) return

    throw new PubsubTargetError(PUBSUB_ERROR_CODES.NO_FINALITY_HEAD, [
      `Dataset "${ctx.stream.dataset.dataset}" reports no finalized head, so no rollback manifest can be ` +
        'kept — and a published message cannot be retracted.',
      'Read the finalized stream instead, or set `assumeNoForks: true` to assert that this dataset cannot ' +
        'fork (a fork reported anyway stays fatal).',
    ])
  }

  async #resolveGoLive(ctx: BatchContext): Promise<void> {
    if (this.#goLive !== undefined) return

    const configured = this.#options.publishFrom

    if (typeof configured === 'number') {
      this.#goLive = configured
    } else {
      const persisted = await this.#options.state.getMeta(META_GO_LIVE)
      if (persisted) {
        this.#goLive = Number(persisted)
      } else {
        const head = ctx.stream.head.latest?.number ?? ctx.stream.head.finalized?.number
        if (head === undefined) {
          throw new PubsubTargetError(PUBSUB_ERROR_CODES.UNRESOLVABLE_GO_LIVE, [
            'publishFrom: "latest" cannot be resolved — the dataset reports neither a chain head nor a ' +
              'finalized head.',
            'Pass an explicit `publishFrom` block.',
          ])
        }
        this.#goLive = head
      }
    }

    // Persisted so restarts keep the same go-live block — a re-resolved 'latest' would silently
    // skip everything published between the two starts.
    await this.#options.state.setMeta(META_GO_LIVE, String(this.#goLive))
    this.#options.logger.info(`go-live block: ${formatBlock(this.#goLive)}`)
  }

  #warnSkippedStreams(data: any, logger: Logger): void {
    if (this.#skippedLogged) return
    this.#skippedLogged = true

    if (!data || typeof data !== 'object' || Array.isArray(data)) return

    const configured = new Set(this.#options.routes.map((r) => r.stream))
    const skipped = Object.keys(data).filter((stream) => !configured.has(stream))
    if (!skipped.length) return

    logger.warn(`no PubSub route configured for stream(s): ${skipped.join(', ')} — they are not published`)
  }

  /**
   * Both histograms measure the same interval from two different anchors, so they are observed
   * together or not at all. `drain()` flushes whatever the outbox holds, which on a restart or a
   * fork is more than this batch — the readings are batch-level, not a per-row ack.
   *
   * Observed on every batch above go-live, including one whose routes mapped nothing. These
   * measure how current the destination is, and a route that legitimately publishes nothing for
   * a stretch is still current; gating on a non-empty drain would make a sparse feed's freshness
   * series go silent and read as a dead pipe. `publish_duration` is the publish-attempt metric.
   */
  #observeCommitLag(ctx: BatchContext, publishAckAtMs: number): void {
    if (!this.#metrics) return

    // Below go-live `#map` drops every draft and nothing is ever published, so a reading here
    // would describe a stage that never runs at all. This is not a general backfill guard: an
    // explicit historic `publishFrom` does publish, and its readings then carry the chain
    // distance the pipe is genuinely behind by.
    if (ctx.stream.state.current.number < (this.#goLive ?? 0)) return

    const publishAckAt = publishAckAtMs / 1000

    const receivedAt = ctx.batch.lastBlockReceivedAt
    if (receivedAt instanceof Date) {
      this.#metrics.portalToCommitLag.observe({ id: ctx.id }, publishAckAt - receivedAt.getTime() / 1000)
    }

    const blockTimestamp = blockTimestampSeconds(ctx.stream.state.current.timestamp)
    if (blockTimestamp === undefined) {
      this.#warnMissingBlockTimestamp(ctx)

      return
    }

    this.#metrics.blockToCommitLag.observe({ id: ctx.id }, publishAckAt - blockTimestamp)
  }

  #warnMissingBlockTimestamp(ctx: BatchContext): void {
    if (this.#lagWarned) return
    this.#lagWarned = true

    // Without this an operator cannot tell an unselected query field from a dead pipe: both
    // render as an empty series.
    this.#options.logger.warn(
      `no usable block timestamp at ${formatBlock(ctx.stream.state.current.number)} — ` +
        'sqd_pubsub_block_to_commit_lag_seconds stays empty for as long as that holds. Select the ' +
        'block `timestamp` field in the pipe’s query if that panel is expected to populate.',
    )
  }

  #map(data: any, ctx: BatchContext): PendingOperation[] {
    const operations: PendingOperation[] = []
    const finalizedNumber = ctx.stream.head.finalized?.number
    const goLive = this.#goLive ?? 0

    for (const resolved of this.#options.routes) {
      const streamData = data?.[resolved.stream]
      if (streamData === undefined) continue

      const drafts = resolved.route.map({ data: streamData, ctx })
      const indexInBlock = new Map<number, number>()
      const seenIds = new Set<string>()

      for (const draft of drafts) {
        this.#assertDraftBlock(draft, resolved)

        // Everything below the go-live block is invisible: no publish, no manifest, so a fork
        // that rewinds under it has nothing to compensate by construction.
        if (draft.block.number < goLive) continue

        const index = indexInBlock.get(draft.block.number) ?? 0
        indexInBlock.set(draft.block.number, index + 1)

        const operation = this.#toOperation({ draft, resolved, index, finalizedNumber, ctx })

        // Only on event routes: a materialized id may legitimately be revised several times in
        // one batch (a window preview followed by its close).
        const identity = JSON.stringify([operation.orderingKey, operation.id])
        if (resolved.mode === 'event') {
          if (seenIds.has(identity)) {
            throw new PubsubTargetError(PUBSUB_ERROR_CODES.DUPLICATE_DRAFT_ID, [
              `Route "${resolved.stream}" produced two operations with id "${operation.id}" in one batch.`,
              'On an `event` route every id is write-once — the second would silently overwrite the first ' +
                'for every consumer. Use `mode: "materialized"` if the row is meant to be revised.',
            ])
          }
          seenIds.add(identity)
        }

        operations.push(operation)
      }
    }

    this.#assertBlockOrder(operations)

    return operations
  }

  /**
   * Forward operations take their numbers in block order, which is what turns a contiguous run
   * into a block boundary: reaching the first operation of block N+1 proves the producer's
   * output for block N is whole. A mapper that steps backwards would silently retire that
   * reading for every consumer, so the batch is refused instead.
   *
   * The check walks the batch in sequencing order, which is route by route. A second route on
   * the same topic therefore restarts at the batch's first block and trips this — the barrier
   * admits one multi-block route per producer.
   */
  #assertBlockOrder(operations: PendingOperation[]): void {
    if (!this.#options.sequenceBarrier) return

    for (const operation of operations) {
      if (operation.blockNumber < this.#lastBlock) {
        throw new PubsubTargetError(PUBSUB_ERROR_CODES.BLOCK_ORDER, [
          `Route "${operation.route}" produced an operation for block ${formatBlock(operation.blockNumber)} ` +
            `after one for block ${formatBlock(this.#lastBlock)}.`,
          'Operations are sequenced in the order they are mapped, and consumers read a contiguous ' +
            'sequence run as a block boundary — a backwards step makes that reading wrong.',
          'Emit drafts in block order. Routes are mapped one after another, so a second route on ' +
            'this topic restarts at the batch’s first block and lands here too — merge such streams ' +
            'into one `map`.',
          'Set `sequenceBarrier: false` if no consumer relies on the block boundary.',
        ])
      }

      this.#lastBlock = operation.blockNumber
    }
  }

  #assertDraftBlock(draft: MessageDraft, resolved: ResolvedRoute): void {
    if (draft.block && Number.isInteger(draft.block.number) && draft.block.number >= 0) return

    throw new PubsubTargetError(PUBSUB_ERROR_CODES.INVALID_DRAFT_BLOCK, [
      `Route "${resolved.stream}" produced a draft without a usable \`block.number\`.`,
      'Every operation is attributed to its block — that attribution is what makes fork compensation possible.',
    ])
  }

  #toOperation({
    draft,
    resolved,
    index,
    finalizedNumber,
    ctx,
  }: {
    draft: MessageDraft
    resolved: ResolvedRoute
    index: number
    finalizedNumber?: number
    ctx: BatchContext
  }): PendingOperation {
    const { namespace, messageOrdering, uidAttribute } = this.#options
    const op: PubsubOp = draft.op ?? 'upsert'

    if (draft.orderingKey !== undefined && !messageOrdering) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ORDERING_KEY_NOT_SUPPORTED, [
        `Route "${resolved.stream}" set an ordering key while PubSub message ordering is disabled.`,
        'Set `publish.messageOrdering: true`, or drop the key.',
      ])
    }

    const orderingKey = messageOrdering ? (draft.orderingKey ?? resolved.topic) : ''

    const { id, idSource } = this.#resolveId({ draft, resolved, index, namespace })
    const attributes = draft.attributes ?? {}

    validateUserAttributes(attributes, {
      topic: resolved.topic,
      route: resolved.stream,
      // `_finalized` always, `_uid` on request, and the producer-wide set every message carries.
      protocolAttributes: 1 + (uidAttribute ? 1 : 0) + Object.keys(this.#options.constantAttributes).length,
    })

    // Every limit the service enforces is checked HERE, before the operation becomes durable.
    // A message PubSub will reject is not a lost message but a stuck one: it sits at the head
    // of its partition's outbox and fails identically on every restart.
    assertWireLimits(
      {
        orderingKey,
        uidNamespace: uidAttribute ? namespace : undefined,
      },
      { topic: resolved.topic, route: resolved.stream },
    )

    const payload = encodeRow(draft.data)

    const rollbackable = this.#rollbackable(ctx) && draft.block.number > (finalizedNumber ?? -1)
    const inverse =
      rollbackable && resolved.route.rollbackWhenMissing
        ? this.#encodeInverse({ draft: { ...draft, id }, resolved })
        : undefined

    const operation: PendingOperation = {
      route: resolved.stream,
      topic: resolved.topic,
      orderingKey,
      mode: resolved.mode,
      op,
      id,
      idSource,
      attributes,
      payload,
      blockNumber: draft.block.number,
      rollbackable,
      inverse,
    }

    this.#assertOperationSize(operation, resolved.route.encode, resolved.stream)
    if (inverse) {
      this.#assertOperationSize(
        { ...operation, op: inverse.op, payload: inverse.payload },
        resolved.route.encode,
        `${resolved.stream} (rollbackWhenMissing)`,
      )
    }

    return operation
  }

  #resolveId({
    draft,
    resolved,
    index,
    namespace,
  }: {
    draft: MessageDraft
    resolved: ResolvedRoute
    index: number
    namespace: string
  }): { id: string; idSource: RowIdSource } {
    const rowId = readRowId(draft.data)
    if (rowId !== undefined) {
      return { id: rowId, idSource: 'row' }
    }

    if (draft.id !== undefined && draft.id !== null) {
      return { id: this.#assertResolvedId(draft.id, 'draft', resolved), idSource: 'draft' }
    }

    const derived = resolved.route.deriveId?.(draft, { namespace, stream: resolved.stream, index })
    if (derived !== undefined && derived !== null) {
      return { id: this.#assertResolvedId(derived, 'derived', resolved), idSource: 'derived' }
    }

    return {
      id: this.#deriveId({ draft, resolved, index, namespace }),
      idSource: 'generated',
    }
  }

  #assertResolvedId(id: unknown, source: RowIdSource, resolved: ResolvedRoute): string {
    if (typeof id === 'string' && id.length > 0) {
      return id
    }

    throw new PubsubTargetError(PUBSUB_ERROR_CODES.INVALID_CDC_ROW, [
      `Route "${resolved.stream}" resolved ${source} id to ` +
        `${typeof id === 'string' ? 'an empty string' : typeof id}; a row id must be a non-empty string.`,
    ])
  }

  #deriveId({
    draft,
    resolved,
    index,
    namespace,
  }: {
    draft: MessageDraft
    resolved: ResolvedRoute
    index: number
    namespace: string
  }): string {
    if (!draft.block.hash && !this.#options.finalizedStream && !this.#options.assumeNoForks) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.MISSING_BLOCK_HASH, [
        `Route "${resolved.stream}" relies on generated ids, but block ${draft.block.number} carries no hash.`,
        'Bare block numbers repeat after a fork, so a hash-less id would alias an orphaned row with a ' +
          'canonical one. Supply an explicit `data._id` or `id` in `map`.',
      ])
    }

    return `${namespace}:${resolved.stream}:${draft.block.number}:${draft.block.hash ?? ''}:${index}`
  }

  #encodeInverse({
    draft,
    resolved,
  }: {
    draft: MessageDraft & { id: string }
    resolved: ResolvedRoute
  }): PendingOperation['inverse'] {
    const inverse = resolved.route.rollbackWhenMissing!(draft)

    if (inverse.op === 'delete') {
      return { op: 'delete', payload: encodeRow(draft.data) }
    }

    return { op: 'upsert', payload: encodeRow(inverse.data) }
  }

  #encoderFor(row: OutboxRow): CdcEncoder | undefined {
    const route = this.#routesByStream.get(row.route)
    if (!route) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ROUTE_NOT_CONFIGURED, [
        `The durable PubSub outbox contains an operation for route "${row.route}", but that route is not configured.`,
        'Restore the route configuration before recovering this state.',
      ])
    }

    return route.route.encode
  }

  #wireMessage(row: OutboxRow): PublishMessage {
    const payload = encodeCdcMessage(row, this.#encoderFor(row))
    const attributes = buildAttributes(row, this.#attributeOptions())
    const wire = { rowId: row.rowId, topic: row.topic, orderingKey: row.orderingKey, attributes, payload }

    assertPublishRequestSize({ ...wire, payloadBytes: payload.byteLength }, { route: row.route })

    return wire
  }

  #attributeOptions(finalized = this.#finalized): {
    namespace: string
    uidAttribute: boolean
    constant: Record<string, string>
    finalized?: number
  } {
    return {
      namespace: this.#options.namespace,
      uidAttribute: this.#options.uidAttribute,
      constant: this.#options.constantAttributes,
      finalized,
    }
  }

  #assertOperationSize(operation: PendingOperation, encode: CdcEncoder | undefined, route: string): void {
    const preview = { ...operation, rowId: 0, seq: MAX_SEQUENCE_VALUE }
    // Both `_finalized` and `_uid` grow with the numbers they carry, so the bound is taken at
    // their widest decimal form — the check has to hold for every later head too.
    const attributes = buildAttributes(preview, this.#attributeOptions(MAX_SEQUENCE_VALUE))
    // A custom encoder can change the document arbitrarily, so it must run before commit.
    // The canonical path can derive the exact size from the already encoded row instead.
    const payloadBytes = encode ? encodeCdcMessage(preview, encode).byteLength : canonicalCdcMessageBytes(preview)

    assertPublishRequestSize(
      { topic: preview.topic, orderingKey: preview.orderingKey, attributes, payloadBytes },
      { route },
    )
  }
}

async function bindWireConfig(
  state: PubsubState,
  config: {
    namespace: string
    uidAttribute: boolean
    messageOrdering: boolean
    attributes: Record<string, string>
    encoders: [stream: string, kind: EncoderKind][]
  },
): Promise<void> {
  const encoders = [...config.encoders].sort(([left], [right]) => left.localeCompare(right))
  const core = [config.namespace, config.uidAttribute, config.messageOrdering, stableAttributes(config.attributes)]
  const current = JSON.stringify([...core, encoders])
  const stored = await state.getMeta(META_WIRE_CONFIG)

  if (stored === undefined) {
    await state.setMeta(META_WIRE_CONFIG, current)
    return
  }

  if (stored === current) {
    return
  }

  const previous = JSON.parse(stored) as [string, boolean, boolean, string, [string, EncoderKind][]?]
  const sameCore = JSON.stringify(previous.slice(0, 4)) === JSON.stringify(core)

  if (sameCore) {
    const previousEncoders = new Map(previous[4] ?? [])
    // A route that was dropped outright has no entry here to compare against and falls through
    // to E2423 when recovery tries to encode its rows.
    const pendingRoutes = new Set((await state.pending()).map((row) => row.route))
    const changedPendingRoute = encoders.find(
      ([stream, kind]) =>
        pendingRoutes.has(stream) && (!previousEncoders.has(stream) || previousEncoders.get(stream) !== kind),
    )

    if (!changedPendingRoute) {
      await state.setMeta(META_WIRE_CONFIG, current)
      return
    }

    throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_WIRE_CONFIG_MISMATCH, [
      `The encoder configuration for route "${changedPendingRoute[0]}" cannot be proven unchanged while ` +
        'its outbox still contains unconfirmed operations.',
      'Restore the previous encoder until the outbox drains, or start a new feed with fresh state and namespace.',
    ])
  }

  throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_WIRE_CONFIG_MISMATCH, [
    'The PubSub state was written with a different namespace, `attributes`, `publish.uidAttribute`, ' +
      'or `publish.messageOrdering` setting.',
    'Those settings determine published metadata, so changing them while reusing an outbox ' +
      'could mutate an unconfirmed operation during recovery. Use the original settings, or start a new feed ' +
      'with fresh state and a fresh namespace.',
  ])
}

type PubsubMetrics = {
  ops: Counter<'id' | 'topic' | 'operation'>
  publishedBytes: Counter<string>
  publishDuration: Histogram<string>
  outboxDepth: Gauge<'id'>
  manifestRows: Gauge<'id'>
  compensations: Histogram<string>
  coldStart: Gauge<'id'>
  saturation: Counter<string>
  blockToCommitLag: Histogram<'id'>
  portalToCommitLag: Histogram<'id'>
}

function registerPubsubMetrics(metrics: Metrics): PubsubMetrics {
  return {
    ops: metrics.counter({
      name: 'sqd_pubsub_operations_total',
      help: 'Row changes enqueued for publishing, by `operation` — the CDC op (upsert/delete).',
      labelNames: ['id', 'topic', 'operation'] as const,
    }),
    publishedBytes: metrics.counter({
      name: 'sqd_pubsub_published_bytes_total',
      help: 'Payload bytes confirmed published (excludes attributes).',
    }),
    publishDuration: metrics.histogram({
      name: 'sqd_pubsub_publish_duration_seconds',
      help: 'Wallclock duration of one outbox drain.',
      buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
    }),
    outboxDepth: metrics.gauge({
      name: 'sqd_pubsub_outbox_depth',
      help: 'Operations enqueued but not yet confirmed published (sampled).',
      labelNames: ['id'] as const,
    }),
    manifestRows: metrics.gauge({
      name: 'sqd_pubsub_manifest_rows',
      help: 'Rollbackable manifest rows held for fork compensation (sampled).',
      labelNames: ['id'] as const,
    }),
    compensations: metrics.histogram({
      name: 'sqd_pubsub_compensations_per_fork',
      help: 'Compensating operations emitted per resolved fork.',
      buckets: [1, 5, 25, 100, 500, 2500],
    }),
    coldStart: metrics.gauge({
      name: 'sqd_pubsub_cold_start',
      help:
        'ALERT ON THIS: 1 when the run started with no state at the configured path. On an ' +
        'already-live namespace that means the change sequence was reset.',
      labelNames: ['id'] as const,
    }),
    saturation: metrics.counter({
      name: 'sqd_pubsub_publish_saturation_seconds',
      help:
        'Message ordering: seconds spent publishing a partition at ≥80% of PubSub’s 1 MB/s ' +
        'per-ordering-key cap. Growing values mean the headroom requirement is eroding.',
    }),
    blockToCommitLag: metrics.histogram({
      name: 'sqd_pubsub_block_to_commit_lag_seconds',
      help:
        'End-to-end freshness: seconds from the committed block’s chain timestamp to the PubSub ' +
        'ack. Includes block production and chain-to-portal propagation, so it is a service-level ' +
        'reading, not an attribution of where the time went. Batch-level, not a per-row ack. ' +
        'Empty while the pipe is below its go-live block or the query selects no block timestamp.',
      labelNames: ['id'] as const,
      buckets: CHAIN_LAG_BUCKETS,
    }),
    portalToCommitLag: metrics.histogram({
      name: 'sqd_pubsub_portal_to_commit_lag_seconds',
      help:
        'In-process latency: seconds from the portal client stamping the batch ' +
        '(batch.lastBlockReceivedAt) to the PubSub ack. Also covers stream-buffer dwell and the ' +
        'pipe’s transformers, so a regression here is not necessarily in this target — split it ' +
        'against sqd_pubsub_publish_duration_seconds and the `pubsub` profiler span.',
      labelNames: ['id'] as const,
      buckets: PIPELINE_LAG_BUCKETS,
    }),
  }
}
