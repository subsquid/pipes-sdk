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
  PubsubOp,
  assertPublishRequestSize,
  assertWireLimits,
  buildAttributes,
  canonicalCdcMessageBytes,
  canonicalJson,
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
  META_FORK_EPOCH,
  OutboxRow,
  PendingCdcOperation,
  PendingOperation,
  PendingSignalOperation,
  PubsubState,
  RouteMode,
  RowIdSource,
  SignalForkContext,
  SqlitePubsubState,
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

export type SignalDraft = {
  /**
   * The message body, published verbatim as canonical JSON. No CDC envelope is added, so the
   * route owns the whole schema — including any id, type tag, or epoch a consumer needs.
   */
  data: object
  /**
   * The block this signal describes. Drives the go-live cut and the `finalized-only` check; it
   * is never used to retract the message, because a signal has no compensating operation.
   */
  block: BlockCursor
  /** User attributes for subscription filtering. `_`-prefixed names are reserved, `goog…` by GCP. */
  attributes?: Record<string, string>
  /**
   * Available only when `publish.messageOrdering` is enabled. Defaults to the topic name, so a
   * route that does not set one publishes its whole topic on a single ordered partition.
   */
  orderingKey?: string
}

/** A boundary signal is emitted by the fork itself, so it has no block of its own to declare. */
export type ForkSignalDraft = Omit<SignalDraft, 'block'>

/**
 * A route that publishes application messages instead of BigQuery CDC rows.
 *
 * Signals are fire-and-forget: they carry no row identity, never enter the rollback manifest,
 * and cannot be retracted. That is the whole trade — a signal route buys an arbitrary payload
 * schema at the cost of the fork repair a `TopicRoute` gets for free. `fork` is how the route
 * pays for it, and there is no default: every route states which of the two strategies it uses.
 */
export type SignalRoute<Data> = {
  topic: string
  /**
   * Map one batch of this stream's data to signals. Must be pure and deterministic (no wall
   * clock, no randomness): a replay after a crash must reproduce identical bytes.
   *
   * `epoch` is the durable fork counter, incremented once per fork and readable by consumers to
   * discard messages from a branch that has since been orphaned. Include it in `data` if the
   * consumer needs to reason about forks at all.
   */
  map: (batch: { data: Data; ctx: BatchContext; epoch: number }) => SignalDraft[]
  /**
   * How this route survives a chain reorg.
   *
   * `boundary` — the route publishes unfinalized data and accepts that some of it is orphaned.
   * On a fork the target publishes one message built by `map` ahead of every CDC compensation,
   * carrying the new `epoch` and the block the feed rewound to. The consumer is expected to
   * discard everything it received above that block, from the previous epoch, on its own.
   *
   * `finalized-only` — the route never publishes anything a fork could orphan, so no boundary
   * message is needed. Enforced: a draft above the finalized head is a fatal `E2427` rather
   * than an unretractable message on the wire. Reading the finalized stream satisfies this
   * trivially.
   */
  fork: { mode: 'boundary'; map: (context: SignalForkContext) => ForkSignalDraft } | { mode: 'finalized-only' }
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
   * Routes that publish application messages without the CDC envelope. Same stream keys as
   * `topics`, and a stream may feed both. At least one route across the two is required.
   */
  signals?: { [K in keyof NoInfer<T>]?: SignalRoute<NoInfer<T>[K]> }
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
  kind: 'cdc'
  stream: string
  topic: string
  mode: RouteMode
  encoderKind: EncoderKind
  route: TopicRoute<any>
}

type ResolvedSignalRoute = {
  kind: 'signal'
  stream: string
  topic: string
  route: SignalRoute<any>
}

type AnyResolvedRoute = ResolvedRoute | ResolvedSignalRoute

type EncoderKind = 'canonical' | 'custom' | 'signal'

export function pubsubTarget<T>(options: PubsubTargetOptions<T>) {
  const messageOrdering = options.publish?.messageOrdering ?? false
  const topicSetup: TopicSetup = options.topicSetup ?? 'validate'

  const routes: ResolvedRoute[] = Object.entries(options.topics ?? {})
    .filter((entry): entry is [string, TopicRoute<any>] => Boolean(entry[1]))
    .map(([stream, route]) => ({
      kind: 'cdc' as const,
      stream,
      topic: route.topic,
      mode: route.mode ?? 'event',
      encoderKind: route.encode ? 'custom' : 'canonical',
      route,
    }))
  const signals: ResolvedSignalRoute[] = Object.entries(options.signals ?? {})
    .filter((entry): entry is [string, SignalRoute<any>] => Boolean(entry[1]))
    .map(([stream, route]) => ({ kind: 'signal' as const, stream, topic: route.topic, route }))
  const allRoutes: AnyResolvedRoute[] = [...routes, ...signals]

  // Refused here rather than at first write: with no routes there is nothing the state file can
  // be opened for, and taking its exclusive lock only to fail would strand a concurrent retry.
  if (!allRoutes.length) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.NO_ROUTES, 'Configure at least one CDC topic or signal route.')
  }

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

      const { coldStart } = await state.open({ cursorKey: options.settings?.id ?? id ?? '', logger })

      // Everything past `open` runs under cleanup: the state holds an exclusive lock on its
      // file, so a failure in topic validation or the recovery drain would otherwise leave it
      // locked for the life of the process and make the retry fail as a second producer.
      try {
        await bindWireConfig(state, {
          namespace,
          uidAttribute,
          messageOrdering,
          encoders: [
            ...routes.map(({ stream, encoderKind }) => [`cdc:${stream}`, encoderKind] as [string, EncoderKind]),
            ...signals.map(({ stream }) => [`signal:${stream}`, 'signal'] as [string, EncoderKind]),
          ],
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

        await publisher.setup([...new Set(allRoutes.map((r) => r.topic))], logger)

        logger.info({
          message:
            `publishing ${allRoutes.length} route(s) to PubSub — message ordering ${messageOrdering ? 'enabled' : 'disabled'}, ` +
            `namespace "${namespace}", ` +
            `state "${'path' in options.state ? options.state.path : 'custom'}"`,
          topics: allRoutes.map((r) => `${r.kind}:${r.stream} → ${r.topic}${r.kind === 'cdc' ? ` (${r.mode})` : ''}`),
        })

        if (coldStart) {
          // A cold start on an already-live namespace means a lost sequencer: the producer can
          // hand out change sequence numbers consumers already hold.
          logger.warn(
            `PubSub state at "${'path' in options.state ? options.state.path : 'custom'}" started EMPTY under ` +
              `namespace "${namespace}". On a first run this is expected. On an existing feed it means the ` +
              `sequencer was reset, so destinations can ignore newer changes as stale. Recover with a fresh ` +
              `namespace and a re-bootstrap, not with a restart.`,
          )
        }

        if (options.assumeNoForks && !finalizedStream) {
          logger.warn(
            'assumeNoForks is set: nothing is recorded for compensation. If this dataset ever forks, the ' +
              'published operations cannot be retracted and the pipe fails.',
          )
        }

        context = new WriteContext({
          state,
          publisher,
          routes: allRoutes,
          namespace,
          messageOrdering,
          logger,
          uidAttribute,
          assumeNoForks: options.assumeNoForks ?? false,
          finalizedStream: finalizedStream ?? false,
          publishFrom: options.publishFrom ?? 'latest',
          coldStart,
        })

        const cursor = await state.getCursor()

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

      const safe = await state.fork(canonicalBlocks, (fork) => context?.forkSignals(fork) ?? [])

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
  routes: AnyResolvedRoute[]
  namespace: string
  messageOrdering: boolean
  logger: Logger
  uidAttribute: boolean
  assumeNoForks: boolean
  finalizedStream: boolean
  publishFrom: number | 'latest'
  coldStart: boolean
}

class WriteContext {
  readonly #options: WriteContextOptions
  readonly #routesByIdentity: Map<string, AnyResolvedRoute>
  readonly #cdcRoutes: ResolvedRoute[]
  readonly #signalRoutes: ResolvedSignalRoute[]
  #metrics?: PubsubMetrics
  #goLive?: number
  #batches = 0
  #finalityChecked = false
  #skippedLogged = false
  #lagWarned = false
  #deferredDrains: DrainSample[] = []

  constructor(options: WriteContextOptions) {
    this.#options = options
    this.#routesByIdentity = new Map(options.routes.map((route) => [`${route.kind}:${route.stream}`, route]))
    this.#cdcRoutes = options.routes.filter((route): route is ResolvedRoute => route.kind === 'cdc')
    this.#signalRoutes = options.routes.filter((route): route is ResolvedSignalRoute => route.kind === 'signal')
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
        this.#metrics.ops.inc(
          {
            id: ctx.id,
            topic: operation.topic,
            kind: operation.kind,
            operation: operation.kind === 'cdc' ? operation.op : operation.signalType,
          },
          1,
        )
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
    const { state, publisher, namespace, uidAttribute, logger } = this.#options

    const rows = await state.pending()
    if (!rows.length) return Date.now()

    const wire = rows.map((row) => this.#wireMessage(row, { namespace, uidAttribute }))

    if (fork) {
      // A signal never compensates anything, so it is not fork blast radius and must not land in
      // this histogram. The CDC remainder can still include rows a failed pre-fork publish left
      // queued — the outbox does not mark compensations, so this stays an upper bound.
      const compensations = rows.filter((row) => row.kind === 'cdc').length
      const signals = rows.length - compensations

      this.#metrics?.compensations.observe(compensations)
      logger.info(
        `publishing ${compensations} compensating operation(s) after a fork` +
          (signals ? ` and ${signals} boundary signal(s)` : ''),
      )
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

  async #map(data: any, ctx: BatchContext): Promise<PendingOperation[]> {
    const operations: PendingOperation[] = []
    const finalizedNumber = ctx.stream.head.finalized?.number
    const goLive = this.#goLive ?? 0

    for (const resolved of this.#cdcRoutes) {
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

    // Read once per batch, and only when a route can use it: the fork counter lives in the same
    // state file every commit already touches.
    const epoch = this.#signalRoutes.length ? await this.#forkEpoch() : 0

    for (const resolved of this.#signalRoutes) {
      const streamData = data?.[resolved.stream]
      if (streamData === undefined) continue

      for (const draft of resolved.route.map({ data: streamData, ctx, epoch })) {
        this.#assertSignalDraftBlock(draft, resolved)

        if (draft.block.number < goLive) continue

        this.#assertSignalFinality(draft, resolved, finalizedNumber)

        operations.push(this.#toSignalOperation({ draft, resolved, signalType: 'data' }))
      }
    }

    return operations
  }

  async #forkEpoch(): Promise<number> {
    return Number((await this.#options.state.getMeta(META_FORK_EPOCH)) ?? '0')
  }

  /**
   * One boundary message per `boundary` route, built inside the fork transaction so it shares the
   * epoch the fork is about to persist. A `finalized-only` route publishes nothing a fork can
   * orphan, so it needs no boundary.
   */
  forkSignals(context: SignalForkContext): PendingSignalOperation[] {
    return this.#signalRoutes.flatMap((resolved) => {
      if (resolved.route.fork.mode !== 'boundary') return []

      const draft = resolved.route.fork.map(context)

      // A dead-end fork rewinds below every recorded block, so there is no cursor to attribute
      // the boundary to. Block 0 is the honest floor — `deadEnd` in the payload carries the rest.
      const block = context.rollbackTo ?? { number: 0 }

      return [this.#toSignalOperation({ draft: { ...draft, block }, resolved, signalType: 'fork' })]
    })
  }

  #assertSignalDraftBlock(draft: SignalDraft, resolved: ResolvedSignalRoute): void {
    if (draft.block && Number.isInteger(draft.block.number) && draft.block.number >= 0) return

    throw new PubsubTargetError(
      PUBSUB_ERROR_CODES.INVALID_SIGNAL_BLOCK,
      `Signal route "${resolved.stream}" produced a draft without a usable block.number.`,
    )
  }

  /**
   * A `finalized-only` route trades the boundary message for a promise that it never publishes
   * anything a fork could orphan. Broken here, the message would already be unretractable on the
   * wire, so it is refused before the batch commits.
   */
  #assertSignalFinality(draft: SignalDraft, resolved: ResolvedSignalRoute, finalizedNumber?: number): void {
    if (resolved.route.fork.mode !== 'finalized-only') return
    if (this.#options.finalizedStream) return
    if (draft.block.number <= (finalizedNumber ?? -1)) return

    throw new PubsubTargetError(PUBSUB_ERROR_CODES.SIGNAL_NOT_FINALIZED, [
      `Signal route "${resolved.stream}" mapped block ${formatBlock(draft.block.number)}, above the ` +
        `finalized head ${finalizedNumber === undefined ? '(none reported)' : formatBlock(finalizedNumber)}, ` +
        'while declaring `fork.mode: "finalized-only"`.',
      'Read the finalized stream, hold the signal until its block finalizes, or switch the route to ' +
        '`fork.mode: "boundary"` and publish a compensating boundary message on a fork.',
    ])
  }

  #toSignalOperation({
    draft,
    resolved,
    signalType,
  }: {
    draft: SignalDraft
    resolved: ResolvedSignalRoute
    signalType: 'data' | 'fork'
  }): PendingSignalOperation {
    const { messageOrdering, namespace, uidAttribute } = this.#options
    const where = { topic: resolved.topic, route: resolved.stream }

    if (draft.orderingKey !== undefined && !messageOrdering) {
      throw new PubsubTargetError(
        PUBSUB_ERROR_CODES.ORDERING_KEY_NOT_SUPPORTED,
        `Signal route "${resolved.stream}" set an ordering key while message ordering is disabled.`,
      )
    }

    const orderingKey = messageOrdering ? (draft.orderingKey ?? resolved.topic) : ''
    const attributes = draft.attributes ?? {}

    validateUserAttributes(attributes, { ...where, protocolAttributes: uidAttribute ? 1 : 0 })
    assertWireLimits({ orderingKey, uidNamespace: uidAttribute ? namespace : undefined }, where)

    const operation: PendingSignalOperation = {
      kind: 'signal',
      route: resolved.stream,
      topic: resolved.topic,
      orderingKey,
      attributes,
      payload: new TextEncoder().encode(canonicalJson(draft.data)),
      blockNumber: draft.block.number,
      signalType,
    }

    // Sized at the worst case the row can reach once the state assigns it a sequence: PubSub
    // rejects an oversized request at publish time, by which point the operation is durable.
    const wire = this.#wireSignalMessage(
      { ...operation, rowId: 0, seq: MAX_SEQUENCE_VALUE },
      { namespace, uidAttribute },
    )
    assertPublishRequestSize({ ...wire, payloadBytes: wire.payload.byteLength }, { route: resolved.stream })

    return operation
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
  }): PendingCdcOperation {
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
      protocolAttributes: uidAttribute ? 1 : 0,
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
      kind: 'cdc',
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
  }): PendingCdcOperation['inverse'] {
    const inverse = resolved.route.rollbackWhenMissing!(draft)

    if (inverse.op === 'delete') {
      return { op: 'delete', payload: encodeRow(draft.data) }
    }

    return { op: 'upsert', payload: encodeRow(inverse.data) }
  }

  #wireMessage(row: OutboxRow, options: { namespace: string; uidAttribute: boolean }) {
    const route = this.#routesByIdentity.get(`${row.kind}:${row.route}`)
    if (!route) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ROUTE_NOT_CONFIGURED, [
        `The durable PubSub outbox contains an operation for route "${row.route}", but that route is not configured.`,
        'Restore the route configuration before recovering this state.',
      ])
    }

    const wire =
      row.kind === 'signal'
        ? this.#wireSignalMessage(row, options)
        : this.#wireMessageWithEncoder(
            row as OutboxRow & { op: PubsubOp; id: string },
            (route as ResolvedRoute).route.encode,
            options,
          )
    assertPublishRequestSize({ ...wire, payloadBytes: wire.payload.byteLength }, { route: route.stream })

    return wire
  }

  #assertOperationSize(operation: PendingCdcOperation, encode: CdcEncoder | undefined, route: string): void {
    const preview = { ...operation, rowId: 0, seq: MAX_SEQUENCE_VALUE }
    const options = {
      namespace: this.#options.namespace,
      uidAttribute: this.#options.uidAttribute,
    }
    const attributes = buildAttributes(preview, options)
    // A custom encoder can change the document arbitrarily, so it must run before commit.
    // The canonical path can derive the exact size from the already encoded row instead.
    const payloadBytes = encode ? encodeCdcMessage(preview, encode).byteLength : canonicalCdcMessageBytes(preview)

    assertPublishRequestSize(
      { topic: preview.topic, orderingKey: preview.orderingKey, attributes, payloadBytes },
      { route },
    )
  }

  #wireMessageWithEncoder(
    row: OutboxRow & { op: PubsubOp; id: string },
    encode: CdcEncoder | undefined,
    options: { namespace: string; uidAttribute: boolean },
  ) {
    const payload = encodeCdcMessage(row, encode)
    const attributes = buildAttributes(row, options)

    return { rowId: row.rowId, topic: row.topic, orderingKey: row.orderingKey, attributes, payload }
  }

  /**
   * A signal's payload is final before it reaches the outbox, so this only attaches attributes.
   * `buildAttributes` derives `_uid` from topic/orderingKey/seq alone; the `op` and `id` it takes
   * are inert here and exist only to satisfy the shared CDC shape.
   */
  #wireSignalMessage(
    row: Pick<OutboxRow, 'rowId' | 'topic' | 'orderingKey' | 'attributes' | 'payload' | 'seq'>,
    options: { namespace: string; uidAttribute: boolean },
  ) {
    const attributes = buildAttributes({ ...row, op: 'upsert', id: '' }, options)

    return { rowId: row.rowId, topic: row.topic, orderingKey: row.orderingKey, attributes, payload: row.payload }
  }
}

/**
 * Read a stored encoder key as a route identity. Schema 2 keyed these by bare stream name, before
 * signal routes made a stream ambiguous; anything without a kind prefix is one of those and was a
 * CDC route by construction. Decided from the stored key's own shape, so the reading does not
 * shift with whatever the current configuration happens to contain.
 */
function routeIdentity(stream: string): string {
  return stream.startsWith('cdc:') || stream.startsWith('signal:') ? stream : `cdc:${stream}`
}

async function bindWireConfig(
  state: PubsubState,
  config: {
    namespace: string
    uidAttribute: boolean
    messageOrdering: boolean
    encoders: [stream: string, kind: EncoderKind][]
  },
): Promise<void> {
  const encoders = [...config.encoders].sort(([left], [right]) => left.localeCompare(right))
  const current = JSON.stringify([config.namespace, config.uidAttribute, config.messageOrdering, encoders])
  const stored = await state.getMeta(META_WIRE_CONFIG)

  if (stored === undefined) {
    await state.setMeta(META_WIRE_CONFIG, current)
    return
  }

  if (stored === current) {
    return
  }

  const previous = JSON.parse(stored) as [string, boolean, boolean, [string, EncoderKind][]?]
  const sameCore =
    previous[0] === config.namespace && previous[1] === config.uidAttribute && previous[2] === config.messageOrdering

  if (sameCore) {
    const previousEncoders = new Map((previous[3] ?? []).map(([stream, kind]) => [routeIdentity(stream), kind]))
    // A route whose kind flipped, or that was dropped outright, has no entry here to compare
    // against and falls through to E2423 when recovery tries to encode its rows.
    const pendingRoutes = new Set((await state.pending()).map((row) => `${row.kind}:${row.route}`))
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
    'The PubSub state was written with a different namespace, `publish.uidAttribute`, or ' +
      '`publish.messageOrdering` setting.',
    'Those settings determine published metadata, so changing them while reusing an outbox ' +
      'could mutate an unconfirmed operation during recovery. Use the original settings, or start a new feed ' +
      'with fresh state and a fresh namespace.',
  ])
}

type PubsubMetrics = {
  ops: Counter<'id' | 'topic' | 'kind' | 'operation'>
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
      help:
        'Wire operations enqueued for publishing. `kind` separates CDC rows from signals; ' +
        '`operation` is the CDC op (upsert/delete) or the signal type (data/fork).',
      labelNames: ['id', 'topic', 'kind', 'operation'] as const,
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
