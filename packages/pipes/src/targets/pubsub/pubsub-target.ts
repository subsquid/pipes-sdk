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
  createTarget,
  formatBlock,
} from '~/core/index.js'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import {
  PubsubOp,
  assertPublishRequestSize,
  assertWireLimits,
  buildEnvelope,
  encodePayload,
  validateUserAttributes,
} from './protocol.js'
import {
  GooglePubsubPublisher,
  Publisher,
  PublisherOptions,
  TopicSetup,
  partitionRows,
  resolvePubsubClient,
} from './publisher.js'
import { DeliveryProfile, PendingOperation, PubsubState, RouteMode, SqlitePubsubState } from './pubsub-state.js'

export type MessageDraft = {
  /**
   * `Uint8Array | string` are sent as-is; plain objects are encoded with the canonical codec
   * (RP-24) — decoded ABI values contain `bigint`, on which plain `JSON.stringify` throws.
   */
  data: Uint8Array | string | object
  /** The block this operation belongs to — drives fork compensation. Required. */
  block: { number: number; hash?: string; timestamp?: number }
  /**
   * `upsert` (default) writes the row; `delete` removes it. Emit `delete` only where the row
   * genuinely disappears (an emptied window) — fork compensation produces its own.
   */
  op?: 'upsert' | 'delete'
  /**
   * Stable row identity, published as the `_id` attribute; a `delete` or a materialized-row
   * restore reuses it verbatim.
   * Default: `${namespace}:${stream}:${block.number}:${block.hash}:<seq-in-block>`.
   */
  id?: string
  /**
   * User attributes for subscription filtering; copied onto the compensating operation. For a
   * materialized id these must be stable across every revision. Names starting with `_` are
   * reserved for the envelope, `goog…` by GCP.
   */
  attributes?: Record<string, string>
  /**
   * Ordered profile only (rejected at init under `lww`). Overrides the topic's constant
   * ordering key. A materialized id must never move between keys.
   */
  orderingKey?: string
}

export type RollbackInverse = { op: 'delete' } | { op: 'upsert'; data: Uint8Array | string | object }

export type TopicRoute<Data> = {
  topic: string
  /**
   * `event` (default): every id is write-once on a chain branch; a fork orphans it outright.
   * `materialized`: ids may be updated; a fork restores the surviving revision.
   */
  mode?: RouteMode
  /**
   * Map one batch of this stream's data to operations. Must be pure and deterministic (no wall
   * clock, no randomness): replays must reproduce identical bytes so duplicates stay
   * recognizable.
   */
  map: (batch: { data: Data; ctx: BatchContext }) => MessageDraft[]
  /** Payload encoder for object drafts. Defaults to the canonical codec (RP-24). Must be pure. */
  encode?: (data: object) => Uint8Array | string
  /**
   * Identity for drafts that leave `id` unset, when the block-derived default does not fit.
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
  topics: { [K in keyof NoInfer<T>]?: TopicRoute<NoInfer<T>[K]> }
  /** Cursor key inside the state file. Defaults to the pipe id (the CursorKey rule, ADR-2). */
  settings?: { id?: string }
  publish?: {
    /**
     * `lww` (default) — no ordering keys, no throughput cap; consumers apply per-id by `_seq`
     * version. `ordered` — one ordering key per topic, dense seq, strict cursor contract with
     * gap detection; caps each partition at 1 MB/s. See IB-28.
     */
    delivery?: DeliveryProfile
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
   * Publish one `heartbeat` operation per topic every N blocks. A liveness/freshness signal for
   * monitoring — NOT a completeness proof: delivery is unordered and filtered subscriptions
   * typically never receive heartbeats. Off by default.
   */
  heartbeat?: { everyBlocks: number }
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

type ResolvedRoute = {
  stream: string
  topic: string
  mode: RouteMode
  route: TopicRoute<any>
}

export function pubsubTarget<T>(options: PubsubTargetOptions<T>) {
  const delivery: DeliveryProfile = options.publish?.delivery ?? 'lww'
  const topicSetup: TopicSetup = options.topicSetup ?? 'validate'

  const routes: ResolvedRoute[] = Object.entries(options.topics ?? {})
    .filter((entry): entry is [string, TopicRoute<any>] => Boolean(entry[1]))
    .map(([stream, route]) => ({
      stream,
      topic: route.topic,
      mode: route.mode ?? 'event',
      route,
    }))

  const state: PubsubState =
    'path' in options.state
      ? new SqlitePubsubState({ path: options.state.path, delivery, id: options.settings?.id })
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
        await bindWireConfig(state, { namespace, uidAttribute })
        await run()
      } finally {
        await state.close()
        await publisher?.close()
      }

      async function run() {
        if (!publisher) {
          const client = await resolvePubsubClient(options.pubsub)
          const publisherOptions: PublisherOptions = {
            delivery,
            topicSetup,
            batching: options.publish?.batching,
            flowControl: options.publish?.flowControl,
          }
          publisher = new GooglePubsubPublisher(client, publisherOptions)
        }

        await publisher.setup([...new Set(routes.map((r) => r.topic))], logger)

        logger.info({
          message:
            `publishing ${routes.length} route(s) to Pub/Sub — profile "${delivery}", namespace "${namespace}", ` +
            `state "${'path' in options.state ? options.state.path : 'custom'}"`,
          topics: routes.map((r) => `${r.stream} → ${r.topic} (${r.mode})`),
        })

        if (coldStart) {
          // A cold start on an already-live namespace means a lost sequencer: the producer will
          // hand out `_seq`s consumers already hold, and nothing on the wire says so (GAP-38).
          logger.warn(
            `Pub/Sub state at "${'path' in options.state ? options.state.path : 'custom'}" started EMPTY under ` +
              `namespace "${namespace}". On a first run this is expected. On an existing feed it means the ` +
              `sequencer was lost — no consumer can detect that for itself. Recover with a fresh namespace and ` +
              `a re-bootstrap, not with a restart.`,
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
          routes,
          namespace,
          delivery,
          logger,
          uidAttribute,
          heartbeat: options.heartbeat,
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

      const safe = await state.fork(canonicalBlocks)

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
  delivery: DeliveryProfile
  logger: Logger
  uidAttribute: boolean
  heartbeat?: { everyBlocks: number }
  assumeNoForks: boolean
  finalizedStream: boolean
  publishFrom: number | 'latest'
  coldStart: boolean
}

class WriteContext {
  readonly #options: WriteContextOptions
  #metrics?: PubsubMetrics
  #goLive?: number
  #batches = 0
  #finalityChecked = false
  #skippedLogged = false

  constructor(options: WriteContextOptions) {
    this.#options = options
  }

  async write({ data, ctx }: { data: any; ctx: BatchContext }): Promise<void> {
    const { state, logger } = this.#options
    const span = ctx.profiler.start({ name: 'pubsub', labels: 'queue' })

    try {
      if (!this.#metrics) {
        this.#metrics = registerPubsubMetrics(ctx.metrics)
        this.#metrics.coldStart.set({ id: ctx.id }, this.#options.coldStart ? 1 : 0)
      }

      this.#assertFinality(ctx)
      await this.#resolveGoLive(ctx)
      this.#warnSkippedStreams(data, logger)

      const { operations, meta } = await span.measure('map', async () => this.#map(data, ctx))
      const forkCapable = this.#rollbackable(ctx)

      await span.measure('state tx', () =>
        state.commit({
          operations,
          forkCapable,
          meta,
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
        this.#metrics.ops.inc({ id: ctx.id, topic: operation.topic, op: operation.op }, 1)
      }

      await this.drain({ span })

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
   */
  async drain({ span, fork }: { span?: Profiler; fork?: boolean } = {}): Promise<void> {
    const { state, publisher, namespace, uidAttribute, logger } = this.#options

    const rows = await state.pending()
    if (!rows.length) return

    const wire = rows.map((row) => ({
      ...row,
      attributes: buildEnvelope(row, { namespace, uidAttribute }),
    }))

    if (fork) {
      this.#metrics?.compensations.observe(rows.length)
      logger.info(`publishing ${rows.length} compensating operation(s) after a fork`)
    }

    const startedAt = Date.now()
    const publish = () => publisher.drain(wire)
    const result = await (span ? span.measure('publish', publish) : publish())
    const elapsed = (Date.now() - startedAt) / 1000

    if (result.confirmed.length) {
      const ack = () => state.confirm(result.confirmed)
      await (span ? span.measure('ack', ack) : ack())
    }

    if (this.#metrics) {
      this.#metrics.publishDuration.observe(elapsed)
      this.#metrics.publishedBytes.inc(result.bytes)

      // Per-partition throughput at or near Pub/Sub's per-key cap: the ordered profile's
      // headroom requirement is a deployment invariant (IB-28), so make its erosion visible.
      if (this.#options.delivery === 'ordered' && elapsed > 0) {
        for (const partition of partitionRows(wire)) {
          const bytes = partition.reduce((sum, row) => sum + row.payload.byteLength, 0)
          if (bytes / elapsed >= 0.8 * CAP_BYTES_PER_SECOND) {
            this.#metrics.saturation.inc(elapsed)
          }
        }
      }
    }

    if (result.error) throw result.error
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

    logger.warn(`no Pub/Sub route configured for stream(s): ${skipped.join(', ')} — they are not published`)
  }

  async #map(data: any, ctx: BatchContext): Promise<{ operations: PendingOperation[]; meta: Record<string, string> }> {
    const operations: PendingOperation[] = []
    // Per topic, not per batch: a heartbeat reports what its own topic published since the
    // last one, so a busy topic cannot inflate a quiet one's count.
    const opsByTopic = new Map<string, number>()
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
        // one batch (a window preview followed by its close), and `_seq` orders those.
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
        opsByTopic.set(operation.topic, (opsByTopic.get(operation.topic) ?? 0) + 1)
      }
    }

    const { heartbeats, meta } = await this.#heartbeats(ctx, opsByTopic)

    return { operations: [...operations, ...heartbeats], meta }
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
    const { namespace, delivery, uidAttribute } = this.#options
    const op: PubsubOp = draft.op ?? 'upsert'

    if (draft.orderingKey !== undefined && delivery === 'lww') {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ORDERING_KEY_NOT_SUPPORTED, [
        `Route "${resolved.stream}" set an ordering key, but the pipe runs the "lww" profile, which uses ` +
          'no Pub/Sub ordering keys at all.',
        'Switch to `publish.delivery: "ordered"` to partition a topic, or drop the key.',
      ])
    }

    // One constant key per topic under `ordered` — the whole topic is a single ordered
    // partition unless the route shards it explicitly.
    const orderingKey = delivery === 'ordered' ? (draft.orderingKey ?? resolved.topic) : ''

    const id =
      draft.id ??
      resolved.route.deriveId?.(draft, { namespace, stream: resolved.stream, index }) ??
      this.#deriveId({ draft, resolved, index, namespace })
    const attributes = draft.attributes ?? {}

    validateUserAttributes(attributes, {
      topic: resolved.topic,
      route: resolved.stream,
      envelopeSize: 4 + (uidAttribute ? 1 : 0),
    })

    // Every limit the service enforces is checked HERE, before the operation becomes durable.
    // A message Pub/Sub will reject is not a lost message but a stuck one: it sits at the head
    // of its partition's outbox and fails identically on every restart.
    assertWireLimits(
      {
        id,
        orderingKey,
        uidNamespace: uidAttribute ? namespace : undefined,
      },
      { topic: resolved.topic, route: resolved.stream },
    )

    const payload = op === 'delete' ? new Uint8Array() : encodePayload(draft.data, resolved.route.encode)
    assertPublishRequestSize(
      { topic: resolved.topic, orderingKey, op, id, attributes, payload },
      { namespace, uidAttribute },
      { route: resolved.stream },
    )

    const rollbackable = this.#rollbackable(ctx) && draft.block.number > (finalizedNumber ?? -1)
    const inverse =
      rollbackable && resolved.route.rollbackWhenMissing
        ? this.#encodeInverse({ draft: { ...draft, id }, resolved })
        : undefined

    if (inverse) {
      // The inverse is published verbatim at fork time, when there is no draft left to
      // re-encode and nowhere to report a rejection to — so it is bounded now.
      assertPublishRequestSize(
        { topic: resolved.topic, orderingKey, op: inverse.op, id, attributes, payload: inverse.payload },
        { namespace, uidAttribute },
        { route: `${resolved.stream} (rollbackWhenMissing)` },
      )
    }

    return {
      topic: resolved.topic,
      orderingKey,
      mode: resolved.mode,
      op,
      id,
      attributes,
      payload,
      blockNumber: draft.block.number,
      rollbackable,
      inverse,
    }
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
          'canonical one. Supply an explicit `id` in `map`.',
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
      return { op: 'delete', payload: new Uint8Array() }
    }

    return { op: 'upsert', payload: encodePayload(inverse.data, resolved.route.encode) }
  }

  /**
   * Block-count cadence, bucketed so the stamped block is the bucket boundary rather than
   * whichever block a batch happened to end on.
   */
  async #heartbeats(
    ctx: BatchContext,
    opsByTopic: Map<string, number>,
  ): Promise<{ heartbeats: PendingOperation[]; meta: Record<string, string> }> {
    const cadence = this.#options.heartbeat?.everyBlocks
    const heartbeats: PendingOperation[] = []
    // Returned rather than written here: the cadence must advance in the same transaction that
    // enqueues the heartbeat, or a failed commit consumes a beat that was never published.
    const meta: Record<string, string> = {}

    if (!cadence || cadence <= 0) return { heartbeats, meta }

    const { state, namespace, delivery, uidAttribute } = this.#options
    const current = ctx.stream.state.current.number
    if (current < (this.#goLive ?? 0)) return { heartbeats, meta }

    const bucket = Math.floor(current / cadence)
    const topics = [...new Set(this.#options.routes.map((r) => r.topic))]

    for (const topic of topics) {
      const stored = await state.getMeta(`hb_bucket:${topic}`)
      const pending = Number((await state.getMeta(`hb_ops:${topic}`)) ?? '0') + (opsByTopic.get(topic) ?? 0)

      // The first batch only anchors the cadence: emitting there would stamp whatever bucket
      // the pipe happened to start in, which says nothing about liveness.
      if (stored === undefined || bucket <= Number(stored)) {
        meta[`hb_bucket:${topic}`] = stored ?? String(bucket)
        meta[`hb_ops:${topic}`] = String(pending)
        continue
      }

      const heartbeat: PendingOperation = {
        topic,
        orderingKey: delivery === 'ordered' ? topic : '',
        mode: 'event',
        op: 'heartbeat',
        attributes: {},
        payload: encodePayload({ namespace, block: bucket * cadence, opsSinceLast: pending }),
        blockNumber: current,
        // A heartbeat is not a row: it has nothing to compensate and never enters the manifest.
        rollbackable: false,
      }
      assertPublishRequestSize(heartbeat, { namespace, uidAttribute }, { route: `${topic} (heartbeat)` })
      heartbeats.push(heartbeat)

      meta[`hb_bucket:${topic}`] = String(bucket)
      meta[`hb_ops:${topic}`] = '0'
    }

    return { heartbeats, meta }
  }
}

async function bindWireConfig(state: PubsubState, config: { namespace: string; uidAttribute: boolean }): Promise<void> {
  const current = JSON.stringify([config.namespace, config.uidAttribute])
  const stored = await state.getMeta(META_WIRE_CONFIG)

  if (stored === undefined) {
    await state.setMeta(META_WIRE_CONFIG, current)
    return
  }

  if (stored === current) return

  throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_WIRE_CONFIG_MISMATCH, [
    'The Pub/Sub state was written with a different namespace or `publish.uidAttribute` setting.',
    'Those settings determine the published identity envelope, so changing them while reusing an outbox ' +
      'could mutate an unconfirmed operation during recovery. Use the original settings, or start a new feed ' +
      'with fresh state and a fresh namespace.',
  ])
}

type PubsubMetrics = {
  ops: Counter<'id' | 'topic' | 'op'>
  publishedBytes: Counter<string>
  publishDuration: Histogram<string>
  outboxDepth: Gauge<'id'>
  manifestRows: Gauge<'id'>
  compensations: Histogram<string>
  coldStart: Gauge<'id'>
  saturation: Counter<string>
}

function registerPubsubMetrics(metrics: Metrics): PubsubMetrics {
  return {
    ops: metrics.counter({
      name: 'sqd_pubsub_ops_total',
      help: 'Wire operations enqueued for publishing, by topic and operation.',
      labelNames: ['id', 'topic', 'op'] as const,
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
        'already-live namespace that means a lost sequencer, which no consumer can detect for itself.',
      labelNames: ['id'] as const,
    }),
    saturation: metrics.counter({
      name: 'sqd_pubsub_publish_saturation_seconds',
      help:
        'Ordered profile: seconds spent publishing a partition at ≥80% of Pub/Sub’s 1 MB/s ' +
        'per-ordering-key cap. Growing values mean the headroom requirement is eroding.',
    }),
  }
}
