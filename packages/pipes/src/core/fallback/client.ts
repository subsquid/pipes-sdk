import { ApiDataset, BlockRef, PortalRequestOptions } from '~/portal-client/client.js'
import {
  BlockStreamClient,
  GetBlock,
  PortalBlockStream,
  PortalBlockStreamOptions,
  Query,
  StreamData,
  isForkException,
} from '~/portal-client/index.js'

import { Logger, defaultLogger } from '../logger.js'
import { cursorFromHeader } from '../portal-source.js'
import { BlockCursor } from '../types.js'
import { abortableSleep, delay, safeReturn, withTimeout } from './async.js'
import { ProbeResult, makeCapabilityProbe } from './capability.js'
import { SourceErrorInfo, capabilityFailure, classifyError, freshnessFailure } from './diagnostics.js'
import {
  AllSourcesDownError,
  FallbackDetectionOptions,
  FallbackHealth,
  ResolvedFallbackDetection,
  SourceHealth,
  resolveFallbackDetection,
} from './health.js'
import {
  FallbackCommand,
  FallbackEvent,
  FallbackStrategy,
  FallbackStrategyOptions,
  SourceSnapshot,
  fallbackStrategy,
  fresherSourceAhead,
} from './strategy.js'

/** One ranked underlying source: any {@link BlockStreamClient} plus its display name. */
export interface FallbackClientSource {
  name: string
  client: BlockStreamClient
  /**
   * Custom capability probe for this source, replacing the generic one built from the stream's
   * query. It should confirm the source can serve the configured data at `atCursor` and
   * resolve not-`ok` (with a cause) when it cannot.
   */
  probeCapability?: (atCursor?: BlockCursor) => Promise<ProbeResult>
}

export interface FallbackClientOptions {
  /** Underlying sources in preference order — index 0 is the primary. */
  sources: FallbackClientSource[]
  /** How failure and recovery are detected. See {@link FallbackDetectionOptions}. */
  detection?: FallbackDetectionOptions
  /** What to do about the detected state. See {@link FallbackStrategyOptions}. */
  strategy?: FallbackStrategyOptions
  logger?: Logger
}

/** A structured snapshot of the fallback's observable state, for a metrics surface. */
export interface FallbackMetrics {
  activeIndex: number | undefined
  switchCount: number
  /**
   * Blocks the active source is behind the independent head, or absent when that is not
   * computable — no independent reference, or no delivered block to measure from. Absent is not
   * zero: "0 blocks behind" for "we cannot tell" misleads a dashboard.
   */
  lag: number | undefined
  /** The active source's unproductive wait in ms (DEF-69). */
  staleness: number
  chainHead: number | undefined
  /** Set when every source is stuck at the same head (no fresher alternative to switch to). */
  chainStalled: boolean
  sources: { name: string; health: FallbackHealth; active: boolean; cause?: SourceErrorInfo }[]
}

/** Blocks of ordinary ingestion jitter tolerated when `maxLagBlocks` is disabled (see `#isBehind`). */
const DEFAULT_BEHIND_ALLOWANCE = 10

type Probe = (atCursor?: BlockCursor) => Promise<ProbeResult>

/** One source's state for the stream in flight. */
type SourceRun = {
  probe: Probe | undefined
  /** A probe is in flight; one source never has two. */
  probing: boolean
  /** Throttles the full-query standby probe. */
  lastProbeAt: number
  /** The last head poll, with the clock and the pipe position it was sampled at. */
  head: { value: number | undefined; at: number; cursorAt?: number } | undefined
  /** Left for not making progress: it must be level with the pipe before a reclaim (ADR-25 §4). */
  leftUnproductive: boolean
}

/** One configured source: its spec and health outlive a stream, `run` does not. */
type Source = {
  readonly index: number
  readonly name: string
  readonly client: BlockStreamClient
  readonly probeCapability?: Probe
  readonly health: SourceHealth
  run: SourceRun
}

/**
 * The state of the stream in flight. Each stream starts from a fresh one: heads depend on the
 * forced commitment, the tip latch must arm again, and a probe that outlives its stream must not
 * rule on the next one.
 */
type StreamRun = {
  /** `true` when the consumer forces finalized blocks; a consumer only raises a commitment (INV-63). */
  finalized: true | undefined
  sourceOptions: PortalBlockStreamOptions | undefined
  /** The last block delivered to the pipe: the resume point of any switch. */
  cursor: BlockCursor | undefined
  /** Lag failover arms once the tip is first reached, so a deep backfill never trips it. */
  lagArmed: boolean
  /** The active source's unproductive wait before the request in flight (DEF-69, ADR-25 §3). */
  unproductiveMs: number
  /** When the request in flight went out; unset while none is on the stall clock. */
  requestSince: number | undefined
  /** The independent head at the last boundary, and the cursor it was measured against. */
  reference: { head: number | undefined; cursor: number | undefined } | undefined
}

/** How a read of one source ended while the stream goes on. */
type Handoff =
  | { type: 'fault'; cause: SourceErrorInfo }
  | { type: 'failover'; cause: SourceErrorInfo }
  | { type: 'use'; source: Source }

/** The stall-aware fetch: a batch, a source error, or a failover decided mid-request. */
type Fetched = IteratorResult<StreamData<any>> | { error: unknown } | { failover: SourceErrorInfo }

function idleRun(probe?: Probe): SourceRun {
  return { probe, probing: false, lastProbeAt: 0, head: undefined, leftUnproductive: false }
}

/**
 * A {@link BlockStreamClient} over an ordered list of them: it drives one source at a time and,
 * when that source fails or falls behind, resumes another from the last delivered block (spec/16).
 * One stream at a time (INV-65): the freshness state describes the stream in flight.
 */
export class FallbackClient implements BlockStreamClient {
  readonly #sources: Source[]
  readonly #detection: ResolvedFallbackDetection
  readonly #strategy: FallbackStrategy
  readonly #reclaims: boolean
  readonly #logger: Logger
  readonly finalized: boolean

  #active: Source | undefined
  /** The last source driven: survives the all-down gap, so a resume on the same source is no switch. */
  #lastActive: Source | undefined
  #switchCount = 0
  /** The stream in flight. Every method below `getStream` runs inside one. */
  #current: StreamRun | undefined

  constructor(options: FallbackClientOptions) {
    if (options.sources.length === 0) {
      throw new Error('FallbackClient requires at least one source')
    }
    // One hot source is enough to make a fork reachable, so the list is finalized only if every
    // source is (ADR-25 §2).
    this.finalized = options.sources.every((s) => s.client.finalized)

    this.#detection = resolveFallbackDetection(options.detection)
    this.#strategy = fallbackStrategy(options.strategy)
    this.#reclaims = (options.strategy?.preferPrimary ?? 'eager') === 'eager'
    this.#logger = options.logger ?? defaultLogger({ id: 'fallback' })

    const probed = (s: FallbackClientSource) => s.probeCapability != null || this.#detection.capabilityProbe !== false
    this.#sources = options.sources.map((s, index) => ({
      index,
      name: s.name,
      client: s.client,
      probeCapability: s.probeCapability,
      health: new SourceHealth(this.#detection, probed(s)),
      run: idleRun(),
    }))
  }

  getUrl(): string {
    return (this.#active ?? this.#lastActive ?? this.#sources[0]).client.getUrl()
  }

  /** First source that can answer — so a portal's metadata wins over an RPC client's synthetic one. */
  getMetadata(options?: PortalRequestOptions): Promise<ApiDataset> {
    return this.#firstAnswer((client) => client.getMetadata(options))
  }

  /**
   * The highest head any source reports. It throws when no source answers: resolving
   * `from: 'latest'` runs through here, and `undefined` would resolve to block 0 (WP-69).
   *
   * The first answer has no time limit, because a cold start can take seconds on every source.
   * After it, the other sources get `headPollTimeoutMs`, so one hung endpoint cannot block startup.
   */
  async getHead(options?: PortalRequestOptions & { finalized?: boolean }): Promise<BlockRef | undefined> {
    const polls = this.#sources.map((s) => s.client.getHead(options))
    const heads: (PromiseSettledResult<BlockRef | undefined> | undefined)[] = polls.map(() => undefined)
    const allSettled = Promise.all(
      polls.map((poll, i) =>
        poll.then(
          (value) => {
            heads[i] = { status: 'fulfilled', value }
          },
          (reason) => {
            heads[i] = { status: 'rejected', reason }
          },
        ),
      ),
    )

    // Wait for the first answer (or for every poll to fail), then give the rest a grace window.
    await Promise.any(polls).catch(() => allSettled)
    await this.#headWithTimeout(allSettled).catch(() => {})

    let best: BlockRef | undefined
    let answered = false
    for (const h of heads) {
      if (h?.status !== 'fulfilled') {
        continue
      }

      answered = true
      if (h.value && (!best || h.value.number > best.number)) {
        best = h.value
      }
    }

    if (!answered) {
      throw new AggregateError(
        heads.map((h) => (h?.status === 'rejected' ? h.reason : undefined)).filter(Boolean),
        'no fallback source could report a chain head',
      )
    }

    return best
  }

  /** First source that can resolve it (an RPC-backed client typically cannot; a portal can). */
  resolveTimestamp(seconds: number, options?: PortalRequestOptions): Promise<number> {
    return this.#firstAnswer((client) => client.resolveTimestamp(seconds, options))
  }

  async #firstAnswer<T>(ask: (client: BlockStreamClient) => Promise<T>): Promise<T> {
    let lastError: unknown
    for (const s of this.#sources) {
      try {
        return await ask(s.client)
      } catch (e) {
        lastError = e
      }
    }

    throw lastError
  }

  getStream<Q extends Query>(query: Q, options?: PortalBlockStreamOptions): PortalBlockStream<GetBlock<Q>> {
    return this.#run(query, options)
  }

  /** The supervisor: switches sources internally; only `ForkException` (and completion) escape. */
  async *#run<Q extends Query>(query: Q, options?: PortalBlockStreamOptions): AsyncGenerator<StreamData<GetBlock<Q>>> {
    if (this.#current) {
      throw new Error(
        'FallbackClient drives one stream at a time: its freshness state and gauges describe the ' +
          'stream in flight. Build a separate client (or `evmStream`) per concurrent stream.',
      )
    }

    this.#current = this.#startRun(query, options)
    try {
      yield* this.#drive(query, options?.request?.abort)
    } finally {
      // A probe still out now answers for no stream (OB-60).
      this.#current = undefined
      this.#active = undefined
    }
  }

  #startRun(query: Query, options: PortalBlockStreamOptions | undefined): StreamRun {
    // `PortalStream` forwards `false` for a mixed list, and a source lets that option win over its
    // own config: forwarded as is, it would put a finalized-only source on the hot stream (INV-63).
    const finalized = options?.finalized === true ? true : undefined
    const sourceOptions = options == null ? undefined : { ...options, finalized }

    const probeOptions = this.#detection.capabilityProbe
    for (const source of this.#sources) {
      let probe = source.probeCapability
      if (!probe && probeOptions !== false) {
        const tuning = probeOptions === true ? undefined : probeOptions
        probe = makeCapabilityProbe(source.client, query, tuning, sourceOptions)
      }

      source.run = idleRun(probe)
    }

    return {
      finalized,
      sourceOptions,
      cursor: undefined,
      lagArmed: false,
      unproductiveMs: 0,
      requestSince: undefined,
      reference: undefined,
    }
  }

  async *#drive<Q extends Query>(query: Q, abort: AbortSignal | undefined): AsyncGenerator<StreamData<GetBlock<Q>>> {
    let forced: Source | undefined

    while (true) {
      // Sources end their streams quietly on abort; without this check that end would look like a
      // fault and the supervisor would re-drive another source.
      if (abort?.aborted) {
        return
      }

      const active = forced ?? (await this.#selectSource(abort))
      if (active === undefined) {
        return
      }

      forced = undefined
      this.#setActive(active)

      const handoff = yield* this.#readSource(active, query, abort)
      if (handoff === undefined) {
        return
      }

      if (handoff.type === 'use') {
        forced = handoff.source
        continue
      }

      this.#failSource(active, handoff.cause)
    }
  }

  /** Reads the active source from just after the cursor. No handoff ends the stream; a fork throws. */
  async *#readSource<Q extends Query>(
    active: Source,
    query: Q,
    abort: AbortSignal | undefined,
  ): AsyncGenerator<StreamData<GetBlock<Q>>, Handoff | undefined> {
    const run = this.#current!

    const q: Query = { ...query }
    if (run.cursor) {
      q.fromBlock = run.cursor.number + 1
      q.parentBlockHash = run.cursor.hash
    }

    let iterator: AsyncIterator<StreamData<any>>
    try {
      iterator = active.client.getStream(q, run.sourceOptions)[Symbol.asyncIterator]()
    } catch (e) {
      return this.#asFault(e, abort)
    }

    try {
      while (true) {
        const next = await this.#nextWithStallTicks(iterator, active)
        if ('error' in next) {
          return this.#asFault(next.error, abort)
        }
        if ('failover' in next) {
          return { type: 'failover', cause: next.failover }
        }
        if (next.done) {
          return undefined
        }

        const batch = next.value

        // A delivered batch proves liveness and capability. The standby probe never runs for the
        // active source, so without this a cold-start primary would never leave `unknown`.
        active.health.onBatch()
        active.health.onCapability(true)

        if (batch.blocks.length > 0) {
          try {
            run.cursor = cursorFromHeader(batch.blocks[batch.blocks.length - 1])
          } catch (e) {
            // A malformed batch is the source's fault: resume before it on another source.
            return this.#asFault(e, abort)
          }
        }

        yield batch as StreamData<GetBlock<Q>>

        const event = await this.#observeBoundary(active)
        const command = this.#decide(event)
        if (command.action === 'use' && command.index !== active.index) {
          return { type: 'use', source: this.#sources[command.index] }
        }
        if (command.action === 'failover') {
          return { type: 'failover', cause: this.#boundaryFailoverCause(event) }
        }
      }
    } finally {
      safeReturn(iterator)
    }
  }

  /** A source error as a handoff. A fork is not a source fault (INV-62), and after an abort nothing is. */
  #asFault(error: unknown, abort: AbortSignal | undefined): Handoff | undefined {
    if (isForkException(error)) {
      throw error
    }
    if (abort?.aborted) {
      return undefined
    }

    return { type: 'fault', cause: classifyError('stream', error) }
  }

  /**
   * The *behind* verdict: taking this source over would only stall the pipe again. Why the source
   * was left selects the check (ADR-25 §4). The jitter allowance is `maxLagBlocks`, and its default
   * still applies when lag failover is off.
   */
  #isBehind(source: Source): boolean {
    const sample = source.run.head
    // Head and cursor are compared as sampled at the same instant: against the live cursor, a head
    // cached for `headTtlMs` reads as behind by everything the chain produced since.
    if (sample?.cursorAt == null) {
      return false
    }

    const head = sample.value
    // Dropped for not making progress: it must be level with the pipe again, or the pipe would
    // stall, fail over and crawl back in a loop. Ahead of a tip-following source is a bar nothing clears.
    if (source.run.leftUnproductive) {
      return head == null || head < sample.cursorAt
    }

    // Dropped for a fault: only a structural gap disqualifies it, never ordinary ingestion jitter.
    const allowance = this.#detection.maxLagBlocks ?? DEFAULT_BEHIND_ALLOWANCE

    return head != null && head + allowance < sample.cursorAt
  }

  /** The active source's unproductive wait, the request in flight included. */
  #staleness(): number {
    const run = this.#current
    if (!run || !this.#active) {
      return 0
    }

    const outstandingMs = run.requestSince === undefined ? 0 : this.#detection.clock() - run.requestSince

    return run.unproductiveMs + outstandingMs
  }

  #isStale(): boolean {
    const max = this.#detection.maxStalenessMs

    return max != null && this.#staleness() > max
  }

  #decide(event: FallbackEvent, allDownMs?: number): FallbackCommand {
    return this.#strategy({
      event,
      activeIndex: this.#active?.index,
      sources: this.#snapshot(),
      cursor: this.#current!.cursor,
      allDownMs,
    })
  }

  #snapshot(): SourceSnapshot[] {
    return this.#sources.map((s) => ({
      index: s.index,
      health: s.health.state,
      active: s === this.#active,
      head: s.run.head?.value,
      behind: this.#isBehind(s),
    }))
  }

  /** Pick the source to drive, waiting out an all-down gap. `undefined` when `abort` fires. */
  async #selectSource(abort: AbortSignal | undefined): Promise<Source | undefined> {
    let allDownSince: number | undefined

    while (true) {
      if (abort?.aborted) {
        this.#active = undefined

        return undefined
      }

      const eligible = this.#sources.some((s) => s.health.state !== 'unhealthy')
      if (eligible) {
        allDownSince = undefined
      } else {
        allDownSince ??= this.#detection.clock()
      }

      const allDownMs = allDownSince !== undefined ? this.#detection.clock() - allDownSince : undefined
      const command = this.#decide({ type: 'select' }, allDownMs)

      if (command.action === 'use') {
        return this.#sources[command.index]
      }

      this.#active = undefined
      if (command.action === 'abort') {
        throw new AllSourcesDownError()
      }

      await abortableSleep(this.#detection.allDownPollMs, abort)
    }
  }

  async #observeBoundary(active: Source): Promise<Extract<FallbackEvent, { type: 'batch' }>> {
    const run = this.#current!
    const maxLag = this.#detection.maxLagBlocks
    const stale = this.#isStale()

    // Poll standbys only when something reads the heads (WP-63): at tip pace the batch cadence
    // outruns `headTtlMs`, so this gate, not the cache, bounds the standby traffic.
    const canReclaim = this.#reclaims && active.index > 0
    const wantHeads = maxLag != null || stale || canReclaim
    const head = wantHeads ? await this.#chainHeadOthers(active) : undefined

    // A boundary can come before any block was delivered (a portal answers 204 with an empty
    // batch), so the cursor may be unset: the lag is then not computable rather than measured from -1.
    const cursor = run.cursor?.number
    run.reference = { head, cursor }

    // Arm at the tip only: within the threshold and not ahead of the reference. A reference behind
    // the pipe is itself stale, and arming on it would let a later jump trip a spurious failover.
    if (maxLag != null && head != null && cursor != null) {
      const gap = head - cursor
      if (gap >= 0 && gap <= maxLag) {
        run.lagArmed = true
      }
    }

    const lag = this.#lag()
    const lagging = maxLag != null && run.lagArmed && lag != null && lag > maxLag

    return { type: 'batch', lagging, stale }
  }

  #boundaryFailoverCause(event: Extract<FallbackEvent, { type: 'batch' }>): SourceErrorInfo {
    if (event.stale) {
      return freshnessFailure('stream', 'stale', 'stopped making progress while a fresher source was ahead')
    }

    return freshnessFailure(
      'stream',
      'lag',
      `fell behind the chain head by more than ${this.#detection.maxLagBlocks} blocks`,
    )
  }

  /**
   * The highest head reported by the *other* eligible sources — an independent reference: a source
   * that stalls head and data together reads lag ≈ 0 against its own head. An `unhealthy` source
   * cannot define the tip.
   */
  async #chainHeadOthers(active: Source): Promise<number | undefined> {
    const polled = this.#sources.filter((s) => s !== active && s.health.state !== 'unhealthy')
    const heads = await Promise.all(polled.map((s) => this.#getCachedHead(s)))
    const known = heads.filter((h): h is number => h != null)

    return known.length ? Math.max(...known) : undefined
  }

  /** A head poll, time-boxed by `headPollTimeoutMs` so a hung standby cannot stall the active source (FM-64). */
  #headWithTimeout<T>(p: Promise<T>): Promise<T> {
    const timeoutMs = this.#detection.headPollTimeoutMs

    return withTimeout(p, timeoutMs, () => new Error(`head poll timed out after ${timeoutMs}ms`))
  }

  /** Poll a source's head, cached for `headTtlMs`. It doubles as the standby's liveness probe. */
  async #getCachedHead(source: Source): Promise<number | undefined> {
    const run = this.#current!
    const now = this.#detection.clock()
    const cursorAt = run.cursor?.number

    const cached = source.run.head
    if (cached && now - cached.at < this.#detection.headTtlMs) {
      return cached.value
    }

    try {
      // Each source at its own commitment unless the stream forces one; the cheaper number-only
      // poll when the client has it (WP-66).
      const client = source.client
      const headOptions = run.finalized != null ? { finalized: run.finalized } : undefined
      const value = client.getHeight
        ? await this.#headWithTimeout(client.getHeight(headOptions))
        : (await this.#headWithTimeout(client.getHead(headOptions)))?.number

      source.run.head = { value, at: now, cursorAt }
      if (value != null) {
        source.health.onLivenessPass()
      }
      this.#maybeProbeCapability(source)

      return value
    } catch (e) {
      source.run.head = { value: undefined, at: now, cursorAt }
      this.#failSource(source, classifyError('liveness', e))

      return undefined
    }
  }

  /**
   * Fire a source's capability probe, so a probed standby can become `healthy` at all. It runs in
   * the background (a full query slice must not block the boundary), at most once per
   * `capabilityProbeIntervalMs`, and never twice at once for one source.
   */
  #maybeProbeCapability(source: Source): void {
    const run = this.#current!
    const sourceRun = source.run
    const probe = sourceRun.probe
    if (!probe || source.health.capabilityConfirmed || sourceRun.probing) {
      return
    }

    const now = this.#detection.clock()
    if (now - sourceRun.lastProbeAt < this.#detection.capabilityProbeIntervalMs) {
      return
    }

    sourceRun.lastProbeAt = now
    sourceRun.probing = true
    // A probe can outlive its stream; its verdict then describes a query nothing serves (INV-66).
    const ended = () => this.#current !== run
    const cursor = run.cursor
    // `Promise.resolve().then` turns a synchronous throw into a rejection, so `probing` is always reset.
    Promise.resolve()
      .then(() => probe(cursor))
      .then(
        (r) => {
          if (ended()) {
            return
          }
          if (r.ok) {
            source.health.onLivenessPass()
            source.health.onCapability(true)
          } else {
            this.#failSource(source, r.cause ?? capabilityFailure('probe reported not-capable'))
          }
        },
        (e) => {
          if (!ended()) {
            this.#failSource(source, classifyError('capability', e))
          }
        },
      )
      .finally(() => {
        sourceRun.probing = false
      })
  }

  /** `iterator.next()` under the stall clock. A source error comes back as a value, never a throw. */
  async #nextWithStallTicks(iterator: AsyncIterator<StreamData<any>>, active: Source): Promise<Fetched> {
    if (this.#detection.maxStalenessMs == null) {
      return iterator.next().catch((error: unknown) => ({ error }))
    }

    const run = this.#current!

    // The clock runs only while the request is out, so the consumer's time between yields is
    // never counted against the source.
    run.requestSince = this.#detection.clock()
    // The rejection handler also keeps a later abandon from surfacing as unhandled.
    const settled = iterator.next().then(
      (v) => ({ type: 'next' as const, v }),
      (e) => ({ type: 'error' as const, e }),
    )

    try {
      while (true) {
        const tick = delay(this.#detection.freshnessTickMs)
        const r = await Promise.race([settled, tick.promise.then(() => ({ type: 'tick' as const }))])
        tick.cancel()

        if (r.type === 'next') {
          // An empty batch is an answer, not progress: the wait stays on the clock, so a source
          // that only ever answers empty is still seen as stalled.
          const delivered = !r.v.done && (r.v.value as StreamData<any>).blocks.length > 0
          run.unproductiveMs = delivered ? 0 : this.#staleness()

          return r.v
        }
        if (r.type === 'error') {
          return { error: r.e }
        }

        // Re-polling the other sources feeds the decision and keeps probing their liveness, so a
        // held source notices when the chain comes back.
        const stale = this.#isStale()
        if (stale) {
          await this.#chainHeadOthers(active)
        }

        const command = this.#decide({ type: 'stall', stale })
        if (command.action === 'failover') {
          return { failover: freshnessFailure('stream', 'stale', 'no batch progress while a fresher source was ahead') }
        }
      }
    } finally {
      run.requestSince = undefined
    }
  }

  /** Logs only when the failure flips the source unhealthy, so a log line marks a real transition. */
  #failSource(source: Source, cause: SourceErrorInfo): void {
    // Only the supervisor's own stall verdict demotes a source; a custom probe is free to report
    // `stale` about something else entirely.
    if (cause.check === 'stream' && cause.reason === 'stale') {
      source.run.leftUnproductive = true
    }

    const before = source.health.state
    switch (cause.check) {
      case 'stream':
        source.health.onStreamError(cause)
        break
      case 'liveness':
        source.health.onLivenessFail(cause)
        break
      case 'capability':
        source.health.onCapability(false, cause)
        break
    }

    if (before !== 'unhealthy' && source.health.state === 'unhealthy') {
      this.#logger.warn(
        { source: source.name, check: cause.check, reason: cause.reason, code: cause.code },
        `fallback source "${source.name}" marked unhealthy: ${cause.detail}`,
      )
    }
  }

  #setActive(source: Source): void {
    // Compare with the last source driven, not the active one, which an all-down gap clears: a
    // resume on another source after the gap is a switch, a resume on the same one is not.
    if (this.#lastActive !== undefined && this.#lastActive !== source) {
      this.#switchCount++
    }

    this.#lastActive = source
    this.#active = source

    // A new read starts with a clean stall clock and no lag reference: it must not inherit the
    // measurements of whatever ran before it, an earlier read of itself included.
    const run = this.#current!
    run.unproductiveMs = 0
    run.reference = undefined
    source.run.leftUnproductive = false
  }

  #lag(): number | undefined {
    const reference = this.#current?.reference
    if (reference?.head == null || reference.cursor == null) {
      return undefined
    }

    return Math.max(0, reference.head - reference.cursor)
  }

  metrics(): FallbackMetrics {
    const active = this.#active
    // The gauges describe the source being driven; with none, there is nothing to measure.
    const reference = active ? this.#current?.reference : undefined
    const known = [reference?.head, reference?.cursor].filter((n): n is number => n != null)
    const chainStalled =
      active !== undefined && this.#isStale() && !fresherSourceAhead(this.#snapshot(), this.#current!.cursor)

    return {
      activeIndex: active?.index,
      switchCount: this.#switchCount,
      lag: active ? this.#lag() : undefined,
      staleness: this.#staleness(),
      chainHead: known.length ? Math.max(...known) : undefined,
      chainStalled,
      sources: this.#sources.map((s) => ({
        name: s.name,
        health: s.health.state,
        active: s === active,
        cause: s.health.cause,
      })),
    }
  }
}
