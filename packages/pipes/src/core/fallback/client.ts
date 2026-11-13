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
import { SourceErrorInfo, capabilityFailure, classifyError, freshnessFailure, strategyFailure } from './diagnostics.js'
import {
  AllSourcesDownError,
  FallbackDetectionOptions,
  FallbackHealth,
  ResolvedFallbackDetection,
  SourceHealth,
  resolveFallbackDetection,
} from './health.js'
import {
  DefaultFallbackStrategyOptions,
  FallbackCommand,
  FallbackEvent,
  FallbackSourceSnapshot,
  FallbackStrategy,
  FallbackStrategyContext,
  defaultFallbackStrategy,
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
  /**
   * How failure and recovery are *detected*: capability probes, head polls, liveness thresholds,
   * cooldowns, and the freshness conditions whose verdicts the strategy receives as events
   * (`stall.stale`, `batch.lagging`). See {@link FallbackDetectionOptions}.
   */
  detection?: FallbackDetectionOptions
  /**
   * What to *do* about the detected state. Plain options tune the stock strategy
   * ({@link DefaultFallbackStrategyOptions}); a function replaces its decisions per event — it is
   * consulted with the measurements *and* the stock decision (`ctx.defaultCommand`), whatever it
   * returns wins, and `undefined` lets the stock decision stand. See {@link FallbackStrategy}.
   */
  strategy?: FallbackStrategy | DefaultFallbackStrategyOptions
  logger?: Logger
}

/** A structured snapshot of the fallback's observable state, for a metrics surface. */
export interface FallbackMetrics {
  activeIndex: number | undefined
  switchCount: number
  /**
   * Blocks the active source is behind the independent head, or absent when that is not
   * computable — no independent reference, or no delivered block to measure from. Absent is not
   * zero: reporting "0 blocks behind" for "we cannot tell" misleads a dashboard and a strategy
   * alike.
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

/** How a read of one source ended while the stream goes on. */
type Handoff =
  /** The source failed: select again, and tell the strategy why. */
  | { type: 'fault'; cause: SourceErrorInfo }
  /** The strategy dropped the source. */
  | { type: 'failover'; cause: SourceErrorInfo }
  /** The strategy moved the read to another source. */
  | { type: 'use'; index: number }

/** The position a read of one source reached, and how it ended; no handoff ends the stream. */
type ReadEnd = { cursor: BlockCursor | undefined; handoff?: Handoff }

/** The stall-aware fetch: a batch, a source error, or a failover the strategy decided mid-request. */
type Fetched = IteratorResult<StreamData<any>> | { error: unknown } | { failover: SourceErrorInfo }

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

/**
 * The state of the stream in flight. Each stream starts from a fresh one: heads depend on the
 * forced commitment, the tip latch must arm again, and a probe that outlives its stream must not
 * rule on the next one.
 */
type StreamRun = {
  /** `true` when the consumer forces finalized blocks; a consumer only raises a commitment (INV-63). */
  finalized: true | undefined
  sourceOptions: PortalBlockStreamOptions | undefined
  /** Lag failover arms once the tip is first reached, so a deep backfill never trips it. */
  lagArmed: boolean
  /** The active source's unproductive wait (DEF-69; ADR-25 §3 says why no simpler clock works). */
  unproductiveMs: number
  sources: SourceRun[]
}

/**
 * A {@link BlockStreamClient} over an ordered list of them: it drives one source at a time and, as
 * the {@link FallbackStrategy} decides, resumes another from the last delivered block (spec/16).
 *
 * The strategy cannot override the safety rules: a fork propagates unchanged (INV-62), a switch
 * resumes right after the last delivered block (INV-61), and the consuming `PortalStream` owns the
 * finalized floor (INV-64).
 *
 * One stream at a time (INV-65): the freshness state and the gauges describe the stream in flight,
 * so a second concurrent `getStream` is refused.
 */
export class FallbackClient implements BlockStreamClient {
  readonly #sources: FallbackClientSource[]
  readonly #detection: ResolvedFallbackDetection
  readonly #strategy: FallbackStrategy | undefined
  readonly #defaultStrategy: FallbackStrategy
  /** Whether the stock strategy takes a more-preferred source back at a boundary (`eager`). */
  readonly #reclaims: boolean
  readonly #health: SourceHealth[]
  readonly #logger: Logger
  readonly finalized: boolean

  /** Observable state (for metrics). */
  activeIndex: number | undefined
  switchCount = 0
  /** Freshness gauges. `lag` is absent while it cannot be computed (see {@link FallbackMetrics}). */
  lag: number | undefined
  staleness = 0
  chainHead: number | undefined
  /** Set when every source is stuck at the same head (no fresher alternative to switch to). */
  chainStalled = false

  /** The last source actually driven — survives the all-down gap so switch counting stays correct. */
  #lastActive: number | undefined
  /** The stream in flight. Every method below `getStream` runs inside one. */
  #current: StreamRun | undefined

  constructor(options: FallbackClientOptions) {
    if (options.sources.length === 0) {
      throw new Error('FallbackClient requires at least one source')
    }
    // "No fork can arrive" only holds if EVERY source is finalized-only: one hot source is enough
    // to make a fork reachable, and the flag gates whether a target keeps its rollback machinery
    // (and whether a finalized-requiring target forces the finalized stream). Conservative by
    // construction — mixing is allowed, claiming finality for a mixed set is not.
    this.finalized = options.sources.every((s) => s.client.finalized)

    this.#sources = options.sources
    this.#detection = resolveFallbackDetection(options.detection)
    const stockOptions = typeof options.strategy === 'function' ? undefined : options.strategy
    this.#strategy = typeof options.strategy === 'function' ? options.strategy : undefined
    this.#defaultStrategy = defaultFallbackStrategy(stockOptions)
    this.#reclaims = (stockOptions?.preferPrimary ?? 'eager') === 'eager'
    this.#health = options.sources.map(
      (s) => new SourceHealth(this.#detection, s.probeCapability != null || this.#detection.capabilityProbe !== false),
    )
    this.#logger = options.logger ?? defaultLogger({ id: 'fallback' })
  }

  getUrl(): string {
    return this.#sources[this.activeIndex ?? this.#lastActive ?? 0].client.getUrl()
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

  /** The answer of the first source, in preference order, that does not throw. */
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
    const self = this
    return {
      [Symbol.asyncIterator]() {
        return self.#run(query, options)[Symbol.asyncIterator]()
      },
    }
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
      // Nothing is read once the stream ends (OB-60), and a probe still out now answers for no stream.
      this.#current = undefined
      this.#clearActive()
    }
  }

  #startRun(query: Query, options: PortalBlockStreamOptions | undefined): StreamRun {
    // `PortalStream` forwards `false` for a mixed list, and a source lets that option win over its
    // own config: forwarded as is, it would put a finalized-only source on the hot stream (INV-63).
    const finalized = options?.finalized === true ? true : undefined
    const sourceOptions = options == null ? undefined : { ...options, finalized }

    const probeOptions = this.#detection.capabilityProbe
    const probeOf = (source: FallbackClientSource): Probe | undefined => {
      if (source.probeCapability) return source.probeCapability
      if (probeOptions === false) return undefined

      return makeCapabilityProbe(source.client, query, probeOptions === true ? undefined : probeOptions, sourceOptions)
    }

    return {
      finalized,
      sourceOptions,
      lagArmed: false,
      unproductiveMs: 0,
      sources: this.#sources.map((source) => ({
        probe: probeOf(source),
        probing: false,
        lastProbeAt: 0,
        head: undefined,
        leftUnproductive: false,
      })),
    }
  }

  async *#drive<Q extends Query>(query: Q, abort: AbortSignal | undefined): AsyncGenerator<StreamData<GetBlock<Q>>> {
    const run = this.#current!
    let cursor: BlockCursor | undefined
    let lastError: SourceErrorInfo | undefined
    let forced: number | undefined

    while (true) {
      // Sources end their streams quietly on abort; without this check that end would look like a
      // fault and the supervisor would re-drive another source.
      if (abort?.aborted) return

      const active = forced ?? (await this.#selectSource(lastError, cursor, abort))
      if (active === undefined) {
        return
      }

      forced = undefined
      lastError = undefined
      this.#setActive(active)

      // A freshly driven source starts with a clean stall clock: it must not inherit the wait of
      // whatever ran before it, including an earlier run of itself after an all-down gap.
      run.unproductiveMs = 0
      run.sources[active].leftUnproductive = false

      const read = yield* this.#readSource(active, query, cursor, abort)
      cursor = read.cursor

      const handoff = read.handoff
      if (handoff === undefined) return

      if (handoff.type === 'use') {
        forced = handoff.index
        continue
      }

      this.#failSource(active, handoff.cause)
      if (handoff.type === 'fault') lastError = handoff.cause
    }
  }

  /**
   * Reads the active source from just after `from`, and yields its batches. A source fault comes
   * back as a handoff; a fork, a strategy error and an `abort` command throw and end the stream.
   */
  async *#readSource<Q extends Query>(
    active: number,
    query: Q,
    from: BlockCursor | undefined,
    abort: AbortSignal | undefined,
  ): AsyncGenerator<StreamData<GetBlock<Q>>, ReadEnd> {
    let cursor = from

    const q: Query = { ...query }
    if (cursor) {
      q.fromBlock = cursor.number + 1
      q.parentBlockHash = cursor.hash
    }

    let iterator: AsyncIterator<StreamData<any>>
    try {
      iterator = this.#sources[active].client.getStream(q, this.#current!.sourceOptions)[Symbol.asyncIterator]()
    } catch (e) {
      return { cursor, handoff: this.#asFault(e, abort) }
    }

    try {
      while (true) {
        const next = await this.#nextWithStallTicks(iterator, active, cursor)
        if ('error' in next) return { cursor, handoff: this.#asFault(next.error, abort) }
        if ('failover' in next) return { cursor, handoff: { type: 'failover', cause: next.failover } }
        if (next.done) return { cursor } // source completed (bounded stream)

        const batch = next.value

        // A delivered batch proves both liveness *and* capability: the active source just served
        // exactly the configured query (an incapable source throws rather than yields). The
        // standby capability probe never runs for the active source, so without this a cold-start
        // primary would serve forever yet never leave `unknown` for `healthy`.
        this.#health[active].onBatch()
        this.#health[active].onCapability(true)

        if (batch.blocks.length > 0) {
          try {
            cursor = cursorFromHeader(batch.blocks[batch.blocks.length - 1])
          } catch (e) {
            // A malformed batch is the source's fault: resume before it on another source.
            return { cursor, handoff: this.#asFault(e, abort) }
          }
        }

        yield batch as StreamData<GetBlock<Q>>

        const event = await this.#observeBoundary(active, cursor)
        const command = this.#decide(event, cursor)
        if (command.action === 'use' && command.index !== active) {
          this.#assertSourceIndex(command.index)
          return { cursor, handoff: { type: 'use', index: command.index } }
        }
        if (command.action === 'failover') {
          return { cursor, handoff: { type: 'failover', cause: this.#boundaryFailoverCause(event) } }
        }
        if (command.action === 'abort') {
          this.#clearActive()
          throw command.error ?? new AllSourcesDownError()
        }
      }
    } finally {
      safeReturn(iterator)
    }
  }

  /** A source error as a handoff. A fork is not a source fault (INV-62), and after an abort nothing is. */
  #asFault(error: unknown, abort: AbortSignal | undefined): Handoff | undefined {
    if (isForkException(error)) throw error
    if (abort?.aborted) return undefined

    return { type: 'fault', cause: classifyError('stream', error) }
  }

  /**
   * The *behind* verdict: taking this source over would only stall the pipe again. Why the source
   * was left selects the check (ADR-25 §4). The jitter allowance is `maxLagBlocks`, and its default
   * still applies when lag failover is off.
   */
  #isBehind(i: number): boolean {
    const source = this.#current!.sources[i]
    const sample = source.head
    // Head and cursor are compared as sampled at the same instant. Reading the live cursor against
    // a head cached up to `headTtlMs` ago would count everything the chain produced in between as
    // "behind" — on a sub-second chain that skew alone exceeds the whole allowance.
    if (sample?.cursorAt == null) return false
    const head = sample.value

    // Dropped for not making progress: it has to be level with the pipe again before we go back,
    // or we would stall, fail over and crawl back in a loop. Being *level* is enough — demanding
    // it be ahead of a tip-following source is a bar nothing can clear.
    if (source.leftUnproductive) return head == null || head < sample.cursorAt

    // Dropped for a fault: only a structural gap disqualifies it, never ordinary ingestion jitter.
    const allowance = this.#detection.maxLagBlocks ?? DEFAULT_BEHIND_ALLOWANCE
    return head != null && head + allowance < sample.cursorAt
  }

  /** The *stalled* verdict: unproductive wait past the window, optionally plus an open request. */
  #isStale(outstandingMs = 0): boolean {
    return (
      this.#detection.maxStalenessMs != null &&
      this.#current!.unproductiveMs + outstandingMs > this.#detection.maxStalenessMs
    )
  }

  /** Consult the strategy (custom first, default for unanswered events). */
  #decide(event: FallbackEvent, cursor: BlockCursor | undefined, allDownMs?: number): FallbackCommand {
    const ctx: FallbackStrategyContext = {
      event,
      activeIndex: this.activeIndex,
      sources: this.#snapshot(),
      cursor,
      lagBlocks: this.lag,
      atTip: this.#current!.lagArmed,
      allDownMs,
    }

    // The stock decision is always computed first and handed to the custom strategy as
    // `ctx.defaultCommand`, so it can inspect/veto it instead of re-deriving the algorithm.
    const defaultCommand = this.#defaultStrategy(ctx) ?? { action: 'hold' }
    if (!this.#strategy) return defaultCommand

    ctx.defaultCommand = defaultCommand
    return this.#strategy(ctx) ?? defaultCommand
  }

  /** What the strategy sees of each source. */
  #snapshot(): FallbackSourceSnapshot[] {
    return this.#sources.map((s, i) => ({
      index: i,
      name: s.name,
      health: this.#health[i].state,
      active: this.activeIndex === i,
      cause: this.#health[i].cause,
      head: this.#current!.sources[i].head?.value,
      behind: this.#isBehind(i),
    }))
  }

  /**
   * Pick the source to drive: consult the strategy, waiting out an all-down gap (`hold`) with
   * `allDownPollMs` naps — cooldown expiry and head polls make recovery visible between naps.
   * Resolves `undefined` when `abort` fires during the wait.
   */
  async #selectSource(
    lastError: SourceErrorInfo | undefined,
    cursor: BlockCursor | undefined,
    abort: AbortSignal | undefined,
  ): Promise<number | undefined> {
    let allDownSince: number | undefined

    while (true) {
      if (abort?.aborted) {
        this.#clearActive()
        return undefined
      }

      const eligible = this.#health.some((h) => h.state !== 'unhealthy')
      if (eligible) {
        allDownSince = undefined
      } else {
        allDownSince ??= this.#detection.clock()
      }

      const command = this.#decide(
        { type: 'select', error: lastError },
        cursor,
        allDownSince !== undefined ? this.#detection.clock() - allDownSince : undefined,
      )

      if (command.action === 'use') {
        this.#assertSourceIndex(command.index)
        return command.index
      }
      if (command.action === 'abort') {
        this.#clearActive()
        throw command.error ?? new AllSourcesDownError()
      }
      // 'hold' (and a nonsensical 'failover' with nothing active) both wait and ask again.
      this.#clearActive()
      await abortableSleep(this.#detection.allDownPollMs, abort)
    }
  }

  #assertSourceIndex(index: number): void {
    if (Number.isInteger(index) && index >= 0 && index < this.#sources.length) return

    throw new Error(
      `fallback strategy selected source index ${index}, but there are only ${this.#sources.length} sources`,
    )
  }

  /** Refresh other-source heads and the lag gauges, and produce the boundary event's verdict. */
  async #observeBoundary(
    active: number,
    cursor: BlockCursor | undefined,
  ): Promise<Extract<FallbackEvent, { type: 'batch' }>> {
    const staleAtBoundary = this.#isStale()
    // Poll standbys only when something reads the heads (WP-63): at tip pace the batch cadence
    // outruns `headTtlMs`, so this gate, not the cache, bounds the standby traffic.
    const canReclaim = this.#reclaims && active > 0
    const wantHeads = this.#strategy != null || this.#detection.maxLagBlocks != null || staleAtBoundary || canReclaim
    const others = wantHeads ? await this.#chainHeadOthers(active, cursor) : undefined
    // A boundary can be reached before any block has been delivered — a source may yield an empty
    // batch (the portal answers 204 that way) — so there is not always a position to measure from.
    // Both gauges stay unset rather than falling back to a `-1` sentinel, which would surface as a
    // chain-height-sized lag on the metrics and in a strategy's `ctx.lagBlocks`.
    const lastNumber = cursor?.number
    const known = [others, lastNumber].filter((n): n is number => n != null)
    this.chainHead = known.length ? Math.max(...known) : undefined
    if (others == null || lastNumber == null) {
      this.lag = undefined // no independent reference, or no position yet ⇒ not computable
    } else {
      const lag = others - lastNumber
      this.lag = Math.max(0, lag)
      // Arm only once we are genuinely at/near the tip: within the threshold *and* not ahead of the
      // reference (`lag >= 0`). A negative lag means the independent reference is itself behind us
      // (stale/lagging) — arming on that would let a later-recovering source's head trip a spurious
      // failover while we are still backfilling.
      if (this.#detection.maxLagBlocks != null && lag >= 0 && lag <= this.#detection.maxLagBlocks) {
        this.#current!.lagArmed = true // arm at tip (latched)
      }
    }

    // The detection verdicts ride on the event: the strategy decides what to do about them. A
    // boundary carries `stale` as well as `lagging`, because a source can keep answering without
    // making progress (empty batches at a finality frontier) — that never reaches the stall ticker.
    const lagging =
      this.#detection.maxLagBlocks != null &&
      this.#current!.lagArmed &&
      this.lag != null &&
      this.lag > this.#detection.maxLagBlocks
    this.staleness = this.#current!.unproductiveMs
    this.chainStalled = staleAtBoundary && !fresherSourceAhead(this.#snapshot(), cursor)

    return { type: 'batch', lagging, stale: staleAtBoundary }
  }

  /** Why the active source was failed over at a boundary — the detection verdict that tripped. */
  #boundaryFailoverCause(event: Extract<FallbackEvent, { type: 'batch' }>): SourceErrorInfo {
    if (event.stale) {
      return freshnessFailure('stream', 'stale', 'stopped making progress while a fresher source was ahead')
    }
    if (event.lagging) {
      return freshnessFailure(
        'stream',
        'lag',
        `fell behind the chain head by more than ${this.#detection.maxLagBlocks} blocks`,
      )
    }

    return strategyFailure('failover decided by the fallback strategy at a batch boundary')
  }

  /**
   * The highest head reported by the *other* eligible sources — an independent reference that avoids
   * the circular-lag trap (a source that stalls head and data together reads lag ≈ 0 against its own
   * head). Excludes `unhealthy` sources so a flagged-bad one can't define the tip. Heads are cached
   * for `headTtlMs`; polling a standby's head doubles as its liveness/capability driver.
   */
  async #chainHeadOthers(active: number, last?: BlockCursor): Promise<number | undefined> {
    const results = await Promise.all(
      this.#sources.map((_, i) =>
        i === active || this.#health[i].state === 'unhealthy'
          ? Promise.resolve(undefined)
          : this.#getCachedHead(i, last),
      ),
    )
    const vals = results.filter((h): h is number => h != null)

    return vals.length ? Math.max(...vals) : undefined
  }

  /** A head poll, time-boxed by `headPollTimeoutMs` so a hung standby cannot stall the active source (FM-64). */
  #headWithTimeout<T>(p: Promise<T>): Promise<T> {
    const timeoutMs = this.#detection.headPollTimeoutMs
    return withTimeout(p, timeoutMs, () => new Error(`head poll timed out after ${timeoutMs}ms`))
  }

  /**
   * Poll a source's independent head (cached for `headTtlMs`). The poll doubles as a liveness probe
   * — a fresh head promotes a standby toward `healthy` — and is when we (re)fire its capability
   * probe.
   */
  async #getCachedHead(i: number, last?: BlockCursor): Promise<number | undefined> {
    const run = this.#current!
    const source = run.sources[i]
    const now = this.#detection.clock()
    const cached = source.head
    if (cached && now - cached.at < this.#detection.headTtlMs) return cached.value

    try {
      // Each source at its own commitment unless the stream forces one; the cheaper number-only
      // poll when the client has it (WP-66).
      const client = this.#sources[i].client
      const headOptions = run.finalized != null ? { finalized: run.finalized } : undefined
      const value = client.getHeight
        ? await this.#headWithTimeout(client.getHeight(headOptions))
        : (await this.#headWithTimeout(client.getHead(headOptions)))?.number
      source.head = { value, at: now, cursorAt: last?.number }
      if (value != null) this.#health[i].onLivenessPass()
      this.#maybeProbeCapability(i, last)
      return value
    } catch (e) {
      source.head = { value: undefined, at: now, cursorAt: last?.number }
      this.#failSource(i, classifyError('liveness', e))
      return undefined
    }
  }

  /**
   * Fire a source's capability probe once it is reachable, feeding the result into health. A probed
   * source cannot become `healthy` on liveness alone — capability must be confirmed — so without
   * this it could never be switched up to. Fire-and-forget (a full query slice must not block the
   * boundary), throttled by `capabilityProbeIntervalMs`, and never concurrent for the same source.
   * The gating in {@link SourceHealth} drops the confirmation when a source goes unhealthy, so it
   * must re-prove before recovering.
   */
  #maybeProbeCapability(i: number, last?: BlockCursor): void {
    const run = this.#current!
    const source = run.sources[i]
    const probe = source.probe
    if (!probe || this.#health[i].capabilityConfirmed || source.probing) return

    const now = this.#detection.clock()
    if (now - source.lastProbeAt < this.#detection.capabilityProbeIntervalMs) return

    source.lastProbeAt = now
    source.probing = true
    // A probe can outlive its stream (its own timeout is 30s by default): its verdict then describes
    // a query, and possibly a commitment, that nothing serves any more (INV-66).
    const ended = () => this.#current !== run
    // `Promise.resolve().then(...)` normalizes a *synchronously*-throwing probe into a rejection,
    // so the throw can't escape this method and strand `probing` at `true`.
    Promise.resolve()
      .then(() => probe(last))
      .then(
        (r) => {
          if (ended()) return
          if (r.ok) {
            this.#health[i].onLivenessPass()
            this.#health[i].onCapability(true)
          } else {
            this.#failSource(i, r.cause ?? capabilityFailure('probe reported not-capable'))
          }
        },
        (e) => {
          if (ended()) return
          this.#failSource(i, classifyError('capability', e))
        },
      )
      .finally(() => {
        source.probing = false
      })
  }

  /**
   * `iterator.next()` under the stall clock. While the request is outstanding, a ticker fires every
   * `freshnessTickMs`; each tick refreshes the other sources' heads (TTL-bounded), updates the
   * staleness gauges, and consults the strategy with a `stall` event — `failover` ends the wait,
   * anything else keeps waiting. Disabled (plain `next()`) when `maxStalenessMs` is null and no
   * custom strategy is installed. A source error comes back as a value, never a throw.
   */
  async #nextWithStallTicks(
    iterator: AsyncIterator<StreamData<any>>,
    active: number,
    cursor?: BlockCursor,
  ): Promise<Fetched> {
    if (this.#detection.maxStalenessMs == null && !this.#strategy) {
      this.staleness = 0
      return iterator.next().catch((error: unknown) => ({ error }))
    }

    const run = this.#current!

    // The clock starts when the request goes out and stops when it settles, so time the *consumer*
    // spends between yields is never counted against the source.
    const start = this.#detection.clock()
    // The rejection handler here also keeps a later abandon from surfacing as unhandled.
    const settled = iterator.next().then(
      (v) => ({ type: 'next' as const, v }),
      (e) => ({ type: 'error' as const, e }),
    )

    while (true) {
      const tick = delay(this.#detection.freshnessTickMs)
      const r = await Promise.race([settled, tick.promise.then(() => ({ type: 'tick' as const }))])
      tick.cancel()

      if (r.type === 'next') {
        const delivered = !r.v.done && (r.v.value as StreamData<any>).blocks.length > 0
        // An empty batch is an answer, not progress: keep the wait on the clock instead of
        // restarting it, so a source that only ever answers empty is still seen as stalled.
        run.unproductiveMs = delivered ? 0 : run.unproductiveMs + (this.#detection.clock() - start)
        this.staleness = run.unproductiveMs
        this.chainStalled = false
        return r.v
      }
      if (r.type === 'error') {
        this.staleness = 0
        this.chainStalled = false
        return { error: r.e }
      }

      const outstandingMs = this.#detection.clock() - start
      const pendingMs = run.unproductiveMs + outstandingMs
      this.staleness = pendingMs
      const stale = this.#isStale(outstandingMs)

      // Re-polling the other sources both feeds the stall decision and (re)probes their
      // liveness/capability, so a held source keeps noticing when the chain comes back. Polls are
      // gated on the stalled verdict (a healthy-but-slow request should not fan out head queries)
      // — except under a custom strategy, which may want fresh snapshots on every tick; the
      // `headTtlMs` cache bounds the poll rate either way.
      if (stale || this.#strategy) {
        await this.#chainHeadOthers(active, cursor)
      }
      // Observability: every source is stuck at the same head — switching would not help.
      this.chainStalled = stale && !fresherSourceAhead(this.#snapshot(), cursor)

      const command = this.#decide({ type: 'stall', pendingMs, stale }, cursor)
      if (command.action === 'failover') {
        this.staleness = 0
        const failover = stale
          ? freshnessFailure('stream', 'stale', 'no batch progress while a fresher source was ahead')
          : strategyFailure('failover decided by the fallback strategy while a request was outstanding')

        return { failover }
      }
      if (command.action === 'abort') {
        this.#clearActive()
        throw command.error ?? new AllSourcesDownError()
      }
      // 'hold' / 'use' keep waiting ('use' mid-request would tear a batch; switches happen at boundaries)
    }
  }

  /**
   * Feed a failure to a source's health (the `check` selects the signal), then log *why* — but only
   * when it actually flips the source unhealthy, so a log line always marks a real transition
   * (liveness fails are noisy until they trip the threshold). The bounded `reason`/`code`/`check`
   * also reach {@link metrics}; the full `detail` (incl. the request) is logged, never a label.
   */
  #failSource(i: number, cause: SourceErrorInfo): void {
    // Only the supervisor's own stall verdict demotes a source; a custom probe is free to report
    // `stale` about something else entirely.
    if (cause.check === 'stream' && cause.reason === 'stale') this.#current!.sources[i].leftUnproductive = true
    const before = this.#health[i].state
    switch (cause.check) {
      case 'stream':
        this.#health[i].onStreamError(cause)
        break
      case 'liveness':
        this.#health[i].onLivenessFail(cause)
        break
      case 'capability':
        this.#health[i].onCapability(false, cause)
        break
    }
    if (before !== 'unhealthy' && this.#health[i].state === 'unhealthy') {
      this.#logger.warn(
        { source: this.#sources[i].name, check: cause.check, reason: cause.reason, code: cause.code },
        `fallback source "${this.#sources[i].name}" marked unhealthy: ${cause.detail}`,
      )
    }
  }

  #setActive(i: number): void {
    // Count a switch against the last source we drove (not `activeIndex`, which is cleared to
    // `undefined` during an all-down gap) so resuming on a *different* source after the gap still
    // registers, and resuming on the *same* one does not.
    if (this.#lastActive !== undefined && this.#lastActive !== i) {
      this.switchCount++
      // The freshness gauges describe the previous source until the new one repopulates them.
      this.#resetGauges()
    }
    this.#lastActive = i
    this.activeIndex = i
  }

  /** No source is being driven: report no active source and clear the per-source gauges. */
  #clearActive(): void {
    this.activeIndex = undefined
    this.#resetGauges()
  }

  #resetGauges(): void {
    this.lag = undefined
    this.staleness = 0
    this.chainStalled = false
    this.chainHead = undefined
  }

  /** Snapshot of the observable state for export to a metrics surface. */
  metrics(): FallbackMetrics {
    return {
      activeIndex: this.activeIndex,
      switchCount: this.switchCount,
      lag: this.lag,
      staleness: this.staleness,
      chainHead: this.chainHead,
      chainStalled: this.chainStalled,
      sources: this.#sources.map((s, i) => ({
        name: s.name,
        health: this.#health[i].state,
        active: this.activeIndex === i,
        cause: this.#health[i].cause,
      })),
    }
  }
}
