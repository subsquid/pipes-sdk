import { sleep } from '~/internal/function.js'
import { ApiDataset, BlockRef } from '~/portal-client/client.js'
import {
  BlockStreamClient,
  GetBlock,
  PortalBlockStream,
  PortalBlockStreamOptions,
  Query,
  StreamData,
  isForkException,
} from '~/portal-client/index.js'

import { delay, safeReturn, withTimeout } from './fallback-async.js'
import { CapabilityProbeOptions, ProbeResult, makeCapabilityProbe } from './fallback-capability.js'
import { SourceErrorInfo, classifyError, freshnessFailure, strategyFailure } from './fallback-diagnostics.js'
import {
  AllSourcesDownError,
  FallbackDetectionOptions,
  FallbackHealth,
  ResolvedFallbackDetection,
  SourceHealth,
  resolveFallbackDetection,
} from './fallback-health.js'
import {
  DefaultFallbackStrategyOptions,
  FallbackCommand,
  FallbackEvent,
  FallbackStrategy,
  FallbackStrategyContext,
  defaultFallbackStrategy,
} from './fallback-strategy.js'
import { Logger, defaultLogger } from './logger.js'
import { cursorFromHeader } from './portal-source.js'
import { BlockCursor } from './types.js'

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
  /**
   * The active source's accumulated unproductive wait (ms): time it spends answering without
   * delivering a block, excluding time the consumer holds the stream (ADR-25). Not a per-request
   * clock — a source that promptly answers "nothing yet" forever still accrues it.
   */
  staleness: number
  chainHead: number | undefined
  /** Set when every source is stuck at the same head (no fresher alternative to switch to). */
  chainStalled: boolean
  sources: { name: string; health: FallbackHealth; active: boolean; cause?: SourceErrorInfo }[]
}

/** Returned by the stall-aware fetch when the strategy decided to fail the active source over. */
const FAILOVER = Symbol('failover')

/** Blocks of ordinary ingestion jitter tolerated when `maxLagBlocks` is disabled (see `#isBehind`). */
const DEFAULT_BEHIND_ALLOWANCE = 10

/**
 * Wraps a decision that must end the stream, so the supervisor's own source-error handling — which
 * treats any throw as "this source failed, try another" — cannot swallow it into a retry loop.
 */
class StreamAbort extends Error {
  constructor(readonly reason: unknown) {
    super('fallback stream aborted')
  }
}

/**
 * A meta-client over an ordered list of {@link BlockStreamClient}s — itself a `BlockStreamClient`,
 * so it slots into a `PortalStream` exactly where a single portal client goes. Its `getStream`
 * drives one underlying source at a time and, guided by the {@link FallbackStrategy}, resumes
 * another source from the last delivered block on failure, staleness, lag, or recovery of a more
 * preferred source.
 *
 * What is NOT delegated to the strategy (safety invariants):
 * - a `ForkException` propagates untouched — a fork straddling a switch is handled by the same
 *   `pipeTo` rewind path as an ordinary reorg;
 * - a switch only ever happens at a batch boundary and resumes from the last delivered block
 *   (`fromBlock = last + 1`, `parentBlockHash = last.hash`), so the pipe never sees a gap or
 *   an overlap;
 * - the finalized-head watermark is owned by the consuming `PortalStream`, which clamps every
 *   batch — a source switch can never un-finalize already-committed data.
 *
 * Drives **one stream at a time**. The freshness bookkeeping (forced commitment, capability
 * probes, head cache, tip latch) describes the stream in flight and the observable gauges describe
 * the source being driven — neither is meaningful for two streams at once. A second concurrent
 * `getStream` is refused rather than silently interleaved; build one client per stream, which is
 * what `evmStream` already does.
 */
export class FallbackClient implements BlockStreamClient {
  readonly #sources: FallbackClientSource[]
  readonly #detection: ResolvedFallbackDetection
  readonly #strategy: FallbackStrategy | undefined
  readonly #defaultStrategy: FallbackStrategy
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

  /** Finality forced on the current stream by the consumer, if any (see `#run`). */
  #streamFinalized: boolean | undefined
  /**
   * Time the active source has spent answering *without delivering a block*, accumulated across
   * consecutive empty batches and reset by the first batch that carries one.
   *
   * Neither simpler measure works. Timing a single outstanding request misses a source that keeps
   * answering but never progresses — a portal parked at its finalized head returns 204 in a tight
   * loop, and each empty batch would restart the clock. Timing from the last delivered block
   * instead would count the time a *slow consumer* spends between yields, spuriously failing over
   * a perfectly healthy source because the target is slow. Only the source's own unproductive wait
   * counts here.
   */
  #unproductiveMs = 0
  /** Per-source capability probes for the *current* stream's query (built per `getStream`). */
  #probes: (((atCursor?: BlockCursor) => Promise<ProbeResult>) | undefined)[] = []
  /** Guards against firing a second capability probe for a source while one is still in flight. */
  readonly #capabilityProbing: boolean[] = []
  /** Clock of the last capability probe per source — throttles the (full-query) standby probe. */
  readonly #lastProbeAt: number[] = []
  /** Cached independent head per source, with the clock it was fetched at (TTL `headTtlMs`). */
  readonly #headCache: ({ value: number | undefined; at: number; cursorAt?: number } | undefined)[] = []
  /** Lag failover arms only once the tip is first reached, so a deep backfill never trips it. */
  #lagArmed = false
  /** The last source actually driven — survives the all-down gap so switch counting stays correct. */
  #lastActive: number | undefined
  /** Guards the one-stream-at-a-time invariant that the per-stream state above depends on. */
  #streaming = false
  /** Sources abandoned for not making progress — they must prove they can serve before a reclaim. */
  readonly #leftUnproductive: boolean[] = []
  /**
   * Whether the last mid-request failover followed the *stalled* verdict. Captured when the
   * decision is taken because it cannot be recomputed afterwards: a source that hangs carries its
   * wait in the outstanding request, which is gone by the time the cause is built.
   */
  #stallWasStale = false
  /**
   * Bumped per stream. A capability probe is fire-and-forget and can outlive the stream that
   * started it (its own timeout is 30s by default), so its verdict is stamped with the generation
   * it was asked in — a late answer describes a query, and possibly a commitment, that is no
   * longer being served, and must not be allowed to rule a source healthy or unhealthy for the
   * stream that came after.
   */
  #streamGeneration = 0

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
    this.#strategy = typeof options.strategy === 'function' ? options.strategy : undefined
    this.#defaultStrategy = defaultFallbackStrategy(
      typeof options.strategy === 'function' ? undefined : options.strategy,
    )
    this.#health = options.sources.map(
      (s) => new SourceHealth(this.#detection, s.probeCapability != null || this.#detection.capabilityProbe !== false),
    )
    this.#logger = options.logger ?? defaultLogger({ id: 'fallback' })
  }

  getUrl(): string {
    return this.#sources[this.activeIndex ?? this.#lastActive ?? 0].client.getUrl()
  }

  /** First source that can answer — so a portal's metadata wins over an RPC client's synthetic one. */
  async getMetadata(): Promise<ApiDataset> {
    let lastError: unknown
    for (const s of this.#sources) {
      try {
        return await s.client.getMetadata()
      } catch (e) {
        lastError = e
      }
    }

    throw lastError
  }

  /**
   * The highest head any source reports — the best independent view of the chain tip.
   *
   * Each poll is time-boxed by `headPollTimeoutMs`, the same guard the internal polls use: one
   * unresponsive endpoint must not hang a lookup the others can answer. Resolving `from: 'latest'`
   * runs through here, and a silent `undefined` there would resolve to block 0 and backfill the
   * whole chain — so if *nothing* answered, this throws rather than passing off "we could not ask"
   * as "there is no head".
   */
  async getHead(options?: { finalized: boolean }): Promise<BlockRef | undefined> {
    const heads = await Promise.allSettled(this.#sources.map((s) => this.#headWithTimeout(s.client.getHead(options))))

    let best: BlockRef | undefined
    let answered = false
    for (const h of heads) {
      if (h.status !== 'fulfilled') continue
      answered = true
      if (h.value && (!best || h.value.number > best.number)) best = h.value
    }

    if (!answered) {
      throw new AggregateError(
        heads.map((h) => (h.status === 'rejected' ? h.reason : undefined)).filter(Boolean),
        'no fallback source could report a chain head',
      )
    }

    return best
  }

  /** First source that can resolve it (an RPC-backed client typically cannot; a portal can). */
  async resolveTimestamp(seconds: number): Promise<number> {
    let lastError: unknown
    for (const s of this.#sources) {
      try {
        return await s.client.resolveTimestamp(seconds)
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
    if (this.#streaming) {
      throw new Error(
        'FallbackClient drives one stream at a time: its freshness state and gauges describe the ' +
          'stream in flight. Build a separate client (or `evmStream`) per concurrent stream.',
      )
    }

    this.#streaming = true
    try {
      yield* this.#drive(query, options)
    } finally {
      this.#streaming = false
    }
  }

  async *#drive<Q extends Query>(
    query: Q,
    options?: PortalBlockStreamOptions,
  ): AsyncGenerator<StreamData<GetBlock<Q>>> {
    // Re-arm the lag trigger per stream: a reused instance starting a later (far-behind-head)
    // backfill must not inherit "reached the tip" from a previous run and false-fire on lag.
    this.#lagArmed = false
    // A consumer may only ever *raise* finality, never lower it. `PortalStream` forwards its own
    // effective commitment as a plain boolean, which for a mixed set is `false` — and a source
    // client lets that option win over its own config, so forwarding it verbatim would silently
    // put a source the user declared finalized-only onto the hot stream. Only `true` forces;
    // anything else leaves each source at its own commitment.
    this.#streamFinalized = options?.finalized === true ? true : undefined
    const sourceOptions: PortalBlockStreamOptions | undefined =
      options == null ? undefined : { ...options, finalized: this.#streamFinalized }
    // Heads are commitment-specific, so a cache filled by a previous stream (possibly at a
    // different forced commitment) must not carry over into this one.
    this.#headCache.length = 0
    this.#leftUnproductive.length = 0
    this.#capabilityProbing.length = 0
    this.#lastProbeAt.length = 0
    this.#unproductiveMs = 0
    this.#streamGeneration++
    const probeOptions = this.#detection.capabilityProbe
    this.#probes = this.#sources.map((s) => {
      if (s.probeCapability) return s.probeCapability
      if (probeOptions === false) return undefined
      return makeCapabilityProbe(s.client, query, probeOptions === true ? undefined : probeOptions, sourceOptions)
    })

    let cursor: BlockCursor | undefined
    let lastError: SourceErrorInfo | undefined
    let forced: number | undefined

    while (true) {
      const active = forced ?? (await this.#selectSource(lastError, cursor))
      forced = undefined
      lastError = undefined
      this.#setActive(active)

      // A freshly driven source starts with a clean stall clock: it must not inherit the wait of
      // whatever ran before it, including an earlier run of itself after an all-down gap.
      this.#unproductiveMs = 0
      this.#leftUnproductive[active] = false

      const q: Query = { ...query }
      if (cursor) {
        q.fromBlock = cursor.number + 1
        q.parentBlockHash = cursor.hash
      }

      try {
        const iterator = this.#sources[active].client.getStream(q, sourceOptions)[Symbol.asyncIterator]()
        try {
          while (true) {
            const next = await this.#nextWithStallTicks(iterator, active, cursor)
            if (next === FAILOVER) {
              this.#failSource(active, this.#stallFailoverCause())
              break
            }
            if (next.done) return // source completed (bounded stream)
            const batch = next.value

            // A delivered batch proves both liveness *and* capability: the active source just served
            // exactly the configured query (an incapable source throws rather than yields). The
            // standby capability probe never runs for the active source, so without this a cold-start
            // primary would serve forever yet never leave `unknown` for `healthy`.
            this.#health[active].onBatch()
            this.#health[active].onCapability(true)

            if (batch.blocks.length > 0) {
              cursor = cursorFromHeader(batch.blocks[batch.blocks.length - 1])
            }

            yield batch as StreamData<GetBlock<Q>>

            // Observe, then decide: refresh the other sources' heads (which doubles as their
            // liveness/capability driver) and update the lag gauges to produce the boundary event,
            // then let the strategy decide whether to stay, fail over, or reclaim a source.
            const event = await this.#observeBoundary(active, cursor)
            const command = this.#decideInStream(event, cursor)
            if (command.action === 'use' && command.index !== active) {
              this.#assertSourceIndex(command.index, true)
              forced = command.index
              break
            }
            if (command.action === 'failover') {
              this.#failSource(active, this.#boundaryFailoverCause(event))
              break
            }
            if (command.action === 'abort') {
              this.#clearActive()
              throw new StreamAbort(command.error ?? new AllSourcesDownError())
            }
          }
        } finally {
          safeReturn(iterator)
        }
      } catch (e) {
        if (isForkException(e)) throw e // propagate; do NOT switch
        if (e instanceof StreamAbort) throw e.reason // a decision to end the stream, not a fault

        const cause = classifyError('stream', e)
        this.#failSource(active, cause)
        lastError = cause
        // re-select and resume from `cursor` on the next iteration
      }
    }
  }

  /**
   * The *behind* verdict: taking this source over would only stall the pipe again.
   *
   * Distance alone cannot tell an exhausted source from a healthy one right after a handoff — both
   * sit a block or so under the cursor — so *why we left it* decides which question to ask:
   *
   * - abandoned for not making progress ⇒ it has to prove it can serve the resume point before we
   *   go back, otherwise the pipe would stall, fail over, and crawl back in a loop;
   * - abandoned for a fault ⇒ only a structural gap disqualifies it. Ordinary ingestion jitter (a
   *   portal trailing the block a hot source just delivered) must not, or an eager reclaim could
   *   never take the cheap primary back at the tip. The allowance is `maxLagBlocks`, the same
   *   "acceptable distance behind" the lag check uses; disabling lag *failover* does not abolish
   *   the notion of jitter, so the default still applies.
   */
  #isBehind(i: number): boolean {
    const sample = this.#headCache[i]
    // Head and cursor are compared as sampled at the same instant. Reading the live cursor against
    // a head cached up to `headTtlMs` ago would count everything the chain produced in between as
    // "behind" — on a sub-second chain that skew alone exceeds the whole allowance.
    if (sample?.cursorAt == null) return false
    const head = sample.value

    // Dropped for not making progress: it has to be level with the pipe again before we go back,
    // or we would stall, fail over and crawl back in a loop. Being *level* is enough — demanding
    // it be ahead of a tip-following source is a bar nothing can clear.
    if (this.#leftUnproductive[i]) return head == null || head < sample.cursorAt

    // Dropped for a fault: only a structural gap disqualifies it, never ordinary ingestion jitter.
    const allowance = this.#detection.maxLagBlocks ?? DEFAULT_BEHIND_ALLOWANCE
    return head != null && head + allowance < sample.cursorAt
  }

  /** The *stalled* verdict: unproductive wait past the window, optionally plus an open request. */
  #isStale(outstandingMs = 0): boolean {
    return (
      this.#detection.maxStalenessMs != null && this.#unproductiveMs + outstandingMs > this.#detection.maxStalenessMs
    )
  }

  /**
   * Consult the strategy from inside the read loop. A strategy that throws is the same class of
   * programmer error as one returning `abort` or a bad index — the supervisor's own catch would
   * otherwise blame the source and re-drive it forever.
   */
  #decideInStream(event: FallbackEvent, cursor: BlockCursor | undefined): FallbackCommand {
    try {
      return this.#decide(event, cursor)
    } catch (e) {
      throw new StreamAbort(e)
    }
  }

  /** Consult the strategy (custom first, default for unanswered events). */
  #decide(event: FallbackEvent, cursor: BlockCursor | undefined, allDownMs?: number): FallbackCommand {
    const ctx: FallbackStrategyContext = {
      event,
      activeIndex: this.activeIndex,
      sources: this.#sources.map((s, i) => ({
        index: i,
        name: s.name,
        health: this.#health[i].state,
        active: this.activeIndex === i,
        cause: this.#health[i].cause,
        head: this.#headCache[i]?.value,
        behind: this.#isBehind(i),
      })),
      cursor,
      lagBlocks: this.lag,
      atTip: this.#lagArmed,
      allDownMs,
    }

    // The stock decision is always computed first and handed to the custom strategy as
    // `ctx.defaultCommand`, so it can inspect/veto it instead of re-deriving the algorithm.
    const defaultCommand = this.#defaultStrategy(ctx) ?? { action: 'hold' }
    if (!this.#strategy) return defaultCommand

    ctx.defaultCommand = defaultCommand
    return this.#strategy(ctx) ?? defaultCommand
  }

  /**
   * Pick the source to drive: consult the strategy, waiting out an all-down gap (`hold`) with
   * `allDownPollMs` naps — cooldown expiry and head polls make recovery visible between naps.
   */
  async #selectSource(lastError: SourceErrorInfo | undefined, cursor: BlockCursor | undefined): Promise<number> {
    let allDownSince: number | undefined

    while (true) {
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
      await sleep(this.#detection.allDownPollMs)
    }
  }

  #assertSourceIndex(index: number, fromStream = false): void {
    if (Number.isInteger(index) && index >= 0 && index < this.#sources.length) return

    const error = new Error(
      `fallback strategy selected source index ${index}, but there are only ${this.#sources.length} sources`,
    )
    // Raised inside the read loop it would look like a source failure and be retried forever;
    // it is a programmer error and has to reach the caller.
    throw fromStream ? new StreamAbort(error) : error
  }

  /** Refresh other-source heads and the lag gauges, and produce the boundary event's verdict. */
  async #observeBoundary(
    active: number,
    cursor: BlockCursor | undefined,
  ): Promise<Extract<FallbackEvent, { type: 'batch' }>> {
    const staleAtBoundary = this.#isStale()
    // Boundary head polls feed the lag verdict, a custom strategy's snapshot, the
    // stale-vs-chain-stall distinction, and the reclaim of a recovered more-preferred source.
    // When none of those can consume them — lag detection off, not stale, stock strategy already
    // driving the most-preferred source — the poll is pure standby traffic, and at tip pace the
    // batch cadence outruns `headTtlMs`, so this gate (not the cache) is what bounds it: one
    // `eth_getBlockByNumber`-class call per block per standby otherwise.
    const wantHeads = this.#strategy != null || this.#detection.maxLagBlocks != null || staleAtBoundary || active > 0
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
        this.#lagArmed = true // arm at tip (latched)
      }
    }

    // The detection verdicts ride on the event: the strategy decides what to do about them. A
    // boundary carries `stale` as well as `lagging`, because a source can keep answering without
    // making progress (empty batches at a finality frontier) — that never reaches the stall ticker.
    const lagging =
      this.#detection.maxLagBlocks != null &&
      this.#lagArmed &&
      this.lag != null &&
      this.lag > this.#detection.maxLagBlocks
    this.staleness = this.#unproductiveMs
    this.chainStalled = staleAtBoundary && !this.#fresherThanCursor(cursor)

    return { type: 'batch', lagging, stale: staleAtBoundary }
  }

  /**
   * Is any other *eligible* source known to be ahead of where we are? Unhealthy sources are
   * excluded: their cached head is not refreshed while they are out (only the head-poll failure
   * path clears it), so a source poisoned by a capability probe would otherwise keep looking
   * fresher forever — and failing the active source over towards a source the selector will refuse
   * to pick just walks the pipe into an all-down gap.
   */
  #fresherThanCursor(cursor: BlockCursor | undefined): boolean {
    const lastNumber = cursor?.number ?? -1
    return this.#sources.some((_, i) => {
      const head = this.#headCache[i]?.value
      return i !== this.activeIndex && this.#health[i].state !== 'unhealthy' && head != null && head > lastNumber
    })
  }

  /** Why the active source was failed over mid-request — a real stall, or the strategy's call. */
  #stallFailoverCause(): SourceErrorInfo {
    if (this.#stallWasStale) {
      return freshnessFailure('stream', 'stale', 'no batch progress while a fresher source was ahead')
    }

    return strategyFailure('failover decided by the fallback strategy while a request was outstanding')
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

  /**
   * A head poll, time-boxed by `headPollTimeoutMs`. The poll is `await`ed on the batch-critical path
   * (lag check, stall hold, switch-up), so a sick standby whose `getHead` hangs — TCP up, no
   * response — must not stall the healthy active source: on timeout this rejects and `#getCachedHead`
   * records a liveness failure. `null` defers to the underlying client's own request timeout.
   */
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
    const now = this.#detection.clock()
    const cached = this.#headCache[i]
    if (cached && now - cached.at < this.#detection.headTtlMs) return cached.value

    try {
      // Poll each source at its OWN commitment (a finalized-only portal reports its finalized
      // head, a hot source the chain tip) — the reference we want is "how far can this source
      // serve", which is what both the lag and the "is anything fresher" checks mean. When the
      // stream is forced to a single commitment, that wins for every source.
      //
      // Only the NUMBER is consumed here, so a client offering the cheaper number-only poll
      // (`eth_blockNumber` on an RPC source, vs a full block-header lookup) is preferred.
      const client = this.#sources[i].client
      const headOptions = this.#streamFinalized != null ? { finalized: this.#streamFinalized } : undefined
      const value = client.getHeight
        ? await this.#headWithTimeout(client.getHeight(headOptions))
        : (await this.#headWithTimeout(client.getHead(headOptions)))?.number
      this.#headCache[i] = { value, at: now, cursorAt: last?.number }
      if (value != null) this.#health[i].onLivenessPass()
      this.#maybeProbeCapability(i, last)
      return value
    } catch (e) {
      this.#headCache[i] = { value: undefined, at: now, cursorAt: last?.number }
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
    const probe = this.#probes[i]
    if (!probe || this.#health[i].capabilityConfirmed || this.#capabilityProbing[i]) return

    const now = this.#detection.clock()
    if (now - (this.#lastProbeAt[i] ?? 0) < this.#detection.capabilityProbeIntervalMs) return

    const generation = this.#streamGeneration
    this.#lastProbeAt[i] = now
    this.#capabilityProbing[i] = true
    // `Promise.resolve().then(...)` normalizes a *synchronously*-throwing probe into a rejection,
    // so the throw can't escape this method (stranding `#capabilityProbing[i]` at `true` and
    // blocking all future probes for the source).
    Promise.resolve()
      .then(() => probe(last))
      .then(
        (r) => {
          if (generation !== this.#streamGeneration) return // answers a stream that has ended
          if (r.ok) {
            this.#health[i].onLivenessPass()
            this.#health[i].onCapability(true)
          } else {
            this.#failSource(i, r.cause ?? classifyError('capability', new Error('probe reported not-capable')))
          }
        },
        (e) => {
          if (generation !== this.#streamGeneration) return
          this.#failSource(i, classifyError('capability', e))
        },
      )
      .finally(() => {
        // Only release the slot this generation claimed; a later stream's probe owns its own.
        if (generation === this.#streamGeneration) this.#capabilityProbing[i] = false
      })
  }

  /**
   * `iterator.next()` under the stall clock. While the request is outstanding, a ticker fires every
   * `freshnessTickMs`; each tick refreshes the other sources' heads (TTL-bounded), updates the
   * staleness gauges, and consults the strategy with a `stall` event — `failover` resolves to
   * {@link FAILOVER}, anything else keeps waiting. Disabled (plain `next()`) when `maxStalenessMs`
   * is null and no custom strategy is installed.
   */
  async #nextWithStallTicks(
    iterator: AsyncIterator<StreamData<any>>,
    active: number,
    cursor?: BlockCursor,
  ): Promise<IteratorResult<StreamData<any>> | typeof FAILOVER> {
    if (this.#detection.maxStalenessMs == null && !this.#strategy) {
      this.staleness = 0
      return iterator.next()
    }

    // The clock starts when the request goes out and stops when it settles, so time the *consumer*
    // spends between yields is never counted against the source.
    const start = this.#detection.clock()
    const nextP = iterator.next()
    nextP.catch(() => {}) // a later abandon must not surface as an unhandled rejection
    const settled = nextP.then(
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
        this.#unproductiveMs = delivered ? 0 : this.#unproductiveMs + (this.#detection.clock() - start)
        this.staleness = this.#unproductiveMs
        this.chainStalled = false
        return r.v
      }
      if (r.type === 'error') {
        this.#unproductiveMs = 0
        this.staleness = 0
        this.chainStalled = false
        throw r.e
      }

      const outstandingMs = this.#detection.clock() - start
      const pendingMs = this.#unproductiveMs + outstandingMs
      this.staleness = pendingMs
      const stale = this.#isStale(outstandingMs)

      // Re-polling the other sources both feeds the stall decision and (re)probes their
      // liveness/capability, so a held source keeps noticing when the chain comes back. Polls are
      // gated on the stalled verdict (a healthy-but-slow request should not fan out head queries)
      // — except under a custom strategy, which may want fresh snapshots on every tick; the
      // `headTtlMs` cache bounds the poll rate either way.
      const others = stale || this.#strategy ? await this.#chainHeadOthers(active, cursor) : undefined
      const lastNumber = cursor?.number ?? -1
      const fresherAhead = others != null && others > lastNumber
      // Observability: every source is stuck at the same head — switching would not help.
      this.chainStalled = stale && !fresherAhead

      const command = this.#decideInStream({ type: 'stall', pendingMs, stale }, cursor)
      if (command.action === 'failover') {
        this.#stallWasStale = stale
        this.staleness = 0
        return FAILOVER
      }
      if (command.action === 'abort') {
        this.#clearActive()
        throw new StreamAbort(command.error ?? new AllSourcesDownError())
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
    if (cause.check === 'stream' && cause.reason === 'stale') this.#leftUnproductive[i] = true
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
      // The freshness gauges describe the *active* source; on a switch the previous source's
      // values are stale, so clear them until the new source's next batch/head poll repopulates.
      this.#unproductiveMs = 0
      this.lag = undefined
      this.staleness = 0
      this.chainStalled = false
      this.chainHead = undefined
    }
    this.#lastActive = i
    this.activeIndex = i
  }

  /** No source is being driven: report no active source and clear the per-source gauges. */
  #clearActive(): void {
    this.activeIndex = undefined
    this.#unproductiveMs = 0
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
