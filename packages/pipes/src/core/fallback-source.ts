import { isForkException } from '~/portal-client/index.js'

import { ForkCursorMissingError, MissingForkAncestorError, TargetForkNotSupportedError } from './errors.js'
import { safeReturn, withTimeout } from './fallback-async.js'
import type { ProbeResult } from './fallback-capability.js'
import { SourceErrorInfo, capabilityFailure, classifyError, freshnessFailure } from './fallback-diagnostics.js'
import {
  AllSourcesDownError,
  FallbackHealth,
  FallbackPolicy,
  ResolvedFallbackPolicy,
  Selector,
  SourceHealth,
  resolveFallbackPolicy,
} from './fallback-health.js'
import { FinalizedWatermark } from './finalized-watermark.js'
import { Logger, defaultLogger } from './logger.js'
import { PortalBatch } from './portal-source.js'
import { Target, TargetState } from './target.js'
import { BlockCursor } from './types.js'

/**
 * One ranked underlying source. `read(cursor)` must yield `PortalBatch`es starting just after
 * `cursor` and throw a `ForkException` when the chain diverges at the resume point — exactly the
 * contract `PortalStream` already satisfies, so a Portal stream (or, later, an RPC stream) drops
 * straight in.
 */
export interface FallbackUnderlyingSource<T> {
  name: string
  read: (cursor?: BlockCursor) => AsyncIterable<PortalBatch<T>>
  /**
   * Full, infrequent capability probe — verifies the source can still serve the query's data.
   * `atCursor` is the indexing frontier (last committed cursor); the probe should confirm the
   * source can serve the configured data just past it and resolve not-`ok` (with a cause) if it
   * cannot.
   */
  probeCapability?: (atCursor?: BlockCursor) => Promise<ProbeResult>
  /**
   * Optional independent chain-head query (not tied to the stream). When present, it powers
   * staleness/lag detection — failing a stalled or far-behind source over to one that is ahead —
   * and serves as a cheap liveness signal for standby sources. Without it, a source still works but
   * gets neither (a silently-stalled source can only be left once it errors). `PortalStream` and the
   * RPC source both provide it.
   */
  getHead?: () => Promise<BlockCursor | undefined>
}

/** Returned by the staleness-aware fetch when the active source must be failed over. */
const STALE = Symbol('stale')

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function delay(ms: number): { promise: Promise<void>; cancel: () => void } {
  let timer: ReturnType<typeof setTimeout>
  const promise = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, ms)
  })
  return { promise, cancel: () => clearTimeout(timer) }
}

/** A structured snapshot of the fallback's observable state, for a metrics surface (§4). */
export interface FallbackMetrics {
  activeIndex: number | undefined
  switchCount: number
  /** Blocks the active source is behind the independent head; ms its current request has been pending. */
  lag: number
  staleness: number
  chainHead: number | undefined
  /** Set when every source is stuck at the same head (no fresher alternative to switch to). */
  chainStalled: boolean
  sources: { name: string; health: FallbackHealth; active: boolean; cause?: SourceErrorInfo }[]
}

/**
 * A meta-source over an ordered list of sources. It drives the lowest-index healthy (or
 * optimistically `unknown`) source and, on a non-fork error, resumes the next source from the
 * last committed cursor. A `ForkException` is propagated untouched so a fork straddling a switch
 * is handled by the same `pipeTo` rewind path as an ordinary reorg.
 *
 * Like `PortalStream`, it owns the single monotonic finalized high-watermark for the pipe (the
 * targets no longer do): it seeds the floor from the target's persisted finalized head and clamps
 * every batch's finalized head through it before yielding. This is what makes a source *switch*
 * safe — a new source reporting a deeper/transiently-missing finalized head can never un-finalize
 * already-committed data.
 *
 * Drop-in for a `PortalStream`: it exposes the same `AsyncIterable<PortalBatch<T>>` + `pipeTo`.
 */
export class FallbackSource<T> {
  readonly #sources: FallbackUnderlyingSource<T>[]
  readonly #policy: ResolvedFallbackPolicy
  readonly #health: SourceHealth[]
  readonly #selector: Selector
  readonly #logger: Logger
  /** Single monotonic finalized high-watermark for the whole pipe, seeded from the target's floor. */
  readonly #watermark = new FinalizedWatermark()

  /** Observable state (for metrics). */
  activeIndex: number | undefined
  switchCount = 0
  /** Freshness gauges (only populated for sources that implement `getHead`). */
  lag = 0
  staleness = 0
  chainHead: number | undefined
  /** Set when every source is stuck at the same head (no fresher alternative to switch to). */
  chainStalled = false

  /** Guards against firing a second capability probe for a source while one is still in flight. */
  readonly #capabilityProbing: boolean[] = []
  /** Clock of the last capability probe per source — throttles the (full-query) standby probe. */
  readonly #lastProbeAt: number[] = []
  /** Cached independent head per source, with the clock it was fetched at (TTL `headTtlMs`). */
  readonly #headCache: ({ value: number | undefined; at: number } | undefined)[] = []
  /** Lag failover arms only once the tip is first reached, so a deep backfill never trips it. */
  #lagArmed = false
  /** The last source actually driven — survives the all-down gap so switch counting stays correct. */
  #lastActive: number | undefined

  constructor(sources: FallbackUnderlyingSource<T>[], policy?: FallbackPolicy, logger?: Logger) {
    if (sources.length === 0) {
      throw new Error('FallbackSource requires at least one source')
    }
    this.#sources = sources
    this.#policy = resolveFallbackPolicy(policy)
    this.#health = sources.map((s) => new SourceHealth(this.#policy, !!s.probeCapability))
    this.#selector = new Selector(this.#health)
    this.#logger = logger ?? defaultLogger({ id: 'fallback' })
  }

  /** The supervisor: switches sources internally; only `ForkException` (and completion) escape. */
  async *read(cursor?: BlockCursor): AsyncIterable<PortalBatch<T>> {
    let last = cursor
    let allDownSince: number | undefined

    // Re-arm the lag trigger per stream: a reused instance starting a later (far-behind-head)
    // backfill must not inherit "reached the tip" from a previous run and false-fire on lag.
    this.#lagArmed = false

    while (true) {
      const active = this.#selector.pickForFailover()
      if (active === undefined) {
        this.#clearActive()
        if (await this.#waitAllDown(allDownSince ?? (allDownSince = this.#policy.clock()))) continue

        throw new AllSourcesDownError()
      }
      allDownSince = undefined
      this.#setActive(active)

      try {
        const iterator = this.#sources[active].read(last)[Symbol.asyncIterator]()
        try {
          while (true) {
            const next = await this.#nextWithStaleness(iterator, active, last)
            if (next === STALE) {
              // Source stopped delivering while a fresher source is ahead.
              this.#failSource(
                active,
                freshnessFailure('stream', 'stale', 'no batch progress while a fresher source was ahead'),
              )
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

            // Clamp the source's finalized head through the shared monotonic watermark before it
            // reaches the target, so a source switch reporting a deeper/transiently-missing finalized
            // head can never un-finalize already-committed data. (The rollback chain is left as the
            // source derived it; the clamp can only raise the head, so the chain stays a safe superset
            // of the unfinalized tail.)
            batch.ctx.stream.head.finalized = this.#watermark.clamp(batch.ctx.stream.head.finalized)

            yield batch
            last = batch.ctx.stream.state.current

            // Lag-based freshness: the active fell too far behind the independent head.
            if (await this.#laggingTooFar(active, last)) {
              this.#failSource(
                active,
                freshnessFailure(
                  'stream',
                  'lag',
                  `fell behind the chain head by more than ${this.#policy.maxLagBlocks} blocks`,
                ),
              )
              break
            }

            // Eager switch-up: reclaim a recovered higher-preference source at the batch boundary
            // (never mid-batch). Probe those candidates first so a recovered one can re-prove its
            // capability and reach `healthy` — switch-up only ever promotes to a `healthy` source.
            if (this.#policy.preferPrimary === 'eager') {
              await this.#probeHigherPreference(active, last)
              if (this.#selector.pickSwitchUp(active) !== undefined) break
            }
          }
        } finally {
          safeReturn(iterator)
        }
      } catch (e) {
        if (isForkException(e)) throw e // propagate; do NOT switch

        this.#failSource(active, classifyError('stream', e))
        // re-select and resume from `last` on the next iteration
      }
    }
  }

  pipeTo(target: Target<T>): Promise<void> {
    const self = this

    return target.write({
      logger: this.#logger,
      read: async function* (state?: TargetState) {
        // Seed the monotonic floor from the target's persisted finalized head so the watermark
        // survives an unclean restart mid-fork (null ⇒ no floor ⇒ no-finality passthrough). The
        // floor then persists in `self.#watermark` across fork re-invocations below.
        self.#watermark.seed(state?.finalized ?? undefined)

        let cursor = state?.latest
        while (true) {
          try {
            for await (const batch of self.read(cursor)) {
              yield batch
            }

            return
          } catch (e) {
            if (!isForkException(e)) throw e

            if (!e.canonicalBlocks.length) {
              throw new MissingForkAncestorError()
            }
            if (!target.resolveFork) {
              throw new TargetForkNotSupportedError()
            }

            const forked = await target.resolveFork(e.canonicalBlocks)
            if (!forked) {
              throw new ForkCursorMissingError()
            }

            cursor = forked
          }
        }
      },
    })
  }

  async *[Symbol.asyncIterator](): AsyncIterator<PortalBatch<T>> {
    yield* this.read()
  }

  /**
   * The highest head reported by the *other* eligible sources — an independent reference that avoids
   * the circular-lag trap (a source that stalls head and data together reads lag ≈ 0 against its own
   * head). Excludes `unhealthy` sources so a flagged-bad one can't define the tip, and sources
   * without `getHead` (they contribute no head). Heads are cached for `headTtlMs`.
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
   * (lag check, staleness hold, switch-up), so a sick standby whose `getHead` hangs — TCP up, no
   * response — must not stall the healthy active source: on timeout this rejects and `#getCachedHead`
   * records a liveness failure. `null` defers to the underlying client's own request timeout.
   */
  #headWithTimeout(p: Promise<BlockCursor | undefined>): Promise<BlockCursor | undefined> {
    const timeoutMs = this.#policy.headPollTimeoutMs
    return withTimeout(p, timeoutMs, () => new Error(`head poll timed out after ${timeoutMs}ms`))
  }

  /**
   * Poll a source's independent head (cached for `headTtlMs`). The poll doubles as a liveness probe
   * — a fresh head promotes a standby toward `healthy` — and is when we (re)fire its capability
   * probe. A source without `getHead` contributes no head, but still has its capability probe driven
   * here so a probe-only source can recover (for it, the probe also carries liveness).
   */
  async #getCachedHead(i: number, last?: BlockCursor): Promise<number | undefined> {
    const src = this.#sources[i]
    if (!src.getHead) {
      this.#maybeProbeCapability(i, last)
      return undefined
    }

    const now = this.#policy.clock()
    const cached = this.#headCache[i]
    if (cached && now - cached.at < this.#policy.headTtlMs) return cached.value

    try {
      const head = await this.#headWithTimeout(src.getHead())
      const value = head?.number
      this.#headCache[i] = { value, at: now }
      if (value != null) this.#health[i].onLivenessPass()
      this.#maybeProbeCapability(i, last)
      return value
    } catch (e) {
      this.#headCache[i] = { value: undefined, at: now }
      this.#failSource(i, classifyError('liveness', e))
      return undefined
    }
  }

  /**
   * Fire a source's optional capability probe once it is reachable, feeding the result into health.
   * A source that declares a `probeCapability` cannot become `healthy` on liveness alone — capability
   * must be confirmed — so without this it could never be switched up to. Fire-and-forget (a full
   * query slice must not block the boundary), throttled by `capabilityProbeIntervalMs`, and never
   * concurrent for the same source. The gating in {@link SourceHealth} drops the confirmation when a
   * source goes unhealthy, so it must re-prove before recovering.
   */
  #maybeProbeCapability(i: number, last?: BlockCursor): void {
    const probe = this.#sources[i].probeCapability
    if (!probe || this.#health[i].capabilityConfirmed || this.#capabilityProbing[i]) return

    const now = this.#policy.clock()
    if (now - (this.#lastProbeAt[i] ?? 0) < this.#policy.capabilityProbeIntervalMs) return

    this.#lastProbeAt[i] = now
    this.#capabilityProbing[i] = true
    // `Promise.resolve().then(() => probe(last))` normalizes a *synchronously*-throwing custom probe
    // into a rejection, so the throw can't escape this method (stranding `#capabilityProbing[i]` at
    // `true` and blocking all future probes for the source) — it flows to the rejection handler and
    // the `.finally` still clears the flag.
    Promise.resolve()
      .then(() => probe(last))
      .then(
        (r) => {
          if (r.ok) {
            this.#health[i].onLivenessPass()
            this.#health[i].onCapability(true)
          } else {
            this.#failSource(i, r.cause ?? capabilityFailure('probe reported not-capable'))
          }
        },
        (e) => this.#failSource(i, classifyError('capability', e)),
      )
      .finally(() => {
        this.#capabilityProbing[i] = false
      })
  }

  /**
   * Drive head/liveness/capability for the not-yet-active *higher-preference* sources, so a recovered
   * one can reach `healthy` and be reclaimed by eager switch-up. When the active source is the primary
   * (index 0) there are none, so this is a no-op on the hot path; it only does work after a failover —
   * exactly when a recovered primary needs noticing.
   */
  async #probeHigherPreference(active: number, last?: BlockCursor): Promise<void> {
    await Promise.all(
      this.#sources.map((_, i) =>
        i < active && this.#health[i].state !== 'unhealthy' ? this.#getCachedHead(i, last) : Promise.resolve(undefined),
      ),
    )
  }

  /**
   * `iterator.next()` with the source-pending staleness clock. While the request is outstanding, a
   * ticker checks how long it has been pending; past `maxStalenessMs` it fails the source over **iff**
   * a fresher source is ahead. If every source sits at the same stale head, it is a global chain
   * stall: hold the active source (re-arm and keep waiting + probing) and flag `chainStalled` rather
   * than churn through sources that are all equally stuck. Disabled (plain `next()`) when
   * `maxStalenessMs` is null.
   */
  async #nextWithStaleness(
    iterator: AsyncIterator<PortalBatch<T>>,
    active: number,
    last?: BlockCursor,
  ): Promise<IteratorResult<PortalBatch<T>> | typeof STALE> {
    if (this.#policy.maxStalenessMs == null) {
      this.staleness = 0
      return iterator.next()
    }

    const lastNumber = last?.number ?? -1
    let start = this.#policy.clock()
    const nextP = iterator.next()
    nextP.catch(() => {}) // a later abandon must not surface as an unhandled rejection
    const settled = nextP.then(
      (v) => ({ type: 'next' as const, v }),
      (e) => ({ type: 'error' as const, e }),
    )

    while (true) {
      const tick = delay(this.#policy.freshnessTickMs)
      const r = await Promise.race([settled, tick.promise.then(() => ({ type: 'tick' as const }))])
      tick.cancel()

      if (r.type === 'next') {
        this.staleness = 0
        this.chainStalled = false
        return r.v
      }
      if (r.type === 'error') {
        this.staleness = 0
        this.chainStalled = false
        throw r.e
      }

      const elapsed = this.#policy.clock() - start
      this.staleness = elapsed
      if (elapsed > this.#policy.maxStalenessMs) {
        // Re-polling the other sources both decides failover and (re)probes their liveness/capability,
        // so a held source keeps noticing when the chain comes back.
        const others = await this.#chainHeadOthers(active, last)
        if (others != null && others > lastNumber) {
          this.chainStalled = false
          return STALE
        }
        this.chainStalled = true
        start = this.#policy.clock() // hold; re-arm and keep waiting
      }
    }
  }

  /** Boundary-evaluated lag trigger (armed only once the tip is first reached). */
  async #laggingTooFar(active: number, last?: BlockCursor): Promise<boolean> {
    if (this.#policy.maxLagBlocks == null) return false

    const lastNumber = last?.number ?? -1
    const others = await this.#chainHeadOthers(active, last)
    this.chainHead = others != null ? Math.max(others, lastNumber) : lastNumber
    if (others == null) {
      this.lag = 0 // no independent reference ⇒ lag is not computable; don't report a stale value
      return false
    }

    const lag = others - lastNumber
    this.lag = Math.max(0, lag)
    // Arm only once we are genuinely at/near the tip: within the threshold *and* not ahead of the
    // reference (`lag >= 0`). A negative lag means the independent reference is itself behind us
    // (stale/lagging) — arming on that would let a later-recovering source's head trip a spurious
    // failover while we are still backfilling.
    if (lag >= 0 && lag <= this.#policy.maxLagBlocks) this.#lagArmed = true // arm at tip (latched)

    if (this.#lagArmed && lag > this.#policy.maxLagBlocks) {
      this.chainStalled = false
      return true
    }

    return false
  }

  /**
   * Feed a failure to a source's health (the `check` selects the signal), then log *why* — but only
   * when it actually flips the source unhealthy, so a log line always marks a real transition
   * (liveness fails are noisy until they trip the threshold). The bounded `reason`/`code`/`check`
   * also reach {@link metrics}; the full `detail` (incl. the request) is logged, never a label.
   */
  #failSource(i: number, cause: SourceErrorInfo): void {
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

  /** Returns true if it waited (retry), false if the all-down timeout elapsed. */
  async #waitAllDown(since: number): Promise<boolean> {
    if (this.#policy.allDownTimeoutMs != null && this.#policy.clock() - since >= this.#policy.allDownTimeoutMs) {
      return false
    }
    await sleep(this.#policy.allDownPollMs)

    return true
  }

  #setActive(i: number): void {
    // Count a switch against the last source we drove (not `activeIndex`, which is cleared to
    // `undefined` during an all-down gap) so resuming on a *different* source after the gap still
    // registers, and resuming on the *same* one does not.
    if (this.#lastActive !== undefined && this.#lastActive !== i) {
      this.switchCount++
      // The freshness gauges describe the *active* source; on a switch the previous source's
      // values are stale, so clear them until the new source's next batch/head poll repopulates.
      this.lag = 0
      this.staleness = 0
      this.chainStalled = false
      this.chainHead = undefined
    }
    this.#lastActive = i
    this.activeIndex = i
  }

  /** No source is eligible (all unhealthy): nothing is being driven, so report no active source. */
  #clearActive(): void {
    this.activeIndex = undefined
    // The freshness gauges describe the active source; with none, they would otherwise keep
    // reporting the last source's lag/staleness/stall, so clear them for the all-down gap.
    this.lag = 0
    this.staleness = 0
    this.chainStalled = false
    this.chainHead = undefined
  }

  /** Snapshot of the observable state for export to a metrics surface (§4). */
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
