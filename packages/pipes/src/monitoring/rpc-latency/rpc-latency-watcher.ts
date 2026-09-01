import { createTransformer } from '~/core/index.js'
import { arrayify, last } from '~/internal/array.js'

// `hash` is optional because Solana's `slotsUpdatesSubscribe` ships only `{ slot, timestamp }`
// — the hash would require a follow-up `getBlock(slot)` round-trip we don't want on the
// hot path. EVM (`eth_subscribe newHeads`) and Bitcoin (`getbestblockhash`) both populate it.
type RpcHead = { number: number; hash?: string; timestamp: Date; receivedAt: Date }

/** Lookups only move forward, so older heads are dead weight. */
const MAX_HEADS_PER_NODE = 512

/**
 * How long a head the reference RPC has not reported yet is held before it is closed as
 * `rpc-behind`. This is the horizon of the measurement: a portal lead longer than the window
 * is reported as "ahead by at least this much" instead of an exact figure.
 */
const DEFAULT_RESOLVE_TIMEOUT_MS = 60_000

export interface RpcLatencyListener {
  stop(): void
}

export abstract class RpcLatencyWatcher {
  nodes: Map<string, Map<number, RpcHead>> = new Map()
  watchers: RpcLatencyListener[] = []
  #highest: Map<string, number> = new Map()
  #running = false

  constructor(protected rpcUrl: string | string[]) {
    this.rpcUrl = arrayify(rpcUrl)
  }

  /**
   * Subscribes each URL via `watch()`. Idempotent and re-runnable after `stop()`:
   * the stream stops and starts transformers around every restart, and a one-way
   * stop would leave `lookup()` empty forever while the stream kept indexing.
   *
   * Driven by the transformer, not a subclass constructor, so `watch()` always
   * sees initialized subclass fields.
   */
  start() {
    if (this.#running) return
    this.#running = true

    for (const url of arrayify(this.rpcUrl)) {
      if (!this.nodes.has(url)) {
        this.nodes.set(url, new Map())
      }

      this.watchers.push(this.watch(url))
    }
  }

  stop() {
    if (!this.#running) return
    this.#running = false

    for (const listener of this.watchers) {
      listener.stop()
    }

    this.watchers = []
  }

  lookup(number: number) {
    const res: { url: string; hash?: string; timestamp: Date; receivedAt: Date }[] = []

    for (const [url, blocks] of this.nodes) {
      const block = blocks.get(number)

      if (block) {
        res.push({
          url,
          hash: block.hash,
          timestamp: block.timestamp,
          receivedAt: block.receivedAt,
        })
      }
    }

    return res
  }

  /**
   * Whether `number` can still arrive from this node. A node already past that head — or one
   * that has reported nothing at all — never will, so a sample waiting on it is closed at once
   * rather than sitting out the whole resolve window.
   */
  mayObserve(url: string, number: number) {
    const highest = this.#highest.get(url)

    return highest !== undefined && highest < number
  }

  addBlock(url: string, block: RpcHead) {
    const chain = this.nodes.get(url)
    if (!chain) throw new Error('RPC not found')

    chain.set(block.number, block)

    const highest = this.#highest.get(url)
    if (highest === undefined || block.number > highest) {
      this.#highest.set(url, block.number)
    }

    // Insertion order is ascending, so the first key is the oldest. Evicting here
    // rather than on portal batches holds the bound while the portal side is stalled.
    while (chain.size > MAX_HEADS_PER_NODE) {
      const oldest = chain.keys().next()
      if (oldest.done) break

      chain.delete(oldest.value)
    }
  }

  abstract watch(url: string): RpcLatencyListener
}

export type RpcObservation = {
  url: string
  /** Block hash as observed by this RPC. Omitted on Solana (no hash on slot updates). */
  hash?: string
  receivedAt?: Date
  /**
   * Signed `portal.receivedAt - rpc.receivedAt`. **Negative means the portal delivered the head
   * first.** Absent whenever `unresolved` is set — treating a missing value as zero would fold
   * the two censored cases back into the distribution.
   */
  portalDelayMs?: number
  /**
   * Set when no delay could be computed:
   * - `rpc-behind` — the RPC had not reached this head before the resolve window closed, so the
   *   portal is ahead by at least that window. A real, one-sided latency observation.
   * - `rpc-missing` — the RPC is past this head but never recorded it: reorged away, a dropped
   *   notification, a head evicted while the portal was backfilling, or a node that never
   *   reported anything. Carries no latency information; count it, don't chart it.
   */
  unresolved?: 'rpc-behind' | 'rpc-missing'
}

export type LatencySample = {
  number: number
  timestamp: Date
  portal: {
    receivedAt: Date
  }
  rpc: RpcObservation[]
}

type PendingHead = {
  number: number
  timestamp: Date
  portalReceivedAt: Date
}

export function rpcLatencyWatcher({
  watcher,
  resolveTimeoutMs = DEFAULT_RESOLVE_TIMEOUT_MS,
}: {
  watcher: RpcLatencyWatcher
  /** How long to wait for a reference RPC to report a head before recording it as `rpc-behind`. */
  resolveTimeoutMs?: number
}) {
  /**
   * Portal-side heads still waiting on the reference RPCs.
   *
   * Sampling only what the RPCs happened to hold at delivery time silently dropped every head
   * the portal won, so the delay distribution was truncated at zero and its tail described the
   * reference node rather than the portal. Buffering both sides and emitting once the join can
   * be decided keeps those samples — with a negative delay.
   */
  const pending = new Map<number, PendingHead>()

  function resolve(head: PendingHead, now: number): LatencySample | undefined {
    const observed = new Map(watcher.lookup(head.number).map((r) => [r.url, r]))
    const expired = now - head.portalReceivedAt.getTime() >= resolveTimeoutMs

    const rpc: RpcObservation[] = []

    for (const url of watcher.nodes.keys()) {
      const hit = observed.get(url)

      if (hit) {
        rpc.push({
          url,
          hash: hit.hash,
          receivedAt: hit.receivedAt,
          portalDelayMs: head.portalReceivedAt.getTime() - hit.receivedAt.getTime(),
        })
      } else if (!watcher.mayObserve(url, head.number)) {
        rpc.push({ url, unresolved: 'rpc-missing' })
      } else if (expired) {
        rpc.push({ url, unresolved: 'rpc-behind' })
      } else {
        return
      }
    }

    return {
      number: head.number,
      timestamp: head.timestamp,
      portal: { receivedAt: head.portalReceivedAt },
      rpc,
    }
  }

  return createTransformer<
    {
      header: { number: number; timestamp: number }
    }[],
    LatencySample[]
  >({
    profiler: { name: 'rpc latency' },
    start() {
      watcher.start()
    },
    transform: (data, ctx): LatencySample[] => {
      // Never started, or configured with no endpoints: every sample would be an empty join.
      if (watcher.nodes.size === 0) {
        return []
      }

      const block = last(data)

      // `lastBlockReceivedAt` timestamps the batch's last block, so that is the only head this
      // batch can measure. A queued head keeps its first receipt — re-stamping would move the
      // freshness reading and push the resolve deadline out indefinitely.
      if (block && !pending.has(block.header.number)) {
        pending.set(block.header.number, {
          number: block.header.number,
          timestamp: new Date(block.header.timestamp * 1000),
          portalReceivedAt: ctx.batch.lastBlockReceivedAt,
        })
      }

      // Flush is batch-driven — the transformer has no way to emit out of band — so a sample
      // surfaces one portal batch after it becomes decidable. The delay it carries is computed
      // from stored timestamps and is unaffected.
      const now = Date.now()
      const resolved: LatencySample[] = []

      for (const [number, head] of pending) {
        const sample = resolve(head, now)
        if (!sample) {
          continue
        }

        pending.delete(number)
        resolved.push(sample)
      }

      return resolved
    },
    rollback: (cursor) => {
      // Heads above the fork point are gone; waiting on them would time out as `rpc-behind`
      // and read as a portal lead that never happened.
      for (const number of pending.keys()) {
        if (number > cursor.number) {
          pending.delete(number)
        }
      }
    },
    stop() {
      pending.clear()
      watcher.stop()
    },
  })
}
