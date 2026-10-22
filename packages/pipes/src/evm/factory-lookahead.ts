import { Logger } from '~/core/logger.js'
import { PlanRequestArgs, PlannedRequest } from '~/core/query-builder.js'
import { HttpError } from '~/http-client/index.js'
import { PortalClient } from '~/portal-client/client.js'
import { DataRequest, Log, LogRequest } from '~/portal-client/query/evm.js'

import { Factory } from './factory.js'

/** How often the scan reads the finalized head once it has caught up with it. */
const FINALIZED_HEAD_POLL_MS = 5_000

/** The main stream may be waiting for the scan, so its progress is not held back for long. */
const DISCOVERY_MAX_WAIT_MS = 1_000

const DISCOVERY_FIELDS = {
  block: { number: true, hash: true },
  log: { address: true, topics: true, data: true, transactionIndex: true, logIndex: true },
} as const

/**
 * A child value the portal accepts in an address filter. Any other value matches no log address,
 * so it is left out of the filter instead of making the portal refuse the query.
 */
function isAddress(value: string) {
  return /^0x[0-9a-f]{40}$/.test(value)
}

export type DiscoveredChild = {
  /** Lowercased child address */
  address: string
  /** The block of its creation event */
  block: number
}

export type SegmentDecision =
  | { kind: 'filter'; to: number; addresses: string[] }
  | { kind: 'full-scan'; reason: string }
  | { kind: 'wait' }

/**
 * Decides how the main stream fetches child events from block `from` on.
 *
 * A filter for blocks `from…to` is correct when it holds every child the decoder can accept there:
 * all stored children, and every child created up to `to`. The scan has seen every child created
 * from its start up to `frontier`, so a filter can never reach past it, nor past the finalized head
 * the scan stops at.
 *
 * @internal
 */
export function decideSegment({
  from,
  to,
  frontier,
  finalizedHead,
  stored,
  discovered,
  requestsPerQuery,
  maxAddresses,
  minSegmentBlocks,
  previousSegment,
}: {
  from: number
  /** The end of the range the stream is in. Absent for an open range. */
  to?: number
  /** The scan has seen every factory event from its start up to here */
  frontier: number
  finalizedHead: number
  /** Children in the factory database */
  stored: string[]
  /** Children the scan found, in block order */
  discovered: readonly DiscoveredChild[]
  /** How many child-event requests repeat the address list */
  requestsPerQuery: number
  maxAddresses: number
  minSegmentBlocks: number
  /** Length of the last filtered segment, 0 before the first */
  previousSegment: number
}): SegmentDecision {
  const rangeEnd = to ?? Number.POSITIVE_INFINITY
  const ceiling = Math.min(rangeEnd, finalizedHead)

  const nearHead = ceiling < rangeEnd && ceiling - from + 1 < minSegmentBlocks
  if (nearHead) {
    return { kind: 'full-scan', reason: 'it is close to the finalized head' }
  }

  const fits = (count: number) => count * requestsPerQuery <= maxAddresses
  const tooMany = { kind: 'full-scan', reason: `its children no longer fit into ${maxAddresses} addresses` } as const

  const addresses = new Set(stored)
  if (!fits(addresses.size)) return tooMany

  // The address budget ends right before the block of the first child that does not fit
  const reachable = Math.min(frontier, ceiling)
  let budgetEnd = Number.POSITIVE_INFINITY
  for (const child of discovered) {
    if (child.block > reachable) break
    if (addresses.has(child.address)) continue

    if (!fits(addresses.size + 1)) {
      budgetEnd = child.block - 1
      break
    }

    addresses.add(child.address)
  }

  if (budgetEnd < from) return tooMany

  const end = Math.min(reachable, budgetEnd)
  const lead = end - from + 1
  if (lead < 1) return { kind: 'wait' }

  // Sorted, so the same children always make the same query
  const filter = (): SegmentDecision => ({ kind: 'filter', to: end, addresses: [...addresses].sort() })
  if (end === rangeEnd) return filter()

  // Waiting for the scan cannot make the segment longer
  const budgetBound = budgetEnd <= reachable
  const caughtUp = frontier >= ceiling
  if (budgetBound || caughtUp) {
    if (lead >= minSegmentBlocks) return filter()

    return budgetBound ? tooMany : { kind: 'full-scan', reason: 'it is close to the finalized head' }
  }

  // Each filter covers at least twice the blocks of the previous one, so a run changes it only a
  // logarithmic number of times even when the scan is barely faster than the main stream
  const required = Math.max(minSegmentBlocks, 2 * previousSegment)
  if (lead >= required) return filter()

  return { kind: 'wait' }
}

/** The finalized head, or `undefined` for a dataset that does not finalize blocks. */
async function readFinalizedHead(portal: PortalClient, abort?: AbortSignal): Promise<number | undefined> {
  try {
    const head = await portal.getHead({ finalized: true, abort })

    return head?.number
  } catch (error) {
    if (error instanceof HttpError && error.response.status === 404) return undefined

    throw error
  }
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve()

    const timer = setTimeout(done, ms)
    // Polling is never a reason to keep the process alive
    timer.unref?.()
    signal.addEventListener('abort', done, { once: true })

    function done() {
      clearTimeout(timer)
      signal.removeEventListener('abort', done)
      resolve()
    }
  })
}

/**
 * Scans the factory-creation events of finalized blocks from `start` on, ahead of the main stream.
 * A finalized block cannot be reorganized away, so a child found here is never taken back.
 *
 * @internal
 */
export class FactoryDiscovery {
  readonly start: number
  readonly #factory: Factory<any>
  readonly #portal: PortalClient
  readonly #abort = new AbortController()
  readonly #children: DiscoveredChild[] = []
  readonly #seen = new Set<string>()

  readonly #maxChildren: number

  #frontier: number
  #finalizedHead: number
  #until: number
  #error: unknown = undefined
  #waiters: (() => void)[] = []
  #closeScan?: () => void

  constructor({
    factory,
    portal,
    start,
    finalizedHead,
    until,
    maxChildren,
  }: {
    factory: Factory<any>
    portal: PortalClient
    start: number
    finalizedHead: number
    /** The last block anyone needs children for */
    until: number
    /** More children than this fit into no filter, so the scan stops at the block where it finds them */
    maxChildren: number
  }) {
    this.start = start
    this.#factory = factory
    this.#portal = portal
    this.#maxChildren = maxChildren
    this.#frontier = start - 1
    this.#finalizedHead = finalizedHead
    this.#until = until

    this.#run().catch((error) => {
      if (this.#abort.signal.aborted) return

      this.#error = error
      this.#notify()
    })
  }

  /** Every factory event in `start…frontier` has been seen. */
  get frontier() {
    return this.#frontier
  }

  get finalizedHead() {
    return this.#finalizedHead
  }

  /** Children found so far, in block order. */
  get children(): readonly DiscoveredChild[] {
    return this.#children
  }

  get error() {
    return this.#error
  }

  /** Resolves on the next progress, finalized head change, failure or stop. */
  changed(): Promise<void> {
    return new Promise((resolve) => this.#waiters.push(resolve))
  }

  extend(until: number) {
    this.#until = Math.max(this.#until, until)
  }

  stop() {
    if (this.#abort.signal.aborted) return

    this.#abort.abort()
    this.#closeScan?.()
    this.#error ??= new Error('the factory scan was stopped')
    this.#notify()
  }

  #notify() {
    const waiters = this.#waiters
    this.#waiters = []

    for (const resolve of waiters) {
      resolve()
    }
  }

  get #saturated() {
    return this.#children.length > this.#maxChildren
  }

  async #run() {
    const signal = this.#abort.signal

    while (!signal.aborted && !this.#saturated) {
      const target = Math.min(this.#finalizedHead, this.#until)

      if (this.#frontier < target) {
        const before = this.#frontier
        await this.#scan(this.#frontier + 1, target)

        // A stream that ended short of its target is retried at once only if it moved
        if (this.#frontier > before) continue
      }

      await sleep(FINALIZED_HEAD_POLL_MS, signal)
      if (signal.aborted) return

      const head = await readFinalizedHead(this.#portal, signal)
      if (head === undefined) {
        throw new Error('the dataset stopped reporting a finalized head')
      }

      if (head > this.#finalizedHead) {
        this.#finalizedHead = head
        this.#notify()
      }
    }
  }

  async #scan(from: number, to: number) {
    const query = {
      type: 'evm' as const,
      fromBlock: from,
      toBlock: to,
      fields: DISCOVERY_FIELDS,
      logs: [this.#factory.buildFactoryEventRequest()],
    }

    const iterator = this.#portal
      .getStream(query, {
        finalized: true,
        headPollIntervalMs: FINALIZED_HEAD_POLL_MS,
        maxWaitTimeMs: DISCOVERY_MAX_WAIT_MS,
      })
      [Symbol.asyncIterator]()
    this.#closeScan = () => {
      iterator.return?.().catch(() => {})
    }

    try {
      while (true) {
        const next = await iterator.next()
        if (next.done || this.#abort.signal.aborted) break

        this.#take(next.value.blocks)
        if (this.#saturated) break
      }
    } finally {
      this.#closeScan = undefined
      await iterator.return?.()
    }
  }

  #take(blocks: { header: { number: number }; logs?: Log<(typeof DISCOVERY_FIELDS)['log']>[] }[]) {
    for (const block of blocks) {
      // The portal leaves out the item list of a block that has no items
      for (const log of block.logs ?? []) {
        const address = this.#childAddress(log as Log, block.header.number)
        if (!address || this.#seen.has(address)) continue

        this.#seen.add(address)
        this.#children.push({ address, block: block.header.number })
      }
    }

    // The portal sends the last block of every range it covers, so this is how far the scan got
    const last = blocks[blocks.length - 1]
    if (last && last.header.number > this.#frontier) {
      this.#frontier = last.header.number
      this.#notify()
    }
  }

  #childAddress(log: Log, blockNumber: number): string | null {
    if (!this.#factory.isFactoryEvent(log)) return null

    let child: ReturnType<Factory<any>['childFromLog']>
    try {
      child = this.#factory.childFromLog(log, blockNumber)
    } catch {
      // The decoder fails on this log too, or drops it through its error hook: no child either way
      return null
    }

    if (!child || !this.#factory.ownsChild(child)) return null

    const address = child.childAddress.toLowerCase()

    return isAddress(address) ? address : null
  }
}

/**
 * Plans the child-event requests of a factory decoder: filtered by the children the
 * {@link FactoryDiscovery} found while it can stay ahead, by topic alone after that.
 *
 * Once it falls back to topic-only requests it never filters again: every fallback reason holds
 * for the rest of the run.
 *
 * @internal
 */
export class FactoryLookahead {
  readonly #factory: Factory<any>
  readonly #childRequests: (address?: string[]) => LogRequest[]
  readonly #maxAddresses: number
  readonly #minSegmentBlocks: number

  #discovery?: FactoryDiscovery
  #previousSegment = 0
  #fullScan = false

  constructor({
    factory,
    childRequests,
    maxAddresses,
    minSegmentBlocks,
  }: {
    factory: Factory<any>
    /** The child-event requests, filtered by `address` when it is given */
    childRequests: (address?: string[]) => LogRequest[]
    maxAddresses: number
    minSegmentBlocks: number
  }) {
    this.#factory = factory
    this.#childRequests = childRequests
    this.#maxAddresses = maxAddresses
    this.#minSegmentBlocks = minSegmentBlocks
  }

  plan = async ({ from, to, portal, logger }: PlanRequestArgs): Promise<PlannedRequest<DataRequest>> => {
    if (this.#fullScan) return this.#fullScanRequest()

    const discovery = await this.#discoveryFrom({ from, to, portal, logger })
    if (!discovery) return this.#fullScanRequest()

    discovery.extend(to ?? Number.POSITIVE_INFINITY)

    // Read once: the database only changes while the main stream runs, and it waits here
    const stored = (await this.#factory.storedChildAddresses()).filter(isAddress)

    while (true) {
      if (discovery.error !== undefined) {
        return this.#switchToFullScan(logger, from, `the factory scan failed: ${String(discovery.error)}`)
      }

      const decision = decideSegment({
        from,
        to,
        frontier: discovery.frontier,
        finalizedHead: discovery.finalizedHead,
        stored,
        discovered: discovery.children,
        requestsPerQuery: this.#childRequests().length,
        maxAddresses: this.#maxAddresses,
        minSegmentBlocks: this.#minSegmentBlocks,
        previousSegment: this.#previousSegment,
      })

      if (decision.kind === 'full-scan') {
        return this.#switchToFullScan(logger, from, decision.reason)
      }

      if (decision.kind === 'filter') {
        this.#previousSegment = decision.to - from + 1
        logger.info(
          `factory lookahead: fetching blocks ${from}…${decision.to} filtered by ${decision.addresses.length} child addresses`,
        )

        return { request: { logs: this.#childRequests(decision.addresses) }, to: decision.to }
      }

      await discovery.changed()
    }
  }

  /** The scan only ever saw finalized blocks, but a fork below them must not leave it stale. */
  rollback() {
    this.#stopDiscovery()
  }

  stop() {
    this.#stopDiscovery()
  }

  async #discoveryFrom({ from, to, portal, logger }: PlanRequestArgs): Promise<FactoryDiscovery | null> {
    // A scan that started above `from` has not seen the children created between them
    if (this.#discovery && this.#discovery.start <= from) return this.#discovery

    this.#stopDiscovery()

    const finalizedHead = await readFinalizedHead(portal)
    if (finalizedHead === undefined) {
      this.#switchToFullScan(logger, from, 'the dataset reports no finalized head')

      return null
    }

    // Avoids starting a scan just to learn that the stream is already too close to the head
    const nearHead = decideSegment({
      from,
      to,
      frontier: from - 1,
      finalizedHead,
      stored: [],
      discovered: [],
      requestsPerQuery: 1,
      maxAddresses: this.#maxAddresses,
      minSegmentBlocks: this.#minSegmentBlocks,
      previousSegment: 0,
    })
    if (nearHead.kind === 'full-scan') {
      this.#switchToFullScan(logger, from, nearHead.reason)

      return null
    }

    logger.info(`factory lookahead: scanning factory events ahead of the stream from block ${from}`)

    this.#discovery = new FactoryDiscovery({
      factory: this.#factory,
      portal,
      start: from,
      finalizedHead,
      until: to ?? Number.POSITIVE_INFINITY,
      maxChildren: this.#maxAddresses,
    })

    return this.#discovery
  }

  #switchToFullScan(logger: Logger, from: number, reason: string): PlannedRequest<DataRequest> {
    this.#fullScan = true
    this.#stopDiscovery()

    logger.info(`factory lookahead: fetching child events by topic from block ${from} on, as ${reason}`)

    return this.#fullScanRequest()
  }

  #fullScanRequest(): PlannedRequest<DataRequest> {
    return { request: { logs: this.#childRequests() } }
  }

  #stopDiscovery() {
    this.#discovery?.stop()
    this.#discovery = undefined
  }
}
