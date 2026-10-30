import { selectEvm } from './portal-evm.js'
import type { WireHead } from './portal-wire.js'

/** A block the chain holds: a header plus the item lists of its network (`logs`, `instructions`, …). */
export type ChainBlock = {
  header: { number: number; hash: string; parentHash?: string; timestamp?: number; [field: string]: unknown }
  [items: string]: unknown
}

/** The body of a stream request, as far as the chain reads it. */
export type StreamQuery = {
  type?: string
  fromBlock: number
  toBlock?: number
  parentBlockHash?: string
  includeAllBlocks?: boolean
  [request: string]: unknown
}

export type StreamEndpoint = 'stream' | 'finalized-stream'

export type ChainRequest = {
  endpoint: StreamEndpoint
  query: StreamQuery
  /** `undefined` while `delay` holds the request. */
  status?: number
  /** What the portal answered, and the head it reported with it; `undefined` while `delay` holds the request and on a 204. */
  answer?: ChainAnswer
  head?: WireHead
}

export type ChainFault =
  | {
      status: number
      /** How many stream requests in a row get this answer. Defaults to 1. */
      times?: number
      /** Sent as the `Retry-After` header. */
      retryAfter?: number | string
    }
  | {
      /** Answers 200 with this many blocks of the answer, then holds the response open and sends nothing more. */
      stallAfter: number
      /** How many stream requests in a row get this answer. Defaults to 1. */
      times?: number
    }
  | {
      /** Answers 200 with this many blocks of the answer, then half the line of the next one, and ends the body there. */
      cutAfter: number
      /** How many stream requests in a row get this answer. Defaults to 1. */
      times?: number
    }
  | {
      /** Answers 200 with this many blocks of the answer, then `line` as a line of its own, and ends the body. */
      lineAfter: number
      line: string
      /** How many stream requests in a row get this answer. Defaults to 1. */
      times?: number
    }

export type ChainAnswer =
  | { kind: 'blocks'; blocks: ChainBlock[] }
  | { kind: 'stalled'; blocks: ChainBlock[] }
  | { kind: 'cut'; blocks: ChainBlock[]; cutBlock?: ChainBlock }
  | { kind: 'line'; blocks: ChainBlock[]; line: string }
  | { kind: 'at-head' }
  | { kind: 'refused'; message: string }
  | { kind: 'fork'; previousBlocks: { number: number; hash: string }[] }
  | { kind: 'fault'; status: number; retryAfter?: number | string }

const STATUS = { blocks: 200, stalled: 200, cut: 200, line: 200, 'at-head': 204, refused: 400, fork: 409 } as const

export function statusOf(answer: ChainAnswer): number {
  return answer.kind === 'fault' ? answer.status : STATUS[answer.kind]
}

export type ChainOptions = {
  blocks?: ChainBlock[]
  latest?: number
  finalized?: number | null
  blocksPerResponse?: number
  previousBlocks?: number
  maxAddresses?: number
  /** `metadata.kind` of the dataset; it sets the unit of block timestamps. Defaults to `evm`. */
  kind?: string
}

/** Dataset kinds whose block timestamps the portal reports in milliseconds; the others use seconds. */
const MILLISECOND_TIMESTAMP_KINDS = new Set(['tron', 'substrate', 'hyperliquidFills'])

/**
 * Branch 0 matches `mockBlock`'s default hash, so a block reads the same whether a test gave it or
 * the chain made it up. Every fork moves the blocks above its point to a new branch.
 */
function madeUpHash(number: number, branch: number): string {
  const n = number.toString(16)

  return branch === 0 ? `0x${n.padStart(64, '0')}` : `0x${branch.toString(16).padStart(8, '0')}${n.padStart(56, '0')}`
}

/**
 * The chain a mock portal answers from. Every block number up to the head exists: the blocks a test
 * gives carry its data, every other one is an empty block the chain makes up.
 */
export class PortalChain {
  readonly requests: ChainRequest[] = []

  #blocks = new Map<number, ChainBlock>()
  #forkPoints: number[] = []
  #faults: (ChainFault & { times: number })[] = []
  #latest: number
  #finalized: number | null
  #blocksPerResponse: number
  #previousBlocks: number
  #maxAddresses: number | undefined
  #timestampsPerSecond: number

  constructor(options: ChainOptions = {}) {
    for (const block of options.blocks ?? []) {
      this.#blocks.set(block.header.number, block)
    }

    this.#latest = options.latest ?? Math.max(0, ...this.#blocks.keys())
    this.#finalized = options.finalized === undefined ? this.#latest : options.finalized
    this.#blocksPerResponse = options.blocksPerResponse ?? Number.POSITIVE_INFINITY
    this.#previousBlocks = options.previousBlocks ?? 10
    this.#maxAddresses = options.maxAddresses
    this.#timestampsPerSecond = MILLISECOND_TIMESTAMP_KINDS.has(options.kind ?? 'evm') ? 1000 : 1
  }

  get latest(): number {
    return this.#latest
  }

  get finalized(): number | null {
    return this.#finalized
  }

  hash(number: number): string {
    const given = this.#blocks.get(number)
    if (given) {
      return given.header.hash
    }

    const branch = this.#forkPoints.filter((at) => at < number).length

    return madeUpHash(number, branch)
  }

  header(number: number): ChainBlock['header'] {
    return (
      this.#blocks.get(number)?.header ?? {
        number,
        hash: this.hash(number),
        parentHash: this.hash(Math.max(number - 1, 0)),
        timestamp: number * 1000,
      }
    )
  }

  /**
   * The first block whose timestamp is at or after `seconds`, as `GET timestamps/{seconds}/block` finds
   * it: block 0 for a time before the chain, `undefined` for a time after the head. Assumes timestamps
   * do not decrease along the chain.
   */
  blockAt(seconds: number): number | undefined {
    // The route takes seconds on every network, while block timestamps keep the dataset's unit.
    const time = seconds * this.#timestampsPerSecond
    const timestamp = (number: number) => this.header(number).timestamp ?? 0

    if (timestamp(this.#latest) < time) {
      return undefined
    }

    let low = 0
    let high = this.#latest
    while (low < high) {
      const middle = Math.floor((low + high) / 2)
      if (timestamp(middle) < time) {
        low = middle + 1
      } else {
        high = middle
      }
    }

    return low
  }

  head(): WireHead {
    const head: WireHead = { latest: { number: this.#latest } }

    if (this.#finalized !== null) {
      head.finalized = { number: this.#finalized, hash: this.hash(this.#finalized) }
    }

    return head
  }

  setHead(head: { latest?: number; finalized?: number | null }) {
    if (head.latest !== undefined) {
      this.#latest = head.latest
    }
    if (head.finalized !== undefined) {
      this.#finalized = head.finalized
    }
  }

  /** Extends the chain above the head and moves the head to the last new block. */
  append(blocks: ChainBlock[]) {
    for (const block of blocks) {
      const number = block.header.number
      if (number <= this.#latest) {
        throw new Error(`Block ${number} is not above the head ${this.#latest}; use fork() to replace blocks`)
      }

      this.#blocks.set(number, block)
      this.#latest = number
    }
  }

  /**
   * Replaces every block above `at` with `branch` and new empty blocks. A request anchored on a
   * replaced block then gets a 409, exactly as from a real portal after a reorg.
   */
  fork(at: number, branch: ChainBlock[] = []) {
    for (const block of branch) {
      const number = block.header.number
      if (number <= at) {
        throw new Error(`Block ${number} is not above the fork point ${at}`)
      }
      if (block.header.hash === this.hash(number)) {
        throw new Error(`Block ${number} keeps the hash of the block it replaces, so no request could see the fork`)
      }
    }

    for (const number of [...this.#blocks.keys()]) {
      if (number > at) {
        this.#blocks.delete(number)
      }
    }
    this.#forkPoints.push(at)

    for (const block of branch) {
      this.#blocks.set(block.header.number, block)
      this.#latest = Math.max(this.#latest, block.header.number)
    }
  }

  fail(fault: ChainFault) {
    this.#faults.push({ ...fault, times: fault.times ?? 1 })
  }

  /** Records a stream request on arrival, before `delay` can hold it back. */
  receive(endpoint: StreamEndpoint, query: StreamQuery): ChainRequest {
    const request: ChainRequest = { endpoint, query }
    this.requests.push(request)

    return request
  }

  /** Answers from the chain as it is at this moment, and records the answer and the head on the request. */
  answer(request: ChainRequest): ChainAnswer {
    const answer = this.#answer(request.endpoint, request.query)
    request.status = statusOf(answer)
    request.answer = answer
    // The portal sends no head headers with a 204.
    request.head = answer.kind === 'at-head' ? undefined : this.head()

    return answer
  }

  #answer(endpoint: StreamEndpoint, query: StreamQuery): ChainAnswer {
    const fault = this.#faults[0]
    if (fault) {
      fault.times--
      if (fault.times <= 0) {
        this.#faults.shift()
      }

      if ('stallAfter' in fault) {
        const answer = this.#serve(endpoint, query)
        const blocks = answer.kind === 'blocks' ? answer.blocks.slice(0, fault.stallAfter) : []

        return { kind: 'stalled', blocks }
      }

      if ('cutAfter' in fault) {
        const answer = this.#serve(endpoint, query)
        const blocks = answer.kind === 'blocks' ? answer.blocks : []

        return { kind: 'cut', blocks: blocks.slice(0, fault.cutAfter), cutBlock: blocks[fault.cutAfter] }
      }

      if ('lineAfter' in fault) {
        const answer = this.#serve(endpoint, query)
        const blocks = answer.kind === 'blocks' ? answer.blocks.slice(0, fault.lineAfter) : []

        return { kind: 'line', blocks, line: fault.line }
      }

      return { kind: 'fault', status: fault.status, retryAfter: fault.retryAfter }
    }

    return this.#serve(endpoint, query)
  }

  /** The answer of a healthy portal. */
  #serve(endpoint: StreamEndpoint, query: StreamQuery): ChainAnswer {
    const addresses = countLogAddresses(query)
    if (this.#maxAddresses !== undefined && addresses > this.#maxAddresses) {
      return { kind: 'refused', message: `the query carries ${addresses} addresses, more than ${this.#maxAddresses}` }
    }

    // A dataset without finality still streams on the finalized route, just not reorg-safe (DEF-15).
    const available = endpoint === 'finalized-stream' ? (this.#finalized ?? this.#latest) : this.#latest

    // Before the head check: a pipe polling at the head on a replaced block must see the fork,
    // not a 204 that never changes.
    const parent = query.fromBlock - 1
    const parentKnown = parent >= 0 && parent <= available
    const forked = parentKnown && query.parentBlockHash != null && this.hash(parent) !== query.parentBlockHash
    if (forked) {
      return { kind: 'fork', previousBlocks: this.#canonicalBefore(query.fromBlock) }
    }

    if (query.fromBlock > available) {
      return { kind: 'at-head' }
    }

    const last = Math.min(
      query.toBlock ?? Number.POSITIVE_INFINITY,
      available,
      query.fromBlock + this.#blocksPerResponse - 1,
    )

    const blocks: ChainBlock[] = []
    for (let number = query.fromBlock; number <= last; number++) {
      // The covered range's first and last blocks always go out: the client advances by them.
      const boundary = number === query.fromBlock || number === last
      const selected = select(this.#blocks.get(number), query)

      if (selected || boundary || query.includeAllBlocks) {
        blocks.push({ header: this.header(number), ...selected })
      }
    }

    return { kind: 'blocks', blocks }
  }

  /** The canonical blocks just below `fromBlock`, ascending: the `previousBlocks` of a 409. */
  #canonicalBefore(fromBlock: number) {
    const first = Math.max(0, fromBlock - this.#previousBlocks)

    const blocks: { number: number; hash: string }[] = []
    for (let number = first; number < fromBlock; number++) {
      blocks.push({ number, hash: this.hash(number) })
    }

    return blocks
  }
}

function countLogAddresses(query: StreamQuery): number {
  const requests = (query['logs'] ?? []) as { address?: string[] }[]

  return requests.reduce((sum, request) => sum + (request.address?.length ?? 0), 0)
}

/** The items of a block the query selects, or `undefined` when it selects none. */
function select(block: ChainBlock | undefined, query: StreamQuery): Record<string, unknown> | undefined {
  if (!block) {
    return undefined
  }

  const { header, ...items } = block

  // Only EVM queries are filtered; the items of other networks go out as the test gave them.
  const selected = query.type === 'evm' ? selectEvm(items, query) : items

  return Object.keys(selected).length > 0 ? selected : undefined
}
