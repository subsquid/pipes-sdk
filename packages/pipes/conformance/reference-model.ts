import { type BlockRange, isBlockCursor, nextInRanges, validateInRange, validateWatermarks } from './validators.js'

/** Durability classes CN-10…CN-14 and CN-17. `ephemeral` is the spec's class ∅. */
export type DurabilityClass = 'T' | 'W' | 'K' | 'A' | 'C' | 'ephemeral'

export type StreamMode = 'full' | 'finalized-only'

export type Cursor = { number: number; hash: string; timestamp?: number }

/** A committed row, reduced to what the oracle compares: its table, its block and its content. */
export type OracleRow = {
  table: string
  block: number
  value: unknown
}

/** ⟨C, F, RC⟩: the same taxonomy a sink's state read exposes. */
export type OracleState = {
  current?: Cursor
  finalized?: Cursor
  rollbackChain: Cursor[]
}

/** One commit's ⟨RC, F⟩: the unit the WP-42 ancestor search walks. */
export type RollbackRecord = {
  rollbackChain: Cursor[]
  finalized?: Cursor
}

export type PersistedState = OracleState & {
  /** Oldest first. Defaults to one record built from the state itself. */
  records?: RollbackRecord[]
}

/** The finality a portal response reports next to its blocks. */
export type HeadReport = {
  finalized?: Cursor
}

export const ORACLE_ERRORS = {
  STATE_MALFORMED: 'ORACLE_STATE_MALFORMED',
  ORPHAN_DATA: 'ORACLE_ORPHAN_DATA',
  MALFORMED_INPUT: 'ORACLE_MALFORMED_INPUT',
  NOT_ASCENDING: 'ORACLE_NOT_ASCENDING',
  NOT_LINKED: 'ORACLE_NOT_LINKED',
  OUT_OF_RANGE: 'ORACLE_OUT_OF_RANGE',
  NOT_FINALIZED: 'ORACLE_NOT_FINALIZED',
  FORK_ON_FINALIZED_ROUTE: 'ORACLE_FORK_ON_FINALIZED_ROUTE',
  EMPTY_CANONICAL: 'ORACLE_EMPTY_CANONICAL',
  CANONICAL_BELOW_CURSOR: 'ORACLE_CANONICAL_BELOW_CURSOR',
  NO_ANCESTOR: 'ORACLE_NO_ANCESTOR',
  FINALITY_CONFLICT: 'ORACLE_FINALITY_CONFLICT',
  INVARIANT: 'ORACLE_INVARIANT',
} as const

export type OracleErrorCode = (typeof ORACLE_ERRORS)[keyof typeof ORACLE_ERRORS]

export class OracleError extends Error {
  constructor(
    readonly code: OracleErrorCode,
    message: string,
  ) {
    super(message)
    this.name = 'OracleError'
  }
}

/** Classes whose sink requires finalized-only delivery (WP-7, DEF-15). */
const FINALIZED_ONLY: ReadonlySet<DurabilityClass> = new Set<DurabilityClass>(['K', 'ephemeral'])

export type ReferenceModelOptions = {
  durability: DurabilityClass
  /** The configured stream mode. A finalized-only class overrides it (`bind`, WP-7). */
  mode?: StreamMode
  /** Pure per RS-10: the same blocks give the same rows. Defaults to one `blocks` row per block. */
  transform?: (block: Cursor) => OracleRow[]
  /** The resolved range set (DEF-14). Enables the INV-20 linkage and INV-3 range checks. */
  ranges?: BlockRange[]
  /**
   * Whether the binding declares its tracked tables exclusive to one cursor key (IB-27). Only then
   * does CN-44 refuse a cold start over existing data. Defaults to exclusive, the side that refuses.
   */
  tables?: 'exclusive' | 'shared'
}

/**
 * The reference model of spec/13, executable: the oracle a conformance run diffs the sink store
 * against at quiescence.
 *
 * It transcribes the normative pseudocode instead of re-implementing the pipeline, so a divergence
 * from the SUT is a finding about the SUT or the spec, never about a shortcut here. It pins only
 * what the spec declares deterministic: batch partitioning, checkpoint timing and unit boundaries
 * are free variables, so the oracle is told what a run covers, not how the SUT cut it. For the same
 * reason it does not track `V`: coverage windows end at checkpoints, whose timing is free, so
 * INV-4 is checked structurally on the store (`validateCoverage`).
 */
export class ReferenceModel {
  readonly #mode: StreamMode
  readonly #transform: (block: Cursor) => OracleRow[]
  readonly #ranges: BlockRange[]
  readonly #exclusive: boolean

  #current: Cursor | undefined
  #finalized: Cursor | undefined
  #rollbackChain: Cursor[] = []
  #records: RollbackRecord[] = []
  #data: OracleRow[] = []

  constructor(options: ReferenceModelOptions) {
    this.#ranges = options.ranges ?? []
    this.#exclusive = (options.tables ?? 'exclusive') === 'exclusive'
    this.#transform = options.transform ?? ((block) => [{ table: 'blocks', block: block.number, value: block.hash }])

    // bind(target), WP-7: a sink that requires finalized-only delivery overrides the configured mode.
    this.#mode = FINALIZED_ONLY.has(options.durability) ? 'finalized-only' : (options.mode ?? 'full')
  }

  get mode(): StreamMode {
    return this.#mode
  }

  /** Committed rows, in commit order. */
  get data(): readonly OracleRow[] {
    return this.#data
  }

  /** The ⟨RC, F⟩ records, oldest first: what the WP-42 search walks. */
  get records(): readonly RollbackRecord[] {
    return this.#records
  }

  /**
   * The head the effective portal view reports (WP-7): range resolution, transformers and caches
   * all see this one. A dataset without finality serves its finalized route up to the latest head.
   */
  effectiveHead(head: { latest: number; finalized?: Cursor }): number {
    if (this.#mode === 'full') {
      return head.latest
    }

    return head.finalized?.number ?? head.latest
  }

  /**
   * T-INIT: adopt the persisted state, repair the data to it, and check the result. Only
   * `undefined` means no state: a `null` read from a store is a malformed one.
   */
  recover(persisted: PersistedState | undefined): void {
    if (persisted !== undefined) {
      this.#requireWellFormed(persisted)
    }

    // CN-44: without state, data in exclusive tables is an orphan; restarting would duplicate it.
    const orphaned = persisted === undefined && this.#exclusive && this.#data.length > 0
    if (orphaned) {
      throw new OracleError(
        ORACLE_ERRORS.ORPHAN_DATA,
        `CN-44: no cursor state, but the exclusive tables hold ${this.#data.length} rows`,
      )
    }

    this.#atomically(() => {
      this.#current = persisted?.current
      this.#finalized = persisted?.finalized
      this.#rollbackChain = persisted === undefined ? [] : [...persisted.rollbackChain]

      if (persisted?.records !== undefined) {
        this.#records = [...persisted.records]
      } else {
        this.#records = this.#rollbackChain.length ? [this.#record()] : []
      }

      // CN-40…CN-44: whatever the class's crash window let through, nothing above the cursor survives.
      const bound = this.#current?.number ?? -1
      this.#data = this.#data.filter((row) => row.block <= bound)
    })
  }

  /** T-BATCH. `blocks` are the blocks the batch covers, ascending. */
  batch(blocks: Cursor[], head: HeadReport = {}): void {
    requireBlocks('the batch', head.finalized === undefined ? blocks : [...blocks, head.finalized])
    this.#requireLinked(blocks)
    this.#requireFinalizedRoute(blocks, head)

    const rows = blocks.flatMap((block) => this.#attributed(block))

    this.#atomically(() => {
      // INV-12: a regressed or absent report never lowers the floor.
      const raised =
        head.finalized !== undefined && (!this.#finalized || head.finalized.number > this.#finalized.number)
      if (raised) {
        this.#finalized = head.finalized
      }

      // INV-1: the rollback chain is exactly the processed blocks above the floor. The finalized-only
      // route takes no fork, so it keeps no fork history, with or without a floor (WP-47).
      const floor = this.#finalized?.number ?? -1
      const unfinalized = [...this.#rollbackChain, ...blocks].filter((block) => block.number > floor)
      this.#rollbackChain = this.#mode === 'full' ? unfinalized : []

      this.#data.push(...rows)
      this.#current = blocks.at(-1)
      this.#records.push(this.#record())
    })
  }

  /** T-FORK: rewind to the ancestor the WP-42 search finds. Returns that ancestor. */
  fork(canonical: Cursor[]): Cursor {
    if (this.#mode === 'finalized-only') {
      throw new OracleError(
        ORACLE_ERRORS.FORK_ON_FINALIZED_ROUTE,
        'WP-45: a fork arrived on the finalized-only route, where none can arrive',
      )
    }

    if (!canonical.length) {
      throw new OracleError(ORACLE_ERRORS.EMPTY_CANONICAL, 'WP-41: the fork signal carries an empty canonical chain')
    }

    requireBlocks('the canonical chain', canonical)
    const descends = canonical.findIndex((block, i) => i > 0 && block.number <= canonical[i - 1].number)
    if (descends > 0) {
      throw new OracleError(
        ORACLE_ERRORS.MALFORMED_INPUT,
        `the canonical chain steps from ${canonical[descends - 1].number} to ${canonical[descends].number}`,
      )
    }

    const top = Math.max(...canonical.map((block) => block.number))
    if (this.#current && top < this.#current.number) {
      throw new OracleError(
        ORACLE_ERRORS.CANONICAL_BELOW_CURSOR,
        `RP-43: the canonical chain ends at ${top}, below the cursor ${this.#current.number}`,
      )
    }

    const ancestor = searchAncestor(this.#records, canonical)
    if (!ancestor) {
      throw new OracleError(ORACLE_ERRORS.NO_ANCESTOR, 'WP-42: no common ancestor in the rollback records')
    }

    // WP-44: checked against the current floor, not each record's own (GAP-6).
    if (this.#finalized && ancestor.number < this.#finalized.number) {
      throw new OracleError(
        ORACLE_ERRORS.FINALITY_CONFLICT,
        `WP-44: the ancestor ${ancestor.number} is below the floor ${this.#finalized.number}`,
      )
    }

    this.#atomically(() => {
      // Class C compensates instead of deleting (CN-35); a converged consumer sees the same rows.
      this.#data = this.#data.filter((row) => row.block <= ancestor.number)
      this.#current = ancestor
      this.#rollbackChain = this.#rollbackChain.filter((block) => block.number <= ancestor.number)
      this.#records.push(this.#record())
    })

    return ancestor
  }

  readState(): OracleState {
    return {
      current: this.#current,
      finalized: this.#finalized,
      rollbackChain: [...this.#rollbackChain],
    }
  }

  /** INV-1…INV-5 after every transition. INV-4 is a store property: see the class comment. */
  wellformedCheck(): void {
    const state = this.readState()
    const cursor = this.#current?.number ?? -1
    const floor = this.#finalized?.number

    const violations = [...validateWatermarks(state), ...validateInRange(this.#data, this.#ranges)]

    const aboveCursor = this.#data.find((row) => row.block > cursor)
    if (aboveCursor) {
      violations.push({
        validator: 'oracle',
        property: 'INV-3',
        message: `a row at ${aboveCursor.block} is above the cursor ${cursor}`,
      })
    }

    // CN-22: on a finalizing dataset a finalized-only class holds finalized rows only.
    const unfinalized =
      this.#mode === 'finalized-only' && floor !== undefined && this.#data.find((row) => row.block > floor)
    if (unfinalized) {
      violations.push({
        validator: 'oracle',
        property: 'INV-3',
        message: `a row at ${unfinalized.block} is visible above the floor ${floor}`,
      })
    }

    if (violations.length) {
      const lines = violations.map((violation) => `[${violation.property}] ${violation.message}`)

      throw new OracleError(ORACLE_ERRORS.INVARIANT, lines.join('; '))
    }
  }

  #record(): RollbackRecord {
    return { rollbackChain: [...this.#rollbackChain], finalized: this.#finalized }
  }

  /** Applies a transition and checks INV-1…INV-5 on the result. A refused result is undone. */
  #atomically(change: () => void): void {
    const before = {
      current: this.#current,
      finalized: this.#finalized,
      rollbackChain: this.#rollbackChain,
      records: [...this.#records],
      data: [...this.#data],
    }

    try {
      change()
      this.wellformedCheck()
    } catch (error) {
      this.#current = before.current
      this.#finalized = before.finalized
      this.#rollbackChain = before.rollbackChain
      this.#records = before.records
      this.#data = before.data

      throw error
    }
  }

  /** INV-3: the transform attributes every row it makes of a block to that block. */
  #attributed(block: Cursor): OracleRow[] {
    const rows = this.#transform(block)
    const stray = rows.find((row) => row.block !== block.number)
    if (stray) {
      throw new OracleError(
        ORACLE_ERRORS.MALFORMED_INPUT,
        `INV-3: the transform attributes a row of block ${block.number} to ${stray.block}`,
      )
    }

    return rows
  }

  /**
   * INV-5, INV-44: a malformed state is refused before anything is repaired. The rollback records
   * count too, because the ancestor search reads them; a record may hold blocks above the cursor,
   * from a branch a fork left.
   */
  #requireWellFormed(persisted: PersistedState): void {
    const malformed = (reason: string) => new OracleError(ORACLE_ERRORS.STATE_MALFORMED, reason)
    const isRecord = (value: unknown) => typeof value === 'object' && value !== null

    if (!isRecord(persisted)) {
      throw malformed(`INV-5: the persisted state ${JSON.stringify(persisted)} is not a record`)
    }

    const { records: stored } = persisted
    const recordsSound = stored === undefined || (Array.isArray(stored) && stored.every(isRecord))
    if (!recordsSound) {
      throw malformed(`INV-5: the rollback records ${JSON.stringify(stored)} are not a list of records`)
    }

    const records = stored ?? []

    // Every cursor is a block (INV-5) and every chain well-formed (INV-1). A record keeps no
    // cursor; the cursor it was written under was at least the top block of its chain.
    const recordStates = records.map((record) => {
      const chain: unknown[] = Array.isArray(record.rollbackChain) ? record.rollbackChain : []

      return { ...record, current: chain.filter(isBlockCursor).at(-1) }
    })
    const states: OracleState[] = [persisted, ...recordStates]
    const violations = states.flatMap((state) => validateWatermarks(state))
    if (violations.length) {
      const reasons = violations.map((violation) => violation.message)

      throw malformed(`INV-5: ${reasons.join('; ')}`)
    }

    // INV-12: from the oldest record to the current state, the floor never goes down.
    const floors = [...records, persisted].map((state) => state.finalized?.number ?? -1)
    const lowered = floors.findIndex((floor, i) => i > 0 && floor < floors[i - 1])
    if (lowered > 0) {
      throw malformed(
        `INV-12: the floor goes down from ${floors[lowered - 1]} to ${floors[lowered]} across the rollback records`,
      )
    }
  }

  /** INV-20: strictly ascending, and the first block is the next one the range set holds after the cursor. */
  #requireLinked(blocks: Cursor[]): void {
    if (!blocks.length) {
      throw new OracleError(ORACLE_ERRORS.NOT_LINKED, 'INV-20: an empty batch covers no block')
    }

    for (let i = 1; i < blocks.length; i++) {
      if (blocks[i].number <= blocks[i - 1].number) {
        throw new OracleError(
          ORACLE_ERRORS.NOT_ASCENDING,
          `INV-20: the batch steps from ${blocks[i - 1].number} to ${blocks[i].number}`,
        )
      }
    }

    const after = this.#current ? this.#current.number + 1 : undefined
    const expected = this.#ranges.length ? nextInRanges(after ?? 0, this.#ranges) : after

    // A batch after the last range ends can only replay or overrun it.
    const exhausted = this.#ranges.length > 0 && expected === undefined
    if (exhausted) {
      throw new OracleError(
        ORACLE_ERRORS.OUT_OF_RANGE,
        `INV-20: the range set holds no block after the cursor ${this.#current?.number}`,
      )
    }

    if (expected !== undefined && blocks[0].number !== expected) {
      throw new OracleError(
        ORACLE_ERRORS.NOT_LINKED,
        `INV-20: the batch starts at ${blocks[0].number}, the next block is ${expected}`,
      )
    }

    const outside = validateInRange(
      blocks.map((block) => ({ block: block.number })),
      this.#ranges,
    )
    if (outside.length) {
      throw new OracleError(ORACLE_ERRORS.OUT_OF_RANGE, `INV-20: ${outside[0].message}`)
    }
  }

  /** INV-25: on a finalizing dataset, the finalized-only route delivers no block above the floor. */
  #requireFinalizedRoute(blocks: Cursor[], head: HeadReport): void {
    if (this.#mode !== 'finalized-only') {
      return
    }

    const reported = head.finalized?.number ?? -1
    const floor = Math.max(this.#finalized?.number ?? -1, reported)
    const finalizing = this.#finalized !== undefined || head.finalized !== undefined
    const last = blocks[blocks.length - 1].number

    if (finalizing && last > floor) {
      throw new OracleError(
        ORACLE_ERRORS.NOT_FINALIZED,
        `INV-25: the finalized-only route delivered ${last}, above the floor ${floor}`,
      )
    }
  }
}

function requireBlocks(source: string, cursors: readonly Cursor[]): void {
  const malformed = cursors.find((cursor) => !isBlockCursor(cursor))
  if (malformed) {
    throw new OracleError(ORACLE_ERRORS.MALFORMED_INPUT, `${source} holds ${JSON.stringify(malformed)}, not a block`)
  }
}

/**
 * WP-42: walk the records newest → oldest, each chain descending, and narrow the canonical window
 * past every cursor visited.
 */
function searchAncestor(records: readonly RollbackRecord[], canonical: readonly Cursor[]): Cursor | undefined {
  let window = [...canonical]

  for (const record of [...records].reverse()) {
    const chain = [...record.rollbackChain].sort((a, b) => b.number - a.number)

    for (const visited of chain) {
      const match = window.find((entry) => entry.hash === visited.hash)
      if (match) {
        return match
      }

      // Deep-fork restart: the next retained cursor below the exhausted window.
      if (!window.length) {
        return visited
      }

      window = window.filter((entry) => entry.number < visited.number)
    }

    const floorFallback = record.finalized && window.length === 1 && window[0].hash === record.finalized.hash
    if (floorFallback) {
      return record.finalized
    }
  }

  return undefined
}
