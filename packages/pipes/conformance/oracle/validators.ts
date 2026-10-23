import type { Cursor, DurabilityClass, OracleRow, OracleState, RollbackRecord } from './reference-model.js'

/** A structural rule an emission or a persisted state breaks. */
export type Violation = {
  validator: string
  property: string
  message: string
}

/** An inclusive block range; `to` absent means open-ended. */
export type BlockRange = { from: number; to?: number }

/** Anything attributed to a block: the only thing these validators know about a row. */
export type Attributed = { block: number }

/** A published unit (a file, a partition) and the coverage window its name claims. */
export type PublishedUnit = {
  name: string
  from: number
  to: number
  rows: Attributed[]
}

/** One delivered batch, with the cursor the pipe held before it; none on a cold start. */
export type DeliveredBatch = {
  blocks: { number: number }[]
  cursorBefore?: number
}

/** A changelog operation as the class-C sink put it on the wire. */
export type SequencedOperation = {
  topic: string
  seq: number | bigint | string
  /** The operation's identity (op, row id, payload). A republish must repeat it exactly (CN-46). */
  key?: string
}

/** A block height is a non-negative safe integer. */
export function isHeight(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

/** A cursor is a record with a block height and a hash: without one it cannot anchor a request. */
export function isBlockCursor(value: unknown): value is { number: number; hash: string } {
  if (typeof value !== 'object' || value === null) {
    return false
  }

  const { number, hash } = value as { number?: unknown; hash?: unknown }
  const hashed = typeof hash === 'string' && hash.length > 0

  return isHeight(number) && hashed
}

function inRanges(block: number, ranges: readonly BlockRange[]): boolean {
  return ranges.some((range) => block >= range.from && (range.to === undefined || block <= range.to))
}

/** The first block at or after `after` that the range set holds, or undefined past its end. */
export function nextInRanges(after: number, ranges: readonly BlockRange[]): number | undefined {
  const sorted = [...ranges].sort((a, b) => a.from - b.from)

  for (const range of sorted) {
    const ended = range.to !== undefined && range.to < after
    if (!ended) {
      return Math.max(range.from, after)
    }
  }

  return undefined
}

/** The first block at or after `from` that the range set does not hold, or undefined if it holds them all. */
export function nextOutsideRanges(from: number, ranges: readonly BlockRange[]): number | undefined {
  const sorted = [...ranges].sort((a, b) => a.from - b.from)
  let block = from

  for (const range of sorted) {
    if (range.from > block) {
      return block
    }

    if (range.to === undefined) {
      return undefined
    }

    block = Math.max(block, range.to + 1)
  }

  return block
}

/*
 * The kind-agnostic structural validators of spec/13. They hold of any emission or state without
 * domain knowledge, so they stay on in every scenario. None of them reads row content: that is
 * the oracle's job.
 */

/** Decodable: every item parses per its format (INV-5). */
export function validateDecodable<T>(items: readonly T[], decode: (item: T) => unknown): Violation[] {
  const violations: Violation[] = []

  items.forEach((item, index) => {
    try {
      decode(item)
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error)
      violations.push({ validator: 'decodable', property: 'INV-5', message: `item ${index} does not parse: ${reason}` })
    }
  })

  return violations
}

/** Ordered: attribution never descends. Several rows of one block are legal (INV-20). */
export function validateOrdered(rows: readonly Attributed[]): Violation[] {
  const violations: Violation[] = []

  for (let i = 1; i < rows.length; i++) {
    const previous = rows[i - 1].block
    const current = rows[i].block

    if (current < previous) {
      violations.push({
        validator: 'ordered',
        property: 'INV-20',
        message: `attribution descends at index ${i}: ${previous} → ${current}`,
      })
    }
  }

  return violations
}

/**
 * Linked: each batch holds strictly ascending blocks inside the range set, and starts at the next
 * block the range set holds after the cursor; a cold start at the range set's first block (INV-20).
 * Without a range set a cold start's first block is not checked. Rows may share a block; the
 * blocks of a batch may not.
 */
export function validateLinked(batches: readonly DeliveredBatch[], ranges: readonly BlockRange[] = []): Violation[] {
  const violations: Violation[] = []
  const report = (message: string) => {
    violations.push({ validator: 'linked', property: 'INV-20', message })
  }

  batches.forEach((batch, index) => {
    const numbers = batch.blocks.map((block) => block.number)
    if (!numbers.length) {
      report(`batch ${index} covers no block`)

      return
    }

    const cursorUnsound = batch.cursorBefore !== undefined && !isHeight(batch.cursorBefore)
    if (cursorUnsound) {
      report(`batch ${index} follows the cursor ${batch.cursorBefore}, which is not a block`)
    }

    numbers.forEach((number, i) => {
      const previous = numbers[i - 1]

      if (!isHeight(number)) {
        report(`batch ${index} holds ${number}, which is not a block`)
      } else if (i > 0 && number === previous) {
        report(`batch ${index} repeats block ${number}`)
      } else if (i > 0 && number < previous) {
        report(`batch ${index} descends from ${previous} to ${number}`)
      }
    })

    const outside = ranges.length ? numbers.filter((number) => !inRanges(number, ranges)) : []
    for (const number of outside) {
      report(`batch ${index} holds ${number}, which no configured range holds`)
    }

    // Nothing places the first block of a batch after an unsound cursor, or of a cold start without ranges.
    const unplaceable = cursorUnsound || (batch.cursorBefore === undefined && !ranges.length)
    if (unplaceable) {
      return
    }

    const after = batch.cursorBefore === undefined ? 0 : batch.cursorBefore + 1
    const expected = ranges.length ? nextInRanges(after, ranges) : after
    const first = numbers[0]

    if (first !== expected) {
      const wanted =
        expected === undefined
          ? `but the range set holds no block after the cursor ${batch.cursorBefore}`
          : `the next block is ${expected}`

      report(`batch ${index} starts at ${first}, ${wanted}`)
    }
  })

  return violations
}

/** Items-belong-to-parent: every row of a unit lies inside the window the unit claims (RP-22). */
export function validateItemsBelongToParent(units: readonly PublishedUnit[]): Violation[] {
  const violations: Violation[] = []

  for (const unit of units) {
    for (const row of unit.rows) {
      const inside = isHeight(row.block) && row.block >= unit.from && row.block <= unit.to
      if (!inside) {
        violations.push({
          validator: 'items-belong-to-parent',
          property: 'RP-22',
          message: `unit '${unit.name}' claims [${unit.from}, ${unit.to}] but holds a row at ${row.block}`,
        })
      }
    }
  }

  return violations
}

/** In-range: every row is attributed to a block, inside the configured range set when one is given (INV-3). */
export function validateInRange(rows: readonly Attributed[], ranges: readonly BlockRange[]): Violation[] {
  const violations: Violation[] = []
  const report = (message: string) => {
    violations.push({ validator: 'in-range', property: 'INV-3', message })
  }

  for (const row of rows) {
    if (!isHeight(row.block)) {
      report(`a row is attributed to ${row.block}, which is not a block`)
    } else if (ranges.length && !inRanges(row.block, ranges)) {
      report(`a row at ${row.block} falls outside every configured range`)
    }
  }

  return violations
}

/**
 * Coverage partition, per table: the windows tile the processed range from the first configured
 * block up to `next` — disjoint, with no gap inside the configured ranges and no window across a
 * gap between them — and reach no further than the cursor. Blocks from `next` to the cursor are
 * processed but not yet published, which is legal (INV-4, RP-21). The checks against the range set
 * only run when it is given.
 */
export function validateCoverage(
  units: readonly PublishedUnit[],
  { ranges = [], next, cursor }: { ranges?: readonly BlockRange[]; next?: number; cursor?: number } = {},
): Violation[] {
  const violations: Violation[] = []
  const report = (message: string) => {
    violations.push({ validator: 'coverage-partition', property: 'INV-4', message })
  }

  // Whether [from, to] holds a block the pipe had to process: any block, without a range set.
  const holdsRangeBlock = (from: number, to: number) => {
    const first = ranges.length ? nextInRanges(from, ranges) : from

    return first !== undefined && first <= to
  }

  const sorted = [...units].sort((a, b) => a.from - b.from)
  const first = sorted[0]
  const last = sorted.at(-1)
  const start = ranges.length ? Math.min(...ranges.map((range) => range.from)) : undefined

  if (next !== undefined && !isHeight(next)) {
    report(`the next window start ${next} is not a block`)
  }

  if (start !== undefined && first && holdsRangeBlock(start, first.from - 1)) {
    report(`blocks ${start}…${first.from - 1} lie before '${first.name}' uncovered`)
  }

  if (start !== undefined && !first && next !== undefined && holdsRangeBlock(start, next - 1)) {
    report(`the next window starts at ${next}, but no window covers ${start}…${next - 1}`)
  }

  for (const unit of sorted) {
    if (!isHeight(unit.from) || !isHeight(unit.to)) {
      report(`'${unit.name}' claims the window ${unit.from}…${unit.to}, which is not a block range`)
    } else if (unit.from > unit.to) {
      report(`'${unit.name}' claims the empty window ${unit.from}…${unit.to}`)
    }
  }

  // A window claims every block in it was processed; a block no range holds never was.
  if (ranges.length) {
    for (const unit of sorted) {
      const unqueried = nextOutsideRanges(unit.from, ranges)

      if (unqueried !== undefined && unqueried <= unit.to) {
        report(`'${unit.name}' claims block ${unqueried}, which no configured range holds`)
      }
    }
  }

  for (let i = 1; i < sorted.length; i++) {
    const previous = sorted[i - 1]
    const unit = sorted[i]

    if (unit.from <= previous.to) {
      report(`'${unit.name}' overlaps '${previous.name}'`)
      continue
    }

    if (holdsRangeBlock(previous.to + 1, unit.from - 1)) {
      report(`blocks ${previous.to + 1}…${unit.from - 1} lie between '${previous.name}' and '${unit.name}' uncovered`)
    }
  }

  if (last && next !== undefined && next !== last.to + 1) {
    report(`the next window starts at ${next}, but '${last.name}' ends at ${last.to}`)
  }

  if (last && cursor !== undefined && last.to > cursor) {
    report(`'${last.name}' covers up to ${last.to}, past the cursor ${cursor}`)
  }

  if (!last && cursor !== undefined && next !== undefined && next > cursor + 1) {
    report(`the next window starts at ${next}, past the cursor ${cursor}`)
  }

  return violations
}

/**
 * Watermark coherence: the cursor and the floor are blocks (INV-5); the rollback chain is a list,
 * strictly ascending, hashed, above the floor and at or below the cursor (INV-1); and the committed
 * data stays at or below the cursor (INV-5). Only `undefined` is absent: a stored `null`, `0` or
 * `''` is a malformed watermark, not a missing one.
 */
export function validateWatermarks(state: OracleState, { dataBound }: { dataBound?: number } = {}): Violation[] {
  const violations: Violation[] = []
  const report = (property: string, message: string) => {
    violations.push({ validator: 'watermark-coherence', property, message })
  }

  // Every check below reads a field of the state.
  const isRecord = typeof state === 'object' && state !== null
  if (!isRecord) {
    report('INV-5', `the state ${JSON.stringify(state)} is not a record`)

    return violations
  }

  const { current, finalized, rollbackChain } = state

  for (const [label, watermark] of [
    ['cursor', current],
    ['floor', finalized],
  ] as const) {
    if (watermark !== undefined && !isBlockCursor(watermark)) {
      report('INV-5', `the ${label} ${JSON.stringify(watermark)} is not a block`)
    }
  }

  // The bounds below come from sound watermarks only; an unsound one is reported above.
  const floor = isBlockCursor(finalized) ? finalized.number : undefined
  const cursor = isBlockCursor(current) ? current.number : undefined

  const chainIsList = Array.isArray(rollbackChain)
  if (!chainIsList) {
    report('INV-1', `the rollback chain ${JSON.stringify(rollbackChain)} is not a list`)
  }

  // The chain holds processed blocks, and a processed block implies a cursor.
  const chain: unknown[] = chainIsList ? rollbackChain : []
  if (current === undefined && chain.length) {
    report('INV-1', `the rollback chain holds ${chain.length} blocks, but there is no cursor`)
  }

  let previous = Number.NEGATIVE_INFINITY
  for (const entry of chain) {
    if (!isBlockCursor(entry)) {
      report('INV-1', `rollback chain entry ${JSON.stringify(entry)} is not a block`)
    }

    const number = (entry as { number?: unknown } | null)?.number
    if (typeof number !== 'number') {
      continue
    }

    if (number <= previous) {
      report('INV-1', `the rollback chain does not ascend at ${number}`)
    }
    if (floor !== undefined && number <= floor) {
      report('INV-1', `rollback chain entry ${number} is at or below the floor ${floor}`)
    }
    if (cursor !== undefined && number > cursor) {
      report('INV-1', `rollback chain entry ${number} is above the cursor ${cursor}`)
    }

    previous = number
  }

  const dataAboveCursor = dataBound !== undefined && dataBound > (cursor ?? -1)
  if (dataAboveCursor) {
    report('INV-5', `the committed data reaches ${dataBound}, above the cursor ${cursor ?? 'none'}`)
  }

  return violations
}

/** Route-mode coherence: classes K and ∅ read the finalized-only route and nothing else (INV-25). */
export function validateRouteMode(
  durability: DurabilityClass,
  requests: readonly { endpoint: 'stream' | 'finalized-stream' }[],
): Violation[] {
  const finalizedOnly = durability === 'K' || durability === 'ephemeral'
  if (!finalizedOnly) {
    return []
  }

  return requests
    .map((request, index) => ({ request, index }))
    .filter(({ request }) => request.endpoint !== 'finalized-stream')
    .map(({ index }) => ({
      validator: 'route-mode-coherence',
      property: 'INV-25',
      message: `request ${index} of a class-${durability} pipe used /stream instead of /finalized-stream`,
    }))
}

/**
 * Sequence coherence (class C), in publish order: a topic's sequence never rewinds, and while the
 * barrier holds each new number is the next one (RP-45). A repeated number is an outbox republish
 * (CN-46), which at-least-once delivery allows only for the same operation.
 */
export function validateSequence(
  operations: readonly SequencedOperation[],
  { barrier = true }: { barrier?: boolean } = {},
): Violation[] {
  const violations: Violation[] = []
  const seen = new Map<string, Map<bigint, string | undefined>>()
  const top = new Map<string, bigint>()
  const report = (message: string) => {
    violations.push({ validator: 'sequence-coherence', property: 'RP-45', message })
  }

  operations.forEach((operation, index) => {
    const seq = toSequence(operation.seq)
    if (seq === undefined) {
      report(`operation ${index} on '${operation.topic}' carries ${String(operation.seq)}, not a sequence number`)

      return
    }

    const topicSeen = seen.get(operation.topic) ?? new Map<bigint, string | undefined>()
    seen.set(operation.topic, topicSeen)

    if (topicSeen.has(seq)) {
      const first = topicSeen.get(seq)
      if (first !== operation.key) {
        report(`operation ${index} on '${operation.topic}' reuses ${seq} for another operation`)
      }

      return
    }

    const previous = top.get(operation.topic)
    if (previous !== undefined && seq < previous) {
      report(`operation ${index} on '${operation.topic}' rewinds the sequence from ${previous} to ${seq}`)
    } else if (barrier && previous !== undefined && seq !== previous + 1n) {
      report(`operation ${index} on '${operation.topic}' skips from ${previous} to ${seq}`)
    }

    topicSeen.set(seq, operation.key)
    if (previous === undefined || seq > previous) {
      top.set(operation.topic, seq)
    }
  })

  return violations
}

/** A non-negative integer of any width, or a string of decimal digits; `BigInt` alone takes '' and '0x10'. */
function toSequence(value: number | bigint | string): bigint | undefined {
  if (typeof value === 'bigint') {
    return value >= 0n ? value : undefined
  }

  if (typeof value === 'number') {
    return Number.isSafeInteger(value) && value >= 0 ? BigInt(value) : undefined
  }

  return /^\d+$/.test(value) ? BigInt(value) : undefined
}

/** No residue: after recovery or at quiescence, nothing in flight is left in the store (INV-42). */
export function validateResidue(residue: readonly string[]): Violation[] {
  return residue.map((file) => ({
    validator: 'no-residue',
    property: 'INV-42',
    message: `'${file}' is neither a published unit nor the state record`,
  }))
}

/** One table of a class-K store, as one snapshot read it. */
export type ParquetTableSnapshot<Unit extends PublishedUnit = PublishedUnit> = {
  /** The table's published units, in window order. Rows come from these and nowhere else. */
  units: Unit[]
  /** The table's next window start, as the sidecar's coverage records it. */
  next: number | undefined
}

/**
 * One read of a class-K store: the state and every declared table. Every field is required, so no
 * check can be skipped by leaving its input out.
 */
export type ParquetSnapshot<Unit extends PublishedUnit = PublishedUnit, Table extends string = string> = {
  /** The IB-22 sidecar as ⟨C, F, RC⟩; `undefined` only when the store holds no state file. */
  state: OracleState | undefined
  /** Every declared table, by name, in declaration order. */
  tables: Record<Table, ParquetTableSnapshot<Unit>>
  /** Files that are neither a published unit of a declared table nor the state file. */
  residue: string[]
}

/** Every structural validator a class-K store answers to at quiescence, over one snapshot of it. */
export function validateParquetSnapshot(snapshot: ParquetSnapshot, ranges: readonly BlockRange[]): Violation[] {
  const { state, tables, residue } = snapshot
  const entries = Object.entries<ParquetTableSnapshot>(tables)
  const violations: Violation[] = []

  // With no table read, every per-table check below would pass vacuously.
  if (!entries.length) {
    violations.push({ validator: 'decodable', property: 'IB-22', message: 'the snapshot reads no table' })
  }

  // A cursor that is not a block bounds nothing; validateWatermarks reports it.
  const current = state?.current
  const cursor = isBlockCursor(current) ? current.number : undefined
  const rows: Attributed[] = []

  for (const [table, { units, next }] of entries) {
    const tableRows = units.flatMap((unit) => unit.rows)
    const found: Violation[] = []

    // CN-44: a published unit that no state accounts for is an orphan.
    if (state === undefined && units.length) {
      found.push({
        validator: 'watermark-coherence',
        property: 'CN-44',
        message: 'units are published, but the store holds no state',
      })
    }

    // IB-22: a store that published units, or processed blocks for them, records where the next window starts.
    const nextOwed = units.length > 0 || current !== undefined
    if (nextOwed && next === undefined) {
      found.push({
        validator: 'coverage-partition',
        property: 'INV-4',
        message: 'the store holds units or a cursor, but its state records no next window start',
      })
    }

    found.push(
      ...validateOrdered(tableRows),
      ...validateInRange(tableRows, ranges),
      ...validateItemsBelongToParent(units),
      ...validateCoverage(units, { ranges, next, cursor }),
    )

    violations.push(...found.map((violation) => ({ ...violation, message: `${table}: ${violation.message}` })))
    rows.push(...tableRows)
  }

  violations.push(...validateResidue(residue))

  if (state !== undefined) {
    const dataBound = rows.length ? rows.reduce((max, row) => Math.max(max, row.block), 0) : undefined

    violations.push(...validateWatermarks(state, { dataBound }))
  }

  return violations
}

/** One IB-21 sync row: the cursor it was written under, its floor, and its batch's chain fragment. */
export type SyncRecord = {
  current: Cursor
  finalized: Cursor | undefined
  /** The fragment of the batch the row committed; the full chain is assembled from every row (DEF-7). */
  rollbackChain: Cursor[]
}

/** One table of a class-T store, as one snapshot read it. */
export type PostgresTableSnapshot<Row extends Attributed = OracleRow> = {
  rows: Row[]
  /** The block tag of each record in the table's undo log (CN-30). */
  undo: number[]
}

/**
 * One read of a class-T store: the pipe's sync rows, the state assembled from them, and every
 * declared table. Every field is required, so no check can be skipped by leaving its input out.
 */
export type PostgresSnapshot<Table extends string = string, Row extends Attributed = OracleRow> = {
  /** The pipe's sync rows, oldest first. */
  sync: SyncRecord[]
  /** ⟨C, F, RC⟩ from the newest row, RC assembled from every row; `undefined` only with no row. */
  state: OracleState | undefined
  /** One ⟨RC, F⟩ per sync row, oldest first, each RC assembled up to its row. */
  records: RollbackRecord[]
  /** Every declared table, by name, in declaration order. */
  tables: Record<Table, PostgresTableSnapshot<Row>>
}

/** Every structural validator a class-T store answers to, over one snapshot of it. */
export function validatePostgresSnapshot(snapshot: PostgresSnapshot, ranges: readonly BlockRange[]): Violation[] {
  const { sync, state, tables } = snapshot
  const entries = Object.entries<PostgresTableSnapshot>(tables)
  const violations: Violation[] = []

  // With no table read, every per-table check below would pass vacuously.
  if (!entries.length) {
    violations.push({ validator: 'decodable', property: 'IB-21', message: 'the snapshot reads no table' })
  }

  // Every sync row is a commit point, so each must be a sound state of its own.
  sync.forEach((record, index) => {
    const found = validateWatermarks(record)

    violations.push(...found.map((violation) => ({ ...violation, message: `sync row ${index}: ${violation.message}` })))
  })

  // INV-12: from the oldest row to the newest, the floor never goes down.
  const floors = sync.map((record) => record.finalized?.number ?? -1)
  const lowered = floors.findIndex((floor, i) => i > 0 && floor < floors[i - 1])
  if (lowered > 0) {
    violations.push({
      validator: 'watermark-coherence',
      property: 'INV-12',
      message: `the floor goes down from ${floors[lowered - 1]} to ${floors[lowered]} at sync row ${lowered}`,
    })
  }

  // A cursor that is not a block bounds nothing; validateWatermarks reports it.
  const current = state?.current
  const cursor = isBlockCursor(current) ? current.number : -1
  const rows: Attributed[] = []

  for (const [table, { rows: tableRows, undo }] of entries) {
    const found: Violation[] = []

    // CN-44: rows that no state accounts for are orphans.
    if (state === undefined && tableRows.length) {
      found.push({
        validator: 'watermark-coherence',
        property: 'CN-44',
        message: 'the table holds rows, but the store holds no state',
      })
    }

    // INV-42: the undo log is written in the batch's own transaction, so a record above the cursor belongs to no commit.
    const strayUndo = undo.filter((block) => block > cursor)
    if (strayUndo.length) {
      found.push({
        validator: 'no-residue',
        property: 'INV-42',
        message: `the undo log holds records of blocks ${strayUndo.join(', ')}, above the cursor ${cursor}`,
      })
    }

    found.push(...validateOrdered(tableRows), ...validateInRange(tableRows, ranges))

    violations.push(...found.map((violation) => ({ ...violation, message: `${table}: ${violation.message}` })))
    rows.push(...tableRows)
  }

  if (state !== undefined) {
    const dataBound = rows.length ? rows.reduce((max, row) => Math.max(max, row.block), 0) : undefined

    violations.push(...validateWatermarks(state, { dataBound }))
  }

  return violations
}
