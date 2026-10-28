import { describe, expect, it } from 'vitest'

import type { OracleRow, OracleState } from './reference-model.js'
import {
  type ClickhouseSnapshot,
  type PostgresSnapshot,
  type PublishedUnit,
  type SyncRecord,
  type Violation,
  nextInRanges,
  nextOutsideRanges,
  validateClickhouseSnapshot,
  validateCoverage,
  validateDecodable,
  validateInRange,
  validateItemsBelongToParent,
  validateLinked,
  validateOrdered,
  validateParquetSnapshot,
  validatePostgresSnapshot,
  validateResidue,
  validateRouteMode,
  validateSequence,
  validateWatermarks,
} from './validators.js'

const cursor = (number: number) => ({ number, hash: `0x${number}` })
const rows = (...blocks: number[]) => blocks.map((block) => ({ block }))
const unit = (from: number, to: number, ...blocks: number[]): PublishedUnit => ({
  name: `${from}-${to}`,
  from,
  to,
  rows: rows(...blocks),
})

function propertiesOf(violations: Violation[]): string[] {
  return violations.map((violation) => violation.property)
}

function messagesOf(violations: Violation[]): string[] {
  return violations.map((violation) => violation.message)
}

describe('structural validators', () => {
  describe('range helpers', () => {
    it('finds the next block the range set holds', () => {
      const ranges = [{ from: 10 }, { from: 0, to: 3 }]

      expect(nextInRanges(2, ranges)).toBe(2)
      expect(nextInRanges(4, ranges)).toBe(10)
      expect(nextInRanges(4, [{ from: 0, to: 3 }])).toBeUndefined()
    })

    it('finds the next block the range set does not hold, across adjacent ranges', () => {
      const ranges = [{ from: 20 }, { from: 4, to: 9 }, { from: 0, to: 3 }]

      expect(nextOutsideRanges(0, ranges)).toBe(10)
      expect(nextOutsideRanges(12, ranges)).toBe(12)
      expect(nextOutsideRanges(25, ranges)).toBeUndefined()
    })
  })

  describe('decodable', () => {
    it('reports every item that does not parse', () => {
      const violations = validateDecodable(['{}', '{', '[]'], (item) => JSON.parse(item))

      expect(violations).toHaveLength(1)
      expect(violations[0].message).toMatch(/^item 1 does not parse/)
    })
  })

  describe('ordered', () => {
    it('allows several rows of one block and refuses a descent', () => {
      expect(validateOrdered(rows(1, 1, 2, 5))).toEqual([])
      expect(propertiesOf(validateOrdered(rows(1, 3, 2)))).toEqual(['INV-20'])
    })
  })

  describe('linked', () => {
    it('wants each batch to start right after the cursor before it', () => {
      const batches = [
        { blocks: [{ number: 0 }, { number: 3 }] },
        { cursorBefore: 3, blocks: [{ number: 4 }] },
        { cursorBefore: 4, blocks: [{ number: 6 }] },
      ]

      expect(propertiesOf(validateLinked(batches))).toEqual(['INV-20'])
    })

    it('lets a batch jump the gap between two configured ranges', () => {
      const ranges = [
        { from: 0, to: 3 },
        { from: 10, to: 20 },
      ]
      const batches = [{ cursorBefore: 3, blocks: [{ number: 10 }] }]

      expect(validateLinked(batches, ranges)).toEqual([])
    })

    it('refuses any batch after the range set ends, a replay included', () => {
      const batches = [{ cursorBefore: 20, blocks: [{ number: 19 }] }]
      const violations = validateLinked(batches, [{ from: 0, to: 20 }])

      expect(messagesOf(violations)).toEqual([
        'batch 0 starts at 19, but the range set holds no block after the cursor 20',
      ])
    })

    it('checks a cold start against the first block of the range set', () => {
      const ranges = [{ from: 10, to: 20 }]
      const late = [{ blocks: [{ number: 12 }] }]
      const onTime = [{ blocks: [{ number: 10 }] }]

      expect(propertiesOf(validateLinked(late, ranges))).toEqual(['INV-20'])
      expect(validateLinked(onTime, ranges)).toEqual([])
    })

    it('refuses a batch whose blocks repeat or descend, even on a cold start it cannot place', () => {
      const descending = [{ blocks: [{ number: 0 }, { number: 2 }, { number: 1 }] }]
      const repeated = [{ blocks: [{ number: 0 }, { number: 0 }, { number: 2 }] }]

      expect(messagesOf(validateLinked(descending))).toEqual(['batch 0 descends from 2 to 1'])
      expect(messagesOf(validateLinked(repeated))).toEqual(['batch 0 repeats block 0'])
    })

    it('refuses a batch that covers no block, holds a non-block or leaves the range set', () => {
      const batches = [
        { cursorBefore: 1, blocks: [] },
        { cursorBefore: -1, blocks: [{ number: 1 }, { number: 0 }] },
        { cursorBefore: 1, blocks: [{ number: 2 }, { number: 2.5 }, { number: 12 }] },
      ]
      const violations = validateLinked(batches, [{ from: 0, to: 9 }])

      expect(messagesOf(violations)).toEqual([
        'batch 0 covers no block',
        'batch 1 follows the cursor -1, which is not a block',
        'batch 1 descends from 1 to 0',
        'batch 2 holds 2.5, which is not a block',
        'batch 2 holds 12, which no configured range holds',
      ])
    })
  })

  describe('items-belong-to-parent', () => {
    it('refuses a row outside the window its unit claims', () => {
      const violations = validateItemsBelongToParent([unit(0, 9, 0, 9), unit(10, 19, 10, 20)])

      expect(propertiesOf(violations)).toEqual(['RP-22'])
    })

    it('refuses a row attributed to no block, which a comparison would read as 0', () => {
      const nullRow = { name: '0-9', from: 0, to: 9, rows: [{ block: null as unknown as number }] }

      expect(messagesOf(validateItemsBelongToParent([nullRow]))).toEqual([
        "unit '0-9' claims [0, 9] but holds a row at null",
      ])
    })
  })

  describe('in-range', () => {
    it('checks attribution against the range set, and only that it is a block without one', () => {
      const ranges = [{ from: 0, to: 3 }, { from: 10 }]

      expect(propertiesOf(validateInRange(rows(0, 3, 5, 10, 99), ranges))).toEqual(['INV-3'])
      expect(validateInRange(rows(5), [])).toEqual([])
      expect(messagesOf(validateInRange(rows(-1, 2.5), []))).toEqual([
        'a row is attributed to -1, which is not a block',
        'a row is attributed to 2.5, which is not a block',
      ])
    })
  })

  describe('coverage partition', () => {
    it('accepts windows that tile the range, sparse or empty ones included', () => {
      const units = [unit(0, 9, 4), unit(10, 29), unit(30, 30, 30)]

      expect(validateCoverage(units, { ranges: [{ from: 0, to: 30 }], next: 31, cursor: 30 })).toEqual([])
    })

    it('refuses a range start that no window covers', () => {
      const ranges = [{ from: 0, to: 49 }]

      expect(propertiesOf(validateCoverage([unit(10, 49)], { ranges, next: 50 }))).toEqual(['INV-4'])
      expect(propertiesOf(validateCoverage([], { ranges, next: 50, cursor: 49 }))).toEqual(['INV-4'])
      expect(validateCoverage([], { ranges, next: 0, cursor: 49 })).toEqual([])
    })

    it('allows a processed tail not yet published, and no window past the cursor', () => {
      const ranges = [{ from: 0 }]

      expect(validateCoverage([unit(0, 9)], { ranges, next: 10, cursor: 15 })).toEqual([])
      expect(propertiesOf(validateCoverage([unit(0, 9), unit(10, 20)], { ranges, next: 21, cursor: 15 }))).toEqual([
        'INV-4',
      ])
      expect(propertiesOf(validateCoverage([], { ranges, next: 20, cursor: 15 }))).toEqual(['INV-4', 'INV-4'])
    })

    it('refuses an overlap, a gap and a next window that does not follow the last one', () => {
      expect(propertiesOf(validateCoverage([unit(0, 9), unit(9, 12)]))).toEqual(['INV-4'])
      expect(propertiesOf(validateCoverage([unit(0, 9), unit(12, 15)]))).toEqual(['INV-4'])
      expect(propertiesOf(validateCoverage([unit(0, 9)], { next: 12 }))).toEqual(['INV-4'])
    })

    it('allows a gap that lies outside every configured range', () => {
      const ranges = [
        { from: 0, to: 9 },
        { from: 20, to: 29 },
      ]

      expect(validateCoverage([unit(0, 9), unit(20, 29)], { ranges })).toEqual([])
      expect(propertiesOf(validateCoverage([unit(0, 9), unit(22, 29)], { ranges }))).toEqual(['INV-4'])
    })

    it('refuses a window that claims blocks no configured range holds', () => {
      const ranges = [
        { from: 5, to: 9 },
        { from: 20, to: 29 },
      ]
      const messages = (units: PublishedUnit[]) =>
        validateCoverage(units, { ranges }).map((violation) => violation.message)

      expect(messages([unit(5, 29)])).toEqual(["'5-29' claims block 10, which no configured range holds"])
      expect(messages([unit(0, 9), unit(20, 29)])).toEqual(["'0-9' claims block 0, which no configured range holds"])
    })

    it('refuses a window whose bounds are reversed', () => {
      const messages = validateCoverage([unit(10, 5)]).map((violation) => violation.message)

      expect(messages).toEqual(["'10-5' claims the empty window 10…5"])
    })

    it('refuses a window or a next start that is not a block, with no range set to catch it', () => {
      const violations = validateCoverage([unit(-5, 9)], { next: 10.5 })

      expect(messagesOf(violations)).toEqual([
        'the next window start 10.5 is not a block',
        "'-5-9' claims the window -5…9, which is not a block range",
        "the next window starts at 10.5, but '-5-9' ends at 9",
      ])
    })
  })

  describe('watermark coherence', () => {
    it('accepts a chain strictly above the floor and at or below the cursor', () => {
      const state = { current: cursor(8), finalized: cursor(5), rollbackChain: [cursor(6), cursor(8)] }

      expect(validateWatermarks(state, { dataBound: 8 })).toEqual([])
    })

    it('refuses every way the chain or the data can leave its bounds (INV-1, INV-5)', () => {
      const state = {
        current: cursor(8),
        finalized: cursor(5),
        rollbackChain: [cursor(5), cursor(7), cursor(7), { number: 9, hash: '' }],
      }
      const messages = validateWatermarks(state, { dataBound: 10 }).map((violation) => violation.message)

      expect(messages).toEqual([
        'rollback chain entry 5 is at or below the floor 5',
        'the rollback chain does not ascend at 7',
        'rollback chain entry {"number":9,"hash":""} is not a block',
        'rollback chain entry 9 is above the cursor 8',
        'the committed data reaches 10, above the cursor 8',
      ])
    })

    it('refuses data with no cursor to cover it', () => {
      expect(propertiesOf(validateWatermarks({ rollbackChain: [] }, { dataBound: 0 }))).toEqual(['INV-5'])
    })

    it('refuses a cursor or floor that is not a block, and a chain with no cursor', () => {
      const negative = { current: { number: -1, hash: '0xa' }, finalized: { number: 2, hash: '' }, rollbackChain: [] }
      const headless = { rollbackChain: [cursor(3)] }

      expect(propertiesOf(validateWatermarks(negative))).toEqual(['INV-5', 'INV-5'])
      expect(propertiesOf(validateWatermarks(headless))).toEqual(['INV-1'])
    })

    it('reads a stored null, 0, empty string or false as malformed, never as absent', () => {
      const nullish = { current: null, finalized: 0, rollbackChain: null } as unknown as OracleState
      const falsy = { current: '', finalized: false, rollbackChain: [null] } as unknown as OracleState

      expect(messagesOf(validateWatermarks(nullish))).toEqual([
        'the cursor null is not a block',
        'the floor 0 is not a block',
        'the rollback chain null is not a list',
      ])
      expect(messagesOf(validateWatermarks(falsy))).toEqual([
        'the cursor "" is not a block',
        'the floor false is not a block',
        'rollback chain entry null is not a block',
      ])
      expect(messagesOf(validateWatermarks(null as unknown as OracleState))).toEqual(['the state null is not a record'])
    })
  })

  describe('route-mode coherence', () => {
    it('binds classes K and ∅ to the finalized-only route', () => {
      const requests = [{ endpoint: 'finalized-stream' as const }, { endpoint: 'stream' as const }]

      expect(propertiesOf(validateRouteMode('K', requests))).toEqual(['INV-25'])
      expect(propertiesOf(validateRouteMode('ephemeral', requests))).toEqual(['INV-25'])
      expect(validateRouteMode('T', requests)).toEqual([])
    })
  })

  describe('sequence coherence', () => {
    it('accepts a gap-free run and an outbox republish of it (CN-46)', () => {
      const operations = [1, 2, 3, 2, 3, 4].map((seq) => ({ topic: 'a', seq }))

      expect(validateSequence(operations)).toEqual([])
    })

    it('refuses a skip under the barrier and a rewind always (RP-45)', () => {
      const skip = [1, 2, 4].map((seq) => ({ topic: 'a', seq }))
      const rewind = [5, 6, 3].map((seq) => ({ topic: 'a', seq }))

      expect(propertiesOf(validateSequence(skip))).toEqual(['RP-45'])
      expect(validateSequence(skip, { barrier: false })).toEqual([])
      expect(propertiesOf(validateSequence(rewind, { barrier: false }))).toEqual(['RP-45'])
    })

    it('keeps one run per topic and reads big sequence numbers exactly', () => {
      const operations = [
        { topic: 'a', seq: '9007199254740993' },
        { topic: 'b', seq: 1 },
        { topic: 'a', seq: 9007199254740994n },
        { topic: 'b', seq: '2' },
      ]

      expect(validateSequence(operations)).toEqual([])
    })

    it('refuses a value that is not a sequence number, and a number reused for another operation', () => {
      const notNumbers = ['', '0x10', '-1', 1.5].map((seq) => ({ topic: 'a', seq }))
      const reused = [
        { topic: 'a', seq: 1, key: 'insert:x' },
        { topic: 'a', seq: 1, key: 'insert:x' },
        { topic: 'a', seq: 1, key: 'delete:x' },
      ]

      expect(propertiesOf(validateSequence(notNumbers))).toEqual(['RP-45', 'RP-45', 'RP-45', 'RP-45'])
      expect(validateSequence(reused).map((violation) => violation.message)).toEqual([
        "operation 2 on 'a' reuses 1 for another operation",
      ])
    })
  })

  describe('no residue', () => {
    it('reports every file that is neither a unit nor the state', () => {
      expect(propertiesOf(validateResidue(['transfers/.tmp-open.parquet']))).toEqual(['INV-42'])
      expect(validateResidue([])).toEqual([])
    })
  })

  describe('validateParquetSnapshot', () => {
    it('runs every store validator over the snapshot, rows taken from the units', () => {
      const ranges = [
        { from: 0, to: 4 },
        { from: 6, to: 9 },
      ]
      const snapshot = {
        state: { current: cursor(3), rollbackChain: [] },
        tables: { transfers: { units: [unit(0, 4, 0, 4, 2), unit(6, 9, 5)], next: 10 } },
        residue: ['transfers/.tmp-open.parquet'],
      }

      const violations = validateParquetSnapshot(snapshot, ranges)

      expect(messagesOf(violations)).toEqual([
        'transfers: attribution descends at index 2: 4 → 2',
        'transfers: a row at 5 falls outside every configured range',
        "transfers: unit '6-9' claims [6, 9] but holds a row at 5",
        "transfers: '6-9' covers up to 9, past the cursor 3",
        "'transfers/.tmp-open.parquet' is neither a published unit nor the state record",
        'the committed data reaches 5, above the cursor 3',
      ])
    })

    it('checks every table against its own next window start, and the cursor against all their rows', () => {
      const snapshot = {
        state: { current: cursor(9), rollbackChain: [] },
        tables: {
          transfers: { units: [unit(0, 9, 4)], next: 10 },
          approvals: { units: [unit(0, 4), unit(5, 12, 11)], next: 10 },
        },
        residue: [],
      }

      const violations = validateParquetSnapshot(snapshot, [])

      expect(messagesOf(violations)).toEqual([
        "approvals: the next window starts at 10, but '5-12' ends at 12",
        "approvals: '5-12' covers up to 12, past the cursor 9",
        'the committed data reaches 11, above the cursor 9',
      ])
    })

    it('refuses units with no state, and units or a cursor with no next window start', () => {
      const atNine = { current: cursor(9), rollbackChain: [] }
      const check = (state: OracleState | undefined, units: PublishedUnit[], next: number | undefined) =>
        validateParquetSnapshot({ state, tables: { transfers: { units, next } }, residue: [] }, [])

      const orphaned = check(undefined, [unit(0, 9)], 10)
      const unitsOnly = check(atNine, [unit(0, 9)], undefined)
      const cursorOnly = check(atNine, [], undefined)
      const untouched = check(undefined, [], undefined)
      const noNext = 'transfers: the store holds units or a cursor, but its state records no next window start'

      expect(messagesOf(orphaned)).toEqual(['transfers: units are published, but the store holds no state'])
      expect(messagesOf(unitsOnly)).toEqual([noNext])
      expect(messagesOf(cursorOnly)).toEqual([noNext])
      expect(untouched).toEqual([])
    })

    it('refuses a snapshot that reads no table, which every table check would pass', () => {
      const violations = validateParquetSnapshot({ state: undefined, tables: {}, residue: [] }, [])

      expect(messagesOf(violations)).toEqual(['the snapshot reads no table'])
    })

    it('checks a null state instead of reading it as no state', () => {
      const state = null as unknown as OracleState
      const tables = { transfers: { units: [], next: undefined } }
      const violations = validateParquetSnapshot({ state, tables, residue: [] }, [])

      expect(messagesOf(violations)).toEqual(['the state null is not a record'])
    })
  })
  describe('validatePostgresSnapshot', () => {
    const sync = (number: number, floor?: number, chain: number[] = []): SyncRecord => ({
      current: cursor(number),
      finalized: floor === undefined ? undefined : cursor(floor),
      rollbackChain: chain.map(cursor),
    })
    const tableRows = (table: string, ...blocks: number[]): OracleRow[] =>
      blocks.map((block) => ({ table, block, value: block }))
    const snapshotOf = (
      records: SyncRecord[],
      tables: PostgresSnapshot['tables'],
      state: OracleState | undefined = records.at(-1) && { ...records.at(-1), rollbackChain: [] },
    ): PostgresSnapshot => ({ sync: records, state, records: [], tables })

    it('passes a store whose rows, undo log and sync rows all sit at or below the cursor', () => {
      const snapshot = snapshotOf([sync(4, 2, [3, 4]), sync(9, 4, [9])], {
        transfers: { rows: tableRows('transfers', 1, 5, 9), undo: [3, 9] },
      })

      expect(validatePostgresSnapshot(snapshot, [{ from: 0, to: 9 }])).toEqual([])
    })

    it('runs every store validator over the snapshot, table by table', () => {
      const snapshot = snapshotOf([sync(3, 1)], {
        transfers: { rows: tableRows('transfers', 0, 4, 2), undo: [3, 4] },
        approvals: { rows: tableRows('approvals', 7), undo: [] },
      })

      const violations = validatePostgresSnapshot(snapshot, [{ from: 0, to: 4 }])

      expect(messagesOf(violations)).toEqual([
        'transfers: the undo log holds records of blocks 4, above the cursor 3',
        'transfers: attribution descends at index 2: 4 → 2',
        'approvals: a row at 7 falls outside every configured range',
        'the committed data reaches 7, above the cursor 3',
      ])
    })

    it('checks every sync row as a commit point of its own (INV-5, INV-1)', () => {
      const malformed = { current: null, finalized: cursor(2), rollbackChain: [] } as unknown as SyncRecord
      const snapshot = snapshotOf([sync(4, 2, [2, 5]), malformed, sync(9, 4)], {
        transfers: { rows: [], undo: [] },
      })

      const violations = validatePostgresSnapshot(snapshot, [])

      expect(messagesOf(violations)).toEqual([
        'sync row 0: rollback chain entry 2 is at or below the floor 2',
        'sync row 0: rollback chain entry 5 is above the cursor 4',
        'sync row 1: the cursor null is not a block',
      ])
    })

    it('refuses a floor that goes down from one sync row to the next (INV-12)', () => {
      const snapshot = snapshotOf([sync(4, 3), sync(6), sync(9, 2)], { transfers: { rows: [], undo: [] } })

      const violations = validatePostgresSnapshot(snapshot, [])

      expect(messagesOf(violations)).toEqual(['the floor goes down from 3 to -1 at sync row 1'])
    })

    it('refuses rows or undo records with no state: no commit accounts for them (CN-44, INV-42)', () => {
      const snapshot = snapshotOf([], {
        transfers: { rows: tableRows('transfers', 1), undo: [] },
        approvals: { rows: [], undo: [0] },
      })

      const violations = validatePostgresSnapshot(snapshot, [])

      expect(messagesOf(violations)).toEqual([
        'transfers: the table holds rows, but the store holds no state',
        'approvals: the undo log holds records of blocks 0, above the cursor -1',
      ])
    })

    it('checks the assembled state, a chain that repeats a block included', () => {
      const state = { current: cursor(9), finalized: cursor(2), rollbackChain: [3, 3, 9].map(cursor) }
      const snapshot = snapshotOf([sync(9, 2)], { transfers: { rows: [], undo: [] } }, state)

      const violations = validatePostgresSnapshot(snapshot, [])

      expect(messagesOf(violations)).toEqual(['the rollback chain does not ascend at 3'])
    })

    it('refuses a snapshot that reads no table, which every table check would pass', () => {
      const violations = validatePostgresSnapshot(snapshotOf([], {}), [])

      expect(messagesOf(violations)).toEqual(['the snapshot reads no table'])
    })
  })

  describe('validateClickhouseSnapshot', () => {
    const sync = (number: number, floor?: number, chain: number[] = []): SyncRecord => ({
      current: cursor(number),
      finalized: floor === undefined ? undefined : cursor(floor),
      rollbackChain: chain.map(cursor),
    })
    const tableRows = (table: string, ...blocks: number[]): OracleRow[] =>
      blocks.map((block) => ({ table, block, value: block }))
    const snapshotOf = (
      records: SyncRecord[],
      tables: ClickhouseSnapshot['tables'],
      state: OracleState | undefined = records.at(-1) && { ...records.at(-1), rollbackChain: [] },
    ): ClickhouseSnapshot => ({ sync: records, start: undefined, state, tables })

    it('passes a store whose rows and sync rows all sit at or below the cursor', () => {
      const snapshot = snapshotOf([sync(4, 2, [3, 4]), sync(9, 4, [9])], {
        transfers: { rows: tableRows('transfers', 1, 5, 5, 9) },
      })

      expect(validateClickhouseSnapshot(snapshot, [{ from: 0, to: 9 }])).toEqual([])
    })

    it('runs every store validator over the snapshot, table by table', () => {
      const snapshot = snapshotOf([sync(3, 1)], {
        transfers: { rows: tableRows('transfers', 0, 3, 2) },
        approvals: { rows: tableRows('approvals', 7) },
      })

      const violations = validateClickhouseSnapshot(snapshot, [{ from: 0, to: 4 }])

      expect(messagesOf(violations)).toEqual([
        'transfers: attribution descends at index 2: 3 → 2',
        'approvals: a row at 7 falls outside every configured range',
        'the committed data reaches 7, above the cursor 3',
      ])
    })

    it('checks every sync row as a commit point of its own, and the floor across them (INV-5, INV-1, INV-12)', () => {
      const malformed = { current: null, finalized: cursor(2), rollbackChain: [] } as unknown as SyncRecord
      const snapshot = snapshotOf([sync(4, 3, [3, 5]), malformed, sync(9, 2)], { transfers: { rows: [] } })

      const violations = validateClickhouseSnapshot(snapshot, [])

      expect(messagesOf(violations)).toEqual([
        'sync row 0: rollback chain entry 3 is at or below the floor 3',
        'sync row 0: rollback chain entry 5 is above the cursor 4',
        'sync row 1: the cursor null is not a block',
        'the floor goes down from 3 to 2 at sync row 1',
      ])
    })

    it('refuses rows with no state: no commit accounts for them (CN-44)', () => {
      const snapshot = snapshotOf([], { transfers: { rows: tableRows('transfers', 1) }, approvals: { rows: [] } })

      const violations = validateClickhouseSnapshot(snapshot, [])

      expect(messagesOf(violations)).toEqual(['transfers: the table holds rows, but the store holds no state'])
    })

    it('checks the state it is given, a chain that repeats a block included', () => {
      const state = { current: cursor(9), finalized: cursor(2), rollbackChain: [3, 3, 9].map(cursor) }
      const snapshot = snapshotOf([sync(9, 2)], { transfers: { rows: [] } }, state)

      const violations = validateClickhouseSnapshot(snapshot, [])

      expect(messagesOf(violations)).toEqual(['the rollback chain does not ascend at 3'])
    })

    it('refuses a snapshot that reads no table, which every table check would pass', () => {
      const violations = validateClickhouseSnapshot(snapshotOf([], {}), [])

      expect(messagesOf(violations)).toEqual(['the snapshot reads no table'])
    })
  })
})
