import { isDeepStrictEqual } from 'node:util'

import { describe, expect, it } from 'vitest'

import {
  type Cursor,
  ORACLE_ERRORS,
  OracleError,
  type OracleRow,
  type PersistedState,
  ReferenceModel,
} from './reference-model.js'

/** Branch `a` is the chain the pipe saw; any other branch replaces it after a fork. */
function block(number: number, branch = 'a'): Cursor {
  return { number, hash: `0x${branch}${number}` }
}

function chain(from: number, to: number, branch = 'a'): Cursor[] {
  return Array.from({ length: to - from + 1 }, (_, i) => block(from + i, branch))
}

/**
 * The code of the oracle refusal a transition throws, or undefined when it goes through. A refusal
 * that leaves the model changed is a partial transition, and fails the test.
 */
function refusal(model: ReferenceModel, transition: () => unknown): string | undefined {
  const before = snapshot(model)

  try {
    transition()
  } catch (error) {
    if (!(error instanceof OracleError)) {
      throw error
    }

    const after = snapshot(model)
    if (!isDeepStrictEqual(after, before)) {
      throw new Error(`the refusal ${error.code} changed the model to ${JSON.stringify(after)}`)
    }

    return error.code
  }

  return undefined
}

function snapshot(model: ReferenceModel) {
  return { state: model.readState(), data: [...model.data], records: [...model.records] }
}

function blocksOf(model: ReferenceModel): number[] {
  return model.data.map((row) => row.block)
}

function numbersOf(cursors: Cursor[]): number[] {
  return cursors.map((cursor) => cursor.number)
}

describe('ReferenceModel', () => {
  describe('bind (WP-7)', () => {
    it('binds classes K and ∅ to the finalized-only route whatever the configured mode', () => {
      const parquet = new ReferenceModel({ durability: 'K', mode: 'full' })
      const memory = new ReferenceModel({ durability: 'ephemeral', mode: 'full' })

      expect([parquet.mode, memory.mode]).toEqual(['finalized-only', 'finalized-only'])
    })

    it('keeps the configured mode for a full-stream class', () => {
      const postgres = new ReferenceModel({ durability: 'T' })
      const bigquery = new ReferenceModel({ durability: 'W', mode: 'finalized-only' })

      expect([postgres.mode, bigquery.mode]).toEqual(['full', 'finalized-only'])
    })

    it('resolves the head through the effective view', () => {
      const finalizedOnly = new ReferenceModel({ durability: 'K' })
      const full = new ReferenceModel({ durability: 'T' })
      const head = { latest: 20, finalized: block(12) }

      expect(finalizedOnly.effectiveHead(head)).toBe(12)
      expect(finalizedOnly.effectiveHead({ latest: 20 })).toBe(20)
      expect(full.effectiveHead(head)).toBe(20)
    })
  })

  describe('T-BATCH', () => {
    it('moves the cursor to the last block of the batch', () => {
      const model = new ReferenceModel({ durability: 'T', ranges: [{ from: 0 }] })

      model.batch(chain(0, 3))

      expect(model.readState().current).toEqual(block(3))
    })

    it('starts at the range start and continues from the cursor (INV-20)', () => {
      const model = new ReferenceModel({ durability: 'T', ranges: [{ from: 5 }] })

      expect(refusal(model, () => model.batch(chain(4, 6)))).toBe(ORACLE_ERRORS.NOT_LINKED)

      model.batch(chain(5, 6))

      expect(refusal(model, () => model.batch(chain(8, 9)))).toBe(ORACLE_ERRORS.NOT_LINKED)
    })

    it('continues across the gap between two configured ranges (INV-20)', () => {
      const model = new ReferenceModel({ durability: 'T', ranges: [{ from: 0, to: 3 }, { from: 10 }] })

      model.batch(chain(0, 3))
      model.batch(chain(10, 12))

      expect(model.readState().current).toEqual(block(12))
    })

    it('refuses a batch that is empty, descends or leaves the range', () => {
      const model = new ReferenceModel({ durability: 'T', ranges: [{ from: 0, to: 3 }] })

      expect(refusal(model, () => model.batch([]))).toBe(ORACLE_ERRORS.NOT_LINKED)
      expect(refusal(model, () => model.batch([block(0), block(2), block(1)]))).toBe(ORACLE_ERRORS.NOT_ASCENDING)
      expect(refusal(model, () => model.batch(chain(0, 5)))).toBe(ORACLE_ERRORS.OUT_OF_RANGE)
    })

    it('refuses a batch or a reported floor that is not a block', () => {
      const model = new ReferenceModel({ durability: 'T' })

      expect(refusal(model, () => model.batch([{ number: 0, hash: '' }]))).toBe(ORACLE_ERRORS.MALFORMED_INPUT)
      expect(refusal(model, () => model.batch(chain(0, 1), { finalized: { number: -1, hash: '0xa' } }))).toBe(
        ORACLE_ERRORS.MALFORMED_INPUT,
      )
      expect(model.readState().current).toBeUndefined()
    })

    it('refuses any batch once the range set is done, a replay included (INV-20)', () => {
      const model = new ReferenceModel({ durability: 'T', ranges: [{ from: 0, to: 1 }] })
      model.batch(chain(0, 1))

      expect(refusal(model, () => model.batch(chain(0, 1)))).toBe(ORACLE_ERRORS.OUT_OF_RANGE)
      expect(refusal(model, () => model.batch(chain(2, 3)))).toBe(ORACLE_ERRORS.OUT_OF_RANGE)
      expect(blocksOf(model)).toEqual([0, 1])
    })

    it('never lowers the floor, whatever the head reports (INV-12)', () => {
      const model = new ReferenceModel({ durability: 'T' })

      model.batch(chain(0, 3), { finalized: block(3) })
      model.batch(chain(4, 5), { finalized: block(1) })
      model.batch(chain(6, 7))

      expect(model.readState().finalized).toEqual(block(3))
    })

    it('keeps the rollback chain to the processed blocks above the floor (INV-1)', () => {
      const model = new ReferenceModel({ durability: 'T' })

      model.batch(chain(0, 5), { finalized: block(2) })
      model.batch(chain(6, 7), { finalized: block(4) })

      expect(numbersOf(model.readState().rollbackChain)).toEqual([5, 6, 7])
    })

    it('records one ⟨RC, F⟩ per commit for the ancestor search (WP-42)', () => {
      const model = new ReferenceModel({ durability: 'T' })

      model.batch(chain(0, 2), { finalized: block(0) })
      model.batch(chain(3, 4), { finalized: block(3) })

      expect(model.records).toEqual([
        { rollbackChain: chain(1, 2), finalized: block(0) },
        { rollbackChain: [block(4)], finalized: block(3) },
      ])
    })

    it('keeps every processed block on a full stream without finality (INV-1, GAP-42)', () => {
      const model = new ReferenceModel({ durability: 'T' })

      model.batch(chain(0, 2))
      model.batch(chain(3, 4))

      expect(numbersOf(model.readState().rollbackChain)).toEqual([0, 1, 2, 3, 4])
    })

    it('keeps no fork history on the finalized-only route, bound or configured (WP-47)', () => {
      const bound = new ReferenceModel({ durability: 'K' })
      const configured = new ReferenceModel({ durability: 'W', mode: 'finalized-only' })

      bound.batch(chain(0, 3))
      configured.batch(chain(0, 3))

      expect(bound.readState().rollbackChain).toEqual([])
      expect(configured.readState().rollbackChain).toEqual([])
    })

    it('commits what the transform makes of each block (RS-10)', () => {
      const transform = (cursor: Cursor): OracleRow[] => [
        { table: 'blocks', block: cursor.number, value: cursor.hash },
        { table: 'logs', block: cursor.number, value: cursor.number * 2 },
      ]
      const model = new ReferenceModel({ durability: 'T', transform })

      model.batch(chain(0, 1))

      expect(model.data).toEqual([
        { table: 'blocks', block: 0, value: '0xa0' },
        { table: 'logs', block: 0, value: 0 },
        { table: 'blocks', block: 1, value: '0xa1' },
        { table: 'logs', block: 1, value: 2 },
      ])
    })

    it('refuses a transform that attributes a row to another block, before it commits anything (INV-3)', () => {
      const transform = (cursor: Cursor): OracleRow[] => [{ table: 'blocks', block: cursor.number - 1, value: 0 }]
      const model = new ReferenceModel({ durability: 'T', transform })

      expect(refusal(model, () => model.batch(chain(1, 2)))).toBe(ORACLE_ERRORS.MALFORMED_INPUT)
    })

    it('refuses a block above the floor on the finalized-only route (INV-25)', () => {
      const model = new ReferenceModel({ durability: 'K' })

      model.batch(chain(0, 3), { finalized: block(3) })

      expect(refusal(model, () => model.batch(chain(4, 6), { finalized: block(5) }))).toBe(ORACLE_ERRORS.NOT_FINALIZED)
    })

    it('passes every block through on a dataset without finality, and keeps no floor or chain (DEF-15)', () => {
      const model = new ReferenceModel({ durability: 'K' })

      model.batch(chain(0, 6))

      expect(blocksOf(model)).toEqual([0, 1, 2, 3, 4, 5, 6])
      expect(model.readState()).toEqual({ current: block(6), finalized: undefined, rollbackChain: [] })
    })
  })

  describe('T-FORK', () => {
    it('rewinds to the newest block both chains hold, and keeps the floor (WP-42, INV-13)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 5), { finalized: block(1) })

      const ancestor = model.fork([...chain(0, 3), ...chain(4, 6, 'b')])

      expect(ancestor).toEqual(block(3))
      expect(model.readState()).toEqual({ current: block(3), finalized: block(1), rollbackChain: chain(2, 3) })
      expect(blocksOf(model)).toEqual([0, 1, 2, 3])
    })

    it('refuses any fork on the finalized-only route (WP-45)', () => {
      const model = new ReferenceModel({ durability: 'K' })
      model.batch(chain(0, 3), { finalized: block(3) })

      expect(refusal(model, () => model.fork(chain(0, 4, 'b')))).toBe(ORACLE_ERRORS.FORK_ON_FINALIZED_ROUTE)
    })

    it('refuses an empty canonical chain (WP-41)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 3))

      expect(refusal(model, () => model.fork([]))).toBe(ORACLE_ERRORS.EMPTY_CANONICAL)
    })

    it('refuses a canonical chain that is not ascending blocks, before it deletes anything', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 3))

      const descending = [block(3, 'b'), block(2, 'b')]
      const fractional = [block(2), { number: 3.5, hash: '0xb' }]

      expect(refusal(model, () => model.fork(descending))).toBe(ORACLE_ERRORS.MALFORMED_INPUT)
      expect(refusal(model, () => model.fork(fractional))).toBe(ORACLE_ERRORS.MALFORMED_INPUT)
      expect(blocksOf(model)).toEqual([0, 1, 2, 3])
    })

    it('refuses a canonical chain that ends below the cursor (RP-43)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 5))

      expect(refusal(model, () => model.fork(chain(0, 2, 'b')))).toBe(ORACLE_ERRORS.CANONICAL_BELOW_CURSOR)
    })

    it('falls back to the floor when only the floor is left in the window (WP-42)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 3), { finalized: block(1) })

      const ancestor = model.fork([block(1), ...chain(2, 3, 'b')])

      expect(ancestor).toEqual(block(1))
      expect(model.readState()).toEqual({ current: block(1), finalized: block(1), rollbackChain: [] })
      expect(blocksOf(model)).toEqual([0, 1])
    })

    it('restarts below the window when the fork reaches past it (WP-42 deep fork)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 5))

      const ancestor = model.fork(chain(4, 6, 'b'))

      expect(ancestor).toEqual(block(3))
      expect(model.readState()).toEqual({ current: block(3), finalized: undefined, rollbackChain: chain(0, 3) })
      expect(blocksOf(model)).toEqual([0, 1, 2, 3])
    })

    it('reports no ancestor when no record is left to search', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 3), { finalized: block(3) })

      expect(refusal(model, () => model.fork(chain(2, 4, 'b')))).toBe(ORACLE_ERRORS.NO_ANCESTOR)
    })

    it('halts when the ancestor lies below the current floor (WP-44, GAP-6)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 5), { finalized: block(1) })
      model.batch(chain(6, 8), { finalized: block(6) })

      // Blocks 7 and 8 are replaced. The newest record holds nothing at or below the floor, and
      // the window keeps more than the floor, so the search reaches the older record and matches
      // block 5. As written, WP-44 halts here although the floor itself is canonical (GAP-6).
      const canonical = [...chain(4, 6), ...chain(7, 8, 'b')]

      expect(refusal(model, () => model.fork(canonical))).toBe(ORACLE_ERRORS.FINALITY_CONFLICT)
      expect(model.readState().current).toEqual(block(8))
    })
  })

  describe('T-INIT', () => {
    it('drops the rows the recovered cursor does not cover (CN-40…CN-44, INV-42)', () => {
      const model = new ReferenceModel({ durability: 'K' })
      model.batch(chain(0, 5), { finalized: block(5) })

      model.recover({ current: block(3), finalized: block(3), rollbackChain: [] })

      expect(blocksOf(model)).toEqual([0, 1, 2, 3])
      expect(model.readState()).toEqual({ current: block(3), finalized: block(3), rollbackChain: [] })
    })

    it('undoes a recovery that leaves unfinalized rows on the finalized-only route (CN-22)', () => {
      const model = new ReferenceModel({ durability: 'K' })
      model.batch(chain(0, 5))

      const refused = refusal(model, () => model.recover({ current: block(5), finalized: block(3), rollbackChain: [] }))

      expect(refused).toBe(ORACLE_ERRORS.INVARIANT)
    })

    it('refuses a cold start over data in exclusive tables (CN-44)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 3))

      expect(refusal(model, () => model.recover(undefined))).toBe(ORACLE_ERRORS.ORPHAN_DATA)
      expect(blocksOf(model)).toEqual([0, 1, 2, 3])
    })

    it('starts cold over shared tables, where the guard must not run (CN-44)', () => {
      const model = new ReferenceModel({ durability: 'T', tables: 'shared' })
      model.batch(chain(0, 3))

      model.recover(undefined)

      expect(model.readState()).toEqual({ current: undefined, finalized: undefined, rollbackChain: [] })
      expect(model.data).toEqual([])
    })

    it('starts cold with nothing persisted and nothing stored', () => {
      const model = new ReferenceModel({ durability: 'T' })

      model.recover(undefined)

      expect(model.readState()).toEqual({ current: undefined, finalized: undefined, rollbackChain: [] })
      expect(model.data).toEqual([])
    })

    it('refuses a malformed state, falsy values included, before it repairs anything (INV-5, INV-44)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 3))

      // A store reads back untyped values, so a stored `null`, `0`, `''` or `false` must not pass as absent.
      const malformed: Record<string, unknown> = {
        nullCursor: { current: null, finalized: block(1), rollbackChain: [] },
        zeroCursor: { current: 0, rollbackChain: [] },
        emptyFloor: { current: block(3), finalized: '', rollbackChain: [] },
        falseFloor: { current: block(3), finalized: false, rollbackChain: [] },
        nullChain: { current: block(3), rollbackChain: null },
        nullChainEntry: { current: block(3), rollbackChain: [null] },
        nullState: null,
        nullRecords: { current: block(3), rollbackChain: [], records: null },
        nullRecord: { current: block(3), rollbackChain: [], records: [null] },
        nullRecordChain: { current: block(3), rollbackChain: [], records: [{ rollbackChain: null }] },
        noNumber: { current: { hash: '0xa3' } as Cursor, rollbackChain: [] },
        negativeHeight: { current: { number: -1, hash: '0xa' }, rollbackChain: [] },
        unsafeHeight: { current: { number: 2 ** 53, hash: '0xa' }, rollbackChain: [] },
        noHash: { current: block(3), finalized: { number: 1, hash: '' }, rollbackChain: [] },
        chainBelowFloor: { current: block(3), finalized: block(2), rollbackChain: [block(2), block(3)] },
        recordNegativeHeight: {
          current: block(3),
          rollbackChain: [],
          records: [{ rollbackChain: [block(-1), block(1)] }],
        },
        recordDescending: { current: block(3), rollbackChain: [], records: [{ rollbackChain: [block(2), block(1)] }] },
        recordBelowItsFloor: {
          current: block(3),
          rollbackChain: [],
          records: [{ rollbackChain: [block(1), block(2)], finalized: block(1) }],
        },
      }

      const refuse = (state: unknown) => refusal(model, () => model.recover(state as PersistedState))
      const refusals = Object.entries(malformed).map(([name, state]) => [name, refuse(state)])
      const expected = Object.keys(malformed).map((name) => [name, ORACLE_ERRORS.STATE_MALFORMED])

      expect(refusals).toEqual(expected)
      expect(blocksOf(model)).toEqual([0, 1, 2, 3])
    })

    it('refuses rollback records whose floor goes down (INV-12)', () => {
      const model = new ReferenceModel({ durability: 'T' })
      const state = {
        current: block(3),
        finalized: block(1),
        rollbackChain: chain(2, 3),
        records: [{ rollbackChain: [block(3)], finalized: block(2) }],
      }

      expect(() => model.recover(state)).toThrow(/^INV-12: the floor goes down from 2 to 1/)
    })

    it('accepts a record that keeps a branch above the cursor, as a fork leaves it', () => {
      const model = new ReferenceModel({ durability: 'T' })
      const forkedAway = { rollbackChain: chain(2, 5, 'b'), finalized: block(1) }
      const current = { rollbackChain: chain(2, 3), finalized: block(1) }

      model.recover({
        current: block(3),
        finalized: block(1),
        rollbackChain: chain(2, 3),
        records: [forkedAway, current],
      })

      expect(model.readState().current).toEqual(block(3))
      expect(model.records).toEqual([forkedAway, current])
    })

    it('searches the recovered records on the next fork', () => {
      const model = new ReferenceModel({ durability: 'T' })
      model.batch(chain(0, 5), { finalized: block(1) })

      model.recover({ current: block(5), finalized: block(1), rollbackChain: chain(2, 5) })
      const ancestor = model.fork([...chain(0, 2), ...chain(3, 6, 'b')])

      expect(ancestor).toEqual(block(2))
      expect(model.readState()).toEqual({ current: block(2), finalized: block(1), rollbackChain: [block(2)] })
      expect(blocksOf(model)).toEqual([0, 1, 2])
    })
  })
})
