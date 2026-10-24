import { describe, expect, it } from 'vitest'

import { RollbackRecord, resolveForkCursor } from './fork.js'
import { BlockCursor } from './types.js'

function block(number: number, hash?: string) {
  return { number, hash: hash ?? `0x${number}` }
}

describe('resolveForkCursor', () => {
  describe('single record (memory target scenario)', () => {
    it('should find the last common block before a fork', async () => {
      const result = await resolveForkCursor(
        [
          {
            rollbackChain: [block(5), block(6), block(7)],
            finalized: block(4),
          },
        ],
        [block(5), block(6, '0x6a'), block(7, '0x7a')],
      )

      expect(result).toEqual(block(5))
    })

    it('should return null when all chain blocks are forked', async () => {
      const result = await resolveForkCursor(
        [
          {
            rollbackChain: [block(5, '0x5a'), block(6, '0x6a')],
            finalized: block(4),
          },
        ],
        [block(5, '0x5b'), block(6, '0x6b')],
      )

      // Block 6: no match, remaining filtered to [{5, 0x5b}]
      // Block 5: no match, remaining filtered to []
      // After loop: remaining.length === 0, finalized fallback requires length === 1
      expect(result).toBeNull()
    })

    it('should return null for empty rollback chain', async () => {
      const result = await resolveForkCursor(
        [
          {
            rollbackChain: [],
            finalized: block(4),
          },
        ],
        [block(5), block(6)],
      )

      expect(result).toBeNull()
    })

    it('should return null for no records', async () => {
      const result = await resolveForkCursor([], [block(5), block(6)])

      expect(result).toBeNull()
    })

    it('should fall back to finalized block when no chain blocks match', async () => {
      const result = await resolveForkCursor(
        [
          {
            rollbackChain: [block(6, '0x6a')],
            finalized: block(5),
          },
        ],
        // previousBlocks: block 6 doesn't match, filter leaves only block 5 which matches finalized
        [block(5), block(6, '0x6b')],
      )

      expect(result).toEqual(block(5))
    })

    it('should fall back to finalized when the window holds other blocks too', async () => {
      const result = await resolveForkCursor(
        [
          {
            rollbackChain: [block(7, '0x7a')],
            finalized: block(5),
          },
        ],
        [block(4), block(5), block(6, '0x6b'), block(7, '0x7b')],
      )

      expect(result).toEqual(block(5))
    })
  })

  describe('multiple records (database target scenario)', () => {
    it('should find a match in the first record', async () => {
      const result = await resolveForkCursor(
        [
          {
            rollbackChain: [block(8), block(9), block(10)],
            finalized: block(7),
          },
          {
            rollbackChain: [block(5), block(6), block(7)],
            finalized: block(4),
          },
        ],
        [block(8), block(9, '0x9a'), block(10, '0x10a')],
      )

      expect(result).toEqual(block(8))
    })

    it('should search across multiple records when first has no match', async () => {
      const result = await resolveForkCursor(
        [
          {
            // Most recent record — all blocks forked
            rollbackChain: [block(8, '0x8a'), block(9, '0x9a')],
            finalized: block(7),
          },
          {
            // Older record — has a common ancestor
            rollbackChain: [block(6), block(7)],
            finalized: block(5),
          },
        ],
        [block(6), block(7), block(8, '0x8b'), block(9, '0x9b')],
      )

      expect(result).toEqual(block(7))
    })

    it('should handle records with empty rollback chains', async () => {
      const result = await resolveForkCursor(
        [
          { rollbackChain: [], finalized: block(4) },
          { rollbackChain: [], finalized: block(4) },
          {
            rollbackChain: [block(5), block(6)],
            finalized: block(4),
          },
        ],
        [block(5), block(6, '0x6a')],
      )

      expect(result).toEqual(block(5))
    })
  })

  describe('deep fork handling', () => {
    it('should return chain block when previousBlocks is exhausted and block > finalized', async () => {
      const result = await resolveForkCursor(
        [
          {
            rollbackChain: [block(5), block(6), block(7)],
            finalized: block(3),
          },
        ],
        // Only one previousBlock, doesn't match anything
        [block(7, '0x7a')],
      )

      // After checking block 7 (desc): no match, previousBlocks filtered to empty
      // Block 6 > finalized 3 → return block 6
      expect(result).toEqual(block(6))
    })

    it('should return null when deep fork goes below finalized', async () => {
      const result = await resolveForkCursor(
        [
          { rollbackChain: [block(7, '0x7a'), block(8, '0x8a')], finalized: block(6) },
          { rollbackChain: [block(4), block(5)], finalized: block(3) },
        ],
        [block(6, '0x6b'), block(7, '0x7b'), block(8, '0x8b')],
      )

      // The window empties at block 5, and the restart cursor 4 lies below the floor 6
      expect(result).toBeNull()
    })
  })

  describe('finalized floor', () => {
    const records = [
      { rollbackChain: [block(7), block(8)], finalized: block(6) },
      { rollbackChain: [block(2), block(3), block(4), block(5)], finalized: block(1) },
    ]

    it('should resolve to the floor when the window also holds blocks below it', async () => {
      const result = await resolveForkCursor(records, [
        block(4),
        block(5),
        block(6),
        block(7, '0x7b'),
        block(8, '0x8b'),
      ])

      expect(result).toEqual(block(6))
    })

    it('should search older records for blocks above the floor before falling back to it', async () => {
      const result = await resolveForkCursor(
        [
          { rollbackChain: [block(7), block(8)], finalized: block(4) },
          { rollbackChain: [block(5), block(6)], finalized: block(4) },
        ],
        [block(3), block(4), block(5), block(6), block(7, '0x7b'), block(8, '0x8b')],
      )

      expect(result).toEqual(block(6))
    })

    it('should restart from a retained block at the floor when the window empties', async () => {
      const result = await resolveForkCursor(
        [
          { rollbackChain: [block(7, '0x7a'), block(8, '0x8a')], finalized: block(6) },
          { rollbackChain: [block(5), block(6)], finalized: block(4) },
        ],
        [block(7, '0x7b'), block(8, '0x8b')],
      )

      expect(result).toEqual(block(6))
    })

    it('should return null when a canonical match lies below the floor', async () => {
      const result = await resolveForkCursor(records, [
        block(4),
        block(5),
        block(6, '0x6b'),
        block(7, '0x7b'),
        block(8, '0x8b'),
      ])

      expect(result).toBeNull()
    })

    it('should return null when only an older record floor is canonical', async () => {
      const result = await resolveForkCursor(records, [block(1), block(6, '0x6b'), block(7, '0x7b'), block(8, '0x8b')])

      expect(result).toBeNull()
    })

    it('should take the floor from a record after the match', async () => {
      const result = await resolveForkCursor(
        [
          { rollbackChain: [block(5), block(7)], finalized: block(4) },
          { rollbackChain: [], finalized: block(6) },
        ],
        [block(5), block(6, '0x6b'), block(7, '0x7b')],
      )

      expect(result).toBeNull()
    })

    it('should hold the highest floor when records arrive out of order', async () => {
      const result = await resolveForkCursor(
        [
          { rollbackChain: [block(8)], finalized: block(4) },
          { rollbackChain: [block(7)], finalized: block(6) },
          { rollbackChain: [block(4), block(5)], finalized: block(3) },
        ],
        [block(5), block(6, '0x6b'), block(7, '0x7b'), block(8, '0x8b')],
      )

      expect(result).toBeNull()
    })

    it('should take the floor from a record with an empty chain', async () => {
      const result = await resolveForkCursor(
        [
          { rollbackChain: [], finalized: block(9) },
          { rollbackChain: [block(5), block(6)], finalized: block(4) },
        ],
        [block(5), block(6, '0x6a')],
      )

      expect(result).toBeNull()
    })

    it('should resolve to the floor of a record with an empty chain', async () => {
      const result = await resolveForkCursor(
        [
          { rollbackChain: [], finalized: block(9) },
          { rollbackChain: [block(5), block(6)], finalized: block(4) },
        ],
        [block(8), block(9), block(10, '0x10b')],
      )

      expect(result).toEqual(block(9))
    })

    it('should treat a finalized cursor without a block number as absent', async () => {
      const result = await resolveForkCursor(
        [{ rollbackChain: [], finalized: {} as BlockCursor }, ...records],
        [block(4), block(5), block(6, '0x6b'), block(7, '0x7b'), block(8, '0x8b')],
      )

      expect(result).toBeNull()
    })
  })

  describe('async iterable support', () => {
    it('should work with async generators', async () => {
      async function* records(): AsyncIterable<RollbackRecord> {
        yield {
          rollbackChain: [block(5), block(6)],
          finalized: block(4),
        }
      }

      const result = await resolveForkCursor(records(), [block(5), block(6, '0x6a')])

      expect(result).toEqual(block(5))
    })

    it('should read every record of an async source before the search', async () => {
      async function* records(): AsyncIterable<RollbackRecord> {
        yield { rollbackChain: [block(5), block(7)], finalized: block(4) }
        yield { rollbackChain: [], finalized: block(6) }
      }

      const result = await resolveForkCursor(records(), [block(5), block(6, '0x6b'), block(7, '0x7b')])

      expect(result).toBeNull()
    })
  })

  describe('edge cases', () => {
    it('should handle single-block rollback chain', async () => {
      const result = await resolveForkCursor([{ rollbackChain: [block(5)], finalized: block(4) }], [block(5)])

      expect(result).toEqual(block(5))
    })

    it('should handle previousBlocks with no overlapping block numbers', async () => {
      const result = await resolveForkCursor(
        [
          {
            rollbackChain: [block(5), block(6)],
            finalized: block(4),
          },
        ],
        [block(10), block(11)],
      )

      // Block 6 (desc): hash 0x6 not in previousBlocks, filter to blocks < 6 → empty
      // Block 5: previousBlocks empty, 5 > finalized 4 → return block 5
      expect(result).toEqual(block(5))
    })

    it('should handle record without finalized block', async () => {
      const result = await resolveForkCursor([{ rollbackChain: [block(5)], finalized: undefined }], [block(5)])

      expect(result).toEqual(block(5))
    })

    it('should return null for single-block chain with no match and no finalized', async () => {
      const result = await resolveForkCursor([{ rollbackChain: [block(5, '0x5a')] }], [block(5, '0x5b')])

      // Block 5: no hash match, remaining filtered to empty after loop ends
      // No finalized fallback available
      expect(result).toBeNull()
    })
  })
})
