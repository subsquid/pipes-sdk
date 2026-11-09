import { describe, expect, it } from 'vitest'

import { BlockRangeConfigurationError } from '~/core/errors.js'
import { PortalClient } from '~/portal-client/index.js'
import { MOCK_GENESIS_TIMESTAMP } from '~/testing/evm/index.js'
import { withPortal } from '~/testing/portal-fixture.js'
import { stubPortal } from '~/testing/stub-portal.js'

import { EvmQueryBuilder } from './evm-query-builder.js'

describe('EvmQuery', () => {
  describe('calculateRanges', () => {
    it('should add default range', async () => {
      const qb = new EvmQueryBuilder()
      const { bounded } = await qb.calculateRanges({ portal: stubPortal() })

      expect(bounded).toEqual([{ range: { from: 0 } }])
    })

    it('should throw if to < from', async () => {
      const qb = new EvmQueryBuilder()
      qb.addRange({ from: 2, to: new Date('2026-10-01') })

      await expect(qb.calculateRanges({ portal: stubPortal() })).rejects.toThrow(BlockRangeConfigurationError)
      await expect(qb.calculateRanges({ portal: stubPortal() })).rejects.toThrow(
        "Invalid block range: 'from' (2) must be less than or equal to 'to' (0)",
      )
    })
  })

  describe('calculateRanges with resumeFrom', () => {
    it('drops the blocks below the resume block', async () => {
      const qb = new EvmQueryBuilder().addRange({ from: 1, to: 9 })

      const { raw, bounded } = await qb.calculateRanges({ portal: stubPortal(), resumeFrom: 4 })

      expect(raw).toEqual([{ range: { from: 1, to: 9 }, request: {} }])
      expect(bounded).toEqual([{ range: { from: 4, to: 9 }, request: {} }])
    })

    it('accepts a range that starts at the resume block', async () => {
      const qb = new EvmQueryBuilder().addRange({ from: 4, to: 9 })

      const { bounded } = await qb.calculateRanges({ portal: stubPortal(), resumeFrom: 4 })

      expect(bounded).toEqual([{ range: { from: 4, to: 9 }, request: {} }])
    })

    it('refuses a range that starts one block above the resume block', async () => {
      const qb = new EvmQueryBuilder().addRange({ from: 5, to: 9 })

      await expect(qb.calculateRanges({ portal: stubPortal(), resumeFrom: 4 })).rejects.toThrow(
        new BlockRangeConfigurationError([
          'The range starts at block 5, but the saved cursor is at block 3, so the pipe must continue at block 4. Block 4 would be skipped.',
          'If `from` is computed from the saved cursor, use a fixed start: the pipe continues from its cursor by itself, also after a fork.',
          'To skip these blocks on purpose, reset the pipe state or use a new pipe id.',
        ]),
      )
    })

    it('names every block a range far above the resume block would skip', async () => {
      const qb = new EvmQueryBuilder().addRange({ from: 1000 })

      await expect(qb.calculateRanges({ portal: stubPortal(), resumeFrom: 501 })).rejects.toThrow(
        'The range starts at block 1000, but the saved cursor is at block 500, so the pipe must continue at block 501. Blocks 501–999 would be skipped.',
      )
    })

    it('accepts a resume block in a gap between configured ranges', async () => {
      const qb = new EvmQueryBuilder().addRange({ from: 1, to: 1 }).addRange({ from: 5, to: 9 })

      const { bounded } = await qb.calculateRanges({ portal: stubPortal(), resumeFrom: 3 })

      expect(bounded).toEqual([{ range: { from: 5, to: 9 }, request: {} }])
    })

    it("resumes a 'latest' range at the resume block, below the head", async () => {
      const qb = new EvmQueryBuilder().addRange({ from: 'latest' })
      const portal = stubPortal({ getHead: async () => ({ number: 15, hash: '0x15' }) })

      const { bounded } = await qb.calculateRanges({ portal, resumeFrom: 4 })

      expect(bounded).toEqual([{ range: { from: 4 }, request: {} }])
    })
  })

  describe('from: timestamp', () => {
    it('should resolve a Date to a block number', async () => {
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: new Date('2024-01-01T00:00:00Z') })

      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({
          resolveTimestamp: async (ts) => {
            expect(ts).toBe(1704067200)
            return 18908900
          },
        }),
      })

      expect(bounded).toEqual([{ range: { from: 18908900 }, request: {} }])
    })

    it('should resolve a date string to a block number', async () => {
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: '2024-01-01' })

      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({
          resolveTimestamp: async (ts) => {
            expect(ts).toBe(1704067200)
            return 18908900
          },
        }),
      })

      expect(bounded).toEqual([{ range: { from: 18908900 }, request: {} }])
    })

    it('should resolve both from and to timestamps', async () => {
      const builder = new EvmQueryBuilder()
      builder.addRange({
        from: '2024-01-01',
        to: new Date('2024-02-01T00:00:00Z'),
      })

      const resolved = new Map([
        [1704067200, 18908900],
        [1706745600, 19145700],
      ])

      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({ resolveTimestamp: async (ts) => resolved.get(ts)! }),
      })

      expect(bounded).toEqual([{ range: { from: 18908900, to: 19145700 }, request: {} }])
    })

    // Mock chain blocks sit one second apart from the mock genesis, so block 10 is the head at genesis + 10 s.
    withPortal('resolves a date through the portal', async ({ portal: open }) => {
      const portal = await open({ latest: 10 })
      const builder = new EvmQueryBuilder()
      builder.addRange({
        from: new Date((MOCK_GENESIS_TIMESTAMP + 5) * 1000),
        to: new Date((MOCK_GENESIS_TIMESTAMP + 7) * 1000),
      })

      const { bounded } = await builder.calculateRanges({ portal: new PortalClient({ url: portal.url }) })

      expect(bounded).toEqual([{ range: { from: 5, to: 7 }, request: {} }])
    })

    withPortal('refuses a date after the head with a coded range error', async ({ portal: open }) => {
      const portal = await open({ latest: 10 })
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: new Date('2030-01-01T00:00:00Z') })

      const resolving = builder.calculateRanges({ portal: new PortalClient({ url: portal.url }) })

      await expect(resolving).rejects.toThrow(BlockRangeConfigurationError)
      await expect(resolving).rejects.toThrow(
        'Failed to resolve timestamp 2030-01-01T00:00:00.000Z to a block number. The block for this timestamp may not have been produced yet.',
      )
    })

    it('passes through a portal failure other than a missing block', async () => {
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: new Date('2024-01-01T00:00:00Z') })

      const outage = new Error('connect ECONNREFUSED')
      const resolving = builder.calculateRanges({
        portal: stubPortal({
          resolveTimestamp: async () => {
            throw outage
          },
        }),
      })

      await expect(resolving).rejects.toBe(outage)
    })

    it('should deduplicate identical timestamps', async () => {
      const builder = new EvmQueryBuilder()
      const date = new Date('2024-01-01T00:00:00Z')
      builder.addRange({ from: date })
      builder.addRange({ from: date })

      let callCount = 0
      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({
          resolveTimestamp: async () => {
            callCount++
            return 18908900
          },
        }),
      })

      expect(callCount).toBe(1)
    })
  })

  describe('from: latest', () => {
    it('should resolve to the latest', async () => {
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: 'latest' })

      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({
          getHead: async () => ({ number: 15, hash: '0x' }),
        }),
      })

      expect(bounded).toMatchInlineSnapshot(`
        [
          {
            "range": {
              "from": 15,
            },
            "request": {},
          },
        ]
      `)
    })

    it('should resolve latest with a block number as to', async () => {
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: 'latest', to: 100 })

      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({
          getHead: async () => ({ number: 15, hash: '0x' }),
        }),
      })

      expect(bounded).toMatchInlineSnapshot(`
        [
          {
            "range": {
              "from": 15,
              "to": 100,
            },
            "request": {},
          },
        ]
      `)
    })

    // An end below the head is an inverted range, also when it is block 0.
    it('refuses an end of block 0 below the latest head', async () => {
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: 'latest', to: 0 })

      const resolving = builder.calculateRanges({
        portal: stubPortal({ getHead: async () => ({ number: 10, hash: '0x' }) }),
      })

      await expect(resolving).rejects.toThrow(BlockRangeConfigurationError)
      await expect(resolving).rejects.toThrow("'from' (10) must be less than or equal to 'to' (0)")
    })

    it('should take bound over latest', async () => {
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: 'latest' })

      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({
          getHead: async () => ({ number: 15, hash: '0x' }),
        }),
        resumeFrom: 10,
      })

      expect(bounded).toMatchInlineSnapshot(`
        [
          {
            "range": {
              "from": 10,
            },
            "request": {},
          },
        ]
      `)
    })
  })
})
