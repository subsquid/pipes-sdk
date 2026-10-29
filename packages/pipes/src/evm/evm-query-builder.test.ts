import { describe, expect, it } from 'vitest'

import { BlockRangeConfigurationError } from '~/core/errors.js'
import { PortalClient } from '~/portal-client/index.js'
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

    // Mock chain timestamps are `number * 1000` seconds, so block 10 is the head at 10,000 s.
    withPortal('resolves a date through the portal', async ({ portal: open }) => {
      const portal = await open({ latest: 10 })
      const builder = new EvmQueryBuilder()
      builder.addRange({ from: new Date(4_500 * 1000), to: new Date(7_000 * 1000) })

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
        bound: { from: 10 },
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
