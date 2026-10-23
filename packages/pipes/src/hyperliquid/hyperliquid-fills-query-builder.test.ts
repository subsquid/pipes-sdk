import { describe, expect, it } from 'vitest'

import { stubPortal } from '~/testing/stub-portal.js'

import { HyperliquidFillsQueryBuilder } from './hyperliquid-fills-query-builder.js'

describe('HyperliquidFillsQueryBuilder', () => {
  describe('from: latest', () => {
    it('should resolve to the latest', async () => {
      const builder = new HyperliquidFillsQueryBuilder()
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

    it('should take bound over latest', async () => {
      const builder = new HyperliquidFillsQueryBuilder()
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
