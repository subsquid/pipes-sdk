import { afterEach, describe, expect, it } from 'vitest'

import { MockPortal, mockPortal } from '../testing/index.js'
import { hyperliquidFillsPortalStream } from './hyperliquid-fills-portal-source.js'
import { HyperliquidFillsQueryBuilder } from './hyperliquid-fills-query-builder.js'

describe('Portal abstract stream', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  it('should add default fields', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0xabcd1', timestamp: 1000 } },
          { header: { number: 2, hash: '0xabcd2', timestamp: 2000 } },
        ],
      },
    ])

    const stream = hyperliquidFillsPortalStream({
      id: 'test',
      source: portal.url,
      outputs: new HyperliquidFillsQueryBuilder()
        .addFields({
          block: {
            number: true,
            hash: true,
            timestamp: true,
          },
          fill: {
            user: true,
            coin: true,
            px: true,
            sz: true,
            side: true,
          },
        })
        .addRange({ from: 0, to: 2 }),
    })

    for await (const { data } of stream) {
      const [block] = data

      expect(block.fills).toBeInstanceOf(Array)
    }
  })
})
