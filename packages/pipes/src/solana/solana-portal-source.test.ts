import { afterEach, describe, expect, it } from 'vitest'

import * as solana from '~/portal-client/query/solana.js'
import { solanaQuery } from '~/solana/solana-query-builder.js'

import { MockPortal, mockPortal } from '../testing/index.js'
import { solanaPortalStream } from './solana-portal-source.js'

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
          { header: { number: 1, hash: 'abcd1', timestamp: 1000 } },
          { header: { number: 2, hash: 'abcd2', timestamp: 2000 } },
        ],
      },
    ])

    const fields = {
      log: { programId: true, message: true },
      block: { number: true, hash: true, timestamp: true },
      transaction: { version: true },
      instruction: { data: true },
      tokenBalance: { account: true },
      balance: { account: true },
      reward: { lamports: true },
    } satisfies solana.FieldSelection

    const stream = solanaPortalStream({
      id: 'test',
      source: portal.url,
      outputs: solanaQuery().addFields(fields).addRange({ from: 0, to: 2 }),
    })

    for await (const { data } of stream) {
      const [block] = data

      expect(block.logs).toBeInstanceOf(Array)
      expect(block.transactions).toBeInstanceOf(Array)
      expect(block.instructions).toBeInstanceOf(Array)
      expect(block.tokenBalances).toBeInstanceOf(Array)
      expect(block.balances).toBeInstanceOf(Array)
      expect(block.rewards).toBeInstanceOf(Array)
    }
  })
})
