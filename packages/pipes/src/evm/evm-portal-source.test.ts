import { describe, expect } from 'vitest'

import { BlockValidationError } from '~/core/errors.js'
import { mockBlock } from '~/testing/evm/index.js'
import { blockDecoder, readAll } from '~/testing/index.js'
import { withPortal } from '~/testing/portal-fixture.js'

import { evmPortalStream } from './evm-portal-source.js'
import { evmQuery } from './evm-query-builder.js'

describe('evmPortalStream', () => {
  withPortal('should add default fields', async ({ portal: open }) => {
    const portal = await open([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0x123', timestamp: 1000 } },
          { header: { number: 2, hash: '0x456', timestamp: 2000 } },
        ],
      },
    ])

    const fields = {
      log: { address: true, data: true, topics: true },
      block: { number: true, hash: true, timestamp: true },
      transaction: { from: true, to: true, hash: true },
      stateDiff: { address: true, key: true },
      trace: { error: true },
    }

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: evmQuery().addFields(fields).addRange({ from: 0, to: 2 }),
    })

    for await (const { data } of stream) {
      const [block] = data

      expect(block.logs).toBeInstanceOf(Array)
      expect(block.traces).toBeInstanceOf(Array)
      expect(block.transactions).toBeInstanceOf(Array)
      expect(block.stateDiffs).toBeInstanceOf(Array)
    }
  })

  // A query with no item requests gets only the range's last block, so the bad block ends the range.
  withPortal('names the block a field of which does not match the schema', async ({ portal: open }) => {
    const bad = mockBlock({ number: 2 })
    const portal = await open({ blocks: [{ ...bad, header: { ...bad.header, timestamp: 'noon' as any } }] })

    const stream = evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 2 }) })
    const reading = readAll(stream)

    await expect(reading).rejects.toThrow(BlockValidationError)
    await expect(reading).rejects.toThrow('Block 2 does not match the schema of the selected fields')
  })
})
