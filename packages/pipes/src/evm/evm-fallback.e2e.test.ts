import { describe, expect, it } from 'vitest'

import { BlockStreamClient, StreamData } from '~/portal-client/index.js'
import { FieldSelection } from '~/portal-client/query/evm.js'

import { createEvmFallbackClient } from './evm-fallback.js'

/**
 * Live end-to-end test: drive a fallback built from a real Portal source and a real RPC source,
 * and assert it streams correctly and fails over when the primary is unavailable.
 * Network-gated (`RPC_E2E=1`, `RPC_URL=…`); skipped by default.
 */

const ENABLED = process.env['RPC_E2E'] === '1' && !!process.env['RPC_URL']
const RPC_URL = process.env['RPC_URL']!
const PORTAL_URL = process.env['PORTAL_URL'] || 'https://portal.sqd.dev/datasets/ethereum-mainnet'
const FROM = Number(process.env['RPC_E2E_BLOCK'] || 22000000)
const TO = FROM + 2

const FIELDS = {
  transaction: { from: true, to: true, value: true },
  log: { address: true, topics: true },
} satisfies FieldSelection

const QUERY: any = {
  type: 'evm',
  fields: FIELDS,
  fromBlock: FROM,
  toBlock: TO,
  transactions: [{}],
  logs: [{}],
}

async function streamNumbers(client: BlockStreamClient): Promise<number[]> {
  const out: number[] = []
  for await (const batch of client.getStream(QUERY, { finalized: true }) as AsyncIterable<StreamData<any>>) {
    out.push(...batch.blocks.map((b: any) => b.header.number))
  }
  return out
}

describe.skipIf(!ENABLED)('EVM fallback — live', () => {
  it('streams a range through the primary (Portal)', async () => {
    const fb = createEvmFallbackClient([PORTAL_URL, { type: 'rpc', name: 'rpc-standby', url: RPC_URL, capacity: 5 }], {
      finalized: true,
    })

    expect(await streamNumbers(fb)).toEqual([FROM, FROM + 1, TO])
  }, 120_000)

  it('fails over to the next source when the primary is down', async () => {
    const fb = createEvmFallbackClient(
      [
        // A portal URL that resolves but serves no such dataset — liveness dies, stream errors.
        { url: 'https://portal.sqd.dev/datasets/definitely-not-a-dataset', name: 'broken' },
        PORTAL_URL,
      ],
      { finalized: true },
    )

    expect(fb.activeIndex).toBeUndefined()
    expect(await streamNumbers(fb)).toEqual([FROM, FROM + 1, TO])
    expect(fb.activeIndex).toBe(1) // the good Portal took over
    expect(fb.switchCount).toBeGreaterThanOrEqual(1)
  }, 120_000)
})
