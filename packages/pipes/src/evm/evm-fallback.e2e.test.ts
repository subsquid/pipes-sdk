import { EvmRpcClient, Rpc } from '@subsquid/evm-rpc'
import { describe, expect, it } from 'vitest'

import { BlockCursor, FallbackSource, FallbackUnderlyingSource, PortalBatch } from '~/core/index.js'
import { FieldSelection } from '~/portal-client/query/evm.js'

import { createEvmFallback, evmPortalReadSource } from './evm-fallback.js'

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

const REQUEST = { transactions: [{}], logs: [{}] }

function rpc(): Rpc {
  return new Rpc({ client: new EvmRpcClient({ url: RPC_URL, capacity: 5 }) })
}

async function streamNumbers(source: { read(c?: BlockCursor): AsyncIterable<PortalBatch<any[]>> }): Promise<number[]> {
  const out: number[] = []
  for await (const batch of source.read()) {
    out.push(...batch.data.map((b: any) => b.header.number))
  }
  return out
}

describe.skipIf(!ENABLED)('EVM fallback — live', () => {
  it('streams a range through the primary (Portal)', async () => {
    const fb = createEvmFallback({
      fields: FIELDS,
      request: REQUEST,
      from: FROM,
      to: TO,
      sources: [
        { type: 'portal', portal: PORTAL_URL },
        { type: 'rpc', rpc: rpc() },
      ],
    })

    expect(await streamNumbers(fb)).toEqual([FROM, FROM + 1, TO])
  }, 120_000)

  it('fails over to the next source when the primary is down', async () => {
    const broken: FallbackUnderlyingSource<any[]> = {
      name: 'broken',
      // biome-ignore lint/correctness/useYield: intentionally throwing
      read: async function* () {
        throw new Error('primary down')
      },
    }
    const portal = evmPortalReadSource({
      portal: PORTAL_URL,
      fields: FIELDS,
      request: REQUEST,
      from: FROM,
      to: TO,
    })

    const fb = new FallbackSource<any[]>([broken, portal])

    expect(fb.activeIndex).toBeUndefined()
    expect(await streamNumbers(fb)).toEqual([FROM, FROM + 1, TO])
    expect(fb.activeIndex).toBe(1) // Portal took over
    expect(fb.switchCount).toBeGreaterThanOrEqual(1)
  }, 120_000)
})
