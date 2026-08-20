import { EvmRpcClient, Rpc } from '@subsquid/evm-rpc'
import { cast } from '@subsquid/util-internal-validation'
import { describe, expect, it } from 'vitest'

import { PortalClient } from '~/portal-client/index.js'
import { FieldSelection, getBlockSchema } from '~/portal-client/query/evm.js'

import { EvmRpcSource } from './evm-rpc-source.js'
import { withRequiredFields } from './rpc/decode.js'

/**
 * Live parity test: fetch the same historical block from a real RPC endpoint (through the RPC
 * source) and from the Portal (raw + the same cast), and assert the two decode to identical
 * blocks. Network-gated: set `RPC_E2E=1` and `RPC_URL=https://rpc.subsquid.io/eth/<key>`.
 */

const ENABLED = process.env['RPC_E2E'] === '1' && !!process.env['RPC_URL']
const RPC_URL = process.env['RPC_URL']!
const PORTAL_URL = process.env['PORTAL_URL'] || 'https://portal.sqd.dev/datasets/ethereum-mainnet'
const BLOCK = Number(process.env['RPC_E2E_BLOCK'] || 22000000)

const FIELDS = {
  block: { timestamp: true, gasUsed: true, miner: true },
  transaction: { from: true, to: true, value: true, gas: true, gasUsed: true, status: true, input: true },
  log: { address: true, topics: true, data: true },
} satisfies FieldSelection

async function portalBlock(): Promise<any> {
  const portal = new PortalClient({ url: PORTAL_URL })
  const schema = getBlockSchema(withRequiredFields(FIELDS))
  const query: any = {
    type: 'evm',
    fields: withRequiredFields(FIELDS),
    fromBlock: BLOCK,
    toBlock: BLOCK,
    transactions: [{}],
    logs: [{}],
  }
  for await (const batch of portal.getStream(query, { finalized: true })) {
    for (const raw of batch.blocks) {
      const block: any = cast(schema, raw)
      if (block.header.number === BLOCK) return block
    }
  }
  throw new Error(`portal: block ${BLOCK} not found`)
}

async function rpcBlock(): Promise<any> {
  const source = new EvmRpcSource({
    rpc: new Rpc({ client: new EvmRpcClient({ url: RPC_URL, capacity: 5 }) }),
    fields: FIELDS,
    request: { transactions: [{}], logs: [{}] },
    from: BLOCK,
    to: BLOCK,
    finalized: true,
  })
  for await (const batch of source.read()) {
    for (const b of batch.data as any[]) {
      if (b.header.number === BLOCK) return b
    }
  }
  throw new Error(`rpc: block ${BLOCK} not found`)
}

describe.skipIf(!ENABLED)('EvmRpcSource — Portal parity', () => {
  it(`block ${BLOCK}: transactions + logs match the Portal output`, async () => {
    const [portal, rpc] = await Promise.all([portalBlock(), rpcBlock()])

    expect(rpc.transactions).toHaveLength(portal.transactions.length)
    expect(rpc.logs).toHaveLength(portal.logs.length)
    expect(rpc).toEqual(portal)
  }, 120_000)

  it(`block ${BLOCK}: filtering on an unselected field projects back to exactly F`, async () => {
    // Filter logs by the ERC-20 Transfer topic0 but select only `data` (NOT topics): the RPC
    // source must augment topics to filter, then project back so the output omits topics.
    const TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    const F = { log: { data: true } } satisfies FieldSelection

    const schema = getBlockSchema(withRequiredFields(F))
    const portalP = (async () => {
      const portal = new PortalClient({ url: PORTAL_URL })
      const query: any = {
        type: 'evm',
        fields: withRequiredFields(F),
        fromBlock: BLOCK,
        toBlock: BLOCK,
        logs: [{ topic0: [TRANSFER] }],
      }
      for await (const batch of portal.getStream(query, { finalized: true })) {
        for (const raw of batch.blocks) {
          const block: any = cast(schema, raw)
          if (block.header.number === BLOCK) return block
        }
      }
      throw new Error('portal block not found')
    })()

    const rpcP = (async () => {
      const source = new EvmRpcSource({
        rpc: new Rpc({ client: new EvmRpcClient({ url: RPC_URL, capacity: 5 }) }),
        fields: F,
        request: { logs: [{ topic0: [TRANSFER] }] },
        from: BLOCK,
        to: BLOCK,
        finalized: true,
      })
      for await (const batch of source.read()) {
        for (const b of batch.data as any[]) if (b.header.number === BLOCK) return b
      }
      throw new Error('rpc block not found')
    })()

    const [portal, rpc] = await Promise.all([portalP, rpcP])

    expect(rpc.logs.length).toBeGreaterThan(0)
    expect(rpc.logs).toEqual(portal.logs) // filtered set; no topics field
    expect(rpc.logs.every((l: any) => l.topics === undefined)).toBe(true)
  }, 120_000)
})
