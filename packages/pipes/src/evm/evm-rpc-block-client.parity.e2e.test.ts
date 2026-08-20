import { cast } from '@subsquid/util-internal-validation'
import { describe, expect, it } from 'vitest'

import { BlockStreamClient, PortalClient } from '~/portal-client/index.js'
import { FieldSelection, getBlockSchema } from '~/portal-client/query/evm.js'

import { EvmRpcBlockClient } from './evm-rpc-block-client.js'
import { withRequiredFields } from './rpc/decode.js'

/**
 * Live parity test: fetch the same historical block from a real RPC endpoint (through the RPC
 * block client) and from the Portal, run both through the same downstream cast the facade applies,
 * and assert the two decode to identical blocks. Network-gated: set `RPC_E2E=1` and
 * `RPC_URL=https://rpc.subsquid.io/eth/<key>`.
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

/**
 * Stream one block through any {@link BlockStreamClient} and decode it exactly the way the facade's
 * "normalize data" step does — `cast` at the query's field selection. Wire-shape parity between the
 * RPC client and the Portal is what makes them interchangeable behind one stream.
 */
async function fetchBlock(client: BlockStreamClient, fields: FieldSelection, request: any): Promise<any> {
  const schema = getBlockSchema(withRequiredFields(fields))
  const query: any = {
    type: 'evm',
    fields: withRequiredFields(fields),
    fromBlock: BLOCK,
    toBlock: BLOCK,
    ...request,
  }
  for await (const batch of client.getStream(query, { finalized: true })) {
    for (const raw of batch.blocks) {
      const block: any = cast(schema, raw)
      if (block.header.number === BLOCK) return block
    }
  }
  throw new Error(`block ${BLOCK} not found`)
}

function rpcClient(): EvmRpcBlockClient {
  return new EvmRpcBlockClient({ rpc: { url: RPC_URL, capacity: 5 }, finalized: true })
}

describe.skipIf(!ENABLED)('EvmRpcBlockClient — Portal parity', () => {
  it(`block ${BLOCK}: transactions + logs match the Portal output`, async () => {
    const request = { transactions: [{}], logs: [{}] }
    const [portal, rpc] = await Promise.all([
      fetchBlock(new PortalClient({ url: PORTAL_URL, finalized: true }), FIELDS, request),
      fetchBlock(rpcClient(), FIELDS, request),
    ])

    expect(rpc.transactions).toHaveLength(portal.transactions.length)
    expect(rpc.logs).toHaveLength(portal.logs.length)
    expect(rpc).toEqual(portal)
  }, 120_000)

  it(`block ${BLOCK}: filtering on an unselected field still projects the output to exactly F`, async () => {
    // Filter logs by the ERC-20 Transfer topic0 but select only `data` (NOT topics): the RPC client
    // filters on a decoded throwaway copy, and the downstream cast prunes the wire block to the
    // selection — so the output must omit topics, exactly like the Portal's.
    const TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    const F = { log: { data: true } } satisfies FieldSelection
    const request = { logs: [{ topic0: [TRANSFER] }] }

    const [portal, rpc] = await Promise.all([
      fetchBlock(new PortalClient({ url: PORTAL_URL, finalized: true }), F, request),
      fetchBlock(rpcClient(), F, request),
    ])

    expect(rpc.logs.length).toBeGreaterThan(0)
    expect(rpc.logs).toEqual(portal.logs) // filtered set; no topics field
    expect(rpc.logs.every((l: any) => l.topics === undefined)).toBe(true)
  }, 120_000)
})
