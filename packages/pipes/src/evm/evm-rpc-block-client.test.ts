import { describe, expect, it } from 'vitest'

import { EvmRpcBlockClient } from './evm-rpc-block-client.js'

/**
 * Network-free surface tests for the RPC block client — construction, identity, and the synthetic
 * dataset surface. The streaming pipeline is covered by the wire-mapper unit tests
 * (`rpc/wire.test.ts`) and the gated live parity/e2e suites.
 */

const KEYED_URL = 'https://rpc.example/eth/verysecretapikeyvalue1234567890'

describe('EvmRpcBlockClient', () => {
  it('constructs from plain connection options and redacts credentials in getUrl', () => {
    const client = new EvmRpcBlockClient({ rpc: { url: KEYED_URL, capacity: 5, rateLimit: 10 } })

    expect(client.getUrl()).not.toContain('verysecretapikeyvalue1234567890')
    expect(client.getUrl()).toContain('rpc.example')
  })

  it('defaults to hot (unfinalized) streaming; `finalized` opts into finalized-only', () => {
    expect(new EvmRpcBlockClient({ rpc: { url: KEYED_URL } }).finalized).toBe(false)
    expect(new EvmRpcBlockClient({ rpc: { url: KEYED_URL }, finalized: true }).finalized).toBe(true)
  })

  it('synthesizes real-time dataset metadata named after the source', async () => {
    const client = new EvmRpcBlockClient({ rpc: { url: KEYED_URL }, name: 'my-rpc' })

    expect(await client.getMetadata()).toEqual({ dataset: 'my-rpc', aliases: [], real_time: true, start_block: 0 })
  })

  it('rejects timestamp range resolution with an actionable error', async () => {
    const client = new EvmRpcBlockClient({ rpc: { url: KEYED_URL } })

    await expect(client.resolveTimestamp(1700000000)).rejects.toThrowError(/portal source/)
  })

  it('answers getHeight with eth_blockNumber, not a block lookup', async () => {
    // The head poll runs once per batch per standby at tip pace, so it must be the cheapest call
    // the provider offers. The finalized commitment has no eth_blockNumber equivalent and stays on
    // the block lookup.
    const calls: string[] = []
    const stubRpcClient = {
      call: async (method: string) => {
        calls.push(method)
        if (method === 'eth_blockNumber') return '0x64'
        if (method === 'eth_getBlockByNumber') {
          return { number: '0x63', hash: `0x${'ab'.repeat(32)}`, parentHash: `0x${'cd'.repeat(32)}` }
        }
        throw new Error(`unexpected method ${method}`)
      },
      batchCall: async () => [],
      getConcurrency: () => 10,
      url: KEYED_URL,
    }
    const { Rpc } = await import('@subsquid/evm-rpc')
    const client = new EvmRpcBlockClient({ rpc: new Rpc({ client: stubRpcClient as never }) })

    expect(await client.getHeight()).toBe(0x64)
    expect(calls).toEqual(['eth_blockNumber'])

    calls.length = 0
    expect(await client.getHeight({ finalized: true })).toBe(0x63)
    expect(calls).toEqual(['eth_getBlockByNumber'])
  })

  it('rejects a non-EVM query with an actionable error', async () => {
    // It implements the chain-generic client contract, but only speaks EVM.
    const client = new EvmRpcBlockClient({ rpc: { url: KEYED_URL } })
    const solanaQuery = { type: 'solana', fields: {}, fromBlock: 0 } as never

    await expect(
      (async () => {
        for await (const b of client.getStream(solanaQuery)) void b
      })(),
    ).rejects.toThrowError(/only serve EVM queries, got type "solana"/)
  })
})
