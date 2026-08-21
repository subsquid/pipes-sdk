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
})
