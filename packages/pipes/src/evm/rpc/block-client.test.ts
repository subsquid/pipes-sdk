import { setTimeout as wait } from 'node:timers/promises'

import { EvmRpcDataSource, Rpc } from '@subsquid/evm-rpc'
import { describe, expect, it, vi } from 'vitest'

import { makeCapabilityProbe } from '~/core/fallback/capability.js'
import { ForkException } from '~/portal-client/index.js'
import { withPortal } from '~/testing/portal-fixture.js'

import { EvmRpcBlockClient, RPC_FORK_WINDOW } from './block-client.js'

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
        if (method === 'eth_blockNumber') {
          return '0x64'
        }
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
        for await (const b of client.getStream(solanaQuery)) {
          void b
        }
      })(),
    ).rejects.toThrowError(/only serve EVM queries, got type "solana"/)
  })
})

type InnerBatch = { blocks: unknown[]; finalizedHead?: { number: number; hash: string } }

/** Replaces the evm-rpc hot stream with `batches`, then throws `end` if it is an error, or waits forever on `'hang'`. */
const withInnerStream = withPortal.extend<{ innerStream: (batches: InnerBatch[], end?: Error | 'hang') => void }>({
  innerStream: async ({}, use) => {
    const spy = vi.spyOn(EvmRpcDataSource.prototype, 'getStream')

    await use((batches, end) => {
      spy.mockImplementation(async function* () {
        yield* batches
        if (end === 'hang') {
          await new Promise(() => {})
        }
        if (end instanceof Error) {
          throw end
        }
      } as never)
    })

    spy.mockRestore()
  },
})

const evmQuery = { type: 'evm', fields: {}, fromBlock: 100 } as never

async function collect(client: EvmRpcBlockClient) {
  const batches = []
  for await (const batch of client.getStream(evmQuery)) {
    batches.push(batch)
  }

  return batches
}

describe('EvmRpcBlockClient stream', () => {
  withInnerStream(
    'ends the stream on abort while evm-rpc waits for the next block',
    async ({ innerStream, portal: open }) => {
      // evm-rpc cannot cancel a request; without the race a graceful stop waits for a block forever.
      innerStream([{ blocks: [], finalizedHead: { number: 90, hash: '0x5a' } }], 'hang')
      const portal = await open({ latest: 100, finalized: 2 })
      const abort = new AbortController()
      const client = new EvmRpcBlockClient({ rpc: { url: portal.rpcUrl } })

      let batches = 0
      for await (const _ of client.getStream(evmQuery, { request: { abort: abort.signal } })) {
        batches++
        setTimeout(() => abort.abort(), 10)
      }

      expect(batches).toBe(1)
    },
    2000,
  )

  withInnerStream('passes a finality update that carries no blocks', async ({ innerStream, portal: open }) => {
    const finalizedHead = { number: 90, hash: '0x5a' }
    innerStream([{ blocks: [] }, { blocks: [], finalizedHead }])
    const portal = await open({ latest: 100, finalized: 2 })

    const batches = await collect(new EvmRpcBlockClient({ rpc: { url: portal.rpcUrl } }))

    expect(batches).toHaveLength(1)
    expect(batches[0]).toMatchObject({ blocks: [], head: { finalized: finalizedHead } })
  })

  /** The fork that evm-rpc throws: only the parent of the forked block. */
  function sqdFork(parent: { number: number; hash: string }) {
    return Object.assign(new Error('fork'), {
      isSqdForkException: true,
      expectedParentHash: '0xbb',
      previousBlocks: [parent],
    })
  }

  withInnerStream(
    'gives the translated fork error the forked block and the canonical window below it',
    async ({ innerStream, portal: open }) => {
      const portal = await open({ latest: 40, finalized: 2 })
      const client = new EvmRpcBlockClient({ rpc: { url: portal.rpcUrl } })
      const parent = await client.getHead()
      innerStream([], sqdFork(parent!))

      const error = await collect(client).catch((e) => e)
      const numbers = error.canonicalBlocks.map((block: { number: number }) => block.number)

      expect(error).toBeInstanceOf(ForkException)
      expect(error.query).toEqual({ fromBlock: 41, parentBlockHash: '0xbb' })
      expect(numbers).toEqual(Array.from({ length: RPC_FORK_WINDOW }, (_, i) => 41 - RPC_FORK_WINDOW + i))
      expect(error.canonicalBlocks.at(-1)).toEqual(parent)
    },
  )

  withInnerStream(
    'keeps the one parent block when the chain no longer holds it',
    async ({ innerStream, portal: open }) => {
      const portal = await open({ latest: 8, finalized: 2 })
      const replaced = { number: 5, hash: '0x05' }
      innerStream([], sqdFork(replaced))

      const error = await collect(new EvmRpcBlockClient({ rpc: { url: portal.rpcUrl } })).catch((e) => e)

      expect(error).toBeInstanceOf(ForkException)
      expect(error.canonicalBlocks).toEqual([replaced])
    },
  )

  withInnerStream('keeps the one parent block when the window read fails', async ({ innerStream, portal: open }) => {
    const portal = await open({ latest: 8, finalized: 2 })
    const client = new EvmRpcBlockClient({ rpc: { url: portal.rpcUrl } })
    const parent = await client.getHead()
    innerStream([], sqdFork(parent!))
    const read = vi.spyOn(Rpc.prototype, 'getBlockBatch').mockRejectedValue(new Error('unreachable'))

    const error = await collect(client).catch((e) => e)
    read.mockRestore()

    expect(error).toBeInstanceOf(ForkException)
    expect(error.canonicalBlocks).toEqual([parent])
  })

  withPortal('gives hot blocks above the finalized head the finalized head of the chain', async ({ portal: open }) => {
    // Without it, a pipe with no stored watermark keeps no rollback chain for these blocks.
    const portal = await open({ latest: 110, finalized: 100 })
    const client = new EvmRpcBlockClient({ rpc: { url: portal.rpcUrl } })
    const finalized = await client.getHead({ finalized: true })
    const query = { type: 'evm', fields: {}, includeAllBlocks: true, fromBlock: 101, toBlock: 103 } as never

    const first = await client.getStream(query)[Symbol.asyncIterator]().next()

    expect(first.value.blocks[0].header.number).toBe(101)
    expect(first.value.head.finalized).toEqual(finalized)
  })

  withPortal('stops polling the endpoint once the stream is stopped', async ({ portal: open }) => {
    // The stream asks past the head, so evm-rpc polls the head until it is stopped.
    const portal = await open({ latest: 5, finalized: 5 })
    const client = new EvmRpcBlockClient({ rpc: { url: portal.rpcUrl } })
    const abort = new AbortController()
    const query = { type: 'evm', fields: {}, fromBlock: 10 } as never

    setTimeout(() => abort.abort(), 50)
    for await (const _ of client.getStream(query, { request: { abort: abort.signal } })) {
    }
    await wait(150)
    const callsAtStop = portal.rpcCalls.length
    await wait(400)

    expect(portal.rpcCalls.length).toBe(callsAtStop)
  })

  withPortal('stops polling the endpoint after a capability probe times out', async ({ portal: open }) => {
    const portal = await open({ latest: 5, finalized: 5 })
    const client = new EvmRpcBlockClient({ rpc: { url: portal.rpcUrl } })
    const probe = makeCapabilityProbe(client, { type: 'evm', fields: {}, fromBlock: 0 } as never, { timeoutMs: 50 })

    const result = await probe({ number: 10, hash: '0x10' })
    await wait(150)
    const callsAtStop = portal.rpcCalls.length
    await wait(400)

    expect(result.ok).toBe(false)
    expect(portal.rpcCalls.length).toBe(callsAtStop)
  })
})
