import { afterEach, describe, expect, it } from 'vitest'

import { PortalClient } from '~/portal-client/client.js'

import { type MockEvmChainPortal, mockEvmChainPortal } from './evm-chain-portal.js'
import { mockBlock } from './mock-block.js'

const A = '0x00000000000000000000000000000000000000aa'
const B = '0x00000000000000000000000000000000000000bb'
const X = '0x000000000000000000000000000000000000000000000000000000000000000a'
const Y = '0x000000000000000000000000000000000000000000000000000000000000000b'

function log(address: `0x${string}`, topic: `0x${string}`) {
  return { address, topics: [topic], data: '0x' as const }
}

const blocks = [
  mockBlock({ number: 3, transactions: [{ logs: [log(A, X)] }, { logs: [log(B, X)] }] }),
  mockBlock({ number: 5, transactions: [{ logs: [log(B, X)] }] }),
  mockBlock({ number: 7, transactions: [{ logs: [log(A, Y)] }] }),
]

const FIELDS = {
  block: { number: true, hash: true },
  log: { address: true, topics: true, transactionIndex: true },
  transaction: { transactionIndex: true },
} as const

async function streamAll(portal: MockEvmChainPortal, query: Record<string, unknown>) {
  const client = new PortalClient({ url: portal.url })
  const stream = client.getStream({ type: 'evm', fromBlock: 0, fields: FIELDS, ...query } as any)

  const result: any[] = []
  for await (const batch of stream) {
    result.push(...batch.blocks)
  }

  return result
}

async function post(portal: MockEvmChainPortal, path: string, query: Record<string, unknown>) {
  return fetch(`${portal.url}/${path}`, { method: 'POST', body: JSON.stringify({ type: 'evm', ...query }) })
}

describe('mockEvmChainPortal', () => {
  let portal: MockEvmChainPortal

  afterEach(async () => {
    await portal?.close()
  })

  it('sends the selected logs and the first and last block of the covered range', async () => {
    portal = await mockEvmChainPortal({ blocks, latest: 10 })

    const result = await streamAll(portal, { fromBlock: 1, toBlock: 10, logs: [{ address: [A], topic0: [X] }] })

    expect(result.map((b) => [b.header.number, (b.logs ?? []).map((l: any) => l.address)])).toEqual([
      [1, []],
      [3, [A]],
      [10, []],
    ])
  })

  it('matches addresses and topics in any letter case', async () => {
    portal = await mockEvmChainPortal({ blocks, latest: 10 })

    const result = await streamAll(portal, { fromBlock: 3, toBlock: 3, logs: [{ address: [B.toUpperCase()] }] })

    expect(result.map((b) => b.logs.map((l: any) => l.address))).toEqual([[B]])
  })

  it('adds the transaction of a log only when a request that matches it asks for it', async () => {
    portal = await mockEvmChainPortal({ blocks, latest: 10 })

    const result = await streamAll(portal, {
      fromBlock: 3,
      toBlock: 3,
      logs: [{ address: [A], transaction: true }, { address: [B] }],
    })

    expect(result[0].logs).toHaveLength(2)
    expect(result[0].transactions.map((tx: any) => tx.transactionIndex)).toEqual([0])
  })

  it('covers at most `blocksPerResponse` blocks per response', async () => {
    portal = await mockEvmChainPortal({ blocks, latest: 10, blocksPerResponse: 4 })

    const result = await streamAll(portal, { fromBlock: 1, toBlock: 10, logs: [{ topic0: [X, Y] }] })

    expect(portal.requests.map((r) => r.query.fromBlock)).toEqual([1, 5, 9])
    expect(result.map((b) => b.header.number)).toEqual([1, 3, 4, 5, 7, 8, 9, 10])
  })

  it('stops the finalized stream at the finalized head', async () => {
    portal = await mockEvmChainPortal({ blocks, finalized: 5, latest: 10 })

    const below = await post(portal, 'finalized-stream', { fromBlock: 4, toBlock: 10 })
    const above = await post(portal, 'finalized-stream', { fromBlock: 6 })

    expect(
      (await below.text())
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line).header.number),
    ).toEqual([4, 5])
    expect(above.status).toBe(204)
  })

  it('answers 409 to a parent hash that is not on the chain', async () => {
    portal = await mockEvmChainPortal({ blocks, latest: 10 })

    const res = await post(portal, 'stream', { fromBlock: 4, parentBlockHash: '0xdead' })

    expect(res.status).toBe(409)
    expect(((await res.json()) as any).previousBlocks).toEqual([{ number: 3, hash: blocks[0].header.hash }])
  })

  it('refuses a query with more addresses than allowed', async () => {
    portal = await mockEvmChainPortal({ blocks, latest: 10, maxAddresses: 2 })

    const res = await post(portal, 'stream', { fromBlock: 1, logs: [{ address: [A, B] }, { address: [A] }] })

    expect(res.status).toBe(400)
  })

  it('reports heads, and no finalized head when it has none', async () => {
    portal = await mockEvmChainPortal({ blocks, finalized: null, latest: 10 })
    const client = new PortalClient({ url: portal.url })

    await expect(client.getHead({ finalized: true })).rejects.toThrow('404')

    portal.setHead({ finalized: 8, latest: 12 })

    expect(await client.getHead({ finalized: true })).toMatchObject({ number: 8 })
    expect(await client.getHead({ finalized: false })).toMatchObject({ number: 12 })
  })
})
