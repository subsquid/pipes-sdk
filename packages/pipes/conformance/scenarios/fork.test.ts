import { describe, expect, it } from 'vitest'

import { MOCK_GENESIS_TIMESTAMP } from '~/testing/evm/index.js'
import type { MockRpcCall } from '~/testing/index.js'

import { numbers } from './erc20.js'
import { S3, hashAt, highestAsked, s3ChainFollower, s3Oracle, s3World } from './fork.js'

function cursor(number: number, branch: 0 | 1 = 0) {
  return { number, hash: hashAt(number, branch), timestamp: MOCK_GENESIS_TIMESTAMP + number }
}

function floor(number: number) {
  return { number, hash: hashAt(number, 0) }
}

function followerOf(reported: number[] = [S3.finalized]) {
  const model = s3Oracle('T')

  return { model, follow: s3ChainFollower(model, () => reported) }
}

describe('s3ChainFollower', () => {
  it('takes batches of the chain and a fork to the chain the run ends on', () => {
    const { model, follow } = followerOf()

    follow.batch({ current: cursor(29), finalized: floor(S3.finalized) })
    follow.fork(numbers(S3.ancestor - 2, S3.ancestor).map(floor))
    follow.batch({ current: cursor(S3.to, 1), finalized: floor(S3.finalized) })

    expect(model.readState().current).toStrictEqual(cursor(S3.to, 1))
  })

  it('refuses a batch that ends on a block the chain does not hold', () => {
    const { follow } = followerOf()
    const foreign = { number: 6, hash: hashAt(7, 0), timestamp: MOCK_GENESIS_TIMESTAMP + 6 }

    expect(() => follow.batch({ current: foreign, finalized: undefined })).toThrow('S3 has no block 6')
  })

  it('refuses a batch cursor with no hash', () => {
    const { follow } = followerOf()

    expect(() => follow.batch({ current: { number: 6 }, finalized: undefined })).toThrow('has no hash')
  })

  it('refuses a batch cursor with another timestamp than the chain block', () => {
    const { follow } = followerOf()
    const shifted = { ...cursor(6), timestamp: MOCK_GENESIS_TIMESTAMP }

    expect(() => follow.batch({ current: shifted, finalized: undefined })).toThrow('the batch cursor')
  })

  it('refuses a floor the chain never reported (INV-2)', () => {
    const { follow } = followerOf()

    expect(() => follow.batch({ current: cursor(6), finalized: floor(5) })).toThrow('INV-2')
  })

  it('gives the oracle the floor the chain last reported, not the one the pipe holds', () => {
    const { model, follow } = followerOf([S3.finalized, S3.raisedTo])

    follow.batch({ current: cursor(S3.raisedTo + 1), finalized: floor(S3.finalized) })

    expect(model.readState().finalized).toStrictEqual(floor(S3.raisedTo))
  })

  it('refuses a fork whose canonical chain is not the chain the run ends on', () => {
    const { follow } = followerOf()
    follow.batch({ current: cursor(29), finalized: floor(S3.finalized) })

    const stale = numbers(S3.ancestor - 1, S3.ancestor + 1).map((number) => floor(number))

    expect(() => follow.fork(stale)).toThrow('the canonical chain')
  })
})

describe('highestAsked', () => {
  function call(method: string, params: unknown[]): MockRpcCall {
    return { method, params }
  }

  it('reads block numbers and log ranges, and skips tags and other methods', () => {
    const calls = [
      call('eth_getBlockByNumber', ['0x5', false]),
      call('eth_getBlockByNumber', ['finalized', false]),
      call('eth_getLogs', [{ fromBlock: '0x6', toBlock: '0x9' }]),
      call('eth_blockNumber', []),
    ]

    expect(highestAsked(calls)).toBe(9)
    expect(highestAsked([call('eth_blockNumber', [])])).toBe(-1)
  })
})

describe('s3World', () => {
  async function rpcCall(url: string, method: string, params: unknown[] = []) {
    const res = await fetch(url, { method: 'POST', body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    const body = (await res.json()) as { result: any }

    return body.result
  }

  it('forks an RPC reader at its first head poll after the head block, never inside a fetch round', async () => {
    const world = s3World()
    const portal = await world.portal()

    try {
      await rpcCall(portal.rpcUrl, 'eth_getBlockByNumber', [`0x${S3.latest.toString(16)}`, true])
      const sameRound = await rpcCall(portal.rpcUrl, 'eth_getLogs', [{ fromBlock: '0x1b', toBlock: '0x1d' }])
      const beforePoll = await rpcCall(portal.rpcUrl, 'eth_getBlockByNumber', ['0x1d', false])
      const head = await rpcCall(portal.rpcUrl, 'eth_blockNumber')

      expect(sameRound.every((log: { blockHash: string }) => log.blockHash.startsWith('0x00000000'))).toBe(true)
      expect(beforePoll.hash).toBe(hashAt(S3.latest, 0))
      expect(head).toBe(`0x${S3.to.toString(16)}`)
      expect(world.reported).toEqual([S3.finalized, S3.raisedTo])
    } finally {
      await portal.close()
    }
  })
})
