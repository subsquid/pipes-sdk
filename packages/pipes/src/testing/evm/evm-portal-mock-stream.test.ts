import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from 'vitest'

import { commonAbis } from '~/evm/abi/common.js'
import { evmEventDecoder } from '~/evm/evm-decoder.js'
import { evmPortalStream } from '~/evm/evm-stream.js'

import { MockPortal, readAll } from '../index.js'
import { mockEvmPortalStream } from './evm-portal-mock-stream.js'
import { encodeEvent, mockBlock, resetMockBlockCounter } from './mock-block.js'

const WETH_ADDRESS = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as const
const ALICE = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' as const
const BOB = '0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f' as const

describe('test-evm-data helpers', () => {
  let mockPortal: MockPortal

  beforeEach(() => {
    resetMockBlockCounter()
  })

  afterEach(async () => {
    await mockPortal?.close()
  })

  describe('encodeEvent', () => {
    it('should encode an ERC20 Transfer event', () => {
      const log = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Transfer',
        address: WETH_ADDRESS,
        args: { from: ALICE, to: BOB, value: 100n },
      })

      expect(log.address).toBe(WETH_ADDRESS)
      expect(log.topics).toHaveLength(3)
      // topic0 = Transfer signature
      expect(log.topics[0]).toBe('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
      // topic1 = from (padded to 32 bytes)
      expect(log.topics[1]).toBe('0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d')
      // topic2 = to (padded to 32 bytes)
      expect(log.topics[2]).toBe('0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f')
      // data = value (uint256)
      expect(log.data).toBe('0x0000000000000000000000000000000000000000000000000000000000000064')
    })

    it('should infer args types from ABI', () => {
      // Type-level test: args should include from, to, AND value
      type Args = Parameters<typeof encodeEvent<typeof commonAbis.erc20.abi, 'Transfer'>>[0]['args']
      expectTypeOf<Args>().toEqualTypeOf<{ from: `0x${string}`; to: `0x${string}`; value: bigint } | undefined>()
    })
  })

  describe('mockBlock', () => {
    it('should auto-generate block metadata', () => {
      const block = mockBlock()

      expect(block.header.number).toBe(1)
      expect(block.header.timestamp).toBe(1000)
      expect(block.header.hash).toMatch(/^0x/)
      expect(block.header.parentHash).toMatch(/^0x/)
      expect(block.transactions).toEqual([])
      expect(block.logs).toEqual([])
    })

    it('should auto-increment block numbers', () => {
      const b1 = mockBlock()
      const b2 = mockBlock()
      const b3 = mockBlock()

      expect(b1.header.number).toBe(1)
      expect(b2.header.number).toBe(2)
      expect(b3.header.number).toBe(3)
    })

    it('should allow overriding metadata', () => {
      const block = mockBlock({
        number: 42,
        timestamp: 9999,
        hash: '0xabc',
      })

      expect(block.header.number).toBe(42)
      expect(block.header.timestamp).toBe(9999)
      expect(block.header.hash).toBe('0xabc')
    })

    it('should create transactions and logs from events', () => {
      const event1 = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Transfer',
        address: WETH_ADDRESS,
        args: { from: ALICE, to: BOB, value: 100n },
      })

      const event2 = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Approval',
        address: WETH_ADDRESS,
        args: { owner: ALICE, spender: BOB, value: 200n },
      })

      const block = mockBlock({
        transactions: [{ logs: [event1, event2] }, { logs: [event1] }],
      })

      expect(block.transactions).toHaveLength(2)
      expect(block.logs).toHaveLength(3)

      // First tx has 2 logs
      expect(block.logs[0].transactionIndex).toBe(0)
      expect(block.logs[1].transactionIndex).toBe(0)
      // Second tx has 1 log
      expect(block.logs[2].transactionIndex).toBe(1)

      // Log indices are sequential across the block
      expect(block.logs[0].logIndex).toBe(0)
      expect(block.logs[1].logIndex).toBe(1)
      expect(block.logs[2].logIndex).toBe(2)

      // Transaction hashes are auto-generated and match
      expect(block.logs[0].transactionHash).toBe(block.transactions[0].hash)
      expect(block.logs[2].transactionHash).toBe(block.transactions[1].hash)
    })
  })

  describe('mockEvmPortalStream', () => {
    it('should work end-to-end with evmEventDecoder', async () => {
      const transfer = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Transfer',
        address: WETH_ADDRESS,
        args: { from: ALICE, to: BOB, value: 100n },
      })

      mockPortal = await mockEvmPortalStream({
        blocks: [
          mockBlock({ transactions: [{ logs: [transfer] }] }),
          mockBlock({ transactions: [{ logs: [transfer] }] }),
        ],
      })

      const stream = evmPortalStream({
        id: 'test',
        portal: mockPortal.url,
        outputs: evmEventDecoder({
          range: { from: 0, to: 2 },
          events: {
            transfers: commonAbis.erc20.events.Transfer,
          },
        }),
      }).pipe((e) => e.transfers)

      const res = await readAll(stream)

      expect(res).toHaveLength(2)
      expect(res[0].event.from).toBe(ALICE)
      expect(res[0].event.to).toBe(BOB)
      expect(res[0].event.value).toBe(100n)
      expect(res[0].contract).toBe(WETH_ADDRESS)
      expect(res[1].event.from).toBe(ALICE)
    })

    it('should work with multiple events per block', async () => {
      const transfer1 = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Transfer',
        address: WETH_ADDRESS,
        args: { from: ALICE, to: BOB, value: 100n },
      })

      const transfer2 = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Transfer',
        address: WETH_ADDRESS,
        args: { from: BOB, to: ALICE, value: 50n },
      })

      mockPortal = await mockEvmPortalStream({
        blocks: [
          mockBlock({
            transactions: [{ logs: [transfer1, transfer2] }],
          }),
        ],
      })

      const stream = evmPortalStream({
        id: 'test',
        portal: mockPortal.url,
        outputs: evmEventDecoder({
          range: { from: 0, to: 1 },
          events: {
            transfers: commonAbis.erc20.events.Transfer,
          },
        }),
      }).pipe((e) => e.transfers)

      const res = await readAll(stream)

      expect(res).toHaveLength(2)
      expect(res[0].event.value).toBe(100n)
      expect(res[1].event.value).toBe(50n)
    })

    it('should work with mixed event types', async () => {
      const transfer = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Transfer',
        address: WETH_ADDRESS,
        args: { from: ALICE, to: BOB, value: 100n },
      })

      const approval = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Approval',
        address: WETH_ADDRESS,
        args: { owner: ALICE, spender: BOB, value: 200n },
      })

      mockPortal = await mockEvmPortalStream({
        blocks: [
          mockBlock({
            transactions: [{ logs: [transfer, approval] }],
          }),
        ],
      })

      const stream = evmPortalStream({
        id: 'test',
        portal: mockPortal.url,
        outputs: evmEventDecoder({
          range: { from: 0, to: 1 },
          events: {
            transfers: commonAbis.erc20.events.Transfer,
            approvals: commonAbis.erc20.events.Approval,
          },
        }),
      })

      const res: { transfers: any[]; approvals: any[] }[] = []
      for await (const batch of stream) {
        res.push(batch.data)
      }

      expect(res[0].transfers).toHaveLength(1)
      expect(res[0].approvals).toHaveLength(1)
    })
  })
})
