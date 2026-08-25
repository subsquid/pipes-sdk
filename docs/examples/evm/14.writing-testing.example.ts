import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'
import {
  type MockPortal,
  encodeEvent,
  mockBlock,
  mockEvmPortalStream,
  resetMockBlockCounter,
} from '@subsquid/pipes/testing/evm'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

export async function readAll<T>(stream: AsyncIterable<{ data: T[] }>): Promise<T[]> {
  const res: T[] = []

  for await (const chunk of stream) {
    res.push(...chunk.data)
  }

  return res
}

/**
 * Example: Writing tests for EVM pipes using the testing library.
 *
 * The testing library provides utilities to:
 * - Encode events with full type inference from viem ABIs (`encodeEvent`)
 * - Build mock blocks with auto-generated metadata (`mockBlock`)
 * - Spin up a mock portal HTTP server (`mockEvmPortalStream`)
 * - Collect all output from a stream (`readAll`)
 *
 * This lets you test your pipe logic end-to-end without hitting a real portal.
 */

const ERC20_ABI = [
  {
    type: 'event' as const,
    name: 'Transfer',
    inputs: [
      { name: 'from', type: 'address', indexed: true },
      { name: 'to', type: 'address', indexed: true },
      { name: 'value', type: 'uint256', indexed: false },
    ],
  },
  {
    type: 'event' as const,
    name: 'Approval',
    inputs: [
      { name: 'owner', type: 'address', indexed: true },
      { name: 'spender', type: 'address', indexed: true },
      { name: 'value', type: 'uint256', indexed: false },
    ],
  },
] as const

const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' as const
const ALICE = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' as const
const BOB = '0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f' as const

// --- Tests ---

describe('EVM pipe testing example', () => {
  let portal: MockPortal

  beforeEach(() => {
    resetMockBlockCounter()
  })

  afterEach(async () => {
    await portal?.close()
  })

  it('should decode ERC20 transfers from mock blocks', async () => {
    // 1. Encode events — args are fully typed from the ABI
    const transfer1 = encodeEvent({
      abi: ERC20_ABI,
      eventName: 'Transfer',
      address: USDC,
      args: { from: ALICE, to: BOB, value: 1_000_000n },
    })

    const transfer2 = encodeEvent({
      abi: ERC20_ABI,
      eventName: 'Transfer',
      address: USDC,
      args: { from: BOB, to: ALICE, value: 500_000n },
    })

    // 2. Build mock blocks — metadata (number, hash, timestamp) is auto-generated
    portal = await mockEvmPortalStream({
      blocks: [
        mockBlock({ transactions: [{ logs: [transfer1] }] }),
        mockBlock({ transactions: [{ logs: [transfer2] }] }),
      ],
    })

    // 3. Create the pipe exactly as you would in production, but with the mock portal URL
    const stream = evmStream({
      id: 'test',
      source: portal.url,
      outputs: evmEventDecoder({
        range: { from: 0, to: 2 },
        events: {
          transfers: commonAbis.erc20.events.Transfer,
        },
      }),
    }).pipe((batch) => batch.transfers)

    // 4. Collect all output and assert
    const transfers = await readAll(stream)

    expect(transfers).toHaveLength(2)
    expect(transfers[0].event.from).toBe(ALICE)
    expect(transfers[0].event.to).toBe(BOB)
    expect(transfers[0].event.value).toBe(1_000_000n)
    expect(transfers[0].contract).toBe(USDC)

    expect(transfers[1].event.from).toBe(BOB)
    expect(transfers[1].event.to).toBe(ALICE)
    expect(transfers[1].event.value).toBe(500_000n)
  })

  it('should handle multiple event types in a single block', async () => {
    const transfer = encodeEvent({
      abi: ERC20_ABI,
      eventName: 'Transfer',
      address: USDC,
      args: { from: ALICE, to: BOB, value: 1_000_000n },
    })

    const approval = encodeEvent({
      abi: ERC20_ABI,
      eventName: 'Approval',
      address: USDC,
      args: { owner: ALICE, spender: BOB, value: 5_000_000n },
    })

    portal = await mockEvmPortalStream({
      blocks: [
        mockBlock({
          transactions: [{ logs: [transfer, approval] }],
        }),
      ],
    })

    const stream = evmStream({
      id: 'test',
      source: portal.url,
      outputs: evmEventDecoder({
        range: { from: 0, to: 1 },
        events: {
          transfers: commonAbis.erc20.events.Transfer,
          approvals: commonAbis.erc20.events.Approval,
        },
      }),
    })

    const batches: { transfers: any[]; approvals: any[] }[] = []
    for await (const { data } of stream) {
      batches.push(data)
    }

    expect(batches[0].transfers).toHaveLength(1)
    expect(batches[0].approvals).toHaveLength(1)
    expect(batches[0].approvals[0].event.value).toBe(5_000_000n)
  })

  it('should test custom pipe transformations', async () => {
    const transfer = encodeEvent({
      abi: ERC20_ABI,
      eventName: 'Transfer',
      address: USDC,
      args: { from: ALICE, to: BOB, value: 2_000_000n },
    })

    portal = await mockEvmPortalStream({
      blocks: [mockBlock({ transactions: [{ logs: [transfer] }] })],
    })

    // Test a custom transformation pipeline
    const stream = evmStream({
      id: 'test',
      source: portal.url,
      outputs: evmEventDecoder({
        range: { from: 0, to: 1 },
        events: {
          transfers: commonAbis.erc20.events.Transfer,
        },
      }),
    })
      .pipe((batch) => batch.transfers)
      .pipe((transfers) =>
        transfers.map((t) => ({
          from: t.event.from,
          to: t.event.to,
          amount: Number(t.event.value) / 1e6, // USDC has 6 decimals
        })),
      )

    const results = await readAll(stream)

    expect(results).toHaveLength(1)
    expect(results[0]).toEqual({
      from: ALICE,
      to: BOB,
      amount: 2,
    })
  })
})
