import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { commonAbis } from '~/evm/abi/common.js'
import { evmEventDecoder } from '~/evm/evm-decoder.js'
import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { encodeEvent, mockBlock, resetMockBlockCounter } from '~/testing/evm/index.js'
import { MockPortal, mockPortal } from '~/testing/index.js'

import { createMemoryTarget } from './memory-target.js'

const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as const
const ALICE = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' as const
const BOB = '0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f' as const

// One ERC20 Transfer per block, with `value` set to the block number so each
// emitted row is identifiable by where it came from.
function transferLog(value: bigint) {
  return encodeEvent({
    abi: commonAbis.erc20.abi,
    eventName: 'Transfer',
    address: WETH,
    args: { from: ALICE, to: BOB, value },
  })
}

function blockWithTransfer(number: number) {
  return mockBlock({ number, transactions: [{ logs: [transferLog(BigInt(number))] }] })
}

type Row = { blockNumber: number; value: bigint }

function streamTo(portal: MockPortal, to: number, emitted: Row[][]) {
  return evmPortalStream({
    id: 'memory-target-test',
    portal: portal.url,
    outputs: evmEventDecoder({
      range: { from: 1, to },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
    }).pipe((d) => d.transfers.map((t) => ({ blockNumber: t.block.number, value: t.event.value }))),
  }).pipeTo(
    createMemoryTarget({
      onData: (data) => {
        emitted.push(data)
      },
    }),
  )
}

describe('createMemoryTarget', () => {
  let portal: MockPortal

  beforeEach(() => {
    resetMockBlockCounter()
  })

  afterEach(async () => {
    await portal?.close()
  })

  it('emits only finalized rows and releases buffered rows once a later head finalizes them', async () => {
    const [b1, b2, b3, b4, b5] = [1, 2, 3, 4, 5].map(blockWithTransfer)

    portal = await mockPortal([
      // finalized head = 1: block 1 is emitted, blocks 2 & 3 are held back
      {
        statusCode: 200,
        data: [b1, b2, b3],
        head: { finalized: { number: b1.header.number, hash: b1.header.hash } },
      },
      // finalized head = 4: the buffered 2 & 3 plus the new 4 are released; 5 stays held
      {
        statusCode: 200,
        data: [b4, b5],
        head: { finalized: { number: b4.header.number, hash: b4.header.hash } },
      },
    ])

    const emitted: Row[][] = []
    await streamTo(portal, 5, emitted)

    // Block 5 is above the last reported finalized head, so it is held back — until the
    // finality catch-up the bounded stream ends with (the mock finalizes everything it served)
    // releases it. Nothing is ever emitted ahead of a finalized head that covers it.
    const blockNumbers = emitted.flat().map((r) => r.blockNumber)
    expect(blockNumbers).toEqual([1, 2, 3, 4, 5])

    // Buffered rows (2, 3) are released before the current batch's row (4); 5 comes last, alone.
    expect(emitted.flat().map((r) => r.value)).toEqual([1n, 2n, 3n, 4n, 5n])
    expect(emitted.at(-1)!.map((r) => r.blockNumber)).toEqual([5])
  })

  it('passes every row straight through when the dataset has no finalized head', async () => {
    const [b1, b2] = [1, 2].map(blockWithTransfer)

    portal = await mockPortal([
      {
        statusCode: 200,
        data: [b1, b2],
        // no head → no finality → nothing is buffered
      },
    ])

    const emitted: Row[][] = []
    await streamTo(portal, 2, emitted)

    expect(emitted.flat().map((r) => r.blockNumber)).toEqual([1, 2])
  })
})
