import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { commonAbis } from '~/evm/abi/common.js'
import { evmEventDecoder } from '~/evm/evm-decoder.js'
import { evmPortalStream } from '~/evm/evm-stream.js'
import { encodeEvent, mockBlock, resetMockBlockCounter } from '~/testing/evm/index.js'
import { MockPortal, finalizedMockPortal } from '~/testing/index.js'

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
    // `maxBytes: 1` keeps each mock response its own batch; without it they coalesce and a target
    // that only emitted once at stream end would pass the arrival-order assertions unchanged.
    portal: { url: portal.url, maxBytes: 1 },
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

  // The target requires the finalized stream, so every row it is handed is already final: it emits
  // them as they arrive, in order, instead of holding any back.
  it('emits every row in arrival order across batches', async () => {
    const [b1, b2, b3, b4, b5] = [1, 2, 3, 4, 5].map(blockWithTransfer)

    portal = await finalizedMockPortal([
      {
        statusCode: 200,
        data: [b1, b2, b3],
        head: { finalized: { number: b3.header.number, hash: b3.header.hash } },
      },
      {
        statusCode: 200,
        data: [b4, b5],
        head: { finalized: { number: b5.header.number, hash: b5.header.hash } },
      },
    ])

    const emitted: Row[][] = []
    await streamTo(portal, 5, emitted)

    // Per batch, not flattened: the target releases each batch as it arrives rather than
    // accumulating and emitting once at the end.
    expect(emitted.map((batch) => batch.map((r) => r.blockNumber))).toEqual([
      [1, 2, 3],
      [4, 5],
    ])
    expect(emitted.flat().map((r) => r.value)).toEqual([1n, 2n, 3n, 4n, 5n])
  })

  it('emits rows for a dataset that reports no finalized head at all', async () => {
    const [b1, b2] = [1, 2].map(blockWithTransfer)

    portal = await finalizedMockPortal([
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
