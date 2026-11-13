import { Block as RpcBlock } from '@subsquid/evm-rpc'
import { cast } from '@subsquid/util-internal-validation'
import { describe, expect, it } from 'vitest'

import { FieldSelection, getBlockSchema } from '~/portal-client/query/evm.js'

import { createWireBlockMapper, dropEmptyBlocks, shimWireBlock } from './wire.js'

/**
 * The raw-RPC → portal-wire mapping, unit-tested on a hand-built block: filtering happens on a
 * decoded throwaway copy, the wire output keeps full (undecoded) fields, and the downstream cast —
 * the facade's normalize step — both decodes it and prunes it to the user's selection.
 */

const ADDR_A = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
const ADDR_B = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
const TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

function tx(i: number) {
  const h = `0x${String(i).repeat(64).slice(0, 64)}`
  return {
    hash: h,
    blockHash: '0x' + 'f'.repeat(64),
    blockNumber: '0x64',
    transactionIndex: `0x${i.toString(16)}`,
    from: ADDR_A,
    to: ADDR_B,
    value: '0x1',
    gas: '0x5208',
    gasPrice: '0x3b9aca00',
    input: '0x',
    nonce: `0x${i.toString(16)}`,
  }
}

function log(i: number, txIndex: number, address: string) {
  return {
    address,
    topics: [TOPIC],
    data: '0x01',
    blockNumber: '0x64',
    blockHash: '0x' + 'f'.repeat(64),
    transactionHash: `0x${String(txIndex).repeat(64).slice(0, 64)}`,
    transactionIndex: `0x${txIndex.toString(16)}`,
    logIndex: `0x${i.toString(16)}`,
    removed: false,
  }
}

/** A minimal raw block as `@subsquid/evm-rpc` would deliver it: getBlock + eth_getLogs results. */
function rawBlock(): RpcBlock {
  return {
    number: 100,
    hash: '0x' + 'f'.repeat(64),
    block: {
      number: '0x64',
      hash: '0x' + 'f'.repeat(64),
      parentHash: '0x' + 'e'.repeat(64),
      timestamp: '0x68000000',
      nonce: '0x0000000000000000',
      difficulty: '0x0',
      gasLimit: '0x1c9c380',
      gasUsed: '0xa410',
      miner: ADDR_B,
      mixHash: '0x' + '0'.repeat(64),
      size: '0x400',
      sha3Uncles: '0x' + '1'.repeat(64),
      stateRoot: '0x' + '2'.repeat(64),
      transactionsRoot: '0x' + '3'.repeat(64),
      receiptsRoot: '0x' + '4'.repeat(64),
      logsBloom: '0x' + '0'.repeat(512),
      extraData: '0x',
      transactions: [tx(1), tx(2)],
      uncles: [],
    } as any,
    logs: [log(0, 1, ADDR_A), log(1, 2, ADDR_B)],
  }
}

describe('createWireBlockMapper', () => {
  const fields = {
    block: { timestamp: true },
    log: { address: true, data: true },
    transaction: { from: true, to: true },
  } satisfies FieldSelection

  it('keeps only the items the request matches, plus their included relations', () => {
    const mapper = createWireBlockMapper(fields, {
      logs: [{ address: [ADDR_A], transaction: true }],
    })
    const wire = mapper.map(rawBlock())

    expect(wire.header.number).toBe(100) // numeric in the wire shape — cursor-readable
    expect(wire.logs).toHaveLength(1)
    expect((wire.logs[0] as any).address).toBe(ADDR_A)
    // The matched log's transaction is included via the relation; the other tx is filtered out.
    expect(wire.transactions).toHaveLength(1)
    expect((wire.transactions[0] as any).transactionIndex).toBe(1)
  })

  it('produces output the downstream cast decodes and prunes exactly like portal output', () => {
    const mapper = createWireBlockMapper(fields, { logs: [{ address: [ADDR_A] }] })
    const wire = mapper.map(rawBlock())

    // The facade's normalize step: cast at the user selection plus the fields the query always adds.
    const selection = { ...fields, block: { ...fields.block, number: true }, log: { ...fields.log, logIndex: true } }
    const decoded: any = cast(getBlockSchema(selection), wire)

    expect(decoded.header.number).toBe(100)
    expect(typeof decoded.header.timestamp).toBe('number')
    expect(decoded.logs).toHaveLength(1)
    expect(decoded.logs[0]).toMatchObject({ address: ADDR_A, data: '0x01', logIndex: 0 })
    // Fields outside the selection (e.g. topics) are pruned by the cast, not by the mapper.
    expect((wire.logs[0] as any).topics).toBeDefined()
    expect(decoded.logs[0].topics).toBeUndefined()
  })

  it('filters on a where-clause field that is not in the output selection', () => {
    // topic0 is filtered on but `topics` is not selected: the filter reads the full wire block.
    const mapper = createWireBlockMapper({ log: { data: true } }, { logs: [{ topic0: [TOPIC], address: [ADDR_B] }] })
    const wire = mapper.map(rawBlock())

    expect(wire.logs).toHaveLength(1)
    expect((wire.logs[0] as any).address).toBe(ADDR_B)
  })

  it('exposes the coarse fetch toggles for the RPC data request', () => {
    const logsOnly = createWireBlockMapper(fields, { logs: [{}] })
    expect(logsOnly.requiredData).toMatchObject({ logs: true, traces: false, stateDiffs: false })

    const withTraces = createWireBlockMapper({}, { traces: [{}] })
    expect(withTraces.requiredData.traces).toBe(true)
  })

  it('serves a query that selects receipt-backed fields while requesting only logs', () => {
    // Receipts are only fetched when transactions are *requested*, so `gasUsed`/`status` are absent
    // from the normalized block here. The mapper must not decode at the output selection.
    const mapper = createWireBlockMapper(
      { transaction: { gasUsed: true, status: true }, log: { address: true } },
      { logs: [{ address: [ADDR_A] }] },
    )

    const wire = mapper.map(rawBlock())

    expect(mapper.requiredData.receipts).toBe(false)
    expect(wire.logs).toHaveLength(1)
    expect(wire.transactions).toHaveLength(0) // no transactions requested ⇒ none survive filtering
  })
})

describe('shimWireBlock', () => {
  it('rewrites the selfdestruct trace tag to suicide', () => {
    const block = { traces: [{ type: 'selfdestruct', action: { refundAddress: '0xa' } }] }
    shimWireBlock(block)
    expect(block.traces[0].type).toBe('suicide')
  })

  it('renames a reward action rewardType to type', () => {
    const block = { traces: [{ type: 'reward', action: { author: '0xa', rewardType: 'block' } }] }
    shimWireBlock(block)
    expect(block.traces[0].action).toEqual({ author: '0xa', type: 'block' })
    expect('rewardType' in block.traces[0].action).toBe(false)
  })

  it('leaves create / call traces and trace-less blocks untouched', () => {
    const block = {
      traces: [
        { type: 'call', action: { to: '0xa', from: '0xb' } },
        { type: 'create', action: { from: '0xc' } },
      ],
    }
    shimWireBlock(block)
    expect(block.traces[0]).toEqual({ type: 'call', action: { to: '0xa', from: '0xb' } })
    expect(block.traces[1]).toEqual({ type: 'create', action: { from: '0xc' } })

    // no traces at all — must not throw
    expect(() => shimWireBlock({ header: {} })).not.toThrow()
    expect(() => shimWireBlock({})).not.toThrow()
  })

  it('does not throw on malformed wire JSON (non-object action) — leaves it for schema validation', () => {
    // A reward trace whose `action` is a truthy non-object must not make the `in` check throw before
    // the downstream `cast()` can report a proper validation error.
    const block = { traces: [{ type: 'reward', action: 'not-an-object' }] }
    expect(() => shimWireBlock(block)).not.toThrow()
    expect(block.traces[0].action).toBe('not-an-object') // left untouched for the schema to reject
  })
})

describe('dropEmptyBlocks', () => {
  const blk = (
    number: number,
    parts: { logs?: unknown[]; transactions?: unknown[]; traces?: unknown[]; stateDiffs?: unknown[] } = {},
  ) => ({
    header: { number },
    logs: parts.logs ?? [],
    transactions: parts.transactions ?? [],
    traces: parts.traces ?? [],
    stateDiffs: parts.stateDiffs ?? [],
  })

  it('drops empty interior blocks but keeps the boundaries', () => {
    const blocks = [blk(1), blk(2, { logs: [{}] }), blk(3), blk(4)]
    // 1 kept (first), 2 kept (has a log), 3 dropped (empty interior), 4 kept (last)
    expect(dropEmptyBlocks(blocks).map((b) => b.header.number)).toEqual([1, 2, 4])
  })

  it('keeps every block when includeAllBlocks is set', () => {
    const blocks = [blk(1), blk(2), blk(3)]
    expect(dropEmptyBlocks(blocks, true).map((b) => b.header.number)).toEqual([1, 2, 3])
  })

  it('keeps an interior block that has any data (tx / trace / stateDiff)', () => {
    const blocks = [
      blk(1),
      blk(2, { transactions: [{}] }),
      blk(3, { traces: [{}] }),
      blk(4, { stateDiffs: [{}] }),
      blk(5),
    ]
    expect(dropEmptyBlocks(blocks).map((b) => b.header.number)).toEqual([1, 2, 3, 4, 5])
  })

  it('keeps a lone empty block (it is both first and last — the progress cursor)', () => {
    expect(dropEmptyBlocks([blk(7)]).map((b) => b.header.number)).toEqual([7])
  })
})
