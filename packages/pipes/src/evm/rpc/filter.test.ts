import { describe, expect, it } from 'vitest'

import { DataRequest } from '~/portal-client/query/evm.js'

import { EvmQueryBuilder } from '../evm-query-builder.js'
import { filterBlock, setUpRelations } from './filter.js'
import { keptByPosition } from './project.js'
import { toRequiredData } from './request.js'

/**
 * Hand-built flat blocks + an independent expectation of Portal filter semantics (the unit
 * oracle, not golden dumps) — mirrors the Squid evm-rpc-stream filter tests.
 */

function log(transactionIndex: number, address: string, topics: string[] = []): any {
  return { logIndex: transactionIndex * 10, transactionIndex, address, topics }
}
function tx(transactionIndex: number, props: any = {}): any {
  return { transactionIndex, ...props }
}
function callTrace(transactionIndex: number, traceAddress: number[], action: any = {}): any {
  return { transactionIndex, traceAddress, type: 'call', action: { to: '0x0', from: '0x0', ...action } }
}
function rewardTrace(transactionIndex: number, traceAddress: number[] = [0], author = '0xauthor'): any {
  return { transactionIndex, traceAddress, type: 'reward', action: { author, value: '0x1' } }
}
function stateDiff(transactionIndex: number, address: string, kind: string): any {
  return { transactionIndex, address, key: 'balance', kind }
}
function makeBlock(parts: { transactions?: any[]; logs?: any[]; traces?: any[]; stateDiffs?: any[] } = {}): any {
  return {
    header: { number: 1, hash: '0x1', parentHash: '0x0' },
    transactions: parts.transactions ?? [],
    logs: parts.logs ?? [],
    traces: parts.traces ?? [],
    stateDiffs: parts.stateDiffs ?? [],
  }
}
function run(block: any, req: DataRequest) {
  filterBlock(block, req, setUpRelations(block))
  return block
}

describe('filterBlock', () => {
  it('matches logs by address and topic0', () => {
    const block = makeBlock({ logs: [log(0, '0xaaa', ['0xt0']), log(1, '0xbbb', ['0xz'])] })
    run(block, { logs: [{ address: ['0xaaa'] }] })
    expect(block.logs.map((l: any) => l.transactionIndex)).toEqual([0])
  })

  it('matches transactions by to/from (union of requests)', () => {
    const block = makeBlock({ transactions: [tx(0, { to: '0xto' }), tx(1, {}), tx(2, { from: '0xfrom' })] })
    run(block, { transactions: [{ to: ['0xto'] }, { from: ['0xfrom'] }] })
    expect(block.transactions.map((t: any) => t.transactionIndex)).toEqual([0, 2])
  })

  it('expands log → transaction and tx → logs relations', () => {
    const block = makeBlock({
      transactions: [tx(0, { to: '0xa' }), tx(1, { to: '0xb' })],
      logs: [log(0, '0xaaa'), log(0, '0xbbb')],
    })
    run(block, { logs: [{ address: ['0xaaa'], transaction: true }] })
    expect(block.transactions.map((t: any) => t.transactionIndex)).toEqual([0])
    expect(block.logs).toHaveLength(1) // only the matched log (no transactionLogs)
  })

  it('expands trace subtraces to all descendants but not unrelated branches', () => {
    const block = makeBlock({
      traces: [callTrace(0, [0], { to: '0xroot' }), callTrace(0, [0, 0]), callTrace(0, [1]), callTrace(0, [1, 0])],
    })
    run(block, { traces: [{ callTo: ['0xroot'], subtraces: true }] })
    expect(block.traces.map((t: any) => t.traceAddress)).toEqual([[0], [0, 0]])
  })

  it('drops everything when nothing is requested', () => {
    const block = makeBlock({ logs: [log(0, '0xaaa')], transactions: [tx(0)] })
    run(block, {})
    expect(block.logs).toHaveLength(0)
    expect(block.transactions).toHaveLength(0)
  })

  it('matches logs by topic0 alone (regardless of address)', () => {
    const block = makeBlock({ logs: [log(0, '0xaaa', ['0xt0']), log(1, '0xbbb', ['0xz'])] })
    run(block, { logs: [{ topic0: ['0xt0'] }] })
    expect(block.logs.map((l: any) => l.transactionIndex)).toEqual([0])
  })

  it('keeps an item matched by two requests only once (Set union, no duplicates)', () => {
    const block = makeBlock({ logs: [log(0, '0xaaa', ['0xt0'])] })
    // the single log matches BOTH requests — it must appear once, not twice
    run(block, { logs: [{ address: ['0xaaa'] }, { topic0: ['0xt0'] }] })
    expect(block.logs).toHaveLength(1)
  })

  it('an empty where matches every item of that type', () => {
    const block = makeBlock({ logs: [log(0, '0xaaa'), log(1, '0xbbb')] })
    run(block, { logs: [{}] })
    expect(block.logs).toHaveLength(2)
  })

  it('matches traces by type', () => {
    const block = makeBlock({ traces: [callTrace(0, [0]), rewardTrace(1)] })
    run(block, { traces: [{ type: ['reward'] }] })
    expect(block.traces.map((t: any) => t.type)).toEqual(['reward'])
  })

  it('matches stateDiffs by address and, separately, by kind', () => {
    const diffs = () => [stateDiff(0, '0xaaa', '='), stateDiff(1, '0xbbb', '+')]

    const byAddress = makeBlock({ stateDiffs: diffs() })
    run(byAddress, { stateDiffs: [{ address: ['0xaaa'] }] })
    expect(byAddress.stateDiffs.map((d: any) => d.transactionIndex)).toEqual([0])

    const byKind = makeBlock({ stateDiffs: diffs() })
    run(byKind, { stateDiffs: [{ kind: ['+'] }] })
    expect(byKind.stateDiffs.map((d: any) => d.transactionIndex)).toEqual([1])
  })

  it('expands transaction → logs / traces / stateDiffs siblings', () => {
    const block = makeBlock({
      transactions: [tx(0, { to: '0xt' })],
      logs: [log(0, '0xaaa'), log(1, '0xbbb')],
      traces: [callTrace(0, [0]), callTrace(1, [0])],
      stateDiffs: [stateDiff(0, '0xaaa', '='), stateDiff(1, '0xbbb', '=')],
    })
    run(block, { transactions: [{ to: ['0xt'], logs: true, traces: true, stateDiffs: true }] })
    expect(block.transactions.map((t: any) => t.transactionIndex)).toEqual([0])
    expect(block.logs.map((l: any) => l.transactionIndex)).toEqual([0])
    expect(block.traces.map((t: any) => t.transactionIndex)).toEqual([0])
    expect(block.stateDiffs.map((d: any) => d.transactionIndex)).toEqual([0])
  })

  it('expands log → transactionTraces siblings', () => {
    const block = makeBlock({
      logs: [log(0, '0xaaa')],
      traces: [callTrace(0, [0]), callTrace(1, [0])],
    })
    run(block, { logs: [{ address: ['0xaaa'], transactionTraces: true }] })
    expect(block.traces.map((t: any) => t.transactionIndex)).toEqual([0])
  })

  it('filters callSighash without crashing when a call trace has no sighash (optional field)', () => {
    const block = makeBlock({
      traces: [
        { transactionIndex: 0, traceAddress: [0], type: 'call', action: { to: '0x0', from: '0x0' } }, // no sighash
        {
          transactionIndex: 1,
          traceAddress: [0],
          type: 'call',
          action: { to: '0x0', from: '0x0', sighash: '0xdeadbeef' },
        },
      ],
    })
    run(block, { traces: [{ callSighash: ['0xdeadbeef'] }] })
    expect(block.traces.map((t: any) => t.transactionIndex)).toEqual([1]) // the sighash-less trace is a non-match, not a crash
  })

  it('filters suicideRefundAddress without crashing when refundAddress is null (nullable field)', () => {
    const block = makeBlock({
      traces: [
        { transactionIndex: 0, traceAddress: [0], type: 'suicide', action: { refundAddress: null } },
        { transactionIndex: 1, traceAddress: [0], type: 'suicide', action: { refundAddress: '0xbene' } },
      ],
    })
    run(block, { traces: [{ suicideRefundAddress: ['0xbene'] }] })
    expect(block.traces.map((t: any) => t.transactionIndex)).toEqual([1])
  })

  it('expands trace → parents up the traceAddress chain', () => {
    const block = makeBlock({
      traces: [callTrace(0, [0]), callTrace(0, [0, 0]), callTrace(0, [0, 0, 0], { to: '0xdeep' })],
    })
    run(block, { traces: [{ callTo: ['0xdeep'], parents: true }] })
    // the matched deep trace plus its ancestors [0,0] and [0] (original order preserved)
    expect(block.traces.map((t: any) => t.traceAddress)).toEqual([[0], [0, 0], [0, 0, 0]])
  })
})

describe('toRequiredData', () => {
  it('derives coarse toggles incl. relation-implication and receipt upgrade', () => {
    // No `transactions` toggle: the RPC source always fetches full transactions (mapRpcBlock needs
    // them), so it isn't derived here.
    expect(toRequiredData({}, {})).toEqual({
      logs: false,
      receipts: false,
      traces: false,
      stateDiffs: false,
    })

    const upgraded = toRequiredData({ transactions: [{ to: ['0xb'] }], logs: [{}] }, { transaction: { gasUsed: true } })
    expect(upgraded.receipts).toBe(true)
    expect(upgraded.logs).toBe(false)
  })

  it('derives traces / stateDiffs / logs from relation includes on other item types', () => {
    expect(toRequiredData({ logs: [{ transactionTraces: true }] }, {}).traces).toBe(true)
    expect(toRequiredData({ logs: [{ transactionStateDiffs: true }] }, {}).stateDiffs).toBe(true)
    expect(toRequiredData({ transactions: [{ traces: true }] }, {}).traces).toBe(true)
    expect(toRequiredData({ transactions: [{ stateDiffs: true }] }, {}).stateDiffs).toBe(true)
    expect(toRequiredData({ traces: [{ transactionLogs: true }] }, {}).logs).toBe(true)
  })
})

describe('merged multi-request required-data (#510 regression)', () => {
  it('applies the logs↔receipts dedup on the merged aggregate, not per request', () => {
    // Two independent request items, exactly as a multi-`add*` config produces before the stream
    // runs. `mergeDataRequests` concatenates them into one request; a single `toRequiredData` then
    // decides the fetch toggles on the aggregate.
    const merged = new EvmQueryBuilder().mergeDataRequests(
      { logs: [{ address: ['0xaaa'] }] }, // wants raw logs
      { transactions: [{ to: ['0xbbb'] }] }, // wants a tx; its receipt field upgrades to receipts
    )
    const required = toRequiredData(merged, { transaction: { gasUsed: true } })

    // Receipts already carry the logs, so a redundant `eth_getLogs` must NOT be requested — even
    // though one of the merged items asked for logs. Deduping per-request (the pre-#510 bug) would
    // leave `logs: true` here.
    expect(required).toEqual({ logs: false, receipts: true, traces: false, stateDiffs: false })
  })
})

describe('keptByPosition', () => {
  it('projects by position/identity, so structurally identical items never collide', () => {
    // Two structurally identical pre-filter items that a synthesized structural key couldn't tell
    // apart. A keyed projection would confuse them; position + identity won't.
    const preA = { tag: 'reward' }
    const preB = { tag: 'reward' }
    const pre = [preA, preB]
    // The decode at exactly the output fields: distinct objects, aligned 1:1 with `pre` by position.
    const projected = [{ n: 0 }, { n: 1 }]

    // Only the *second* survived filtering — the projection must keep the second, not the first.
    expect(keptByPosition(projected, pre, [preB])).toEqual([{ n: 1 }])
    expect(keptByPosition(projected, pre, [preA])).toEqual([{ n: 0 }])
    expect(keptByPosition(projected, pre, [preA, preB])).toEqual([{ n: 0 }, { n: 1 }])
    expect(keptByPosition(projected, pre, [])).toEqual([])
  })
})
