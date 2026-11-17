import { describe, expect, it } from 'vitest'

import { BitcoinQueryBuilder } from '~/bitcoin/bitcoin-query-builder.js'
import { EvmQueryBuilder } from '~/evm/evm-query-builder.js'
import { HyperliquidFillsQueryBuilder } from '~/hyperliquid/hyperliquid-fills-query-builder.js'
import { SolanaQueryBuilder } from '~/solana/solana-query-builder.js'
import { TronQueryBuilder } from '~/tron/tron-query-builder.js'

import {
  QueryBuilder,
  type RangeRequest,
  applyRangeBound,
  mergeRangeRequests,
  rangeDifference,
  rangeIntersection,
} from './query-builder.js'

// ── Helpers ──

type Req = { ids: string[] }

function rr(from: number, to: number | undefined, ids: string[]): RangeRequest<Req> {
  return { range: to != null ? { from, to } : { from }, request: { ids } }
}

function mergeReqs(a: Req, b: Req): Req {
  return { ids: [...a.ids, ...b.ids] }
}

// ── rangeIntersection ──

describe('rangeIntersection', () => {
  it('returns intersection of overlapping finite ranges', () => {
    expect(rangeIntersection({ from: 0, to: 100 }, { from: 50, to: 150 })).toEqual({ from: 50, to: 100 })
  })

  it('returns intersection when one range contains the other', () => {
    expect(rangeIntersection({ from: 0, to: 200 }, { from: 50, to: 100 })).toEqual({ from: 50, to: 100 })
  })

  it('returns single-block intersection at boundary', () => {
    expect(rangeIntersection({ from: 0, to: 100 }, { from: 100, to: 200 })).toEqual({ from: 100, to: 100 })
  })

  it('returns undefined for non-overlapping ranges', () => {
    expect(rangeIntersection({ from: 0, to: 50 }, { from: 100, to: 150 })).toBeUndefined()
  })

  it('returns undefined for adjacent but non-overlapping ranges', () => {
    expect(rangeIntersection({ from: 0, to: 50 }, { from: 51, to: 100 })).toBeUndefined()
  })

  it('handles infinite range intersecting finite range', () => {
    expect(rangeIntersection({ from: 0 }, { from: 100, to: 200 })).toEqual({ from: 100, to: 200 })
  })

  it('handles two infinite ranges', () => {
    expect(rangeIntersection({ from: 0 }, { from: 100 })).toEqual({ from: 100 })
  })

  it('handles identical ranges', () => {
    expect(rangeIntersection({ from: 50, to: 100 }, { from: 50, to: 100 })).toEqual({ from: 50, to: 100 })
  })

  it('handles zero-width ranges', () => {
    expect(rangeIntersection({ from: 5, to: 5 }, { from: 5, to: 5 })).toEqual({ from: 5, to: 5 })
  })

  it('handles range starting at 0', () => {
    expect(rangeIntersection({ from: 0, to: 0 }, { from: 0, to: 100 })).toEqual({ from: 0, to: 0 })
  })
})

// ── rangeDifference ──

describe('rangeDifference', () => {
  it('returns two parts when b is in the middle of a', () => {
    expect(rangeDifference({ from: 0, to: 100 }, { from: 50, to: 75 })).toEqual([
      { from: 0, to: 49 },
      { from: 76, to: 100 },
    ])
  })

  it('returns left part when b overlaps the end of a', () => {
    expect(rangeDifference({ from: 0, to: 100 }, { from: 50, to: 150 })).toEqual([{ from: 0, to: 49 }])
  })

  it('returns right part when b overlaps the start of a', () => {
    expect(rangeDifference({ from: 50, to: 100 }, { from: 0, to: 75 })).toEqual([{ from: 76, to: 100 }])
  })

  it('returns empty when b fully covers a', () => {
    expect(rangeDifference({ from: 50, to: 100 }, { from: 0, to: 200 })).toEqual([])
  })

  it('returns original range when no intersection', () => {
    expect(rangeDifference({ from: 0, to: 50 }, { from: 100, to: 150 })).toEqual([{ from: 0, to: 50 }])
  })

  it('handles infinite a with finite b in the middle', () => {
    expect(rangeDifference({ from: 0 }, { from: 50, to: 100 })).toEqual([{ from: 0, to: 49 }, { from: 101 }])
  })

  it('handles infinite a with b at the start', () => {
    expect(rangeDifference({ from: 0 }, { from: 0, to: 100 })).toEqual([{ from: 101 }])
  })

  it('returns empty when both ranges are identical', () => {
    expect(rangeDifference({ from: 50, to: 100 }, { from: 50, to: 100 })).toEqual([])
  })
})

// ── mergeRangeRequests ──

describe('mergeRangeRequests', () => {
  it('returns empty array for empty input', () => {
    expect(mergeRangeRequests([], mergeReqs)).toEqual([])
  })

  it('returns single request unchanged', () => {
    const input = [rr(0, 100, ['a'])]
    expect(mergeRangeRequests(input, mergeReqs)).toEqual(input)
  })

  it('keeps non-overlapping requests separate', () => {
    const result = mergeRangeRequests([rr(0, 50, ['a']), rr(100, 150, ['b'])], mergeReqs)

    expect(result).toEqual([rr(0, 50, ['a']), rr(100, 150, ['b'])])
  })

  it('merges overlapping finite ranges into three segments', () => {
    const result = mergeRangeRequests([rr(0, 100, ['a']), rr(50, 150, ['b'])], mergeReqs)

    expect(result).toEqual([rr(0, 49, ['a']), rr(50, 100, ['a', 'b']), rr(101, 150, ['b'])])
  })

  it('merges when one range contains the other', () => {
    const result = mergeRangeRequests([rr(0, 200, ['a']), rr(50, 100, ['b'])], mergeReqs)

    expect(result).toEqual([rr(0, 49, ['a']), rr(50, 100, ['a', 'b']), rr(101, 200, ['a'])])
  })

  it('merges two infinite ranges', () => {
    const result = mergeRangeRequests([rr(0, undefined, ['a']), rr(100, undefined, ['b'])], mergeReqs)

    expect(result).toEqual([rr(0, 99, ['a']), rr(100, undefined, ['a', 'b'])])
  })

  it('merges identical ranges', () => {
    const result = mergeRangeRequests([rr(0, 100, ['a']), rr(0, 100, ['b'])], mergeReqs)

    expect(result).toEqual([rr(0, 100, ['a', 'b'])])
  })

  it('merges three overlapping ranges', () => {
    const result = mergeRangeRequests([rr(0, 100, ['a']), rr(50, 150, ['b']), rr(120, 200, ['c'])], mergeReqs)

    expect(result).toEqual([
      rr(0, 49, ['a']),
      rr(50, 100, ['a', 'b']),
      rr(101, 119, ['b']),
      rr(120, 150, ['b', 'c']),
      rr(151, 200, ['c']),
    ])
  })

  it('merges adjacent ranges (touching but not overlapping)', () => {
    const result = mergeRangeRequests([rr(0, 50, ['a']), rr(51, 100, ['b'])], mergeReqs)

    expect(result).toEqual([rr(0, 50, ['a']), rr(51, 100, ['b'])])
  })

  it('handles ranges starting at block 0', () => {
    const result = mergeRangeRequests([rr(0, 0, ['a']), rr(0, 10, ['b'])], mergeReqs)

    expect(result).toEqual([rr(0, 0, ['a', 'b']), rr(1, 10, ['b'])])
  })
})

// ── applyRangeBound ──

describe('applyRangeBound', () => {
  it('returns original requests when no bound is provided', () => {
    const input = [rr(0, 100, ['a']), rr(200, 300, ['b'])]
    expect(applyRangeBound(input)).toEqual(input)
    expect(applyRangeBound(input, undefined)).toEqual(input)
  })

  it('clips requests to the bound', () => {
    const result = applyRangeBound([rr(0, 100, ['a']), rr(150, 200, ['b'])], { from: 50, to: 175 })

    expect(result).toEqual([rr(50, 100, ['a']), rr(150, 175, ['b'])])
  })

  it('removes requests entirely outside the bound', () => {
    const result = applyRangeBound([rr(0, 50, ['a']), rr(200, 300, ['b'])], { from: 100, to: 150 })

    expect(result).toEqual([])
  })

  it('handles infinite bound', () => {
    const result = applyRangeBound([rr(0, 100, ['a']), rr(200, 300, ['b'])], { from: 50 })

    expect(result).toEqual([rr(50, 100, ['a']), rr(200, 300, ['b'])])
  })

  it('handles infinite request with finite bound', () => {
    const result = applyRangeBound([rr(0, undefined, ['a'])], { from: 50, to: 150 })

    expect(result).toEqual([rr(50, 150, ['a'])])
  })

  it('returns empty for empty input', () => {
    expect(applyRangeBound([], { from: 0, to: 100 })).toEqual([])
  })

  it('handles bound at block 0', () => {
    const result = applyRangeBound([rr(0, 100, ['a'])], { from: 0, to: 0 })

    expect(result).toEqual([rr(0, 0, ['a'])])
  })
})

// ── QueryBuilder ──

type TestRequest = { [family: string]: unknown[] | boolean | undefined }

class TestQueryBuilder extends QueryBuilder<Record<string, never>, TestRequest> {
  protected override readonly requestOrder = ['transactions', 'logs']

  getType() {
    return 'test'
  }

  addFields() {
    return this
  }

  build(): never {
    throw new Error('not used')
  }
}

describe('QueryBuilder.mergeDataRequests', () => {
  const builder = new TestQueryBuilder()
  const merge = (...requests: TestRequest[]) => builder.mergeDataRequests(...requests)

  it('concatenates every list in request order, whatever its key', () => {
    expect(merge({ logs: [1] }, { logs: [2], someNewFamily: [3] })).toStrictEqual({ logs: [1, 2], someNewFamily: [3] })
  })

  it('leaves out lists that are empty in every request', () => {
    expect(merge({ logs: [] }, { traces: [1] })).toStrictEqual({ traces: [1] })
  })

  it('sets includeAllBlocks when any request sets it', () => {
    expect(merge({ includeAllBlocks: false }, { includeAllBlocks: true })).toStrictEqual({ includeAllBlocks: true })
  })

  it('leaves out includeAllBlocks when no request sets it', () => {
    expect(merge({ includeAllBlocks: false }, { logs: [1] })).toStrictEqual({ logs: [1] })
  })
})

describe('QueryBuilder.mergeDataRequests key order', () => {
  it('writes the listed families in request order, then other families, then flags', () => {
    const builder = new TestQueryBuilder()
    const merged = builder.mergeDataRequests(
      { includeAllBlocks: true, someNewFamily: [3] },
      { logs: [1] },
      { transactions: [2] },
    )

    expect(Object.keys(merged)).toEqual(['transactions', 'logs', 'someNewFamily', 'includeAllBlocks'])
  })

  // The query's JSON is the portal-cache key, so a merged request must list its families the way
  // the per-chain merges always did, whatever order the requests were added in.
  it.each<[string, QueryBuilder<any>, string[]]>([
    ['evm', new EvmQueryBuilder(), ['transactions', 'logs', 'traces', 'stateDiffs']],
    [
      'solana',
      new SolanaQueryBuilder(),
      ['transactions', 'logs', 'balances', 'tokenBalances', 'rewards', 'instructions'],
    ],
    ['bitcoin', new BitcoinQueryBuilder(), ['transactions', 'inputs', 'outputs']],
    [
      'tron',
      new TronQueryBuilder(),
      [
        'transactions',
        'transferTransactions',
        'transferAssetTransactions',
        'triggerSmartContractTransactions',
        'logs',
        'internalTransactions',
      ],
    ],
    ['hyperliquid fills', new HyperliquidFillsQueryBuilder(), ['fills']],
  ])('%s lists its families in their wire order', (_, builder, families) => {
    const requests = [{ includeAllBlocks: true }, ...[...families].reverse().map((family) => ({ [family]: [{}] }))]

    expect(Object.keys(builder.mergeDataRequests(...requests))).toEqual([...families, 'includeAllBlocks'])
  })
})
