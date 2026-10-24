import { describe, expect, it } from 'vitest'

import { evmQuery } from '~/evm/evm-query-builder.js'

import {
  type Portal,
  type RangeRequest,
  type RequestPlanner,
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

// ── planned requests ──

describe('planned requests', () => {
  const portal: Portal = {
    getHead: async () => ({ number: 1_000, hash: '0x' }),
    resolveTimestamp: async () => 0,
  }

  const planner: RequestPlanner<any> = async () => ({ request: {} })
  const logs = { logs: [{ topic0: ['0xaa'] }] }

  it('attaches a planner to every merged range it covers, and to no other', async () => {
    const query = evmQuery()
      .addLogRequest({ range: { from: 0, to: 100 }, request: logs.logs[0] })
      .addPlannedRequest({ range: { from: 50 }, planner })

    const { raw } = await query.calculateRanges({ portal })

    expect(raw).toEqual([
      { range: { from: 0, to: 49 }, request: logs },
      { range: { from: 50, to: 100 }, request: logs, planners: [planner] },
      { range: { from: 101 }, request: {}, planners: [planner] },
    ])
    expect(raw[0]).not.toHaveProperty('planners')
  })

  it('applies the resume bound to planned ranges', async () => {
    const query = evmQuery().addPlannedRequest({ range: { from: 0, to: 100 }, planner })

    const { bounded, raw } = await query.calculateRanges({ portal, bound: { from: 60 } })

    expect(raw).toEqual([{ range: { from: 0, to: 100 }, request: {}, planners: [planner] }])
    expect(bounded).toEqual([{ range: { from: 60, to: 100 }, request: {}, planners: [planner] }])
  })

  it('keeps planners through merge', async () => {
    const other: RequestPlanner<any> = async () => ({ request: {} })
    const query = evmQuery()
      .addPlannedRequest({ range: { from: 0, to: 10 }, planner })
      .merge(evmQuery().addPlannedRequest({ range: { from: 0, to: 10 }, planner: other }))

    const { raw } = await query.calculateRanges({ portal })

    expect(raw).toEqual([{ range: { from: 0, to: 10 }, request: {}, planners: [other, planner] }])
  })
})
