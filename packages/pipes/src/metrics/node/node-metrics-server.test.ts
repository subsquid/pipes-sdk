import { describe, expect, it } from 'vitest'

import { packPreview } from './node-metrics-server.js'

/** Wrap `leaf` in `depth` nested `{ v: ... }` objects. */
function nest(leaf: unknown, depth: number): unknown {
  let node: unknown = leaf
  for (let i = 0; i < depth; i++) {
    node = { v: node }
  }

  return node
}

describe('packPreview', () => {
  it('does not overflow the stack on a cyclic object', () => {
    const a: any = {}
    a.self = a

    expect(() => packPreview(a)).not.toThrow()
    expect(JSON.stringify(packPreview(a))).toContain('[Truncated]')
  })

  it('bounds a deeply nested object instead of throwing', () => {
    // 100k levels — overflowed the stack before the depth guard (the reported crash-loop).
    const root: any = {}
    let cur = root
    for (let i = 0; i < 100_000; i++) {
      cur.next = {}
      cur = cur.next
    }
    cur.leaf = 'end'

    expect(() => packPreview(root)).not.toThrow()
    expect(JSON.stringify(packPreview(root))).toContain('[Truncated]')
  })

  it('keeps values up to the depth bound and truncates just past it', () => {
    const withinBound = JSON.stringify(packPreview(nest('LEAF', 8)))
    expect(withinBound).toContain('LEAF')
    expect(withinBound).not.toContain('[Truncated]')

    const pastBound = JSON.stringify(packPreview(nest('LEAF', 9)))
    expect(pastBound).not.toContain('LEAF')
    expect(pastBound).toContain('[Truncated]')
  })

  it('leaves realistic shallow preview data untouched', () => {
    const value = {
      header: { number: 100, hash: '0xabc' },
      transactions: [{ from: '0x1', to: '0x2', logs: [{ topics: ['0xa', '0xb'] }] }],
    }

    expect(packPreview(value)).toEqual(value)
  })

  it('passes primitives, null and Date through unchanged', () => {
    const date = new Date(0)

    expect(packPreview(42)).toBe(42)
    expect(packPreview('x')).toBe('x')
    expect(packPreview(null)).toBe(null)
    expect(packPreview(date)).toBe(date)
  })

  it('returns short primitive arrays verbatim', () => {
    expect(packPreview([1, 2, 3])).toEqual([1, 2, 3])
  })

  it('collapses long arrays to head plus a remainder marker', () => {
    const arr = Array.from({ length: 25 }, (_, i) => ({ i }))

    expect(packPreview(arr)).toEqual([{ i: 0 }, '... 24 more ...'])
  })
})
