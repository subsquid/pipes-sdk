import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

import { buildFuse8 } from './fuse8.js'
import { buildFuse8Set, fuse8Contains } from './index.js'
import { R, hex64 } from './u64.js'
import { xxh3 } from './xxh3.js'

// Both fixtures come from the crates the query engine uses: xxhash-rust 0.8.15 and xorf 0.13.0.
const fixture = (name: string) =>
  readFileSync(fileURLToPath(new URL(`./__fixtures__/${name}`, import.meta.url)), 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => line.split('\t') as [string, string])

const text = (length: number) => createHash('sha512').update(String(length)).digest('base64').repeat(3).slice(0, length)

const addresses = (count: number) =>
  Array.from({ length: count }, (_, i) => '0x' + createHash('sha256').update(`child-${i}`).digest('hex').slice(0, 40))

function fuse8Bytes(values: string[]) {
  const hi = new Uint32Array(values.length)
  const lo = new Uint32Array(values.length)
  values.forEach((value, i) => {
    xxh3(value)
    hi[i] = R[0]!
    lo[i] = R[1]!
  })

  return buildFuse8(hi, lo)
}

describe('fuse8', () => {
  it('hashes every length up to 128 as XXH3-64 does', () => {
    for (const [length, expected] of fixture('xxh3-reference.tsv')) {
      xxh3(text(Number(length)))

      expect(hex64(R[0]!, R[1]!), `length ${length}`).toBe(expected)
    }
  })

  it('builds the same bytes as xorf', () => {
    for (const [count, digest] of fixture('xorf-reference.tsv')) {
      const bytes = fuse8Bytes(addresses(Number(count)))

      expect(createHash('sha256').update(bytes).digest('hex'), `${count} values`).toBe(digest)
    }
  })

  it('holds every value it was built from', () => {
    const values = addresses(20_000)
    const set = buildFuse8Set(values)

    expect(values.every((value) => fuse8Contains(set, value))).toBe(true)
    expect(set.canaries).toHaveLength(4)
    expect(set.canaries.every((canary) => values.includes(canary))).toBe(true)
  })

  it('passes about one other value in 256', () => {
    const set = buildFuse8Set(addresses(10_000))
    const others = addresses(60_000).slice(10_000)

    const passed = others.filter((value) => fuse8Contains(set, value)).length

    expect(passed / others.length).toBeGreaterThan(1 / 512)
    expect(passed / others.length).toBeLessThan(1 / 128)
  })

  it('uses every value as a canary when there are few', () => {
    expect(buildFuse8Set(['0x01', '0x02']).canaries).toEqual(['0x01', '0x02'])
  })

  it('refuses an empty set', () => {
    expect(() => buildFuse8Set([])).toThrow('at least one value')
  })
})
