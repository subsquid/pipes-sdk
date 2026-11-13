import { buildFuse8 } from './fuse8.js'
import { R } from './u64.js'
import { xxh3 } from './xxh3.js'

/** A value set as the query engine reads it under a query's `sets` key. */
export type Fuse8Set = {
  /** Header and fingerprints, as canonical base 64 */
  fuse8: string
  /** A few of the set's values: the engine refuses a structure that does not hold them */
  canaries: string[]
}

const DEFAULT_CANARIES = 4

/**
 * Builds the fuse8 structure over `values`, which must be distinct, non-empty and in the text the
 * response renders: lowercase `0x` hex for addresses.
 */
export function buildFuse8Set(values: readonly string[], canaryCount = DEFAULT_CANARIES): Fuse8Set {
  if (values.length === 0) {
    throw new Error('a fuse8 set needs at least one value')
  }

  const hi = new Uint32Array(values.length)
  const lo = new Uint32Array(values.length)
  for (let i = 0; i < values.length; i++) {
    xxh3(values[i]!)
    hi[i] = R[0]!
    lo[i] = R[1]!
  }

  const bytes = buildFuse8(hi, lo)

  return {
    fuse8: Buffer.from(bytes.buffer, bytes.byteOffset, bytes.length).toString('base64'),
    canaries: pickCanaries(values, canaryCount),
  }
}

/** Random values, so that a mistake that spoils only some of them is caught by one of these (sqd-query-engine ADR-17). */
function pickCanaries(values: readonly string[], count: number): string[] {
  if (values.length <= count) return [...values]

  const picked = new Set<number>()
  while (picked.size < count) {
    picked.add(Math.floor(Math.random() * values.length))
  }

  return [...picked].map((i) => values[i]!)
}

const MASK_64 = (1n << 64n) - 1n

function mix(x: bigint) {
  x = ((x ^ (x >> 33n)) * 0xff51afd7ed558ccdn) & MASK_64
  x = ((x ^ (x >> 33n)) * 0xc4ceb9fe1a85ec53n) & MASK_64

  return x ^ (x >> 33n)
}

/**
 * The engine's lookup, written from the sqd-query-engine spec (§4.2.11) independently of the builder.
 *
 * @internal
 */
export function fuse8Contains(set: Pick<Fuse8Set, 'fuse8'>, value: string): boolean {
  const bytes = Buffer.from(set.fuse8, 'base64')
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.length)

  const seed = view.getBigUint64(0, true)
  const L = BigInt(view.getUint32(8, true))
  const M = BigInt(view.getUint32(12, true))
  const N = BigInt(view.getUint32(16, true))
  const F = bytes.subarray(20)

  xxh3(value)
  const k = (BigInt(R[0]!) << 32n) | BigInt(R[1]!)

  const h = mix((k + seed) & MASK_64)
  const f = Number((h ^ (h >> 32n)) & 0xffn)
  const i0 = (h * N) >> 64n
  const i1 = (i0 + L) ^ ((h >> 18n) & M)
  const i2 = (i0 + 2n * L) ^ (h & M)

  return (f ^ F[Number(i0)]! ^ F[Number(i1)]! ^ F[Number(i2)]!) === 0
}
