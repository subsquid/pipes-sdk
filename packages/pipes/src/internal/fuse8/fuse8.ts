// BinaryFuse8 construction, ported line by line from xorf 0.13
// (`bfuse_from_impl!`, arity 3, at most 1 000 attempts) so that the same keys
// give the same bytes. 64-bit hashes are kept as (hi, lo) uint32 pairs.

import { R, add64, mul64, mulhi32 } from './u64.js'

const MAX_ITERATIONS = 1000

function splitmix64(state: { value: bigint }): bigint {
  const mask = (1n << 64n) - 1n
  state.value = (state.value + 0x9e3779b97f4a7c15n) & mask
  let z = state.value
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & mask
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & mask
  return z ^ (z >> 31n)
}

/** murmur3's mix64 of key + seed, into `R`. */
function mix(keyHi: number, keyLo: number, seedHi: number, seedLo: number): void {
  add64(keyHi, keyLo, seedHi, seedLo)
  let hi = R[0]
  let lo = (R[1] ^ (hi >>> 1)) >>> 0
  mul64(hi, lo, 0xff51afd7, 0xed558ccd)
  hi = R[0]
  lo = (R[1] ^ (hi >>> 1)) >>> 0
  mul64(hi, lo, 0xc4ceb9fe, 0x1a85ec53)
  R[1] = (R[1] ^ (R[0] >>> 1)) >>> 0
}

// The three slots of a hash, from `hash_of_hash`.
const H = new Uint32Array(3)

function hashOfHash(hi: number, lo: number, length: number, mask: number, countLength: number): void {
  // (hash * countLength) >> 64, with countLength < 2^32
  const carry = (Math.imul(hi, countLength) >>> 0) + mulhi32(lo, countLength) >= 4294967296 ? 1 : 0
  H[0] = mulhi32(hi, countLength) + carry
  H[1] = (H[0] + length) ^ (((lo >>> 18) | (hi << 14)) & mask)
  H[2] = (H[0] + 2 * length) ^ (lo & mask)
}

function segmentLengthFor(size: number): number {
  if (size === 0) return 4
  return 2 ** Math.floor(Math.log(size) / Math.log(3.33) + 2.25)
}

function sizeFactorFor(size: number): number {
  return Math.max(1.125, 0.875 + (0.25 * Math.log(1000000)) / Math.log(size))
}

/** The wire bytes: the 20-byte descriptor, then the fingerprints. */
export function buildFuse8(keyHi: Uint32Array, keyLo: Uint32Array): Uint8Array {
  const size = keyHi.length
  let segmentLength = Math.min(segmentLengthFor(size), 262144)
  let segmentLengthMask = segmentLength - 1
  const sizeFactor = sizeFactorFor(size)
  const initialCapacity = size > 1 ? Math.round(size * sizeFactor) : 0
  const initSegmentCount = Math.ceil(initialCapacity / segmentLength)
  const proposed = Math.ceil((initSegmentCount * segmentLength) / segmentLength)
  let segmentCount = proposed < 3 ? 1 : proposed - 2
  const arrayLength = (segmentCount + 2) * segmentLength
  let segmentCountLength = segmentCount * segmentLength

  const out = new Uint8Array(20 + arrayLength)
  const fingerprints = out.subarray(20)

  const rng = { value: 1n }
  let seed = splitmix64(rng)
  const capacity = arrayLength
  const alone = new Uint32Array(capacity)
  const t2count = new Uint8Array(capacity)
  const t2hashHi = new Uint32Array(capacity)
  const t2hashLo = new Uint32Array(capacity)
  const reverseH = new Uint8Array(size)
  const reverseHi = new Uint32Array(size + 1)
  const reverseLo = new Uint32Array(size + 1)
  reverseLo[size] = 1

  const trySmallerSegment = size > 4 && size < 1_000_000
  const maxSegmentCount = trySmallerSegment ? segmentCount * 2 + 2 : segmentCount
  let capBits = 1
  while (1 << capBits < maxSegmentCount) capBits++
  const startPos = new Uint32Array(1 << capBits)
  const h012 = new Uint32Array(6)

  let done = false
  let ultimateSize = 0
  for (let iteration = 1; iteration <= MAX_ITERATIONS; iteration++) {
    if (trySmallerSegment) {
      if (iteration % 4 === 2) {
        segmentLength /= 2
        segmentLengthMask = segmentLength - 1
        segmentCount = segmentCount * 2 + 2
        segmentCountLength = segmentCount * segmentLength
      } else if (iteration % 4 === 3) {
        segmentLength *= 2
        segmentLengthMask = segmentLength - 1
        segmentCount = Math.floor(segmentCount / 2) - 1
        segmentCountLength = segmentCount * segmentLength
      }
    }

    let blockBits = 1
    while (1 << blockBits < segmentCount) blockBits++
    const startPosLength = 1 << blockBits
    for (let i = 0; i < startPosLength; i++) {
      startPos[i] = Math.floor((i * size) / startPosLength)
    }

    const seedHi = Number(seed >> 32n)
    const seedLo = Number(seed & 0xffffffffn)
    for (let k = 0; k < size; k++) {
      mix(keyHi[k], keyLo[k], seedHi, seedLo)
      let segmentIndex = R[0] >>> (32 - blockBits)
      while (reverseHi[startPos[segmentIndex]] !== 0 || reverseLo[startPos[segmentIndex]] !== 0) {
        segmentIndex = (segmentIndex + 1) & (startPosLength - 1)
      }
      reverseHi[startPos[segmentIndex]] = R[0]
      reverseLo[startPos[segmentIndex]] = R[1]
      startPos[segmentIndex] += 1
    }

    let error = false
    let duplicates = 0
    for (let i = 0; i < size; i++) {
      const hi = reverseHi[i]
      const lo = reverseLo[i]
      hashOfHash(hi, lo, segmentLength, segmentLengthMask, segmentCountLength)
      const i1 = H[0]
      const i2 = H[1]
      const i3 = H[2]

      t2count[i1] += 4
      t2hashHi[i1] ^= hi
      t2hashLo[i1] ^= lo
      t2count[i2] += 4
      t2count[i2] ^= 1
      t2hashHi[i2] ^= hi
      t2hashLo[i2] ^= lo
      t2count[i3] += 4
      t2count[i3] ^= 2
      t2hashHi[i3] ^= hi
      t2hashLo[i3] ^= lo

      const anded = (t2hashHi[i1] & t2hashHi[i2] & t2hashHi[i3]) | (t2hashLo[i1] & t2hashLo[i2] & t2hashLo[i3])
      if (anded === 0) {
        const zero1 = t2hashHi[i1] === 0 && t2hashLo[i1] === 0 && t2count[i1] === 8
        const zero2 = t2hashHi[i2] === 0 && t2hashLo[i2] === 0 && t2count[i2] === 8
        const zero3 = t2hashHi[i3] === 0 && t2hashLo[i3] === 0 && t2count[i3] === 8
        if (zero1 || zero2 || zero3) {
          duplicates++
          t2count[i1] -= 4
          t2hashHi[i1] ^= hi
          t2hashLo[i1] ^= lo
          t2count[i2] -= 4
          t2count[i2] ^= 1
          t2hashHi[i2] ^= hi
          t2hashLo[i2] ^= lo
          t2count[i3] -= 4
          t2count[i3] ^= 2
          t2hashHi[i3] ^= hi
          t2hashLo[i3] ^= lo
        }
      }
      // xorf overwrites the flag per key, so only the last key decides.
      error = t2count[i1] < 4 || t2count[i2] < 4 || t2count[i3] < 4
    }

    if (!error) {
      let queueSize = 0
      for (let i = 0; i < capacity; i++) {
        alone[queueSize] = i
        if (t2count[i] >> 2 === 1) queueSize++
      }

      let stackSize = 0
      while (queueSize > 0) {
        queueSize--
        const index = alone[queueSize]
        if (t2count[index] >> 2 !== 1) continue

        const hi = t2hashHi[index]
        const lo = t2hashLo[index]
        const found = t2count[index] & 3
        reverseH[stackSize] = found
        reverseHi[stackSize] = hi
        reverseLo[stackSize] = lo
        stackSize++

        hashOfHash(hi, lo, segmentLength, segmentLengthMask, segmentCountLength)
        h012[1] = H[1]
        h012[2] = H[2]
        h012[3] = H[0]
        h012[4] = h012[1]

        const other1 = h012[found + 1]
        alone[queueSize] = other1
        if (t2count[other1] >> 2 === 2) queueSize++
        t2count[other1] -= 4
        t2count[other1] ^= found + 1 > 2 ? found - 2 : found + 1
        t2hashHi[other1] ^= hi
        t2hashLo[other1] ^= lo

        const other2 = h012[found + 2]
        alone[queueSize] = other2
        if (t2count[other2] >> 2 === 2) queueSize++
        t2count[other2] -= 4
        t2count[other2] ^= found + 2 > 2 ? found - 1 : found + 2
        t2hashHi[other2] ^= hi
        t2hashLo[other2] ^= lo
      }

      if (stackSize + duplicates === size) {
        ultimateSize = stackSize
        done = true
        break
      }
    }

    reverseHi.fill(0, 0, size)
    reverseLo.fill(0, 0, size)
    t2count.fill(0)
    t2hashHi.fill(0)
    t2hashLo.fill(0)
    seed = splitmix64(rng)
  }
  if (!done) throw new Error('Failed to construct binary fuse filter.')

  for (let i = ultimateSize - 1; i >= 0; i--) {
    const hi = reverseHi[i]
    const lo = reverseLo[i]
    hashOfHash(hi, lo, segmentLength, segmentLengthMask, segmentCountLength)
    h012[0] = H[0]
    h012[1] = H[1]
    h012[2] = H[2]
    h012[3] = h012[0]
    h012[4] = h012[1]
    const found = reverseH[i]
    fingerprints[h012[found]] = ((lo ^ hi) & 0xff) ^ fingerprints[h012[found + 1]] ^ fingerprints[h012[found + 2]]
  }

  const view = new DataView(out.buffer, out.byteOffset, 20)
  view.setBigUint64(0, seed, true)
  view.setUint32(8, segmentLength, true)
  view.setUint32(12, segmentLengthMask, true)
  view.setUint32(16, segmentCountLength, true)
  return out
}
