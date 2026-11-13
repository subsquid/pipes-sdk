// XXH3-64 with seed 0 and the default secret, for ASCII text of up to 128
// bytes: every hex and base58 value the engine stores. Reads the text with
// charCodeAt, so it is right for ASCII only.

import { R, add64, mul128Fold64, mul64 } from './u64.js'

// The first 136 bytes of XXH3's default secret, as little-endian u64 words.
const SECRET = new Uint8Array([
  0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c, 0xde, 0xd4, 0x6d,
  0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f, 0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0,
  0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21, 0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0,
  0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c, 0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b,
  0x1b, 0x53, 0x2e, 0xa3, 0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac,
  0xd8, 0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d, 0x8a, 0x51,
  0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64, 0xea, 0xc5, 0xac, 0x83, 0x34,
  0xd3, 0xeb, 0xc3,
])
const SECRET_WORDS = new Uint32Array(SECRET.buffer)
const sLo = (byteOffset: number) => SECRET_WORDS[byteOffset >> 2]
const sHi = (byteOffset: number) => SECRET_WORDS[(byteOffset >> 2) + 1]

const PRIME64_1_HI = 0x9e3779b1
const PRIME64_1_LO = 0x85ebca87
const PRIME64_2_HI = 0xc2b2ae3d
const PRIME64_2_LO = 0x27d4eb4f
const PRIME64_3_HI = 0x165667b1
const PRIME64_3_LO = 0x9e3779f9
const PRIME_MX1_HI = 0x16566791
const PRIME_MX1_LO = 0x9e3779f9
const PRIME_MX2_HI = 0x9fb21c65
const PRIME_MX2_LO = 0x1e98df25

function read32(text: string, at: number): number {
  return (
    (text.charCodeAt(at) |
      (text.charCodeAt(at + 1) << 8) |
      (text.charCodeAt(at + 2) << 16) |
      (text.charCodeAt(at + 3) << 24)) >>>
    0
  )
}

function bswap32(x: number): number {
  return (((x & 0xff) << 24) | ((x & 0xff00) << 8) | ((x >>> 8) & 0xff00) | (x >>> 24)) >>> 0
}

function avalanche(hi: number, lo: number): void {
  lo = (lo ^ (hi >>> 5)) >>> 0
  mul64(hi, lo, PRIME_MX1_HI, PRIME_MX1_LO)
  R[1] = (R[1] ^ R[0]) >>> 0
}

function xxh64Avalanche(hi: number, lo: number): void {
  lo = (lo ^ (hi >>> 1)) >>> 0
  mul64(hi, lo, PRIME64_2_HI, PRIME64_2_LO)
  hi = R[0]
  lo = (R[1] ^ ((R[1] >>> 29) | (R[0] << 3))) >>> 0
  hi = (hi ^ (hi >>> 29)) >>> 0
  mul64(hi, lo, PRIME64_3_HI, PRIME64_3_LO)
  R[1] = (R[1] ^ R[0]) >>> 0
}

function rrmxmx(hi: number, lo: number, len: number): void {
  // h ^= rotl(h, 49) ^ rotl(h, 24)
  const r49Hi = ((lo << 17) | (hi >>> 15)) >>> 0
  const r49Lo = ((hi << 17) | (lo >>> 15)) >>> 0
  const r24Hi = ((hi << 24) | (lo >>> 8)) >>> 0
  const r24Lo = ((lo << 24) | (hi >>> 8)) >>> 0
  hi = (hi ^ r49Hi ^ r24Hi) >>> 0
  lo = (lo ^ r49Lo ^ r24Lo) >>> 0

  mul64(hi, lo, PRIME_MX2_HI, PRIME_MX2_LO)
  hi = R[0]
  lo = R[1]
  add64(0, hi >>> 3, 0, len) // (h >> 35) + len
  hi = (hi ^ R[0]) >>> 0
  lo = (lo ^ R[1]) >>> 0

  mul64(hi, lo, PRIME_MX2_HI, PRIME_MX2_LO)
  R[1] = (R[1] ^ ((R[1] >>> 28) | (R[0] << 4))) >>> 0
  R[0] = (R[0] ^ (R[0] >>> 28)) >>> 0
}

// The accumulator of the 17..128 path.
const ACC = new Uint32Array(2) // [hi, lo]

function mix16(text: string, at: number, secret: number): void {
  const aLo = read32(text, at) ^ sLo(secret)
  const aHi = read32(text, at + 4) ^ sHi(secret)
  const bLo = read32(text, at + 8) ^ sLo(secret + 8)
  const bHi = read32(text, at + 12) ^ sHi(secret + 8)
  mul128Fold64(aHi >>> 0, aLo >>> 0, bHi >>> 0, bLo >>> 0)
  add64(ACC[0], ACC[1], R[0], R[1])
  ACC[0] = R[0]
  ACC[1] = R[1]
}

/** XXH3-64 of an ASCII text, into `R`. */
export function xxh3(text: string): void {
  const len = text.length

  if (len > 16) {
    if (len > 128) throw new Error(`xxh3: ${len} bytes, at most 128`)
    mul64(0, len, PRIME64_1_HI, PRIME64_1_LO)
    ACC[0] = R[0]
    ACC[1] = R[1]
    if (len > 32) {
      if (len > 64) {
        if (len > 96) {
          mix16(text, 48, 96)
          mix16(text, len - 64, 112)
        }
        mix16(text, 32, 64)
        mix16(text, len - 48, 80)
      }
      mix16(text, 16, 32)
      mix16(text, len - 32, 48)
    }
    mix16(text, 0, 0)
    mix16(text, len - 16, 16)
    avalanche(ACC[0], ACC[1])
    return
  }

  if (len > 8) {
    const loLo = (read32(text, 0) ^ sLo(24) ^ sLo(32)) >>> 0
    const loHi = (read32(text, 4) ^ sHi(24) ^ sHi(32)) >>> 0
    const hiLo = (read32(text, len - 8) ^ sLo(40) ^ sLo(48)) >>> 0
    const hiHi = (read32(text, len - 4) ^ sHi(40) ^ sHi(48)) >>> 0

    // len + swap64(lo) + hi + fold(lo, hi)
    add64(0, len, bswap32(loLo), bswap32(loHi))
    add64(R[0], R[1], hiHi, hiLo)
    const sumHi = R[0]
    const sumLo = R[1]
    mul128Fold64(loHi, loLo, hiHi, hiLo)
    add64(sumHi, sumLo, R[0], R[1])
    avalanche(R[0], R[1])
    return
  }

  if (len >= 4) {
    const keyHi = (read32(text, 0) ^ sHi(8) ^ sHi(16)) >>> 0
    const keyLo = (read32(text, len - 4) ^ sLo(8) ^ sLo(16)) >>> 0
    rrmxmx(keyHi, keyLo, len)
    return
  }

  if (len > 0) {
    const c1 = text.charCodeAt(0)
    const c2 = text.charCodeAt(len >> 1)
    const c3 = text.charCodeAt(len - 1)
    const combined = ((c1 << 16) | (c2 << 24) | c3 | (len << 8)) >>> 0
    xxh64Avalanche(0, (combined ^ sLo(0) ^ sHi(0)) >>> 0)
    return
  }

  xxh64Avalanche((sHi(56) ^ sHi(64)) >>> 0, (sLo(56) ^ sLo(64)) >>> 0)
}
