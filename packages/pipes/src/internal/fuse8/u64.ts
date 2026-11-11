// 64-bit arithmetic on (hi, lo) pairs of uint32, without BigInt. Each function
// leaves its result in `R`. A uint32 at or above 2^31 is not a small integer,
// so V8 boxes it when it is stored in an object field or a module variable;
// typed arrays hold it unboxed.

export const R = new Uint32Array(2) // [hi, lo]

const TWO_32 = 4294967296

/**
 * High 32 bits of the 64-bit product of two uint32. The double product and
 * the subtraction each round by at most 2^10, far less than half of 2^32, so
 * rounding the quotient recovers the high word exactly.
 */
export function mulhi32(a: number, b: number): number {
  const lo = Math.imul(a, b) >>> 0
  return Math.round((a * b - lo) / TWO_32)
}

/** a * b mod 2^64. */
export function mul64(aHi: number, aLo: number, bHi: number, bLo: number): void {
  R[1] = Math.imul(aLo, bLo) >>> 0
  R[0] = (mulhi32(aLo, bLo) + Math.imul(aLo, bHi) + Math.imul(aHi, bLo)) >>> 0
}

/** The 128-bit product of a and b, its low half xor its high half. */
export function mul128Fold64(aHi: number, aLo: number, bHi: number, bLo: number): void {
  const w0 = Math.imul(aLo, bLo) >>> 0
  const s1 = mulhi32(aLo, bLo) + (Math.imul(aLo, bHi) >>> 0) + (Math.imul(aHi, bLo) >>> 0)
  const s2 = mulhi32(aLo, bHi) + mulhi32(aHi, bLo) + (Math.imul(aHi, bHi) >>> 0) + Math.floor(s1 / TWO_32)
  const w3 = mulhi32(aHi, bHi) + Math.floor(s2 / TWO_32)

  R[0] = (s1 ^ w3) >>> 0
  R[1] = (w0 ^ s2) >>> 0
}

export function add64(aHi: number, aLo: number, bHi: number, bLo: number): void {
  const lo = aLo + bLo
  R[1] = lo >>> 0
  R[0] = (aHi + bHi + (lo >= TWO_32 ? 1 : 0)) >>> 0
}

export function hex64(hi: number, lo: number): string {
  return hi.toString(16).padStart(8, '0') + lo.toString(16).padStart(8, '0')
}
