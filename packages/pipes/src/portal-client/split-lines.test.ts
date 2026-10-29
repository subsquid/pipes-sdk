import { describe, expect, it } from 'vitest'

import { splitLines } from './split-lines.js'

async function* toChunks(parts: string[]) {
  const encoder = new TextEncoder()

  for (const part of parts) {
    yield encoder.encode(part)
  }
}

async function read(input: AsyncIterable<Uint8Array>) {
  const out: string[] = []
  for await (const { lines } of splitLines(input)) {
    out.push(...lines)
  }
  return out
}

/** The lines no newline ended. */
async function unterminated(input: AsyncIterable<Uint8Array>) {
  const out: string[] = []
  for await (const { lines, unterminated } of splitLines(input)) {
    if (unterminated) {
      out.push(...lines)
    }
  }

  return out
}

describe('splitLines', () => {
  it('should split multiple lines within a single chunk', async () => {
    const input = toChunks(['a\nb'])

    expect(await read(input)).toEqual(['a', 'b'])
  })

  it('should not split lines if newline is missing', async () => {
    const input = toChunks(['ab'])

    expect(await read(input)).toEqual(['ab'])
  })

  it('should handle lines split across chunks', async () => {
    const input = toChunks(['a', '\n', 'b', '\n'])

    expect(await read(input)).toEqual(['a', 'b'])
  })

  it('should drop empty lines', async () => {
    const input = toChunks(['a\n\n', 'b\n'])
    expect(await read(input)).toEqual(['a', 'b'])

    const input2 = toChunks(['\n\na\n\nb\n\n\n'])
    expect(await read(input2)).toEqual(['a', 'b'])
  })

  it('should emit the final partial line at the end', async () => {
    const input = toChunks(['a\nb', 'c'])

    expect(await read(input)).toEqual(['a', 'bc'])
  })

  it('marks only a last line with no newline after it as unterminated', async () => {
    expect(await unterminated(toChunks(['a\nb', 'c']))).toEqual(['bc'])
    expect(await unterminated(toChunks(['a\n', 'b\n']))).toEqual([])
  })
})
