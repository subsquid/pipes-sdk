/**
 * Lines of a body as they arrive. `unterminated` marks the last line of the body when no newline
 * ends it: a body cut inside a line ends this way.
 */
export type Lines = { lines: string[]; unterminated: boolean }

export async function* splitLines(chunks: AsyncIterable<Uint8Array>): AsyncGenerator<Lines> {
  const splitter = new LineSplitter()

  for await (let chunk of chunks) {
    const lines = splitter.push(chunk)
    if (lines.length) {
      yield { lines, unterminated: false }
    }
  }

  const lastLine = splitter.end()
  if (lastLine) {
    yield { lines: [lastLine], unterminated: true }
  }
}

class LineSplitter {
  private decoder = new TextDecoder('utf-8')
  private line = ''

  push(data: Uint8Array): string[] {
    let s = this.decoder.decode(data, { stream: true })
    if (!s) return []

    let lines = s.split('\n')
    if (lines.length === 1) {
      this.line += lines[0]
    } else {
      lines[0] = this.line + lines[0]
      this.line = lines.pop() || ''

      return lines.filter((l) => l)
    }

    return []
  }

  end(): string | undefined {
    // Flush any remaining bytes from the streaming decoder
    const remaining = this.decoder.decode()
    this.line += remaining

    if (this.line) return this.line

    return
  }
}
