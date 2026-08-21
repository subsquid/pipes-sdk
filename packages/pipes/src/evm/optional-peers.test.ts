import { readFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

import { describe, expect, it } from 'vitest'

/**
 * The RPC stack is an *optional* peer: a Portal-only consumer must be able to install
 * `@subsquid/pipes` alone and both run and **typecheck** against `@subsquid/pipes/evm`.
 *
 * Runtime-only laziness is not enough for that. A `export type { … } from './x.js'` still makes
 * TypeScript resolve `x`, so one type re-export from a module that imports the peer drags the peer
 * into the barrel's type graph and a Portal-only consumer stops compiling — which is exactly what
 * happened once. This walks the *static* import graph from the public entry points and asserts the
 * peers are reachable only through `await import(…)`.
 */

const OPTIONAL_PEERS = [
  '@subsquid/evm-rpc',
  '@subsquid/evm-normalization',
  '@subsquid/http-client',
  '@subsquid/rpc-client',
]

const ENTRY_POINTS = ['src/evm/index.ts', 'src/evm/browser.ts']

/** `import …/export … from '…'` — static edges only; `await import(…)` is deliberately not matched. */
const STATIC_EDGE = /(?:^|\n)\s*(?:import|export)\b[^;\n]*?\bfrom\s*['"]([^'"]+)['"]/g

function staticImports(file: string): string[] {
  const source = readFileSync(file, 'utf8')
  const out: string[] = []
  for (const m of source.matchAll(STATIC_EDGE)) out.push(m[1])

  return out
}

/** Every module statically reachable from `entry`, with the path that got us there. */
function reachable(entry: string): Map<string, string[]> {
  const seen = new Map<string, string[]>()
  const queue: { file: string; path: string[] }[] = [{ file: resolve(entry), path: [entry] }]

  while (queue.length) {
    const { file, path } = queue.shift()!
    if (seen.has(file)) continue
    seen.set(file, path)

    for (const spec of staticImports(file)) {
      if (spec.startsWith('.')) {
        // `./x.js` in source refers to `./x.ts` on disk.
        const next = resolve(dirname(file), spec.replace(/\.js$/, '.ts'))
        queue.push({ file: next, path: [...path, spec] })
      } else if (spec.startsWith('~/')) {
        queue.push({ file: resolve('src', spec.slice(2).replace(/\.js$/, '.ts')), path: [...path, spec] })
      } else {
        // A bare package: record the edge itself so the assertion can name it.
        seen.set(spec, [...path, spec])
      }
    }
  }

  return seen
}

describe('optional peer isolation', () => {
  for (const entry of ENTRY_POINTS) {
    it(`${entry} does not statically reach an optional RPC peer`, () => {
      const graph = reachable(entry)
      const leaks = OPTIONAL_PEERS.filter((peer) => graph.has(peer)).map(
        (peer) => `${peer} via ${graph.get(peer)!.join(' -> ')}`,
      )

      expect(leaks).toEqual([])
    })
  }

  it('is a real guard: the RPC client itself does reach them', () => {
    // Proves the walker actually detects a peer edge, so a green result above means something.
    const graph = reachable('src/evm/evm-rpc-block-client.ts')

    expect(OPTIONAL_PEERS.some((peer) => graph.has(peer))).toBe(true)
  })
})
