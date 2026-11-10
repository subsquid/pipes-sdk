import { describe, expect, it } from 'vitest'

import type { MockPortalRequest } from '~/testing/index.js'

import { cursorAt } from './backfill.js'
import { resumeProblem } from './fallback.js'

/** A data answer of the blocks `from`..`to`, as the previous source's log holds it. */
function served(from: number, to: number): MockPortalRequest {
  const blocks = []
  for (let number = from; number <= to; number++) {
    blocks.push({ header: { number, hash: cursorAt(number).hash } })
  }

  return { endpoint: 'stream', query: { fromBlock: from }, status: 200, answer: { kind: 'blocks', blocks } }
}

const PREVIOUS = [served(0, 6), served(7, 13)]

describe('resumeProblem', () => {
  it('takes a read that starts after a served batch, anchored on its last block', () => {
    expect(resumeProblem({ fromBlock: 7, parentBlockHash: cursorAt(6).hash }, PREVIOUS)).toBeUndefined()
    expect(resumeProblem({ fromBlock: 14, parentBlockHash: cursorAt(13).hash }, PREVIOUS)).toBeUndefined()
  })

  it('names a missing read', () => {
    expect(resumeProblem(undefined, PREVIOUS)).toBe('no resumed read')
  })

  it('refuses a read that starts inside a batch', () => {
    expect(resumeProblem({ fromBlock: 10, parentBlockHash: cursorAt(9).hash }, PREVIOUS)).toMatch('resumed after 9')
  })

  it('refuses a read anchored on another block', () => {
    const problem = resumeProblem({ fromBlock: 14, parentBlockHash: cursorAt(12).hash }, PREVIOUS)

    expect(problem).toMatch('anchored on')
  })

  it('refuses a read with no anchor', () => {
    expect(resumeProblem({ fromBlock: 14 }, PREVIOUS)).toMatch('anchored on undefined')
  })
})
