import { describe, expect, it } from 'vitest'

import type { MockPortalRequest } from '~/testing/index.js'

import { ReferenceModel } from './reference-model.js'
import { Replay } from './replay.js'

const RANGES = [{ from: 0, to: 9 }]

function header(number: number) {
  return { number, hash: `0x${number}`, timestamp: number }
}

function cursor(number: number) {
  return { current: header(number), finalized: undefined }
}

function request(answer: MockPortalRequest['answer']): MockPortalRequest {
  return { endpoint: 'stream', query: { fromBlock: 0 }, status: 200, answer, head: {} }
}

function replayOf(requests: MockPortalRequest[]): Replay {
  const model = new ReferenceModel({ durability: 'T', mode: 'full', ranges: RANGES, transform: () => [] })

  return new Replay(model, requests, 0)
}

type Answer = NonNullable<MockPortalRequest['answer']>

const BROKEN: Answer[] = [
  { kind: 'stalled', blocks: [{ header: header(0) }, { header: header(1) }] },
  { kind: 'cut', blocks: [{ header: header(0) }, { header: header(1) }], cutBlock: { header: header(2) } },
  { kind: 'line', blocks: [{ header: header(0) }, { header: header(1) }], line: '{"header":' },
]

describe('Replay', () => {
  for (const answer of BROKEN) {
    it(`takes a batch from the whole blocks a ${answer.kind} answer sent`, () => {
      const replay = replayOf([request(answer)])

      expect(() => replay.batch(cursor(1))).not.toThrow()
    })
  }

  it('refuses the block a cut answer sent only half of', () => {
    const replay = replayOf([request(BROKEN[1])])

    expect(() => replay.batch(cursor(2))).toThrow('no answer delivered block 2')
  })

  it('refuses a block from a fault answer', () => {
    const replay = replayOf([request({ kind: 'fault', status: 503 })])

    expect(() => replay.batch(cursor(0))).toThrow('no answer delivered block 0')
  })
})
