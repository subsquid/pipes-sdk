import { isDeepStrictEqual } from 'node:util'

import type { BlockCursor } from '~/core/index.js'
import type { MockPortalRequest } from '~/testing/index.js'

import type { Observation } from '../scenarios/erc20.js'
import type { Cursor, ReferenceModel } from './reference-model.js'

type Served = { index: number; blocks: Cursor[]; finalized: Cursor | undefined }

function requireSame(label: string, actual: unknown, expected: unknown): void {
  if (!isDeepStrictEqual(actual, expected)) {
    throw new Error(`${label} is ${JSON.stringify(actual)}, the portal sent ${JSON.stringify(expected)}`)
  }
}

/**
 * Feeds the oracle a run's batches and forks in the order the pipe met them, with every block,
 * floor and canonical chain taken from the portal's request log, never from the pipe: a batch's
 * blocks from the answer that served them, its floor from the head reported with that answer, a
 * fork's canonical chain from the 409.
 *
 * It needs every block delivered, so a batch's blocks all appear in the answer: a scenario whose
 * every block carries rows ensures that.
 */
export class Replay {
  readonly #model: ReferenceModel
  readonly #requests: readonly MockPortalRequest[]
  /** The request that served the last batch, or the first of the run. */
  #at: number

  /** `from` is the first request of the run in the portal's log. */
  constructor(model: ReferenceModel, requests: readonly MockPortalRequest[], from: number) {
    this.#model = model
    this.#requests = requests
    this.#at = from
  }

  batch({ current, finalized }: Observation): void {
    const served = this.#serving(current.number)
    const after = this.#model.readState().current?.number ?? -1
    const blocks = served.blocks.filter((block) => block.number > after && block.number <= current.number)

    requireSame('the batch cursor', current, blocks.at(-1))

    // INV-2: the pipe holds only a floor the portal has reported, up to the answer that served the batch.
    const reported = this.#requests.slice(0, served.index + 1).map((request) => request.head?.finalized)
    const invented = finalized !== undefined && !reported.some((floor) => isDeepStrictEqual(floor, finalized))
    if (invented) {
      throw new Error(
        `INV-2: the pipe holds the finalized head ${JSON.stringify(finalized)}, which the portal never reported`,
      )
    }

    this.#model.batch(blocks, served.finalized === undefined ? {} : { finalized: served.finalized })
  }

  fork(canonical: BlockCursor[]): void {
    const previousBlocks = this.#forkAnswer()

    requireSame('the canonical chain', canonical, previousBlocks)
    this.#model.fork(previousBlocks)
  }

  /** The answer, from the last one on, that delivered `number`. A fork on the way was never resolved. */
  #serving(number: number): Served {
    for (; this.#at < this.#requests.length; this.#at++) {
      const { answer, head } = this.#requests[this.#at]
      if (answer === undefined) {
        throw new Error(`request ${this.#at} is still unanswered, yet the pipe delivered block ${number}`)
      }

      if (answer.kind === 'fork') {
        throw new Error(`the pipe delivered block ${number} past the fork answered to request ${this.#at}`)
      }

      const blocks = answer.kind === 'blocks' ? answer.blocks.map(({ header }) => cursorOf(header)) : []
      if (blocks.some((block) => block.number === number)) {
        return { index: this.#at, blocks, finalized: head?.finalized }
      }
    }

    throw new Error(`no answer delivered block ${number}`)
  }

  /**
   * The canonical chain of the next fork answer. An answer on the way may only hold blocks the
   * pipe has already delivered: one it skipped would be data dropped before the fork.
   */
  #forkAnswer(): Cursor[] {
    const cursor = this.#model.readState().current?.number ?? -1

    for (; this.#at < this.#requests.length; this.#at++) {
      const { answer } = this.#requests[this.#at]
      if (answer?.kind === 'fork') {
        this.#at += 1

        return answer.previousBlocks
      }

      const undelivered = answer?.kind === 'blocks' && answer.blocks.some(({ header }) => header.number > cursor)
      if (undelivered) {
        throw new Error(`request ${this.#at} delivered blocks above ${cursor} that the pipe dropped before the fork`)
      }
    }

    throw new Error('the pipe resolved a fork the portal never answered')
  }
}

function cursorOf(header: { number: number; hash: string; timestamp?: number }): Cursor {
  return { number: header.number, hash: header.hash, timestamp: header.timestamp }
}
