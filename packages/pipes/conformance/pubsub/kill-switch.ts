import { PubSub } from '@google-cloud/pubsub'
import Database from 'better-sqlite3'

import type { BlockCursor } from '~/core/index.js'

import type { Observation } from '../scenarios/erc20.js'
import type { BusAddress } from './pipe.js'

/**
 * A step of the class-C commit protocol (CN-17), named by what the producer does: a statement on its
 * state file, or a publish call to the bus (FM-30, FM-31).
 */
export type StepKillPoint = {
  /** The matrix row, written to the journal when the kill lands. */
  name: string
  /** Matches the step's label (`labelOf`): `insert outbox`, `commit`, `delete outbox`, `publish`. */
  step: string
  /**
   * Counts only a step taken after the pipe delivered the batch that ends at this block, and before
   * its next batch or fork: it names the batch by its block, not by its place.
   */
  inBatch?: number
  /** Counts only a step taken after the pipe began a fork, and before its next batch. */
  inFork?: boolean
  /**
   * Counts only a statement whose transaction has already run one this matches: it names the
   * transaction by what it does (`insert cursor` for a batch's, `delete outbox` for a confirmation).
   */
  inTransaction?: string
  /** The matching step the kill lands on, counting from 1 over the run. */
  at: number
  /**
   * Before the step: the statement does not run, the bus does not get the call. After it: the
   * statement has run, or the bus has answered the call without an error and the producer has not
   * seen the answer yet.
   */
  when: 'before' | 'after'
}

/** Where the kill landed: on which step, and what the run had done since its last batch or fork. */
export type KillEvidence = {
  /** The step the kill landed on, as `labelOf` names it. */
  step: string
  /**
   * The steps since the pipe delivered its last batch or began a fork, or since it started: with
   * the step itself unless the kill came first.
   */
  sent: string[]
}

/**
 * One line of a killed class-C run's journal: a batch the pipe delivered, the canonical chain of a
 * fork it is about to roll back to, or the kill itself.
 */
export type StepJournalEntry = { batch: Observation } | { fork: BlockCursor[] } | ({ killed: string } & KillEvidence)

/** What the parent hands the child, as JSON in its one argument. */
export type StepKillRunOptions = {
  range: { from: number; to: number }
  url: string
  bus: BusAddress
  statePath: string
  coldStart: boolean
  journal: string
  point: StepKillPoint
}

/** The label of every publish call. */
export const PUBLISH = 'publish'

const ROW_WRITE = /^(insert(?:\s+or\s+\w+)?\s+into|delete\s+from|update)\s+"?(\w+)"?/i

/** A short name for a statement: its first keyword, and a row write with its table (`insert outbox`). */
export function labelOf(sql: string): string {
  const text = sql.trim()

  const write = ROW_WRITE.exec(text)
  if (write) {
    const verb = write[1].split(/\s+/)[0].toLowerCase()

    return `${verb} ${write[2]}`
  }

  return text.split(/\s+/)[0].toLowerCase()
}

/** Decides, step by step, whether the kill lands. It never kills by itself. */
export class StepKillSwitch {
  readonly #point: StepKillPoint
  readonly #pattern: RegExp
  readonly #transactionPattern: RegExp | undefined
  #matched = 0
  /** What the pipe did last: delivered the batch that ends at a block, or began a fork. */
  #last: number | 'fork' | undefined
  #sent: string[] = []
  /** The labels the open transaction has run, from its `begin`; none outside one. */
  #transaction: string[] | undefined
  #evidence: KillEvidence | undefined

  constructor(point: StepKillPoint) {
    this.#point = point
    this.#pattern = new RegExp(point.step)
    this.#transactionPattern = point.inTransaction === undefined ? undefined : new RegExp(point.inTransaction)
  }

  /** Where the kill landed, once it has. */
  get evidence(): KillEvidence | undefined {
    return this.#evidence
  }

  /** The pipe delivered the batch that ends at `block`. */
  batch(block: number): void {
    this.#last = block
    this.#sent = []
  }

  /** The pipe began a fork. */
  fork(): void {
    this.#last = 'fork'
    this.#sent = []
  }

  /** For a step about to be taken: whether the kill lands, and when. */
  step(label: string): StepKillPoint['when'] | undefined {
    const before = this.#sent
    this.#sent = [...before, label]

    const transaction = this.#transaction ?? []
    if (label === 'begin') {
      this.#transaction = [label]
    } else if (label === 'commit' || label === 'rollback') {
      this.#transaction = undefined
    } else if (this.#transaction !== undefined) {
      this.#transaction = [...transaction, label]
    }

    if (!this.#pattern.test(label)) {
      return undefined
    }

    const inBatch = this.#point.inBatch === undefined || this.#point.inBatch === this.#last
    const inFork = this.#point.inFork !== true || this.#last === 'fork'
    const pattern = this.#transactionPattern
    const inTransaction = pattern === undefined || transaction.some((sent) => pattern.test(sent))
    if (!inBatch || !inFork || !inTransaction) {
      return undefined
    }

    this.#matched += 1
    if (this.#matched !== this.#point.at) {
      return undefined
    }

    const when = this.#point.when
    this.#evidence = { step: label, sent: when === 'before' ? before : this.#sent }

    return when
  }
}

type Run = (this: { source: string }, ...params: unknown[]) => unknown
type Callback = (error: Error | null | undefined, response?: unknown) => void
type Request = (this: object, config: { method: string }, callback: Callback) => unknown

/**
 * Wraps the statement runs of every `better-sqlite3` database and the calls of every Pub/Sub client
 * in this process so `kill` runs at `point`, and returns the switch, to be told of every batch and
 * fork, and what unwraps it. Only the statements that write go through `run`, reads do not. A
 * killed run never unwraps: its process dies at the point.
 */
export function armStepKillSwitch({ point, kill }: { point: StepKillPoint; kill: (evidence: KillEvidence) => never }): {
  killSwitch: StepKillSwitch
  disarm: () => void
} {
  const killSwitch = new StepKillSwitch(point)
  const landed = () => kill(killSwitch.evidence as KillEvidence)

  const scratch = new Database(':memory:')
  const statement = Object.getPrototypeOf(scratch.prepare('SELECT 1')) as { run: Run }
  scratch.close()
  const realRun = statement.run

  statement.run = function (this: { source: string }, ...params) {
    const when = killSwitch.step(labelOf(this.source))
    if (when === 'before') {
      landed()
    }

    const result = realRun.apply(this, params)
    if (when === 'after') {
      landed()
    }

    return result
  }

  const client = PubSub.prototype as unknown as { request: Request }
  const realRequest = client.request

  client.request = function (this: object, config, callback) {
    if (config.method !== PUBLISH) {
      return realRequest.call(this, config, callback)
    }

    const when = killSwitch.step(PUBLISH)
    if (when === 'before') {
      landed()
    }

    if (when !== 'after') {
      return realRequest.call(this, config, callback)
    }

    return realRequest.call(this, config, (error, response) => {
      if (error) {
        callback(error, response)

        return
      }

      // A kill that throws instead, as a test's does, fails the call: the producer never reads the answer.
      try {
        landed()
      } catch (thrown) {
        callback(thrown as Error)
      }
    })
  }

  const disarm = () => {
    statement.run = realRun
    client.request = realRequest
  }

  return { killSwitch, disarm }
}
