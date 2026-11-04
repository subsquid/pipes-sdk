import { BigQuery } from '@google-cloud/bigquery'
import { managedwriter } from '@google-cloud/bigquery-storage'

import type { BlockCursor } from '~/core/index.js'

import type { Observation } from '../scenarios/erc20.js'

/**
 * A step of the class-W commit protocol, named by the client call the kill lands on (FM-30, FM-31):
 * a Storage Write API `appendRows` (`append sync` for a WAL row, `append transfers` for data), or a
 * query job (`delete transfers` for a fork or recovery DELETE, `create sync` for the sync table's
 * DDL). A read is none of these: the client's fast query path sends it to `jobs.query` without a
 * job, and a read changes nothing in the store, so the switch never sees one to kill on.
 */
export type CallKillPoint = {
  /** The matrix row, written to the journal when the kill lands. */
  name: string
  /** Matches the call's label (`labelOf`): `append sync`, `delete transfers`, `create sync`. */
  call: string
  /**
   * Counts only a call sent after the pipe delivered the batch that ends at this block, and before
   * its next batch or fork: it names the batch by its block, not by its place.
   */
  inBatch?: number
  /** Counts only a call sent after the pipe began a fork, and before its next batch. */
  inFork?: boolean
  /** The matching call the kill lands on, counting from 1 over the run. */
  at: number
  /**
   * Before the call goes out; once it is on the wire and unanswered; or once the client call has
   * ended without an error. A job the client created keeps running server-side after the last two,
   * so the parent waits for every journaled job before it reads the store.
   */
  when: 'before' | 'sent' | 'after'
}

/** Where the kill landed: on which call, and what the run had sent since its last batch or fork. */
export type KillEvidence = {
  /** The call the kill landed on, as `labelOf` names it. */
  call: string
  /**
   * What the run sent since the pipe delivered its last batch or began a fork, or since it started:
   * with the call itself unless the kill came first.
   */
  sent: string[]
}

/**
 * One line of a killed class-W run's journal: a batch the pipe delivered, the canonical chain of a
 * fork it is about to roll back to, or the kill itself.
 */
export type CallJournalEntry = { batch: Observation } | { fork: BlockCursor[] } | ({ killed: string } & KillEvidence)

/** What the parent hands the child, as JSON in its one argument. */
export type CallKillRunOptions = {
  range: { from: number; to: number }
  url: string
  projectId: string
  dataset: string
  journal: string
  /** Sidecar the child appends every DML job id to, synchronously, so the parent can wait for them. */
  jobs: string
  point: CallKillPoint
}

/** The table an SQL statement names, without its project and dataset qualifiers. */
const TABLE = String.raw`(?:\`?[\w-]+\.)*\`?([\w-]+)\`?`
const FROM = new RegExp(String.raw`\b(?:from|into)\s+${TABLE}`, 'i')

/**
 * A short name for a query job: its first keyword with the table it reads or writes (`delete
 * transfers`, `create sync`). The pipe sends one statement per job.
 */
export function labelOfSql(sql: string): string {
  const text = sql.replace(/^(\s*--[^\n]*\n)*/, '').trim()
  const verb = text.split(/\s+/)[0]?.toLowerCase() ?? ''
  const table = FROM.exec(text)?.[1]

  return table === undefined ? verb : `${verb} ${table}`
}

/** A short name for an append: the table the stream writes to, from the stream id BigQuery returns. */
export function labelOfStreamId(streamId: string | undefined): string {
  const parts = (streamId ?? '').split('/')
  const table = parts[parts.indexOf('tables') + 1]

  return table === undefined ? 'append unknown' : `append ${table}`
}

/** Decides, call by call, whether the kill lands. It never kills by itself. */
export class CallKillSwitch {
  readonly #point: CallKillPoint
  readonly #pattern: RegExp
  #matched = 0
  /** What the pipe did last: delivered the batch that ends at a block, or began a fork. */
  #last: number | 'fork' | undefined
  #sent: string[] = []
  #evidence: KillEvidence | undefined

  constructor(point: CallKillPoint) {
    this.#point = point
    this.#pattern = new RegExp(point.call)
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

  /** For a call about to go out: whether the kill lands, and when. */
  call(label: string): CallKillPoint['when'] | undefined {
    const before = this.#sent
    this.#sent = [...before, label]

    if (!this.#pattern.test(label)) {
      return undefined
    }

    const inBatch = this.#point.inBatch === undefined || this.#point.inBatch === this.#last
    const inFork = this.#point.inFork !== true || this.#last === 'fork'
    if (!inBatch || !inFork) {
      return undefined
    }

    this.#matched += 1
    if (this.#matched !== this.#point.at) {
      return undefined
    }

    const when = this.#point.when
    this.#evidence = { call: label, sent: when === 'before' ? before : this.#sent }

    return when
  }
}

type CreateQueryJob = (this: BigQuery, opts: { query: string }, callback?: unknown) => unknown
type AppendRows = (this: object, rows: unknown, offset?: unknown) => { getResult(): Promise<unknown> }

/**
 * Wraps every query-job creation and every Storage Write append in this process so `kill` runs at
 * `point`, and returns the switch, to be told of every batch and fork, and what unwraps it. A killed
 * run never unwraps: its process dies at the point.
 *
 * Every DML job whose creation resolved is appended to `jobs` before the kill can land on anything
 * after it: a job keeps running server-side once created, whether or not its creator dies, so the
 * parent waits for every id in the sidecar before it reads the store.
 */
export function armCallKillSwitch({
  point,
  kill,
  jobs,
}: {
  point: CallKillPoint
  kill: (evidence: KillEvidence) => never
  /** Appends a DML job id synchronously; the child hands it the sidecar's path. */
  jobs: (jobId: string) => void
}): {
  killSwitch: CallKillSwitch
  disarm: () => void
} {
  const killSwitch = new CallKillSwitch(point)
  const landed = () => kill(killSwitch.evidence as KillEvidence)

  const bigquery = BigQuery.prototype as unknown as { createQueryJob: CreateQueryJob }
  const realCreateQueryJob = bigquery.createQueryJob
  const writer = managedwriter.Writer.prototype as unknown as { appendRows: AppendRows }
  const realAppendRows = writer.appendRows

  bigquery.createQueryJob = function (this: BigQuery, opts, callback) {
    const label = labelOfSql(opts.query)
    const when = killSwitch.call(label)

    if (when === 'before') {
      landed()
    }

    const dml = /^(delete|insert|update|merge)\b/i.test(label)
    const result = realCreateQueryJob.call(this, opts, callback)

    // A job creation the client began cannot be taken back: die only after noting its id, so no
    // running DELETE outlives the parent's read unseen. The promise form is the pipe's own; a
    // callback form belongs to `query()`, whose reads settle before the store changes.
    if (when === 'after') {
      if (typeof callback === 'function') {
        throw new Error(`the kill point '${point.name}' waits for a job created with a callback`)
      }

      return (result as Promise<{ id?: string }[]>).then(([job]) => {
        if (dml && job?.id) jobs(job.id)

        return landed()
      })
    }

    if (typeof callback !== 'function') {
      ;(result as Promise<{ id?: string }[]>).then(([job]) => {
        if (dml && job?.id) jobs(job.id)
      })
    }

    if (when === 'sent') {
      landed()
    }

    return result
  }

  writer.appendRows = function (this: object, rows, offset) {
    // The writer keeps its connection, and the connection its stream id; both are the SDK's own
    // fields, read defensively so an SDK update fails a label, not a run.
    const connection = (this as { _streamConnection?: { getStreamId?: () => string } })._streamConnection
    const label = labelOfStreamId(connection?.getStreamId?.())
    const when = killSwitch.call(label)

    if (when === 'before') {
      landed()
    }

    const pending = realAppendRows.call(this, rows, offset)

    if (when === 'sent') {
      landed()
    }

    if (when === 'after') {
      return { getResult: () => pending.getResult().then(landed) }
    }

    return pending
  }

  const disarm = () => {
    bigquery.createQueryJob = realCreateQueryJob
    writer.appendRows = realAppendRows
  }

  return { killSwitch, disarm }
}
