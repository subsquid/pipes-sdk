import pg from 'pg'

import type { BlockCursor } from '~/core/index.js'

import type { Observation } from '../scenarios/erc20.js'

/** A step of the class-T commit protocol, named by the SQL statement the kill lands on (FM-30, FM-31). */
export type StatementKillPoint = {
  /** The matrix row, written to the journal when the kill lands. */
  name: string
  /** Matches the statement's SQL, ignoring case. */
  statement: string
  /**
   * Counts only a statement whose transaction has already sent one that this matches, ignoring
   * case: it names the transaction by what it does (`snapshot_block_number = 22`), not by its place.
   */
  inTransaction?: string
  /** The matching statement the kill lands on, counting from 1 over every connection of the run. */
  at: number
  /**
   * Before the statement goes out; once it is on the wire and unanswered; or once the server has
   * answered it without an error.
   */
  when: 'before' | 'sent' | 'after'
}

/** Where the kill landed: on which statement, and in what its transaction had sent. */
export type KillEvidence = {
  /** The statement the kill landed on, as `labelOf` names it. */
  statement: string
  /**
   * What the statement's transaction sent up to the kill: from its `begin`, with the statement
   * itself unless the kill came first. Empty for a statement outside a transaction.
   */
  transaction: string[]
}

/**
 * One line of a killed class-T run's journal: a batch the pipe delivered, the canonical chain of a
 * fork it is about to roll back to, or the kill itself.
 */
export type StatementJournalEntry =
  | { batch: Observation }
  | { fork: BlockCursor[] }
  | ({ killed: string } & KillEvidence)

/** What the parent hands the child, as JSON in its one argument. */
export type StatementKillRunOptions = {
  range: { from: number; to: number }
  url: string
  dsn: string
  schema: string
  /** The child's `application_name`, so the parent can wait for the server to drop its connections. */
  application: string
  journal: string
  point: StatementKillPoint
}

const LEADING_COMMENTS = /^(\s*--[^\n]*\n)*/
const ROW_WRITE = /^(insert\s+into|delete\s+from|update)\s+(?:"[^"]+"\.)?"?(\w+)"?/i

/**
 * A short name for a statement: its first keyword, a row write with its table (`insert sync`), and
 * any advisory lock as `lock`.
 */
export function labelOf(text: string): string {
  const sql = text.replace(LEADING_COMMENTS, '').trim()

  if (/advisory_xact_lock/i.test(sql)) {
    return 'lock'
  }

  const write = ROW_WRITE.exec(sql)
  if (write) {
    const verb = write[1].split(/\s+/)[0].toLowerCase()

    return `${verb} ${write[2]}`
  }

  return sql.split(/\s+/)[0].toLowerCase()
}

/** What a connection's open transaction has sent: each statement's label and its SQL. */
type OpenTransaction = { labels: string[]; texts: string[] }

/** Decides, statement by statement, whether the kill lands. It never kills by itself. */
export class StatementKillSwitch {
  readonly #point: StatementKillPoint
  readonly #pattern: RegExp
  readonly #transactionPattern: RegExp | undefined
  #matched = 0
  readonly #open = new Map<object, OpenTransaction>()
  #evidence: KillEvidence | undefined

  constructor(point: StatementKillPoint) {
    this.#point = point
    this.#pattern = new RegExp(point.statement, 'i')
    this.#transactionPattern = point.inTransaction === undefined ? undefined : new RegExp(point.inTransaction, 'i')
  }

  /** Where the kill landed, once it has. */
  get evidence(): KillEvidence | undefined {
    return this.#evidence
  }

  /**
   * For a statement `connection` is about to send: whether the kill lands, and when. A `begin`
   * inside an open transaction opens no new one, as in Postgres.
   */
  statement(connection: object, text: string): StatementKillPoint['when'] | undefined {
    const label = labelOf(text)
    const open = this.#open.get(connection)
    const inTransaction = open !== undefined || label === 'begin'
    const before = open ?? { labels: [], texts: [] }
    const ends = label === 'commit' || label === 'rollback'

    if (!inTransaction || ends) {
      this.#open.delete(connection)
    } else {
      this.#open.set(connection, { labels: [...before.labels, label], texts: [...before.texts, text] })
    }

    if (!this.#pattern.test(text)) {
      return undefined
    }

    const pattern = this.#transactionPattern
    const transactionMatches = pattern === undefined || before.texts.some((sent) => pattern.test(sent))
    if (!transactionMatches) {
      return undefined
    }

    this.#matched += 1
    if (this.#matched !== this.#point.at) {
      return undefined
    }

    const when = this.#point.when
    const sent = [...before.labels, label]
    const transaction = !inTransaction ? [] : when === 'before' ? before.labels : sent
    this.#evidence = { statement: label, transaction }

    return when
  }
}

type Query = (this: object, ...args: unknown[]) => unknown

/**
 * Wraps the query method of every `pg` client in this process so `kill` runs at `point`, and
 * returns what unwraps it. A killed run never unwraps: its process dies at the point.
 */
export function armStatementKillSwitch({
  point,
  kill,
}: {
  point: StatementKillPoint
  kill: (evidence: KillEvidence) => never
}): () => void {
  const killSwitch = new StatementKillSwitch(point)
  const prototype = pg.Client.prototype as unknown as { query: Query }
  const real = prototype.query

  prototype.query = function (this: object, ...args: unknown[]) {
    const config = args[0]
    const text = typeof config === 'string' ? config : (config as { text?: unknown } | null)?.text
    const when = typeof text === 'string' ? killSwitch.statement(this, text) : undefined
    if (when === undefined) {
      return real.apply(this, args)
    }

    const landed = () => kill(killSwitch.evidence as KillEvidence)
    if (when === 'before') {
      landed()
    }

    // `pg` returns a promise only when no callback comes last, and only a promise can be waited on.
    const callbackForm = typeof args.at(-1) === 'function'
    if (when === 'after' && callbackForm) {
      throw new Error(`the kill point '${point.name}' waits for a statement sent with a callback`)
    }

    // An idle client writes the statement to its socket before `query` returns.
    const result = real.apply(this, args)
    if (when === 'sent') {
      landed()
    }

    if (when === 'after') {
      return (result as Promise<unknown>).then(landed)
    }

    return result
  }

  return () => {
    prototype.query = real
  }
}
