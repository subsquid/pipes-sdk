import { randomUUID } from 'node:crypto'
import type { ClientRequest } from 'node:http'

import { NodeBaseConnection } from '@clickhouse/client/dist/connection/node_base_connection.js'
import { NodeHttpConnection } from '@clickhouse/client/dist/connection/node_http_connection.js'

import type { BlockCursor } from '~/core/index.js'

import type { Observation } from '../scenarios/erc20.js'
import type { ClickhouseServer, Repair } from './pipe.js'

/** A step of the class-A commit protocol, named by the request the kill lands on (FM-30, FM-31). */
export type RequestKillPoint = {
  /** The matrix row, written to the journal when the kill lands. */
  name: string
  /** Matches the request's label (`labelOf`): `insert transfers`, `cancel approvals`, `insert sync`. */
  request: string
  /**
   * Counts only a request sent after the pipe delivered the batch that ends at this block, and
   * before its next batch or fork: it names the batch by its block, not by its place.
   */
  inBatch?: number
  /** Counts only a request sent after the pipe began a fork, and before its next batch. */
  inFork?: boolean
  /** The matching request the kill lands on, counting from 1 over the run. */
  at: number
  /**
   * Before the request goes out; once it is on the wire and unanswered; or once the client call
   * has ended without an error, the whole answer read. A read refuses `after`: its caller reads
   * the rows once the call has returned.
   */
  when: 'before' | 'sent' | 'after'
}

/** Where the kill landed: on which request, and what the run had sent since its last batch or fork. */
export type KillEvidence = {
  /** The request the kill landed on, as `labelOf` names it. */
  request: string
  /**
   * What the run sent since the pipe delivered its last batch or began a fork, or since it
   * started: with the request itself unless the kill came first.
   */
  sent: string[]
}

/**
 * One line of a killed class-A run's journal: a batch the pipe delivered, the canonical chain of a
 * fork it is about to roll back to, or the kill itself.
 */
export type RequestJournalEntry = { batch: Observation } | { fork: BlockCursor[] } | ({ killed: string } & KillEvidence)

/** What the parent hands the child, as JSON in its one argument. */
export type RequestKillRunOptions = {
  range: { from: number; to: number }
  url: string
  server: ClickhouseServer
  database: string
  /** The child's application name, so the parent can wait until the server runs none of its queries. */
  application: string
  repair: Repair
  journal: string
  point: RequestKillPoint
}

/** The table a statement names, without its database and quotes. */
const TABLE = String.raw`(?:["\x60]?\w+["\x60]?\.)?["\x60]?(\w+)`
const INSERT = new RegExp(String.raw`^insert\s+into\s+${TABLE}`, 'i')
const FROM = new RegExp(String.raw`\bfrom\s+${TABLE}`, 'i')
const DDL = new RegExp(String.raw`^(create|alter|drop)\s+table\s+(?:if\s+(?:not\s+)?exists\s+)?${TABLE}`, 'i')

/**
 * A short name for a request: an insert with its table (`insert sync`); a rollback's cancel rows,
 * which go in with insert deduplication off (IB-20), as `cancel`; a rollback's netting read as
 * `net`; any other read as `select` with its table; DDL with its table (`create sync`).
 */
export function labelOf(query: string, settings: Record<string, unknown> = {}): string {
  const sql = query.trim()

  const insert = INSERT.exec(sql)
  if (insert) {
    const cancel = Number(settings['insert_deduplicate']) === 0

    return `${cancel ? 'cancel' : 'insert'} ${insert[1]}`
  }

  const ddl = DDL.exec(sql)
  if (ddl) {
    return `${ddl[1].toLowerCase()} ${ddl[2]}`
  }

  const from = FROM.exec(sql)
  if (/^select\b/i.test(sql) && from) {
    const netting = /\bsum\(\s*sign\s*\)/i.test(sql)

    return `${netting ? 'net' : 'select'} ${from[1]}`
  }

  return sql.split(/\s+/)[0].toLowerCase()
}

/** Decides, request by request, whether the kill lands. It never kills by itself. */
export class RequestKillSwitch {
  readonly #point: RequestKillPoint
  readonly #pattern: RegExp
  #matched = 0
  /** What the pipe did last: delivered the batch that ends at a block, or began a fork. */
  #last: number | 'fork' | undefined
  #sent: string[] = []
  #evidence: KillEvidence | undefined

  constructor(point: RequestKillPoint) {
    this.#point = point
    this.#pattern = new RegExp(point.request)
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

  /** For a request about to go out: whether the kill lands, and when. */
  request(label: string): RequestKillPoint['when'] | undefined {
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
    this.#evidence = { request: label, sent: when === 'before' ? before : this.#sent }

    return when
  }
}

/** The connection methods every client call goes through: each sends one request. */
const CALLS = ['insert', 'command', 'exec', 'query'] as const

type CallParams = { query: string; clickhouse_settings?: Record<string, unknown>; query_id?: string }
type Call = (this: object, params: CallParams) => Promise<unknown>
type CreateClientRequest = (this: object, params: { url: URL }) => ClientRequest

/**
 * Wraps the calls of every ClickHouse HTTP connection in this process so `kill` runs at `point`,
 * and returns the switch, to be told of every batch and fork, and what unwraps it. A killed run
 * never unwraps: its process dies at the point.
 */
export function armRequestKillSwitch({
  point,
  kill,
}: {
  point: RequestKillPoint
  kill: (evidence: KillEvidence) => never
}): {
  killSwitch: RequestKillSwitch
  disarm: () => void
} {
  const killSwitch = new RequestKillSwitch(point)
  const base = NodeBaseConnection.prototype as unknown as Record<(typeof CALLS)[number], Call>
  const http = NodeHttpConnection.prototype as unknown as { createClientRequest: CreateClientRequest }
  const realCalls = CALLS.map((name) => [name, base[name]] as const)
  const realCreate = http.createClientRequest
  // The query ids of the requests to kill once their last byte is handed to the socket.
  const sending = new Set<string>()
  const landed = () => kill(killSwitch.evidence as KillEvidence)

  for (const [name, real] of realCalls) {
    base[name] = function (this: object, params) {
      const when = killSwitch.request(labelOf(params.query, params.clickhouse_settings))
      if (when === 'before') {
        landed()
      }

      if (when === 'after' && name === 'query') {
        throw new Error(`the kill point '${point.name}' waits for a read, whose rows its caller reads later`)
      }

      let sent = params
      if (when === 'sent') {
        sent = { ...params, query_id: params.query_id ?? randomUUID() }
        sending.add(sent.query_id as string)
      }

      // `request` resolves on the answer's headers; the call ends once it has read the rest.
      const result = real.call(this, sent)
      if (when === 'after') {
        return result.then(landed)
      }

      return result
    }
  }

  http.createClientRequest = function (this: object, params) {
    const request = realCreate.call(this, params)
    const queryId = params.url.searchParams.get('query_id')
    if (queryId !== null && sending.has(queryId)) {
      request.once('finish', () => {
        // A kill that throws instead, as a test's does, closes the socket as a dead process would.
        try {
          landed()
        } catch (error) {
          request.destroy(error as Error)
        }
      })
    }

    return request
  }

  const disarm = () => {
    for (const [name, real] of realCalls) {
      base[name] = real
    }

    http.createClientRequest = realCreate
  }

  return { killSwitch, disarm }
}
