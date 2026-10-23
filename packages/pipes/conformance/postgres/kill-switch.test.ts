import type { Socket } from 'node:net'

import pg from 'pg'
import { describe, expect } from 'vitest'

import { type Database, disconnected, it } from './harness.js'
import {
  type KillEvidence,
  type StatementKillPoint,
  StatementKillSwitch,
  armStatementKillSwitch,
  labelOf,
} from './kill-switch.js'

const COMMIT = String.raw`^commit$`

/** The answers of `killSwitch.statement` to `statements`, in order. */
function answers(killSwitch: StatementKillSwitch, statements: [object, string][]) {
  return statements.map(([connection, text]) => killSwitch.statement(connection, text))
}

describe('labelOf', () => {
  it.for([
    ['begin isolation level serializable', 'begin'],
    ['COMMIT', 'commit'],
    ['\n   INSERT INTO "ct_1"."sync" (id) VALUES ($1)', 'insert sync'],
    ['insert into "transfers" ("blockNumber", "value") values ($1, $2)', 'insert transfers'],
    ['DELETE FROM "ct_1"."sync" WHERE "id" = $1', 'delete sync'],
    ['update sync SET id = $1', 'update sync'],
    ['SELECT pg_try_advisory_xact_lock(hashtext($1)::bigint) AS got_lock;', 'lock'],
    ["SELECT pg_advisory_xact_lock(hashtext('sqd_drizzle_triggers')::bigint);", 'lock'],
    ['\n-- ===== SNAPSHOT SETUP =====\nCREATE TABLE IF NOT EXISTS "t__snapshots" ()', 'create'],
    ['\n   SET LOCAL sqd.snapshot_enabled = false;', 'set'],
    ['SELECT * FROM "public"."sync"', 'select'],
  ])('names %j as %j', ([text, label]) => {
    expect(labelOf(text)).toBe(label)
  })
})

describe('StatementKillSwitch', () => {
  it('lands on the n-th matching statement over every connection, ignoring case, and on no other', () => {
    const point: StatementKillPoint = { name: 'second commit', statement: COMMIT, at: 2, when: 'before' }
    const [a, b] = [{}, {}]

    const landed = answers(new StatementKillSwitch(point), [
      [a, 'begin'],
      [a, 'commit'],
      [b, 'begin'],
      [b, 'committed'],
      [b, 'COMMIT'],
      [a, 'commit'],
    ])

    expect(landed).toEqual([undefined, undefined, undefined, undefined, 'before', undefined])
  })

  it.for(['before', 'sent', 'after'] as const)('lands %s the statement when the point says so', (when) => {
    const point: StatementKillPoint = { name: 'commit', statement: COMMIT, at: 1, when }

    const landed = new StatementKillSwitch(point).statement({}, 'commit')

    expect(landed).toBe(when)
  })

  it('shows what the open transaction sent: without the statement when the kill comes first', () => {
    const connection = {}
    const statements: [object, string][] = [
      [connection, 'begin'],
      [connection, 'insert into "t" values (1)'],
      [connection, 'commit'],
    ]
    const kill = (when: StatementKillPoint['when']) => {
      const killSwitch = new StatementKillSwitch({ name: 'commit', statement: COMMIT, at: 1, when })
      answers(killSwitch, statements)

      return killSwitch.evidence
    }

    expect(kill('before')).toEqual({ statement: 'commit', transaction: ['begin', 'insert t'] })
    expect(kill('sent')).toEqual({ statement: 'commit', transaction: ['begin', 'insert t', 'commit'] })
    expect(kill('after')).toEqual({ statement: 'commit', transaction: ['begin', 'insert t', 'commit'] })
  })

  it('counts only statements of a transaction that has sent what the point names', () => {
    const point: StatementKillPoint = {
      name: 'commit of the batch at 13',
      statement: COMMIT,
      inTransaction: String.raw`block = 13\b`,
      at: 1,
      when: 'before',
    }
    const connection = {}

    const landed = answers(new StatementKillSwitch(point), [
      [connection, 'begin'],
      [connection, 'set local block = 6'],
      [connection, 'commit'],
      [connection, 'begin'],
      [connection, 'set local block = 130'],
      [connection, 'commit'],
      [connection, 'begin'],
      [connection, 'set local block = 13'],
      [connection, 'commit'],
    ])

    expect(landed).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      'before',
    ])
  })

  it('opens no new transaction at a begin inside one, as Postgres does', () => {
    const point: StatementKillPoint = { name: 'insert', statement: String.raw`^insert`, at: 1, when: 'before' }
    const killSwitch = new StatementKillSwitch(point)
    const connection = {}

    answers(killSwitch, [
      [connection, 'begin'],
      [connection, 'set local x = 1'],
      [connection, 'begin'],
      [connection, 'insert into "t" values (1)'],
    ])

    expect(killSwitch.evidence).toEqual({ statement: 'insert t', transaction: ['begin', 'set', 'begin'] })
  })

  it('keeps one transaction per connection', () => {
    const point: StatementKillPoint = { name: 'insert', statement: String.raw`^insert`, at: 1, when: 'before' }
    const killSwitch = new StatementKillSwitch(point)
    const [a, b] = [{}, {}]

    answers(killSwitch, [
      [a, 'begin'],
      [b, 'begin'],
      [b, 'set local x = 1'],
      [a, 'insert into "t" values (1)'],
    ])

    expect(killSwitch.evidence).toEqual({ statement: 'insert t', transaction: ['begin'] })
  })

  it('shows no transaction for a statement outside one: before any begin, or after a commit', () => {
    const point = (at: number): StatementKillPoint => ({ name: 'select', statement: '^select', at, when: 'sent' })
    const connection = {}
    const statements: [object, string][] = [
      [connection, 'select 1'],
      [connection, 'begin'],
      [connection, 'commit'],
      [connection, 'select 2'],
    ]

    const evidence = [1, 2].map((at) => {
      const killSwitch = new StatementKillSwitch(point(at))
      answers(killSwitch, statements)

      return killSwitch.evidence
    })

    expect(evidence).toEqual([
      { statement: 'select', transaction: [] },
      { statement: 'select', transaction: [] },
    ])
  })

  it('starts a new transaction at each begin, and ends one at a rollback', () => {
    const point: StatementKillPoint = { name: 'insert', statement: String.raw`^insert`, at: 1, when: 'before' }
    const killSwitch = new StatementKillSwitch(point)
    const connection = {}

    answers(killSwitch, [
      [connection, 'begin'],
      [connection, 'set local x = 1'],
      [connection, 'rollback'],
      [connection, 'begin'],
      [connection, 'insert into "t" values (1)'],
    ])

    expect(killSwitch.evidence).toEqual({ statement: 'insert t', transaction: ['begin'] })
  })
})

/** What a test's `kill` throws instead of dying. */
class Killed extends Error {
  constructor(readonly evidence: KillEvidence) {
    super('killed')
  }
}

describe('armStatementKillSwitch', () => {
  const insert = (database: Database) => `INSERT INTO "${database.schema}"."transfers" VALUES (1, 'a')`

  async function rowsIn({ admin, schema }: Database): Promise<number> {
    const { rows } = await admin.query<{ rows: number }>(`SELECT count(*)::int AS rows FROM "${schema}"."transfers"`)

    return rows[0].rows
  }

  /** Drops the connection at once, as the death of its process would: nothing more goes out. */
  function die(client: pg.Client) {
    const { stream } = (client as unknown as { connection: { stream: Socket } }).connection
    stream.destroy()
  }

  /**
   * Sends one insert with the switch armed at `when`, through `pg`'s callback form or its promise
   * form, and drops the connection as soon as the kill comes. Reports how the kill came, and how
   * many rows the table holds once the server has dropped the connection.
   */
  async function killAt(database: Database, when: StatementKillPoint['when'], form: 'callback' | 'promise') {
    const application = `ct-arm-${when}-${form}`
    const client = new pg.Client({ connectionString: database.dsn, application_name: application })
    client.on('error', () => {})
    await client.connect()
    const disarm = armStatementKillSwitch({
      point: { name: 'insert', statement: '^insert', at: 1, when },
      kill: (evidence) => {
        throw new Killed(evidence)
      },
    })

    let thrown: unknown
    let rejected: unknown
    let rowsAtRejection: number | undefined
    try {
      if (form === 'callback') {
        client.query(insert(database), [], () => {})
      } else {
        await client.query(insert(database)).catch(async (error: unknown) => {
          rejected = error
          rowsAtRejection = await rowsIn(database)
        })
      }
    } catch (error) {
      thrown = error
      // In the same tick as the kill, so a write the client deferred never goes out.
      die(client)
    } finally {
      disarm()
    }

    die(client)
    await disconnected(database.admin, application)

    return { thrown, rejected, rowsAtRejection, rows: await rowsIn(database) }
  }

  it('before: kills before the statement goes out', async ({ database }) => {
    const { thrown, rows } = await killAt(database, 'before', 'callback')

    expect((thrown as Killed).evidence).toEqual({ statement: 'insert transfers', transaction: [] })
    expect(rows).toBe(0)
  })

  it('sent: kills once the statement is on the wire, so the server runs it with no one to answer', async ({
    database,
  }) => {
    const { thrown, rows } = await killAt(database, 'sent', 'callback')

    expect((thrown as Killed).evidence).toEqual({ statement: 'insert transfers', transaction: [] })
    expect(rows).toBe(1)
  })

  it('after: kills once the server has answered', async ({ database }) => {
    const { thrown, rejected, rowsAtRejection } = await killAt(database, 'after', 'promise')

    expect(thrown).toBeUndefined()
    expect((rejected as Killed).evidence).toEqual({ statement: 'insert transfers', transaction: [] })
    expect(rowsAtRejection).toBe(1)
  })

  it('after: refuses a statement sent with a callback, which it cannot wait for', async ({ database }) => {
    const { thrown, rows } = await killAt(database, 'after', 'callback')

    expect(thrown).toEqual(new Error("the kill point 'insert' waits for a statement sent with a callback"))
    expect(rows).toBe(0)
  })

  it('lets every statement through once disarmed', async ({ database }) => {
    const disarm = armStatementKillSwitch({
      point: { name: 'insert', statement: '^insert', at: 1, when: 'before' },
      kill: (evidence) => {
        throw new Killed(evidence)
      },
    })
    disarm()

    await database.admin.query(insert(database))

    expect(await rowsIn(database)).toBe(1)
  })
})
