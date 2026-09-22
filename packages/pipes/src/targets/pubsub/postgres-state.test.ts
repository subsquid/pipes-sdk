import { Pool } from 'pg'
import { afterAll, afterEach, describe, expect, it } from 'vitest'

import { BlockCursor } from '~/core/index.js'
import { testLogger } from '~/testing/index.js'

import { PUBSUB_ERROR_CODES } from './errors.js'
import { PostgresPubsubState } from './pubsub-state.js'
import { CommitInput, PendingOperation } from './pubsub-state-types.js'

const dsn = process.env['TEST_POSTGRES_DSN'] || 'postgresql://postgres:postgres@localhost:5432/postgres'
const adminPool = new Pool({ connectionString: dsn })

afterAll(async () => {
  await adminPool.end()
})

const opened: PostgresPubsubState[] = []
const prefixes: string[] = []

afterEach(async () => {
  for (const state of opened.splice(0)) {
    await state.close()
  }

  for (const prefix of prefixes.splice(0)) {
    const { rows } = await adminPool.query<{ tablename: string }>(
      `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE $1`,
      [`${prefix}%`],
    )
    for (const { tablename } of rows) {
      await adminPool.query(`DROP TABLE IF EXISTS "public"."${tablename}" CASCADE`)
    }
  }
})

/** A fresh table prefix per test so the shared database stays isolated between them. */
function prefix(): string {
  const value = `pgtest_${Math.random().toString(36).slice(2, 10)}_`
  prefixes.push(value)

  return value
}

async function openState(tablePrefix: string, id = 'test-pipe') {
  const state = new PostgresPubsubState({ connection: dsn, tablePrefix })
  const { coldStart } = await state.open({ cursorKey: id, logger: testLogger() })
  opened.push(state)

  return { state, coldStart }
}

async function commit(state: PostgresPubsubState, input: Omit<CommitInput, 'forkCapable'>) {
  await state.commit({ ...input, forkCapable: true })
}

function block(number: number, suffix = 'a'): BlockCursor {
  return { number, hash: `0x${number}${suffix}`, timestamp: 1_700_000_000 + number }
}

function operation(overrides: Partial<PendingOperation> & { id?: string } = {}): PendingOperation {
  return {
    route: 'transfers',
    topic: 'transfers',
    orderingKey: '',
    mode: 'event',
    op: 'upsert',
    id: 'row-1',
    idSource: 'draft',
    attributes: { token: '0x42' },
    payload: new TextEncoder().encode('{"a":1}'),
    blockNumber: 100,
    rollbackable: true,
    ...overrides,
  }
}

describe('PostgresPubsubState', () => {
  it('reports a cold start once and resumes warm afterwards', async () => {
    const tablePrefix = prefix()

    const first = await openState(tablePrefix)
    expect(first.coldStart).toBe(true)
    await first.state.close()

    const second = await openState(tablePrefix)
    expect(second.coldStart).toBe(false)
  })

  it('round-trips the cursor with an explicit finalized floor', async () => {
    const { state } = await openState(prefix())

    expect(await state.getCursor()).toBeUndefined()

    await commit(state, { operations: [], ledger: [], cursor: block(100), finalized: block(90) })

    expect(await state.getCursor()).toEqual({ latest: block(100), finalized: block(90) })
  })

  it('assigns one producer-wide sequence across topics and ordering keys', async () => {
    const { state } = await openState(prefix())

    await commit(state, {
      operations: [
        operation({ id: 'a', topic: 'one', orderingKey: 'one' }),
        operation({ id: 'b', topic: 'two', orderingKey: 'two' }),
        operation({ id: 'c', topic: 'one', orderingKey: 'one' }),
      ],
      ledger: [],
      cursor: block(100),
      finalized: null,
    })

    expect((await state.pending()).map((row) => [row.topic, row.seq])).toEqual([
      ['one', 1],
      ['two', 2],
      ['one', 3],
    ])
  })

  it('keeps unconfirmed outbox rows for the restart drain, and drops them once confirmed', async () => {
    const tablePrefix = prefix()

    const first = await openState(tablePrefix)
    await commit(first.state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null })
    await first.state.close()

    const second = await openState(tablePrefix)
    const pending = await second.state.pending()

    expect(pending).toHaveLength(1)
    expect(pending[0]).toMatchObject({ topic: 'transfers', op: 'upsert', id: 'row-1', seq: 1 })
    expect(typeof pending[0].rowId).toBe('number')
    expect(typeof pending[0].seq).toBe('number')
    expect(new TextDecoder().decode(pending[0].payload)).toBe('{"a":1}')

    await second.state.confirm([pending[0].rowId])
    expect(await second.state.pending()).toHaveLength(0)
  })

  it('round-trips BIGINT-range values (sequence, block number, timestamp) as JS numbers, not strings', async () => {
    const { state } = await openState(prefix())

    // Comfortably past Postgres's 32-bit INTEGER range (2_147_483_647), well within
    // Number.MAX_SAFE_INTEGER — where a driver that returned bigint columns as text would
    // silently break every numeric comparison the state relies on (ORDER BY, <=, >).
    const bigBlock = { number: 5_000_000_000, hash: '0xbig', timestamp: 5_000_000_000 }

    await commit(state, {
      operations: [operation({ blockNumber: bigBlock.number })],
      ledger: [bigBlock],
      cursor: bigBlock,
      finalized: null,
    })

    const pending = await state.pending()
    expect(pending[0].seq).toBe(1)
    expect(typeof pending[0].seq).toBe('number')

    const cursor = await state.getCursor()
    expect(cursor?.latest).toEqual(bigBlock)

    const safe = await state.fork([bigBlock])
    expect(safe).toEqual(bigBlock)
  })

  it('refuses a second producer on the same schema + table prefix (advisory lock)', async () => {
    const tablePrefix = prefix()
    await openState(tablePrefix)

    await expect(openState(tablePrefix)).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.STATE_LOCKED })
  })

  it('does not contend across two different table prefixes in the same schema', async () => {
    const a = await openState(prefix())
    const b = await openState(prefix())

    await commit(a.state, { operations: [operation()], ledger: [], cursor: block(1), finalized: null })
    await commit(b.state, { operations: [operation()], ledger: [], cursor: block(1), finalized: null })

    expect(await a.state.pending()).toHaveLength(1)
    expect(await b.state.pending()).toHaveLength(1)
  })

  it('refuses a state that belongs to another producer', async () => {
    const tablePrefix = prefix()

    const first = new PostgresPubsubState({ connection: dsn, tablePrefix })
    await first.open({ cursorKey: 'pipe-a', logger: testLogger() })
    await first.commit({
      operations: [operation()],
      ledger: [],
      cursor: block(100),
      finalized: null,
      forkCapable: true,
    })
    await first.close()

    const wrong = new PostgresPubsubState({ connection: dsn, tablePrefix })
    await expect(wrong.open({ cursorKey: 'pipe-b', logger: testLogger() })).rejects.toMatchObject({
      code: PUBSUB_ERROR_CODES.STATE_IDENTITY_MISMATCH,
    })
  })

  it('deletes an orphaned write-once event on fork, and rewinds the cursor', async () => {
    const { state } = await openState(prefix())

    await commit(state, {
      operations: [operation({ id: 'row-1', blockNumber: 1 })],
      ledger: [block(1)],
      cursor: block(1),
      finalized: null,
    })
    await commit(state, {
      operations: [operation({ id: 'row-2', blockNumber: 2 })],
      ledger: [block(1), block(2)],
      cursor: block(2),
      finalized: null,
    })
    await state.confirm((await state.pending()).map((row) => row.rowId))

    const safe = await state.fork([block(1)])
    expect(safe).toEqual(block(1))

    const compensations = await state.pending()
    expect(compensations).toHaveLength(1)
    expect(compensations[0]).toMatchObject({ id: 'row-2', op: 'delete' })

    expect(await state.getCursor()).toEqual({ latest: block(1), finalized: null })
  })

  it('accepts a caller-owned Pool, checking out exactly one dedicated connection', async () => {
    const tablePrefix = prefix()
    const pool = new Pool({ connectionString: dsn, max: 3 })

    try {
      const state = new PostgresPubsubState({ connection: pool, tablePrefix })
      await state.open({ cursorKey: 'pool-pipe', logger: testLogger() })
      opened.push(state)

      await commit(state, { operations: [operation()], ledger: [], cursor: block(1), finalized: null })
      expect(await state.pending()).toHaveLength(1)

      // The pool itself is untouched by close() — it belongs to the caller.
      await state.close()
      const probe = await pool.query('SELECT 1 AS ok')
      expect(probe.rows[0]).toEqual({ ok: 1 })
    } finally {
      await pool.end()
    }
  })

  it('accepts a PoolConfig object instead of a connection string', async () => {
    const url = new URL(dsn)
    const tablePrefix = prefix()

    const state = new PostgresPubsubState({
      connection: {
        host: url.hostname,
        port: Number(url.port || 5432),
        user: decodeURIComponent(url.username),
        password: decodeURIComponent(url.password),
        database: url.pathname.replace(/^\//, ''),
      },
      tablePrefix,
    })
    const { coldStart } = await state.open({ cursorKey: 'poolconfig-pipe', logger: testLogger() })
    opened.push(state)

    expect(coldStart).toBe(true)
    await commit(state, { operations: [operation()], ledger: [], cursor: block(1), finalized: null })
    expect(await state.pending()).toHaveLength(1)
  })

  it('reports STATE_UNAVAILABLE when the connection cannot be established', async () => {
    const state = new PostgresPubsubState({
      connection: 'postgresql://postgres:postgres@localhost:1/postgres',
      tablePrefix: prefix(),
    })

    await expect(state.open({ cursorKey: 'unreachable-pipe', logger: testLogger() })).rejects.toMatchObject({
      code: PUBSUB_ERROR_CODES.STATE_UNAVAILABLE,
    })
  })

  it('surfaces a connection dropped mid-session as a rejection, not a crash', async () => {
    // `pg`'s Client is known to emit an unhandled `error` event for a connection that dies out
    // from under it — with no listener, that crashes the process outright, so the only way to
    // prove the fix is to actually kill the backend and show the process is still here to assert.
    const tablePrefix = prefix()
    const appName = `pubsub-drop-test-${Math.random().toString(36).slice(2, 10)}`
    const url = new URL(dsn)
    url.searchParams.set('application_name', appName)

    const state = new PostgresPubsubState({ connection: url.toString(), tablePrefix })
    await state.open({ cursorKey: 'drop-pipe', logger: testLogger() })
    opened.push(state)

    await commit(state, { operations: [operation()], ledger: [], cursor: block(1), finalized: null })

    await adminPool.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1', [
      appName,
    ])
    // Let the termination reach the client socket before the next query races it.
    await new Promise((resolve) => setTimeout(resolve, 200))

    await expect(
      commit(state, { operations: [operation({ id: 'row-2' })], ledger: [], cursor: block(2), finalized: null }),
    ).rejects.toBeTruthy()

    // A retry reuses this same state instance (the pipe framework does not construct a fresh
    // one per attempt) — closing the dead connection and reopening must not leave the previous
    // session's error attached to the new one.
    await state.close()
    await state.open({ cursorKey: 'drop-pipe', logger: testLogger() })

    await commit(state, { operations: [operation({ id: 'row-3' })], ledger: [], cursor: block(3), finalized: null })
    expect((await state.pending()).map((row) => row.id)).toContain('row-3')
  })
})
