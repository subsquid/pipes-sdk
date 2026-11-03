import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import Database from 'better-sqlite3'
import { type MockInstance, describe, expect, test, vi } from 'vitest'

import { NodeSQLite, STATEMENT_CACHE_SIZE } from './node-sqlite.js'

type PrepareSpy = MockInstance<Database.Database['prepare']>

const withClient = test.extend<{ client: NodeSQLite; prepare: PrepareSpy }>({
  client: async ({}, use) => {
    const dir = mkdtempSync(join(tmpdir(), 'node-sqlite-'))
    const client = new NodeSQLite({ path: join(dir, 'test.sqlite') })
    client.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)')

    await use(client)

    client.close()
    rmSync(dir, { recursive: true, force: true })
  },
  prepare: async ({ client: _ }, use) => {
    const spy = vi.spyOn(Database.prototype, 'prepare')

    await use(spy)

    spy.mockRestore()
  },
})

const preparedSql = (prepare: PrepareSpy) => prepare.mock.calls.map(([sql]) => sql)

describe('NodeSQLite statement cache', () => {
  withClient('compiles a statement once and runs it many times', ({ client, prepare }) => {
    for (let id = 1; id <= 3; id++) {
      client.exec('INSERT INTO t (id, v) VALUES (?, ?)', [id, `v${id}`])
    }
    for (let id = 1; id <= 3; id++) {
      expect(client.get('SELECT v FROM t WHERE id = ?', [id])).toEqual({ v: `v${id}` })
    }

    expect(preparedSql(prepare)).toEqual(['INSERT INTO t (id, v) VALUES (?, ?)', 'SELECT v FROM t WHERE id = ?'])
  })

  withClient('a cached statement sees a column added after it was compiled', ({ client }) => {
    client.exec('INSERT INTO t (id, v) VALUES (1, ?)', ['a'])
    expect(client.all('SELECT * FROM t')).toEqual([{ id: 1, v: 'a' }])

    client.exec("ALTER TABLE t ADD COLUMN w TEXT NOT NULL DEFAULT 'x'")

    expect(client.all('SELECT * FROM t')).toEqual([{ id: 1, v: 'a', w: 'x' }])
  })

  withClient('runs the SQL a stream is still iterating', async ({ client }) => {
    for (let id = 1; id <= 3; id++) {
      client.exec('INSERT INTO t (id, v) VALUES (?, ?)', [id, `v${id}`])
    }

    const seen: unknown[] = []
    for await (const row of client.stream('SELECT id FROM t ORDER BY id')) {
      seen.push(row)
      // The same SQL through the cached path, while the stream's own statement is busy.
      expect(client.all('SELECT id FROM t ORDER BY id')).toHaveLength(3)
    }

    expect(seen).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }])
  })

  withClient('evicts the least recently used statement past the bound', ({ client, prepare }) => {
    const select = (n: number) => client.get(`SELECT ${n} AS n`)

    for (let n = 0; n < STATEMENT_CACHE_SIZE; n++) select(n)
    // Touch 0 so 1 becomes the oldest, then overflow the cache by one.
    select(0)
    select(STATEMENT_CACHE_SIZE)
    prepare.mockClear()

    select(0)
    expect(preparedSql(prepare)).toEqual([])

    select(1)
    expect(preparedSql(prepare)).toEqual(['SELECT 1 AS n'])
  })
})
