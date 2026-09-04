import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { SqliteSync, isStorageFailure, loadSqlite, rollbackQuietly } from './sqlite.js'

const dirs: string[] = []
const opened: SqliteSync[] = []

afterEach(() => {
  for (const client of opened.splice(0)) {
    client.close()
  }
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true })
  }
})

async function open(): Promise<SqliteSync> {
  const dir = mkdtempSync(join(tmpdir(), 'sqlite-driver-'))
  dirs.push(dir)

  const client = await loadSqlite({ path: join(dir, 'test.sqlite') })
  opened.push(client)

  return client
}

function sqliteError(code: string, message: string): Error {
  return Object.assign(new Error(message), { code })
}

describe('rollbackQuietly', () => {
  it('unwinds an open transaction and reports the connection clean', async () => {
    const client = await open()
    client.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    client.exec('BEGIN IMMEDIATE')
    client.exec('INSERT INTO t (id) VALUES (1)')

    expect(rollbackQuietly(client)).toBe(true)
    expect(client.get<{ n: number }>('SELECT COUNT(*) AS n FROM t')).toMatchObject({ n: 0 })

    // Clean means clean: the next transaction opens without tripping over the last one.
    client.exec('BEGIN IMMEDIATE')
    client.exec('COMMIT')
  })

  it('swallows the rollback SQLite already performed for the failing statement', async () => {
    const client = await open()

    expect(rollbackQuietly(client)).toBe(true)
  })

  it('reports the connection unclean when the rollback itself could not run', async () => {
    const client = await open()
    const stub: SqliteSync = {
      ...client,
      exec: () => {
        throw sqliteError('SQLITE_BUSY', 'database is locked')
      },
    }

    // Swallowed either way — the failure that got us here must reach the caller — but the
    // transaction is still open, and the caller has to know before its next BEGIN.
    expect(rollbackQuietly(stub)).toBe(false)
  })
})

describe('isStorageFailure', () => {
  it.each([
    ['SQLITE_FULL', true],
    ['SQLITE_IOERR_WRITE', true],
    ['SQLITE_NOMEM', true],
    ['SQLITE_READONLY', true],
    ['SQLITE_READONLY_DBMOVED', true],
    ['SQLITE_CANTOPEN', true],
    ['SQLITE_CONSTRAINT_PRIMARYKEY', false],
    ['SQLITE_BUSY', false],
  ])('%s → %s', (code, expected) => {
    expect(isStorageFailure(sqliteError(code, 'boom'))).toBe(expected)
  })

  it('ignores errors that carry no SQLite code', () => {
    expect(isStorageFailure(new Error('database or disk is full'))).toBe(false)
    expect(isStorageFailure(null)).toBe(false)
  })
})
