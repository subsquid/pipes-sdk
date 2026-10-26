import { SqliteOptions, SqliteSync, isStorageFailure, loadSqlite, rollbackQuietly } from '~/drivers/sqlite/sqlite.js'

import { DriverLockedError, DriverUnavailableError, SqlDriver, renderSchemaDDL } from './sql-driver.js'

function isLockError(e: unknown): boolean {
  const message = e instanceof Error ? e.message : String(e)

  return /database is locked|SQLITE_BUSY/i.test(message)
}

export type SqliteDriverOptions = {
  path: string
  /** Test seam: replaces how the raw connection is obtained. @internal */
  connect?: (options: SqliteOptions) => Promise<SqliteSync>
}

/**
 * One SQLite file, one connection, held open for the state's lifetime. Single-writer is an
 * OS-level fact here (`PRAGMA locking_mode = EXCLUSIVE`), not a convention — a second producer
 * on the same file fails on that pragma before any state logic runs.
 */
export class SqliteDriver implements SqlDriver {
  readonly #options: SqliteDriverOptions
  #db?: SqliteSync
  /** False once a rollback could not run: the connection may still hold an open transaction. */
  #unwound = true

  constructor(options: SqliteDriverOptions) {
    this.#options = options
  }

  get location(): string {
    return this.#options.path
  }

  table(name: string): string {
    return name
  }

  async connect(): Promise<void> {
    let db: SqliteSync
    try {
      db = await (this.#options.connect ?? loadSqlite)({ path: this.#options.path })
    } catch (e) {
      // A live producer holds the file in exclusive locking mode, so the second one fails right
      // here — on the driver's own WAL pragma, before any target code runs.
      if (isLockError(e)) throw new DriverLockedError(e)

      throw new DriverUnavailableError(e)
    }

    this.#db = db

    try {
      // WAL is the shared driver's own default, re-asserted here because an injected one may
      // never have been through it, and rollback-journal mode would change the durability the
      // pragma below is buying.
      db.exec('PRAGMA journal_mode = WAL;')

      // The shared driver defaults to synchronous = NORMAL, under which an OS crash can roll
      // back the most recent commits. Here a rolled-back commit is a published operation the
      // state has no record of, so the target pays for FULL.
      db.exec('PRAGMA synchronous = FULL;')

      db.exec('PRAGMA locking_mode = EXCLUSIVE;')
      db.exec('BEGIN IMMEDIATE')
      db.exec('COMMIT')
    } catch (e) {
      db.close()
      this.#db = undefined

      throw new DriverLockedError(e)
    }
  }

  schemaDDL(): string[] {
    return renderSchemaDDL('sqlite', { table: (n) => n, index: (n) => n })
  }

  async get<T>(sql: string, params: unknown[] = []): Promise<T | undefined> {
    return this.#db!.get<T>(sql, params) ?? undefined
  }

  async all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    return this.#db!.all<T>(sql, params)
  }

  async exec(sql: string, params: unknown[] = []): Promise<void> {
    this.#db!.exec(sql, params)
  }

  async begin(immediate: boolean): Promise<void> {
    if (!this.#unwound) {
      this.#unwound = rollbackQuietly(this.#db!)
    }

    this.#db!.exec(immediate ? 'BEGIN IMMEDIATE' : 'BEGIN')
  }

  async commit(): Promise<void> {
    this.#db!.exec('COMMIT')
  }

  async rollback(): Promise<boolean> {
    this.#unwound = rollbackQuietly(this.#db!)

    return this.#unwound
  }

  isStorageFailure(e: unknown): boolean {
    return isStorageFailure(e)
  }

  async close(): Promise<void> {
    // Releases the exclusive lock — a restart in the same process must be able to reopen it.
    this.#db?.close()
    this.#db = undefined
  }
}
