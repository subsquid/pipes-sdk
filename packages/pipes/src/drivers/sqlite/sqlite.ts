export type SqliteOptions = {
  // File path to the SQLite database
  path: string
  // Enable Write-Ahead Logging mode for better performance in concurrent scenarios
  // defaults to true
  enableWAL?: boolean
}

export interface SqliteSync {
  get<T = unknown, P extends any[] = any[]>(sql: string, params?: P): T | null
  all<T = unknown, P extends any[] = any[]>(sql: string, params?: P): T[]
  exec<P extends any[] = any[]>(sql: string, params?: P): void
  stream<P extends any[], R>(sql: string, params?: P): AsyncIterable<R>
  /** Releases the file handle and any lock the connection holds. Idempotent. */
  close(): void
}

/**
 * SQLite aborts the transaction itself on SQLITE_FULL, SQLITE_IOERR, SQLITE_NOMEM,
 * SQLITE_BUSY and SQLITE_INTERRUPT. An unconditional `ROLLBACK` in a catch block then fails
 * with "cannot rollback - no transaction is active" and replaces the failure that actually
 * happened, so every rollback path has to swallow it and rethrow the original.
 *
 * Returns false when the connection may still hold an open transaction — the rollback failed
 * for some other reason, and the next `BEGIN` on this connection would fail over the top of
 * the real error with the previous statements still uncommitted.
 */
export function rollbackQuietly(client: SqliteSync): boolean {
  try {
    client.exec('ROLLBACK')

    return true
  } catch (e) {
    return isNoActiveTransaction(e)
  }
}

/** SQLite's own wording when the failing statement already unwound the transaction. */
function isNoActiveTransaction(e: unknown): boolean {
  const message = e instanceof Error ? e.message : String(e)

  return /no transaction is active/i.test(message)
}

/**
 * Errors for which the store, not the statement, is at fault: the volume is full, gone,
 * unreadable, or has been remounted read-only under the process.
 */
export function isStorageFailure(e: unknown): boolean {
  const code = (e as { code?: unknown } | null)?.code

  return typeof code === 'string' && /^SQLITE_(FULL|IOERR|NOMEM|READONLY|CANTOPEN)/.test(code)
}

function setupClient(client: SqliteSync, options: SqliteOptions): SqliteSync {
  if (options.enableWAL ?? true) {
    client.exec('PRAGMA journal_mode = WAL;')
    client.exec('PRAGMA synchronous = NORMAL;')
  }

  return client
}

export async function loadSqlite(options: SqliteOptions): Promise<SqliteSync> {
  if (typeof Bun !== 'undefined') {
    const m = await import('./bun-sqlite.js')
    return setupClient(new m.BunSQLite(options), options)
  }

  const m = await import('./node-sqlite.js')
  return setupClient(new m.NodeSQLite(options), options)
}
