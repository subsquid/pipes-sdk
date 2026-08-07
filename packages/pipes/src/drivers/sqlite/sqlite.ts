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
