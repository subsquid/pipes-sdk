import Database from 'better-sqlite3'

import { SqliteOptions, SqliteSync } from './sqlite.js'

export class NodeSQLite implements SqliteSync {
  #db: Database.Database

  constructor(options: SqliteOptions) {
    this.#db = new Database(options.path)
  }

  exec<P extends any[] = any[]>(sql: string, params: P = [] as unknown as P) {
    this.#db.prepare(sql).run(...params)
  }

  get<T = any, P extends any[] = any[]>(sql: string, params?: P): T | null {
    return this.#db.prepare<P, T>(sql).get(...(params || [])) || null
  }

  all<T = any, P extends any[] = any[]>(sql: string, params?: P): T[] {
    return this.#db.prepare<P, T>(sql).all(...(params || []))
  }

  async *stream<P extends any[], R>(sql: string, params?: P): AsyncIterable<R> {
    for await (const message of this.#db.prepare(sql).iterate(...(params || []))) {
      yield message as R
    }
  }

  close() {
    if (this.#db.open) this.#db.close()
  }
}
