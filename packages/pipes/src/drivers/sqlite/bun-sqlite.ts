import { Database } from 'bun:sqlite'

import { SqliteOptions, SqliteSync } from './sqlite.js'

export class BunSQLite implements SqliteSync {
  #db: Database

  constructor(options: SqliteOptions) {
    this.#db = new Database(options.path)
  }

  exec<P extends any[] = any[]>(sql: string, params: P = [] as unknown as P) {
    this.#db.run(sql, params)
  }

  get<T = any, P extends any[] = any[]>(sql: string, params?: P): T | null {
    return this.#db.query<T, P>(sql).get(...(params || ([] as any)))
  }

  all<T = any, P extends any[] = any[]>(sql: string, params?: P): T[] {
    return this.#db.query<T, P>(sql).all(...(params || ([] as any)))
  }

  async *stream<P extends any[], R>(sql: string, params?: P): AsyncIterable<R> {
    for await (const message of this.#db.prepare(sql).iterate(...(params || []))) {
      yield message as R
    }
  }

  close() {
    this.#db.close()
  }
}
