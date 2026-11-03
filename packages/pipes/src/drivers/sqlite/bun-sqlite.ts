import { Database, type Statement } from 'bun:sqlite'

import { SqliteOptions, SqliteSync } from './sqlite.js'

/**
 * Compiled statements kept per connection. Callers issue a small, fixed set of SQL strings, so
 * the bound only matters for a caller that builds SQL dynamically.
 */
export const STATEMENT_CACHE_SIZE = 128

export class BunSQLite implements SqliteSync {
  #db: Database
  // db.run() compiles on every call, and db.query() caches without a bound; this Map replaces
  // both with a bounded cache. Map order doubles as LRU order.
  #statements = new Map<string, Statement<any, any>>()

  constructor(options: SqliteOptions) {
    this.#db = new Database(options.path)
  }

  exec<P extends any[] = any[]>(sql: string, params: P = [] as unknown as P) {
    this.#statement(sql).run(...params)
  }

  get<T = any, P extends any[] = any[]>(sql: string, params?: P): T | null {
    const row = this.#statement(sql).get(...(params || [])) as T | undefined

    return row ?? null
  }

  all<T = any, P extends any[] = any[]>(sql: string, params?: P): T[] {
    return this.#statement(sql).all(...(params || [])) as T[]
  }

  async *stream<P extends any[], R>(sql: string, params?: P): AsyncIterable<R> {
    // Uncached on purpose: an iterating statement stays busy until the iterator finishes, and
    // `exec`/`get`/`all` on the same SQL must not run into it.
    for await (const message of this.#db.prepare(sql).iterate(...(params || []))) {
      yield message as R
    }
  }

  close() {
    this.#statements.clear()
    this.#db.close()
  }

  #statement(sql: string): Statement<any, any> {
    const cached = this.#statements.get(sql)
    if (cached) {
      // set() keeps an existing key's position; delete + set re-inserts at the end, recording
      // the recent use. Without the move, eviction would run in insertion order (FIFO).
      this.#statements.delete(sql)
      this.#statements.set(sql, cached)

      return cached
    }

    const statement = this.#db.prepare(sql)
    this.#statements.set(sql, statement)
    if (this.#statements.size > STATEMENT_CACHE_SIZE) {
      this.#statements.delete(this.#statements.keys().next().value!)
    }

    return statement
  }
}
