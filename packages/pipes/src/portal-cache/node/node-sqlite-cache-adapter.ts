import { mkdir } from 'node:fs/promises'
import path from 'node:path'

import { SqliteOptions, SqliteSync, loadSqlite } from '~/drivers/sqlite/sqlite.js'

import { Options, PortalCacheNodeJs, SaveBatch, StreamBatch } from './node-portal-cache.js'

export type PortalSqliteCacheOptions = Options<SqliteOptions>

class PortalSqliteCache extends PortalCacheNodeJs<PortalSqliteCacheOptions> {
  #db?: SqliteSync
  #statements = {
    table: `CREATE TABLE IF NOT EXISTS data
    (
        query_hash TEXT,
        block_from INTEGER,
        block_to   INTEGER,
        value      BLOB,
        PRIMARY KEY ("block_from", "block_to", "query_hash")
    )`,
    insert: 'INSERT INTO "data" ("block_from", "block_to", "query_hash", "value") VALUES (?, ?, ?, ?)',
    select: 'SELECT * FROM "data" WHERE "block_from" = ? and "query_hash" = ? ORDER BY "block_from"',
  }

  constructor(options: PortalSqliteCacheOptions) {
    super(options)
  }

  assertDb() {
    if (!this.#db) throw new Error('Database not initialized. Call initialize() first.')

    return this.#db
  }

  async initialize(): Promise<void> {
    const dir = path.dirname(this.options.path)
    if (dir !== '.') {
      await mkdir(dir, { recursive: true })
    }

    this.#db = await loadSqlite(this.options)

    this.assertDb().exec(this.#statements.table)
  }

  async *stream({ fromBlock, queryHash }: StreamBatch) {
    while (true) {
      const res = this.assertDb().get<{ value: Uint8Array; block_to: number }>(this.#statements.select, [
        fromBlock,
        queryHash,
      ])
      if (!res) break

      yield res.value

      fromBlock = res.block_to + 1
    }
  }

  protected async save({ queryHash, cursors, data }: SaveBatch) {
    this.assertDb().exec(this.#statements.insert, [cursors.first, cursors.last, queryHash, data])
  }
}

export function portalSqliteCache(opts: PortalSqliteCacheOptions) {
  return new PortalSqliteCache(opts)
}
