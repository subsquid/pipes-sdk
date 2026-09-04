import { AbiEvent } from '@subsquid/evm-abi'

import { SqliteOptions, SqliteSync, loadSqlite, rollbackQuietly } from '~/drivers/sqlite/sqlite.js'
import { jsonParse, jsonStringify } from '~/internal/json.js'

import { IndexedParams } from '../evm-decoder.js'
import { EventArgs, FactoryPersistentAdapter, InternalFactoryEvent } from '../factory.js'

type Row = {
  factory: string
  address: string
  block_number: number
  transaction_index: number
  log_index: number
  event: Buffer
}

const VERSION = '1.0.0'

class SqliteFactoryAdapter<T extends EventArgs> implements FactoryPersistentAdapter<InternalFactoryEvent<T>> {
  #db: SqliteSync
  #lookupCache: Record<string, InternalFactoryEvent<any> | null> = {}

  constructor(
    db: SqliteSync,
    protected options: SqliteOptions,
  ) {
    this.options = options
    this.#db = db
  }

  /**
   * Every write goes through here: a transaction left open by a failed statement makes the
   * next `BEGIN` fail with an error about the transaction rather than about what went wrong.
   */
  #transaction(body: () => void): void {
    this.#db.exec('BEGIN TRANSACTION')

    try {
      body()
      this.#db.exec('COMMIT')
    } catch (error) {
      rollbackQuietly(this.#db)

      throw error
    }
  }

  async migrate(): Promise<void> {
    this.#transaction(() => {
      this.#db.exec(`CREATE TABLE IF NOT EXISTS "metadata" (id TEXT, value TEXT, PRIMARY KEY (id))`)
      this.#db.exec(
        `INSERT INTO "metadata" (id, value) VALUES (?, ?)  ON CONFLICT (id) DO UPDATE SET "value" = excluded.value`,
        ['version', VERSION],
      )
    })

    this.#transaction(() => {
      this.#db.exec(`
        CREATE TABLE IF NOT EXISTS factory (
          address             TEXT,
          factory             TEXT,
          block_number        INTEGER,
          transaction_index   INTEGER,
          log_index           INTEGER,
          event               BLOB,
          PRIMARY KEY (address)
        )
      `)
      this.#db.exec(`CREATE INDEX IF NOT EXISTS factory_block_number_idx ON factory (block_number)`)
    })
  }

  /**
   * Retrieves all factory events from the database, optionally filtered by indexed event parameters.
   *
   * When no parameters are provided, returns all stored factory events.
   * When parameters are provided, only returns factory events whose decoded event data matches the specified parameter values.
   * This filtering is essential to avoid returning contracts from previous pipeline runs that used
   * different or no parameter filters.
   *
   * @param params - Optional indexed parameters to filter factory events by. Each parameter can be
   *   a single value or an array of values. When provided, only factory events whose event data
   *   matches all specified parameter values are returned.
   * @returns All factory events matching the provided parameters, or all events if no parameters are specified.
   */
  async all(params?: IndexedParams<AbiEvent<any>>) {
    const conditions: string[] = []
    const values: any[] = []

    if (params) {
      for (const [paramName, paramValue] of Object.entries(params)) {
        // This value is being casted because TS isn't being able to infer the value of paramValue out of `params`.
        const expectedValues = paramValue as unknown[]
        const placeholders = expectedValues.map((v) => (typeof v === 'string' ? 'LOWER(?)' : '?')).join(', ')
        conditions.push(`LOWER(json_extract(event, '$.${paramName}')) IN (${placeholders})`)
        values.push(...expectedValues)
      }
    }

    const where = conditions.length === 0 ? '' : 'WHERE ' + conditions.join(' AND ')
    const query = `SELECT * FROM "factory" ${where}`

    return this.#db.all<Row>(query, values).map((row): InternalFactoryEvent<any> => {
      return {
        childAddress: row.address,
        factoryAddress: row.factory,
        blockNumber: row.block_number,
        transactionIndex: row.transaction_index,
        logIndex: row.log_index,
        event: jsonParse(row.event.toString()),
      }
    })
  }

  /**
   * TODO: should we consider parameters when looking up a contract address?
   * Or are we okay with doing that filtering in memory?
   */
  async lookup(address: string): Promise<InternalFactoryEvent<any> | null> {
    if (typeof this.#lookupCache[address] !== 'undefined') {
      return this.#lookupCache[address]
    }

    const row = this.#db.get<Row>('SELECT * FROM "factory" WHERE address = ?', [address])
    if (!row) {
      this.#lookupCache[address] = null
      return null
    }

    this.#lookupCache[address] = {
      childAddress: row.address,
      factoryAddress: row.factory,
      blockNumber: row.block_number,
      transactionIndex: row.transaction_index,
      logIndex: row.log_index,
      event: jsonParse(row.event.toString()),
    }

    return this.#lookupCache[address]
  }

  async save(entities: InternalFactoryEvent<any>[]): Promise<void> {
    this.#transaction(() => {
      for (const entity of entities) {
        this.#db.exec(
          `INSERT OR IGNORE INTO factory ('address', 'factory', 'block_number', 'transaction_index', 'log_index', 'event') VALUES (?,?,?,?,?,?)`,
          [
            entity.childAddress,
            entity.factoryAddress,
            entity.blockNumber,
            entity.transactionIndex,
            entity.logIndex,
            jsonStringify(entity.event),
          ],
        )
      }
    })

    this.clearCache()
  }

  async remove(blockNumber: number): Promise<void> {
    this.#db.exec(`DELETE FROM factory WHERE block_number > ?`, [blockNumber])
    this.clearCache()
  }

  private clearCache() {
    this.#lookupCache = {}
  }
}

export async function contractFactorySqliteStore<T extends EventArgs>(options: SqliteOptions) {
  return new SqliteFactoryAdapter<T>(await loadSqlite(options), options)
}
