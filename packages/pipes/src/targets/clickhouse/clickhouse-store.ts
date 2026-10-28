import type {
  ClickHouseClient,
  ClickHouseSettings,
  CommandParams,
  CommandResult,
  DataFormat,
  InsertParams,
  InsertResult,
  QueryParams,
  QueryResult,
} from '@clickhouse/client'

import type { BlockCursor, Logger } from '~/core/index.js'

import { CLICKHOUSE_ERROR_CODES, ClickhouseTargetError } from './errors.js'
import { loadSqlFiles } from './fs.js'

export type QueryParamsWithFormat<Format extends DataFormat> = Omit<QueryParams, 'format'> & {
  format?: Format
}

export const ROLLBACK_INDEX_NAME = '_sqd_rollback_idx'

/*
 * Cancel-row rollback requires a CollapsingMergeTree family engine: inserting the same row
 * with sign = -1 is the only delete mechanism that propagates through materialized views.
 * ClickHouse Cloud transparently substitutes Shared* variants, so those are accepted too.
 */
export const SUPPORTED_ROLLBACK_ENGINES = /^(Shared)?(Replicated)?(Versioned)?CollapsingMergeTree$/

const ROLLBACK_READ_SETTINGS: ClickHouseSettings = {
  date_time_output_format: 'iso',
  output_format_json_quote_64bit_floats: 1,
  output_format_json_quote_64bit_integers: 1,
  // Decimal values must round-trip exactly: unquoted they would be parsed as JS Numbers
  // and the cancel rows would be re-inserted with precision-truncated values
  output_format_json_quote_decimals: 1,
}

const ROLLBACK_INSERT_SETTINGS: ClickHouseSettings = {
  date_time_input_format: 'best_effort',
  // On Replicated engines a cancel block whose checksum matches a recently inserted block
  // would be silently dropped by insert deduplication, turning the rollback into a no-op
  insert_deduplicate: 0,
}

/*
 * Columns excluded from the netting read-back: ALIAS and MATERIALIZED columns cannot be
 * inserted back, and EPHEMERAL columns cannot be selected at all.
 */
const NON_NETTABLE_DEFAULT_KINDS = new Set(['ALIAS', 'MATERIALIZED', 'EPHEMERAL'])

type TableMeta = {
  database: string
  name: string
  engine: string
  engineFull: string
  columns: { name: string; defaultKind: string }[]
}

class TableNotFoundError extends Error {}

const PLAIN_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/

/**
 * A safe cursor as `onRollback` gets it. `upTo`, when set, is the highest block whose rows may be
 * stored above it: after a crash in the first batch, that batch's last block; on a fork, the
 * cursor the fork rolls back from.
 */
export type RollbackCursor = BlockCursor & { upTo?: number }

export class ClickhouseStore {
  #logger?: Logger

  constructor(public client: ClickHouseClient) {}

  bindLogger(logger?: Logger) {
    this.#logger = logger
  }

  insert<T>(params: InsertParams<T>): Promise<InsertResult> {
    return this.client.insert(params as any)
  }

  query<Format extends DataFormat = 'JSON'>(params: QueryParamsWithFormat<Format>): Promise<QueryResult<Format>> {
    return this.client.query(params)
  }

  command(params: CommandParams): Promise<CommandResult> {
    return this.client.command(params)
  }

  close() {
    return this.client.close()
  }

  // FIXME use glob
  async executeFiles(dir: string) {
    const queries = await loadSqlFiles(dir)

    for (const query of queries) {
      try {
        await this.client.command({ query })
      } catch (e: any) {
        console.error(e)

        process.exit(1)
      }
    }
  }

  #parseTableName(table: string): { database: string | null; name: string } {
    // Users pass either `table` or `db.table`; identifiers may be backtick- or
    // double-quoted, and a quoted identifier may itself contain dots
    const parts: string[] = []
    let current = ''
    let quote: '`' | '"' | null = null

    for (const char of table) {
      if (quote) {
        if (char === quote) {
          quote = null
        } else {
          current += char
        }
      } else if (char === '`' || char === '"') {
        quote = char
      } else if (char === '.') {
        parts.push(current)
        current = ''
      } else {
        current += char
      }
    }
    parts.push(current)

    if (parts.length > 2) {
      throw new ClickhouseTargetError(
        CLICKHOUSE_ERROR_CODES.INVALID_TABLE_NAME,
        `Invalid table name "${table}": expected "table" or "database.table"`,
      )
    }

    const [database, name] = parts.length === 2 ? parts : [null, parts[0]]

    return { database, name }
  }

  async #fetchTableMeta(table: string): Promise<TableMeta> {
    const { database, name } = this.#parseTableName(table)

    const tableRes = await this.client.query({
      query: `
        SELECT database, name, engine, engine_full
        FROM system.tables
        WHERE database = coalesce({database:Nullable(String)}, currentDatabase()) AND name = {name:String}
      `,
      format: 'JSON',
      query_params: { database, name },
    })
    const tables = (await tableRes.json()).data as {
      database: string
      name: string
      engine: string
      engine_full: string
    }[]
    if (!tables.length) {
      throw new TableNotFoundError(`Table "${table}" does not exist`)
    }

    const columnsRes = await this.client.query({
      query: `
        SELECT name, default_kind
        FROM system.columns
        WHERE database = {database:String} AND table = {name:String}
        ORDER BY position
      `,
      format: 'JSON',
      query_params: { database: tables[0].database, name },
    })
    const columns = (await columnsRes.json()).data as { name: string; default_kind: string }[]

    return {
      database: tables[0].database,
      name: tables[0].name,
      engine: tables[0].engine,
      engineFull: tables[0].engine_full,
      columns: columns.map((c) => ({ name: c.name, defaultKind: c.default_kind })),
    }
  }

  #assertNotDistributed(table: string, meta: TableMeta) {
    if (meta.engine !== 'Distributed') {
      return
    }

    // Distributed('cluster', 'db', 'table'[, sharding_key])
    const match = meta.engineFull.match(/^Distributed\('[^']*',\s*'([^']*)',\s*'([^']*)'/)
    const underlying = match ? `${match[1]}.${match[2]}` : 'the underlying local table'

    throw new ClickhouseTargetError(
      CLICKHOUSE_ERROR_CODES.DISTRIBUTED_ROLLBACK,
      `Cannot roll back "${table}": it is a Distributed table. ` +
        `Rollback must target the local table (${underlying}) directly; multi-shard rollback is not supported.`,
    )
  }

  #assertCollapsesOnSign(table: string, meta: TableMeta) {
    // CollapsingMergeTree(sign) / VersionedCollapsingMergeTree(sign, version) /
    // ReplicatedCollapsingMergeTree('/path', 'replica', sign): the collapse column is the
    // first unquoted engine argument. Skip the check if the arguments cannot be parsed.
    const engineArgs = meta.engineFull.match(/^\w+\(([^)]*)\)/)?.[1]
    const collapseColumn = engineArgs
      ?.split(',')
      .map((arg) => arg.trim())
      .find((arg) => arg && !arg.startsWith("'"))
    if (collapseColumn && collapseColumn !== 'sign') {
      throw new ClickhouseTargetError(
        CLICKHOUSE_ERROR_CODES.ROLLBACK_COLLAPSE_COLUMN,
        `Cannot roll back "${table}": the engine collapses on "${collapseColumn}", not "sign". ` +
          `Rollback inserts cancel rows with sign = -1, so the collapse column must be named "sign".`,
      )
    }

    if (!meta.columns.some((c) => c.name === 'sign')) {
      throw new ClickhouseTargetError(
        CLICKHOUSE_ERROR_CODES.ROLLBACK_MISSING_SIGN,
        `Cannot roll back "${table}": the table has no "sign" column`,
      )
    }
  }

  /**
   * Ensures a minmax skip index on the given column so rollback reads
   * (`WHERE block_number > {safe}`) prune old parts regardless of the table's ORDER BY.
   *
   * Idempotent and cheap to call on every start (e.g. from `onStart`). When the index is
   * added to a table that already has data, `MATERIALIZE INDEX` builds it for existing
   * parts — for a single-column minmax this only reads that column and writes tiny index
   * files, so it is safe on large tables. The materialization runs as an async mutation:
   * a rollback issued immediately after the index is first added may still scan old parts
   * until the mutation completes, which is why calling this eagerly from `onStart` is
   * preferable to relying on the lazy auto-creation in `removeRowsAfter`.
   */
  async ensureRollbackIndex({ table, column = 'block_number' }: { table: string; column?: string }) {
    // The column is interpolated into DDL below; restrict it to a plain identifier
    if (!PLAIN_IDENTIFIER.test(column)) {
      throw new ClickhouseTargetError(
        CLICKHOUSE_ERROR_CODES.INVALID_ROLLBACK_INDEX_COLUMN,
        `Invalid rollback index column "${column}": expected a plain identifier`,
      )
    }

    const { database, name } = this.#parseTableName(table)

    const existing = await this.client.query({
      query: `
        SELECT 1
        FROM system.data_skipping_indices
        WHERE database = coalesce({database:Nullable(String)}, currentDatabase())
          AND table = {name:String}
          AND name = {index:String}
      `,
      format: 'JSON',
      query_params: { database, name, index: ROLLBACK_INDEX_NAME },
    })
    if ((await existing.json()).data.length > 0) {
      return
    }

    await this.client.command({
      query: `ALTER TABLE ${table} ADD INDEX IF NOT EXISTS ${ROLLBACK_INDEX_NAME} \`${column}\` TYPE minmax GRANULARITY 1`,
    })
    await this.client.command({
      query: `ALTER TABLE ${table} MATERIALIZE INDEX ${ROLLBACK_INDEX_NAME}`,
    })
  }

  /**
   * Removes the rows of the blocks above `cursor` from `tables`: what `onRollback` is for. The
   * store writes the block condition, so `where` is only needed on a table several pipes write, to
   * pick this pipe's rows. `column` names the block column of every table.
   *
   * Given `upTo`, it removes the blocks up to it only: no row of this pipe lies past it, so the
   * bound keeps a removal without `where` from reaching other pipes' rows there.
   *
   * The mechanism follows each table's engine:
   *
   * - CollapsingMergeTree family: inserts cancel rows (sign = -1), so the removal
   *   propagates through materialized views. Rows are netted with a
   *   `GROUP BY all columns / sum(sign)` query instead of `FINAL`: insert-retry duplicates
   *   are cancelled exactly, unmerged cancel rows from a previous rollback are not
   *   double-cancelled, and re-running after a partial failure is a no-op for
   *   already-cancelled groups.
   * - Any other engine: falls back to a lightweight `DELETE` with a warning — the table
   *   itself is cleaned, but materialized views built on it keep the removed data (MVs
   *   fire on INSERT only).
   * - If table metadata cannot be read (e.g. no access to `system.tables` on a
   *   locked-down server), falls back to the legacy `FINAL` read-back with cancel rows.
   */
  async removeRowsAfter(
    cursor: RollbackCursor,
    {
      tables,
      where,
      params,
      column = 'block_number',
    }: {
      tables: string | string[]
      where?: string
      params?: Record<string, unknown>
      column?: string
    },
  ) {
    // The column is interpolated into the condition below; restrict it to a plain identifier
    if (!PLAIN_IDENTIFIER.test(column)) {
      throw new ClickhouseTargetError(
        CLICKHOUSE_ERROR_CODES.INVALID_ROLLBACK_INDEX_COLUMN,
        `Invalid rollback column "${column}": expected a plain identifier`,
      )
    }

    // `>=` one past the cursor, so a start cursor at -1 needs no negative bound
    const from = cursor.number + 1
    const bound =
      cursor.upTo === undefined
        ? { where: `${column} >= {_sqd_from:UInt64}`, params: { _sqd_from: from } }
        : {
            where: `${column} BETWEEN {_sqd_from:UInt64} AND {_sqd_to:UInt64}`,
            params: { _sqd_from: from, _sqd_to: cursor.upTo },
          }

    return this.#removeRows({
      tables,
      where: where === undefined ? bound.where : `(${bound.where}) AND (${where})`,
      params: { ...params, ...bound.params },
    })
  }

  /**
   * Removes all rows matching `where`, by the mechanism `removeRowsAfter` describes.
   *
   * @deprecated Use `removeRowsAfter`. It writes the block condition itself, and limits the
   * removal after a run that stopped inside its first batch to that batch's blocks.
   */
  removeAllRows(options: { tables: string | string[]; where: string; params?: Record<string, unknown> }) {
    return this.#removeRows(options)
  }

  async #removeRows({
    tables,
    params,
    where,
  }: {
    tables: string | string[]
    where: string
    params?: Record<string, unknown>
  }) {
    tables = typeof tables === 'string' ? [tables] : tables

    return Promise.all(
      tables.map(async (table) => {
        let meta: TableMeta
        try {
          meta = await this.#fetchTableMeta(table)
        } catch (error) {
          if (error instanceof TableNotFoundError) {
            throw error
          }

          this.#logger?.warn(
            { error, table },
            `Failed to read table metadata for "${table}" (no access to system.tables/system.columns?); ` +
              `assuming a CollapsingMergeTree table and falling back to the FINAL-based cancel-row rollback`,
          )

          const count = await this.removeAllRowsByQuery({
            table,
            query: `SELECT * FROM ${table} FINAL WHERE ${where}`,
            params,
          })

          return { table, count }
        }

        this.#assertNotDistributed(table, meta)

        if (meta.columns.some((c) => c.name === 'block_number')) {
          try {
            await this.ensureRollbackIndex({ table })
          } catch (error) {
            this.#logger?.warn(
              { error, table },
              `Failed to ensure the rollback skip index on "${table}"; ` +
                `rollback will proceed but may scan the whole table`,
            )
          }
        }

        if (!SUPPORTED_ROLLBACK_ENGINES.test(meta.engine)) {
          this.#logger?.warn(
            { table, engine: meta.engine },
            `Rolling back "${table}" (engine ${meta.engine}) with a lightweight DELETE. ` +
              `Only CollapsingMergeTree cancel rows propagate through materialized views, ` +
              `so any materialized view on this table will keep the rolled-back data. ` +
              `Switch the table to CollapsingMergeTree(sign) to make rollbacks MV-safe.`,
          )

          const count = await this.#deleteRows({ table, where, params })

          return { table, count }
        }

        this.#assertCollapsesOnSign(table, meta)

        const columns = meta.columns
          .filter((c) => c.name !== 'sign' && !NON_NETTABLE_DEFAULT_KINDS.has(c.defaultKind))
          .map((c) => `\`${c.name}\``)
          .join(', ')

        // The sum alias must not collide with a real column of the table
        let netAlias = '_sqd_net'
        while (meta.columns.some((c) => c.name === netAlias)) {
          netAlias = `_${netAlias}`
        }

        const count = await this.#removeNettedRows({
          table,
          netAlias,
          query: `
            SELECT ${columns}, sum(sign) AS ${netAlias}
            FROM ${table}
            WHERE ${where}
            GROUP BY ${columns}
            HAVING ${netAlias} != 0
          `,
          params,
        })

        return { table, count }
      }),
    )
  }

  async #deleteRows({ table, where, params }: { table: string; where: string; params?: Record<string, unknown> }) {
    const countRes = await this.client.query({
      query: `SELECT count() AS count FROM ${table} WHERE ${where}`,
      format: 'JSON',
      query_params: params,
    })
    const [row] = (await countRes.json()).data as { count: string | number }[]
    const count = Number(row?.count ?? 0)

    if (count > 0) {
      await this.client.command({
        query: `DELETE FROM ${table} WHERE ${where}`,
        query_params: params,
      })
    }

    return count
  }

  async #removeNettedRows({
    table,
    netAlias,
    query,
    params,
  }: {
    table: string
    netAlias: string
    query: string
    params?: Record<string, unknown>
  }) {
    let count = 0
    const res = await this.client.query({
      query,
      format: 'JSONEachRow',
      clickhouse_settings: {
        ...ROLLBACK_READ_SETTINGS,
        // A deep rollback aggregates every matching row keyed by the full column tuple;
        // spill to disk instead of failing with a memory limit error on servers where
        // external aggregation is not enabled by default (ClickHouse < 25.1)
        max_bytes_before_external_group_by: '3000000000',
      },
      query_params: params,
    })

    for await (const rows of res.stream()) {
      const values: Record<string, unknown>[] = []

      for (const row of rows) {
        const data = (row as any).json()
        // The net sum arrives as a quoted string under output_format_json_quote_64bit_integers
        const net = Number(data[netAlias])
        delete data[netAlias]

        if (net < 0) {
          // An unmatched cancel row indicates pre-existing corruption; there is
          // nothing to cancel, so surface it instead of silently dropping it
          this.#logger?.warn(
            { table, net, row: data },
            `Rollback found a net-negative row group in "${table}" (more cancel rows than inserts); skipping it`,
          )

          continue
        }

        for (let i = 0; i < net; i++) {
          values.push({ ...data, sign: -1 })
        }
      }

      if (values.length > 0) {
        await this.client.insert({
          table,
          values,
          format: 'JSONEachRow',
          clickhouse_settings: {
            ...ROLLBACK_INSERT_SETTINGS,
            // Cancelling insert-retry duplicates needs several identical cancel rows in one
            // block; by default ClickHouse would collapse them against each other on insert
            optimize_on_insert: 0,
          },
        })

        count += values.length
      }
    }

    return count
  }

  /**
   * Cancels every row returned by the caller-supplied `query` by re-inserting it with
   * sign = -1. Lower-level than `removeRowsAfter` (which should be preferred for rollbacks):
   * the caller owns the query semantics and no engine check is performed, but the cancel
   * rows only take effect on a CollapsingMergeTree family table with a `sign` column.
   */
  async removeAllRowsByQuery({
    table,
    query,
    params,
  }: {
    table: string
    query: string
    params?: Record<string, unknown>
  }) {
    let count = 0
    const res = await this.client.query({
      query,
      format: 'JSONEachRow',
      clickhouse_settings: ROLLBACK_READ_SETTINGS,
      query_params: params,
    })

    for await (const rows of res.stream()) {
      await this.client.insert({
        table,
        values: rows.map((row: any) => {
          const data = row.json()

          data.sign = -1

          return data
        }),
        format: 'JSONEachRow',
        clickhouse_settings: ROLLBACK_INSERT_SETTINGS,
      })

      count += rows.length
    }

    return count
  }
}
