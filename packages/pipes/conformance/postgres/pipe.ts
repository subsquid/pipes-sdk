import { drizzle } from 'drizzle-orm/node-postgres'
import { bigint, pgTable, text } from 'drizzle-orm/pg-core'
import pg from 'pg'

import type { BlockCursor, Logger } from '~/core/index.js'
import { drizzleTarget } from '~/targets/drizzle/node-postgres/index.js'

import { type Erc20Row, type Observation, TABLE_NAMES, type TableName, erc20Stream } from '../scenarios/erc20.js'

/**
 * The ERC-20 scenarios' tables as class T stores them. The block is the key: the undo log needs
 * one (CN-30), and no scenario puts two rows of a table in one block. The column names are the
 * ones the oracle's rows carry, so the probe reads rows back in the oracle's shape.
 */
export const POSTGRES_TABLES = {
  transfers: pgTable('transfers', {
    blockNumber: bigint('blockNumber', { mode: 'number' }).primaryKey(),
    value: text('value').notNull(),
  }),
  approvals: pgTable('approvals', {
    blockNumber: bigint('blockNumber', { mode: 'number' }).primaryKey(),
    value: text('value').notNull(),
  }),
} satisfies Record<TableName, unknown>

/** The DDL of the tables in `schema`. The pipe does not create its tables; their owner does. */
export function postgresDdl(schema: string): string {
  return TABLE_NAMES.map(
    (table) => `CREATE TABLE "${schema}"."${table}" ("blockNumber" int8 PRIMARY KEY, "value" text NOT NULL);`,
  ).join('\n')
}

/**
 * A pool whose every connection resolves unqualified names in `schema`: the target's undo-log DDL
 * names its tables without a schema. `application` names the connections on the server.
 */
export function postgresPool({ dsn, schema, application }: { dsn: string; schema: string; application: string }) {
  return new pg.Pool({ connectionString: dsn, options: `-c search_path=${schema}`, application_name: application })
}

/**
 * Streams `range` into Postgres through `pool`, with the pipe's sync table in `schema` (class T).
 * `onFork` sees every fork's canonical chain before the target rolls back to it.
 */
export function streamPostgres({
  range,
  url,
  pool,
  schema,
  logger,
  observe,
  onFork,
}: {
  range: { from: number; to: number }
  url: string
  pool: pg.Pool
  schema: string
  logger: Logger
  observe: (observation: Observation) => void
  onFork: (canonical: BlockCursor[]) => void
}): Promise<void> {
  const target = drizzleTarget<Record<TableName, Erc20Row[]>>({
    db: drizzle(pool),
    tables: POSTGRES_TABLES,
    settings: { state: { schema } },
    onData: async ({ tx, data }) => {
      for (const table of TABLE_NAMES) {
        if (data[table].length) {
          await tx.insert(POSTGRES_TABLES[table]).values(data[table])
        }
      }
    },
  })
  const resolveFork = target.resolveFork as NonNullable<typeof target.resolveFork>

  return erc20Stream({ range, url, logger, observe }).pipeTo({
    ...target,
    resolveFork: (canonical) => {
      onFork(canonical)

      return resolveFork(canonical)
    },
  })
}
