import { type ClickHouseClient, createClient } from '@clickhouse/client'

import type { BlockCursor, Logger } from '~/core/index.js'
import type { ClickhouseStore } from '~/targets/clickhouse/clickhouse-store.js'
import { clickhouseTarget } from '~/targets/clickhouse/index.js'

import { type Erc20Row, type Observation, TABLE_NAMES, type TableName, erc20Stream } from '../scenarios/erc20.js'

/** Where the ClickHouse server is, and who signs in. */
export type ClickhouseServer = { url: string; username: string; password: string }

/** Whether the author supplies the repair hook RP-42 delegates to them. Without it, ADR-15 applies. */
export type Repair = 'hook' | 'none'

/**
 * The DDL of the ERC-20 scenarios' tables as class A stores them: collapsing tables, so a rollback
 * cancels a row with a `sign = -1` copy (IB-20). The block is the sorting key: no scenario puts two
 * rows of a table in one block. The column names are the ones the oracle's rows carry, so the probe
 * reads rows back in the oracle's shape. The pipe does not create its tables; their owner does.
 */
export function clickhouseDdl(database: string): string[] {
  return TABLE_NAMES.map(
    (table) =>
      `CREATE TABLE "${database}"."${table}" ("blockNumber" UInt64, "value" String, "sign" Int8) ` +
      'ENGINE = CollapsingMergeTree(sign) ORDER BY "blockNumber"',
  )
}

/**
 * A client whose unqualified names resolve in `database`. `application` names its requests on the
 * server. Its inserts go in unfolded: a collapsing table folds rows that share a sorting key inside
 * one insert, as a merge does, so a row a batch sends twice would read as once.
 */
export function clickhouseClient({
  server,
  database,
  application,
}: {
  server: ClickhouseServer
  database: string
  application: string
}): ClickHouseClient {
  return createClient({ ...server, database, application, clickhouse_settings: { optimize_on_insert: 0 } })
}

/** The repair hook RP-42 asks of the author: remove every row above the safe cursor. */
async function removeAbove({ store, safeCursor }: { store: ClickhouseStore; safeCursor: BlockCursor }) {
  await store.removeAllRows({
    tables: [...TABLE_NAMES],
    where: '"blockNumber" > {latest:UInt64}',
    params: { latest: safeCursor.number },
  })
}

/**
 * Streams `range` into ClickHouse through `client`, with the pipe's sync table in the client's
 * database (class A). The target closes the client when the stream ends. `onFork` sees every fork's
 * canonical chain before the target rolls back to it.
 */
export function streamClickhouse({
  range,
  url,
  client,
  repair,
  logger,
  observe,
  onFork,
}: {
  range: { from: number; to: number }
  url: string
  client: ClickHouseClient
  repair: Repair
  logger: Logger
  observe: (observation: Observation) => void
  onFork: (canonical: BlockCursor[]) => void
}): Promise<void> {
  const target = clickhouseTarget<Record<TableName, Erc20Row[]>>({
    client,
    onData: async ({ store, data }) => {
      for (const table of TABLE_NAMES) {
        if (data[table].length) {
          const values = data[table].map((row) => ({ ...row, sign: 1 }))

          await store.insert({ table, values, format: 'JSONEachRow' })
        }
      }
    },
    onRollback: repair === 'hook' ? removeAbove : undefined,
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
