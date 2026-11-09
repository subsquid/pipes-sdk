import { BigQuery } from '@google-cloud/bigquery'

import type { BlockCursor, Logger } from '~/core/index.js'
import { bigqueryTarget } from '~/targets/bigquery/index.js'

import {
  type Erc20Row,
  type Observation,
  PIPE_ID,
  TABLE_NAMES,
  type TableName,
  erc20Stream,
} from '../scenarios/erc20.js'

/**
 * The ERC-20 scenarios' tables as class W stores them: one INT64 partition column the fork DELETE
 * scopes by, and the row's value. The column names are the oracle's, so the probe reads rows back
 * in the oracle's shape.
 */
export const BIGQUERY_TABLES = TABLE_NAMES.map((table) => ({
  table,
  blockNumberColumn: 'blockNumber',
  schema: [
    { name: 'blockNumber', type: 'INT64', mode: 'REQUIRED' } as const,
    { name: 'value', type: 'STRING', mode: 'REQUIRED' } as const,
  ],
}))

/** An admin client of the test's own: the probe reads through it, the fixture creates and drops the dataset. */
export function bigqueryAdmin(projectId: string): BigQuery {
  return new BigQuery({ projectId })
}

/**
 * Streams `range` into BigQuery in `dataset` of `projectId` (class W), through the target's public
 * API alone. The pipe creates its tracked tables and its sync table itself; `onFork` sees every
 * fork's canonical chain before the target rolls back to it.
 */
export function streamBigquery({
  range,
  url,
  projectId,
  dataset,
  logger,
  observe,
  onFork,
  onRollback,
}: {
  range: { from: number; to: number }
  url: string
  projectId: string
  dataset: string
  logger: Logger
  observe: (observation: Observation) => void
  onFork: (canonical: BlockCursor[]) => void
  onRollback?: (cursor: BlockCursor) => void
}): Promise<void> {
  const target = bigqueryTarget<Record<TableName, Erc20Row[]>>({
    client: { bigquery: new BigQuery({ projectId }) },
    projectId,
    dataset,
    tables: BIGQUERY_TABLES,
    onAfterRollback: ({ cursor }) => onRollback?.(cursor),
    onData: ({ store, data }) => {
      for (const table of TABLE_NAMES) {
        if (data[table].length) {
          store.insert(table, data[table])
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

/**
 * Runs the restart's recovery alone, through the target's public API: `write` with nothing to read
 * still starts as a restart does — the tracked-table checks, then `getCursor`, which re-runs the
 * rollback DELETEs an in-flight WAL row names and writes the marker — and ends with no batch taken.
 */
export async function recoverBigquery({
  projectId,
  dataset,
  logger,
}: {
  projectId: string
  dataset: string
  logger: Logger
}): Promise<void> {
  const target = bigqueryTarget<Record<TableName, Erc20Row[]>>({
    client: { bigquery: new BigQuery({ projectId }) },
    projectId,
    dataset,
    tables: BIGQUERY_TABLES,
    onData: () => {},
  })

  await target.write({ read: async function* () {}, logger, id: PIPE_ID })
}
