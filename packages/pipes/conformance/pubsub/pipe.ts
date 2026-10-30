import type { BlockCursor, Logger } from '~/core/index.js'
import { pubsubTarget } from '~/targets/pubsub/index.js'

import { type Erc20Row, type Observation, TABLE_NAMES, type TableName, erc20Stream } from '../scenarios/erc20.js'

/** The project and topic the pipe publishes to. */
export const PROJECT_ID = 'ct'
export const TOPIC = 'ct-erc20'

/** Where the bus listens: `host:port`. */
export type BusAddress = { endpoint: string; projectId: string }

/** One row of either table, as the pipe publishes it. Its id is its table and block. */
export type ChangelogRow = { table: TableName; blockNumber: number; value: string }

/**
 * Both tables in one list, in block order and, inside a block, in table order. One route carries
 * them: the sequence barrier refuses a second route on the same topic (E2429, GAP-40).
 */
function inBlockOrder(tables: Record<TableName, Erc20Row[]>): ChangelogRow[] {
  const rows = TABLE_NAMES.flatMap((table) => tables[table].map((row) => ({ table, ...row })))

  return rows.sort(
    (a, b) => a.blockNumber - b.blockNumber || TABLE_NAMES.indexOf(a.table) - TABLE_NAMES.indexOf(b.table),
  )
}

/** The row id: no scenario puts two rows of a table in one block. */
export function rowId({ table, blockNumber }: { table: string; blockNumber: number }): string {
  return `${table}:${blockNumber}`
}

/**
 * Streams `range` to the bus as a changelog of write-once rows (class C), with the producer's state
 * in the SQLite file at `statePath`. Every row is published, from the range start. `coldStart`
 * declares the bootstrap a first run needs (CN-46). `onFork` sees every fork's canonical chain
 * before the target compensates.
 */
export function streamPubsub({
  range,
  url,
  bus,
  statePath,
  coldStart,
  logger,
  observe,
  onFork,
}: {
  range: { from: number; to: number }
  url: string
  bus: BusAddress
  statePath: string
  coldStart: boolean
  logger: Logger
  observe: (observation: Observation) => void
  onFork: (canonical: BlockCursor[]) => void
}): Promise<void> {
  const target = pubsubTarget<{ rows: ChangelogRow[] }>({
    pubsub: { projectId: bus.projectId, apiEndpoint: bus.endpoint },
    state: { path: statePath },
    allowColdStart: coldStart,
    publishFrom: range.from,
    topics: {
      rows: {
        topic: TOPIC,
        map: ({ data }) =>
          data.map((row) => ({
            data: { _id: rowId(row), ...row },
            block: { number: row.blockNumber },
            attributes: { table: row.table },
          })),
      },
    },
  })
  const resolveFork = target.resolveFork as NonNullable<typeof target.resolveFork>

  return erc20Stream({ range, url, logger, observe })
    .pipe((tables) => ({ rows: inBlockOrder(tables) }))
    .pipeTo({
      ...target,
      resolveFork: (canonical) => {
        onFork(canonical)

        return resolveFork(canonical)
      },
    })
}
