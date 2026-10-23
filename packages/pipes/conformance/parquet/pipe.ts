import type { Logger } from '~/core/index.js'
import { type ParquetTable, parquetTarget } from '~/targets/parquet/index.js'

import { type Observation, TABLE_NAMES, erc20Stream } from '../scenarios/erc20.js'

const COLUMNS: ParquetTable['schema'] = {
  blockNumber: { type: 'INT64' },
  value: { type: 'UTF8' },
}
const TABLES: ParquetTable[] = TABLE_NAMES.map((table) => ({ table, schema: COLUMNS }))

/** Streams `range` into a Parquet store in `dir` (class K). */
export function streamParquet({
  range,
  url,
  dir,
  logger,
  observe,
}: {
  range: { from: number; to: number }
  url: string
  dir: string
  logger: Logger
  observe: (observation: Observation) => void
}): Promise<void> {
  return erc20Stream({ range, url, logger, observe }).pipeTo(
    parquetTarget({
      dir,
      tables: TABLES,
      // Every batch that holds a row closes its window. A row count is exact; the engine reports bytes late.
      settings: { rollover: { maxRows: 1 } },
      onData: ({ store, data }) => {
        for (const table of TABLE_NAMES) {
          store.insert(table, data[table])
        }
      },
    }),
  )
}
