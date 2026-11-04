import type { BigQuery } from '@google-cloud/bigquery'

import type { InFlightRow } from '../oracle/validators.js'
import { type BigquerySnapshot, type SyncRecord, isBlockCursor, isHeight } from '../oracle/validators.js'

/** A raw WAL row, as `SELECT *` reads it back: the cursors and the chain are JSON strings. */
type RawSyncRow = {
  op: 'commit' | 'rollback'
  current: string | null
  finalized: string | null
  rollback_chain: string
  range_low: number | string | null
  range_high: number | string | null
  committed: boolean
}

/** A tracked table's rows, as `SELECT *` reads them back: INT64 arrives as number or string. */
type RawTableRow = { blockNumber: number | string; value: string }

export type BigqueryProbeOptions<Table extends string> = {
  client: BigQuery
  dataset: string
  /** The pipe id every WAL row is keyed by. */
  id: string
  /** The declared tables, in declaration order. */
  tables: readonly Table[]
  /** The sync table's name. */
  sync?: string
}

/**
 * The class-W store probe: one read of the pipe's WAL rows and every declared table.
 *
 * The state it reports is the one a restart recovers to, not the last commit's: an in-flight WAL row
 * carries its own recovery cursor (the pre-batch cursor for a write, the ancestor for a rollback),
 * and `getCursor` rewinds to exactly that. A first batch in flight stores no cursor at all, so the
 * store holds nothing a restart anchors on and the state reads as `undefined`, as an empty store's.
 *
 * No WAL row holds the accumulated rollback chain: each names only its own batch's unfinalized
 * blocks, and the fork search unions them when it runs. The state's chain is that union — the
 * committed rows' fragments together, trimmed to the state's floor and cursor — which is also what
 * an in-flight row's own fragment is not: it lists the blocks the run was about to write, or none
 * for a rollback, and the recovery state stands below them.
 *
 * It reports what is there and hides nothing: a value it cannot parse fails the read instead of
 * reading as absent, and only a table that does not exist reads as empty.
 */
export async function readBigquerySnapshot<Table extends string>({
  client,
  dataset,
  id,
  tables,
  sync = 'sync',
}: BigqueryProbeOptions<Table>): Promise<BigquerySnapshot<Table>> {
  const rows = await readSync(client, dataset, sync, id)

  const syncRecords: SyncRecord[] = []
  let state: BigquerySnapshot<Table>['state']
  let inFlight: InFlightRow | undefined
  /** Every block a committed row names above its floor, keyed by number: the chain the WAL holds. */
  const fragments = new Map<number, NonNullable<SyncRecord['current']>>()

  for (const row of rows) {
    const record = parseSyncRow(row, dataset)

    if (row.committed) {
      syncRecords.push(record)
      for (const block of record.rollbackChain) {
        fragments.set(block.number, block)
      }

      // A recovery ends with a committed marker row after the in-flight row it answers.
      inFlight = undefined
    } else {
      inFlight = {
        op: row.op,
        current: record.current,
        range: { low: blockOf(row.range_low, 'range_low'), high: blockOf(row.range_high, 'range_high') },
      }
    }

    state =
      row.committed || record.current !== undefined
        ? {
            current: record.current,
            finalized: record.finalized,
            rollbackChain: chainOf(fragments, record.current, record.finalized),
          }
        : undefined
  }

  const snapshots = {} as BigquerySnapshot<Table>['tables']

  for (const table of tables) {
    const tableRows = await readTable(client, dataset, table)

    snapshots[table] = {
      rows: tableRows.map((row) => {
        const block = blockOf(row.blockNumber, `${table}.blockNumber`)

        return { table, block, value: { blockNumber: BigInt(block), value: row.value } }
      }),
    }
  }

  return { sync: syncRecords, inFlight, state, tables: snapshots }
}

/** Every row the snapshot holds: table by table in declaration order, each in block order. */
export function rowsOfBigquery<Table extends string>(snapshot: BigquerySnapshot<Table>) {
  return Object.values<BigquerySnapshot['tables'][string]>(snapshot.tables).flatMap((table) => table.rows)
}

async function readSync(client: BigQuery, dataset: string, sync: string, id: string): Promise<RawSyncRow[]> {
  try {
    const [rows] = await client.query({
      query: `SELECT * FROM \`${dataset}.${sync}\` WHERE id = @id ORDER BY timestamp ASC`,
      params: { id },
    })

    return rows as RawSyncRow[]
  } catch (error) {
    // A sync table that does not exist yet holds no row; anything else is a failing read, not an empty one.
    if (/not found: table|does not exist/i.test(String(error))) {
      return []
    }

    throw error
  }
}

async function readTable(client: BigQuery, dataset: string, table: string): Promise<RawTableRow[]> {
  try {
    const [rows] = await client.query({ query: `SELECT * FROM \`${dataset}.${table}\` ORDER BY blockNumber ASC` })

    return rows as RawTableRow[]
  } catch (error) {
    if (/not found: table|does not exist/i.test(String(error))) {
      return []
    }

    throw error
  }
}

/** A WAL row's cursors and chain, as the oracle's shapes; whether they are sound is for the validators. */
function parseSyncRow(row: RawSyncRow, dataset: string): SyncRecord {
  const fail = (reason: string): never => {
    throw new Error(`a WAL row of \`${dataset}.sync\` is not readable: ${reason}`)
  }

  const current = row.current === null ? undefined : (JSON.parse(row.current) as unknown)
  if (current !== undefined && !isBlockCursor(current)) {
    fail(`current ${row.current} is not a block`)
  }

  const finalized = row.finalized === null ? undefined : (JSON.parse(row.finalized) as unknown)
  if (finalized !== undefined && !isBlockCursor(finalized)) {
    fail(`finalized ${row.finalized} is not a block`)
  }

  let rollbackChain: unknown
  try {
    rollbackChain = JSON.parse(row.rollback_chain)
  } catch {
    fail(`rollback_chain ${row.rollback_chain} is not JSON`)
  }

  if (!Array.isArray(rollbackChain) || rollbackChain.some((block) => !isBlockCursor(block))) {
    fail(`rollback_chain ${row.rollback_chain} is not a list of blocks`)
  }

  return {
    current: current as SyncRecord['current'],
    finalized: finalized as SyncRecord['finalized'],
    rollbackChain: rollbackChain as SyncRecord['rollbackChain'],
  }
}

/**
 * The accumulated chain at a state: the committed rows' fragments together — the chain the fork
 * search walks — without the entries at or below the floor or above the cursor (INV-1: the chain is
 * the processed blocks above the floor, and a state anchors at its cursor).
 */
function chainOf(
  fragments: Map<number, NonNullable<SyncRecord['current']>>,
  cursor: SyncRecord['current'],
  floor: SyncRecord['finalized'],
): SyncRecord['rollbackChain'] {
  if (cursor === undefined) {
    return []
  }

  return [...fragments.values()]
    .filter((block) => block.number <= cursor.number && (floor === undefined || block.number > floor.number))
    .sort((a, b) => a.number - b.number)
}

/** INT64 arrives as a number, or as a string when it passed 2^53; both must name a block. */
function blockOf(value: number | string | null, column: string): number {
  if (value === null) {
    throw new Error(`a WAL row's ${column} is NULL`)
  }

  const number = typeof value === 'number' ? value : Number.parseInt(value, 10)
  if (!isHeight(number)) {
    throw new Error(`a ${column} value ${String(value)} is not a block`)
  }

  return number
}
