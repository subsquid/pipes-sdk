import type { ClickHouseClient } from '@clickhouse/client'

import type { Cursor, OracleRow, OracleState } from '../oracle/reference-model.js'
import { type ClickhouseSnapshot, type SyncRecord, isHeight } from '../oracle/validators.js'

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`

/** How many times a row is in a table: its inserts less its cancel rows (IB-20). */
const NET = '_sqd_probe_net'

/** Every 64-bit integer arrives as a string, so none loses precision on the way. */
const READ_SETTINGS = { output_format_json_quote_64bit_integers: 1 } as const

type RawSync = {
  current: string
  finalized: string
  rollback_chain: string
  /** Milliseconds since the epoch: the column the binding orders its rows by. */
  written: string
  [NET]: string
}

export type ClickhouseProbeOptions<Table extends string> = {
  client: ClickHouseClient
  database: string
  /** The pipe id every sync row is keyed by. */
  id: string
  /** The declared tables, in declaration order. */
  tables: readonly Table[]
  /** The sync table; IB-20's default. */
  sync?: string
  /** The column that carries block attribution (INV-3). */
  blockColumn?: string
}

/**
 * The class-A store probe: one read of the pipe's IB-20 sync rows and every declared table, each
 * net of its cancel rows. ClickHouse has no snapshot across tables; the probe reads a store no run
 * writes to.
 *
 * It reports what is there and hides nothing: a row it cannot parse, a row cancelled more often
 * than inserted, or two sync rows the binding cannot order fail the read instead of reading as
 * absent; only a table that does not exist reads as empty. Rows are netted with `sum(sign)`, not
 * read with `FINAL`, which would fold a row inserted twice into one.
 */
export async function readClickhouseSnapshot<Table extends string>({
  client,
  database,
  id,
  tables,
  sync = 'sync',
  blockColumn = 'blockNumber',
}: ClickhouseProbeOptions<Table>): Promise<ClickhouseSnapshot<Table>> {
  const read = async <Row>(query: string, params: Record<string, unknown> = {}) => {
    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
      clickhouse_settings: READ_SETTINGS,
    })

    return result.json<Row>()
  }
  const exists = async (table: string) => {
    const [found] = await read<{ found: string }>(
      'SELECT count() AS found FROM system.tables WHERE database = {database:String} AND name = {table:String}',
      { database, table },
    )

    return found.found !== '0'
  }
  const qualified = (table: string) => `${quote(database)}.${quote(table)}`

  const raw = (await exists(sync))
    ? await read<RawSync>(
        `SELECT current, finalized, rollback_chain, toString(toUnixTimestamp64Milli(timestamp)) AS written,
           sum(sign) AS ${NET}
         FROM ${qualified(sync)} WHERE id = {id:String}
         GROUP BY current, finalized, rollback_chain, timestamp HAVING ${NET} != 0
         ORDER BY timestamp ASC`,
        { id },
      )
    : []
  requireOrdered(raw, sync)

  const records = raw.map((row, index) => parseSync(row, `${sync} row ${index}`))
  const snapshots = {} as ClickhouseSnapshot<Table>['tables']

  for (const table of tables) {
    const rows = (await exists(table))
      ? await read<Record<string, unknown>>(
          `SELECT * EXCEPT (sign), sum(sign) AS ${NET} FROM ${qualified(table)}
           GROUP BY ALL HAVING ${NET} != 0 ORDER BY ALL`,
        )
      : []

    snapshots[table] = { rows: rows.flatMap((row) => netted(row, table, blockColumn)) }
  }

  return { sync: records, state: stateOf(records), tables: snapshots }
}

/** Every row the snapshot holds: table by table in declaration order, each in block order. */
export function rowsOfClickhouse<Table extends string>(snapshot: ClickhouseSnapshot<Table>): OracleRow[] {
  return Object.values<ClickhouseSnapshot['tables'][string]>(snapshot.tables).flatMap((table) => table.rows)
}

/**
 * ⟨C, F, RC⟩ as the binding reads its sync rows (DEF-7): C and F from the newest row, the one it
 * resumes from; RC from the fragment each row committed, the newest row of each block winning, as
 * the WP-42 search walks them. A fork writes no row and deletes none (GAP-21), so the fragments of a
 * forked branch stay beside the ones that replaced it.
 */
function stateOf(sync: SyncRecord[]): OracleState | undefined {
  const newest = sync.at(-1)
  if (newest === undefined) {
    return undefined
  }

  const floor = newest.finalized?.number ?? -1
  const byBlock = new Map<number, Cursor>()

  for (const row of [...sync].reverse()) {
    for (const block of row.rollbackChain) {
      if (!byBlock.has(block.number)) {
        byBlock.set(block.number, block)
      }
    }
  }

  const kept = [...byBlock.values()].filter((block) => block.number > floor && block.number <= newest.current.number)
  const rollbackChain = kept.sort((a, b) => a.number - b.number)

  return { current: newest.current, finalized: newest.finalized, rollbackChain }
}

/** The binding takes the newest row by its timestamp; two rows in one millisecond have no order (GAP-10). */
function requireOrdered(raw: RawSync[], sync: string): void {
  const tie = raw.findIndex((row, index) => index > 0 && row.written === raw[index - 1].written)

  if (tie > 0) {
    throw new Error(
      `${sync} rows ${tie - 1} and ${tie} share the timestamp ${raw[tie].written}: no commit order (GAP-10)`,
    )
  }
}

/** A row as many times as it is net of its cancel rows. */
function netted(row: Record<string, unknown>, table: string, blockColumn: string): OracleRow[] {
  const { [NET]: net, ...columns } = row
  const count = Number(net)

  if (!Number.isSafeInteger(count) || count < 0) {
    throw new Error(`'${table}' holds ${JSON.stringify(columns)} ${String(net)} times: more cancel rows than inserts`)
  }

  const block = blockOf(columns[blockColumn], table)
  const value = { ...columns, [blockColumn]: BigInt(block) }

  return Array.from({ length: count }, () => ({ table, block, value }))
}

/** The IB-20 shape of a sync row. Whether its cursors are sound is for the validators to judge. */
function parseSync(row: RawSync, label: string): SyncRecord {
  const fail = (reason: string): never => {
    throw new Error(`${label} is not an IB-20 sync row: ${reason}`)
  }

  if (row[NET] !== '1') {
    fail(`it is in the table ${row[NET]} times`)
  }

  const current = parseJson(row.current, `current`, fail)
  if (!isCursor(current)) {
    fail(`current ${row.current} has no number and hash`)
  }

  // IB-20 writes '' for an absent finalized head; JSON null is not absence.
  const absent = row.finalized === ''
  const finalized = absent ? undefined : parseJson(row.finalized, 'finalized', fail)
  if (!absent && !isCursor(finalized)) {
    fail(`finalized ${row.finalized} has no number and hash`)
  }

  const chain = parseJson(row.rollback_chain, 'rollback_chain', fail)
  if (!Array.isArray(chain)) {
    fail(`rollback_chain ${row.rollback_chain} is not a list`)
  }

  const stray = (chain as unknown[]).find((block) => !isCursor(block))
  if (stray !== undefined) {
    fail(`rollback_chain holds ${JSON.stringify(stray)}, which has no number and hash`)
  }

  return {
    current: current as Cursor,
    finalized: finalized as Cursor | undefined,
    rollbackChain: chain as Cursor[],
  }
}

function parseJson(text: string, column: string, fail: (reason: string) => never): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return fail(`${column} ${JSON.stringify(text)} is not JSON`)
  }
}

function isCursor(value: unknown): value is Cursor {
  const cursor = value as Cursor | null

  return (
    typeof cursor === 'object' &&
    cursor !== null &&
    typeof cursor.number === 'number' &&
    typeof cursor.hash === 'string'
  )
}

/** A UInt64 arrives as a decimal string; past 2^53 it converts to an unsafe number, which the height check catches. */
function blockOf(value: unknown, table: string): number {
  const number = typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value

  if (!isHeight(number)) {
    throw new Error(`'${table}' holds a row whose block column is not a block: ${String(value)}`)
  }

  return number
}
