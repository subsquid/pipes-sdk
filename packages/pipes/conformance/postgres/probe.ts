import pg from 'pg'

import type { Cursor, OracleRow, OracleState, RollbackRecord } from '../oracle/reference-model.js'
import { type PostgresSnapshot, type SyncRecord, isHeight } from '../oracle/validators.js'

/** IB-21: the undo log of a tracked table, and the column that tags each record with its block. */
const UNDO_SUFFIX = '__snapshots'
const UNDO_BLOCK = '___sqd__block_number'

/** `pg` reads int8 as a string by default; the oracle compares it as the bigint it is. */
const INT8 = 20

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`

type RawSync = {
  current_number: string
  current_hash: unknown
  current_timestamp: Date | null
  finalized: unknown
  rollback_chain: unknown
}

export type PostgresProbeOptions<Table extends string> = {
  pool: pg.Pool
  schema: string
  /** The pipe id every sync row is keyed by. */
  id: string
  /** The declared tables, in declaration order. */
  tables: readonly Table[]
  /** The sync table; IB-21's default. */
  sync?: string
  /** The column that carries block attribution (INV-3). */
  blockColumn?: string
}

/**
 * The class-T store probe: one read of the pipe's IB-21 sync rows, every declared table and its
 * undo log, inside one read-only snapshot transaction.
 *
 * It reports what is there and hides nothing: a row it cannot parse fails the read instead of
 * reading as absent, and only a table that does not exist reads as empty.
 */
export async function readPostgresSnapshot<Table extends string>({
  pool,
  schema,
  id,
  tables,
  sync = 'sync',
  blockColumn = 'blockNumber',
}: PostgresProbeOptions<Table>): Promise<PostgresSnapshot<Table>> {
  const client = await pool.connect()
  let ended = false

  try {
    await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')

    const exists = async (table: string) => {
      const { rows } = await client.query<{ found: boolean }>('SELECT to_regclass($1) IS NOT NULL AS found', [
        `${quote(schema)}.${quote(table)}`,
      ])

      return rows[0].found
    }
    const read = async <Row extends pg.QueryResultRow>(text: string, values: unknown[] = []) => {
      const types = { getTypeParser: (oid: number, format?: string) => readType(oid, format) }
      const { rows } = await client.query<Row>({ text, values, types } as pg.QueryConfig)

      return rows
    }

    const raw = (await exists(sync))
      ? await read<RawSync>(
          `SELECT s.current_number::text AS current_number, s.current_hash, s."current_timestamp", s.finalized, s.rollback_chain
           FROM ${quote(schema)}.${quote(sync)} s WHERE s.id = $1 ORDER BY s.current_number ASC`,
          [id],
        )
      : []
    const records = raw.map((row, index) => parseSync(row, `${sync} row ${index}`))
    const snapshots = {} as PostgresSnapshot<Table>['tables']

    for (const table of tables) {
      const rows = (await exists(table))
        ? await read<Record<string, unknown>>(
            `SELECT * FROM ${quote(schema)}.${quote(table)} ORDER BY ${quote(blockColumn)}`,
          )
        : []
      const undoTable = `${table}${UNDO_SUFFIX}`
      const undo = (await exists(undoTable))
        ? await read<{ block: bigint }>(
            `SELECT ${quote(UNDO_BLOCK)} AS block FROM ${quote(schema)}.${quote(undoTable)} ORDER BY 1`,
          )
        : []

      snapshots[table] = {
        rows: rows.map((value) => ({ table, block: blockOf(value[blockColumn], table), value })),
        undo: undo.map((record) => blockOf(record.block, undoTable)),
      }
    }

    await client.query('COMMIT')
    ended = true

    return { sync: records, ...assemble(records), tables: snapshots }
  } finally {
    // A connection a failed read left inside its transaction goes, rather than back to the pool.
    client.release(!ended)
  }
}

/** Every row the snapshot holds: table by table in declaration order, each in block order. */
export function rowsOfPostgres<Table extends string>(snapshot: PostgresSnapshot<Table>): OracleRow[] {
  return Object.values<PostgresSnapshot['tables'][string]>(snapshot.tables).flatMap((table) => table.rows)
}

function readType(oid: number, format?: string) {
  if (oid === INT8) {
    return (value: string) => BigInt(value)
  }

  return pg.types.getTypeParser(oid, format as 'text')
}

/**
 * ⟨C, F, RC⟩ and the rollback records from the sync rows (DEF-7): each row carries the chain
 * fragment of its own batch, so a record's chain is every fragment up to it, above its floor and at
 * or below its cursor. C and F are the newest row's.
 */
function assemble(sync: SyncRecord[]): { state: OracleState | undefined; records: RollbackRecord[] } {
  const records: RollbackRecord[] = []
  const seen: Cursor[] = []
  let state: OracleState | undefined

  for (const row of sync) {
    seen.push(...row.rollbackChain)

    // A block in two fragments stays twice, so the watermark check reports the chain as not ascending.
    const floor = row.finalized?.number ?? -1
    const kept = seen.filter((block) => block.number > floor && block.number <= row.current.number)
    const rollbackChain = kept.sort((a, b) => a.number - b.number)

    records.push({ rollbackChain, finalized: row.finalized })
    state = { current: row.current, finalized: row.finalized, rollbackChain }
  }

  return { state, records }
}

/** The IB-21 shape of a sync row. Whether its cursors are sound is for the validators to judge. */
function parseSync(row: RawSync, label: string): SyncRecord {
  const fail = (reason: string): never => {
    throw new Error(`${label} is not an IB-21 sync row: ${reason}`)
  }

  const number = Number(row.current_number)
  if (!isHeight(number) || String(number) !== row.current_number) {
    fail(`current_number ${row.current_number} is not a block`)
  }

  if (typeof row.current_hash !== 'string') {
    fail('current_hash is not text')
  }

  const current: Cursor = { number, hash: row.current_hash as string }
  if (row.current_timestamp !== null) {
    current.timestamp = row.current_timestamp.getTime() / 1000
  }

  // IB-21 writes `{}` for an absent finalized head; SQL NULL and JSON null are not absence.
  const finalized = row.finalized
  const finalizedSound = typeof finalized === 'object' && finalized !== null && !Array.isArray(finalized)
  if (!finalizedSound) {
    fail(`finalized ${JSON.stringify(finalized)} is not an object`)
  }

  const absent = Object.keys(finalized as object).length === 0
  if (!absent && !isCursor(finalized)) {
    fail(`finalized ${JSON.stringify(finalized)} has no number and hash`)
  }

  if (!Array.isArray(row.rollback_chain)) {
    fail(`rollback_chain ${JSON.stringify(row.rollback_chain)} is not a list`)
  }

  const stray = (row.rollback_chain as unknown[]).find((block) => !isCursor(block))
  if (stray !== undefined) {
    fail(`rollback_chain holds ${JSON.stringify(stray)}, which has no number and hash`)
  }

  return {
    current,
    finalized: absent ? undefined : (finalized as Cursor),
    rollbackChain: row.rollback_chain as Cursor[],
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

/** A bigint past 2^53 converts to an unsafe number, so the height check also catches lost precision. */
function blockOf(value: unknown, table: string): number {
  const number = typeof value === 'bigint' ? Number(value) : value

  if (typeof number !== 'number' || !isHeight(number)) {
    throw new Error(`'${table}' holds a row whose block column is not a block: ${String(value)}`)
  }

  return number
}
