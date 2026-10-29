import { readFile, readdir } from 'node:fs/promises'
import path from 'node:path'

import { ParquetReader } from '@dsnp/parquetjs'

import type { Cursor, OracleRow, OracleState } from '../oracle/reference-model.js'
import { type ParquetSnapshot, type ParquetTableSnapshot, type PublishedUnit, isHeight } from '../oracle/validators.js'

/** IB-22: published data files are `<table>/<from>-<to>.parquet`, both bounds zero-padded to 12 digits. */
const DATA_FILE = /^(\d{12})-(\d{12})\.parquet$/

/** IB-22: the fields of the state sidecar. A field outside them is a format change, not something to skip. */
const STATE_FIELDS: ReadonlySet<string> = new Set(['id', 'cursor', 'finalized', 'coverage'])

/** IB-22: the cursor of the start state, written before the first checkpoint publishes. */
type StartCursor = { kind: 'start'; number: number }

type ParquetStateFile = {
  id?: string
  cursor: Cursor | StartCursor
  finalized?: Cursor
  coverage?: Record<string, number>
}

function isStartCursor(cursor: Cursor | StartCursor): cursor is StartCursor {
  return 'kind' in cursor
}

/** A published unit with its rows as the store holds them. */
export type ParquetUnit = PublishedUnit & { rows: OracleRow[] }

/** One read of a class-K store: the snapshot the validators judge, and the start state beside it. */
export type ParquetRead<Table extends string> = ParquetSnapshot<ParquetUnit, Table> & {
  /**
   * The stream's first block, when the sidecar is a start state (IB-22): the pipe began its first
   * checkpoint and committed nothing, so `state` is `undefined`.
   */
  start: number | undefined
}

export type ParquetProbeOptions<Table extends string> = {
  dir: string
  /** The pipe id the state sidecar is namespaced by. */
  id: string
  /** The declared tables, in declaration order. Anything else in the store is residue. */
  tables: readonly Table[]
  /** The column that carries block attribution (INV-3). */
  blockColumn?: string
}

/**
 * The class-K store probe: one read of the IB-22 state sidecar, every declared table's published
 * units, and whatever else the store holds.
 *
 * It reports what is there and hides nothing: a store it cannot read or parse fails the read
 * instead of reading as empty, and a file that is neither a unit nor the state is residue.
 */
export async function readParquetSnapshot<Table extends string>({
  dir,
  id,
  tables,
  blockColumn = 'blockNumber',
}: ParquetProbeOptions<Table>): Promise<ParquetRead<Table>> {
  const stateName = `_sqd_parquet_state.${id}.json`
  const statePath = path.join(dir, stateName)

  const raw = await orAbsent(readFile(statePath, 'utf8'))
  const stateFile = raw === undefined ? undefined : parseState(raw, statePath, id)
  const rootNames = (await orAbsent(readdir(dir))) ?? []

  const declared: readonly string[] = tables
  const residue = rootNames.filter((name) => name !== stateName && !declared.includes(name))
  const snapshots = {} as Record<Table, ParquetTableSnapshot<ParquetUnit>>

  for (const table of tables) {
    const tableDir = path.join(dir, table)
    const names = (await orAbsent(readdir(tableDir))) ?? []
    const units: ParquetUnit[] = []

    for (const name of names.sort()) {
      const match = DATA_FILE.exec(name)
      if (!match) {
        residue.push(path.join(table, name))

        continue
      }

      const rows = await readUnitRows(path.join(tableDir, name), table, blockColumn)
      units.push({ name, from: Number(match[1]), to: Number(match[2]), rows })
    }

    snapshots[table] = { units, next: stateFile?.coverage?.[table] }
  }

  const cursor = stateFile?.cursor
  const start = cursor !== undefined && isStartCursor(cursor) ? cursor.number + 1 : undefined

  // IB-22 has no chain field: the finalized-only route keeps no fork history (WP-47).
  const state: OracleState | undefined =
    cursor === undefined || isStartCursor(cursor)
      ? undefined
      : { current: cursor, finalized: stateFile?.finalized, rollbackChain: [] }

  return { state, start, tables: snapshots, residue: residue.sort() }
}

/** Every row the snapshot holds: table by table in declaration order, each in window order. */
export function rowsOf<Table extends string>(snapshot: ParquetSnapshot<ParquetUnit, Table>): OracleRow[] {
  const tables = Object.values<ParquetTableSnapshot<ParquetUnit>>(snapshot.tables)

  return tables.flatMap((table) => table.units.flatMap((unit) => unit.rows))
}

/** Only a missing path reads as absent; any other failure is the store's and fails the read. */
async function orAbsent<T>(read: Promise<T>): Promise<T | undefined> {
  try {
    return await read
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return undefined
    }

    throw error
  }
}

/** The IB-22 shape of the sidecar. Whether its cursors are sound is for the validators to judge. */
function parseState(raw: string, statePath: string, id: string): ParquetStateFile {
  const fail = (reason: string): never => {
    throw new Error(`'${statePath}' is not an IB-22 state record: ${reason}`)
  }
  const isCursor = (value: unknown): value is Cursor => {
    const cursor = value as Cursor | null

    return (
      typeof cursor === 'object' &&
      cursor !== null &&
      typeof cursor.number === 'number' &&
      typeof cursor.hash === 'string'
    )
  }

  const state = JSON.parse(raw) as Record<string, unknown> | null
  if (typeof state !== 'object' || state === null) {
    fail('not an object')
  }

  const { id: stateId, cursor, finalized, coverage } = state as Record<string, unknown>

  const unknown = Object.keys(state as object).find((field) => !STATE_FIELDS.has(field))
  if (unknown !== undefined) {
    fail(`it holds the field '${unknown}', which IB-22 does not define`)
  }

  if (stateId !== undefined && stateId !== id) {
    fail(`it names the pipe '${String(stateId)}', not '${id}'`)
  }

  if (isStartState(cursor)) {
    return parseStart(cursor, { finalized, coverage }, fail, stateId as string | undefined)
  }

  if (!isCursor(cursor)) {
    fail('the cursor is missing or has no number and hash')
  }

  // IB-22 omits an absent finalized head; a `null` one is not absence.
  if (finalized !== undefined && !isCursor(finalized)) {
    fail('the finalized head has no number and hash')
  }

  const coverageMap = typeof coverage === 'object' && coverage !== null && !Array.isArray(coverage)
  if (coverage !== undefined && !coverageMap) {
    fail('the coverage is not a map of tables to window starts')
  }

  const starts = Object.values((coverage ?? {}) as Record<string, unknown>)
  const startsSound = starts.every((start) => typeof start === 'number' && isHeight(start))
  if (!startsSound) {
    fail('a coverage start is not a block')
  }

  return {
    id: stateId as string | undefined,
    cursor: cursor as Cursor,
    finalized: finalized as Cursor | undefined,
    coverage: coverage as Record<string, number> | undefined,
  }
}

/** A cursor with a `kind` claims to be a start cursor; `parseStart` judges the claim. */
function isStartState(cursor: unknown): cursor is object {
  return typeof cursor === 'object' && cursor !== null && 'kind' in cursor
}

/** IB-22: a start cursor is one block below the stream start, and the state records nothing beside it. */
function parseStart(
  cursor: object,
  { finalized, coverage }: { finalized: unknown; coverage: unknown },
  fail: (reason: string) => never,
  id: string | undefined,
): ParquetStateFile {
  const { kind, number, ...rest } = cursor as { kind: unknown; number: unknown }
  const belowBlock = typeof number === 'number' && isHeight(number + 1)

  if (kind !== 'start' || !belowBlock || Object.keys(rest).length > 0) {
    fail(`the cursor ${JSON.stringify(cursor)} is not a start cursor one block below a block`)
  }

  if (finalized !== undefined || coverage !== undefined) {
    fail('a start state carries a finalized head or coverage')
  }

  return { id, cursor: { kind: 'start', number: number as number } }
}

async function readUnitRows(filePath: string, table: string, blockColumn: string): Promise<OracleRow[]> {
  const reader = await ParquetReader.openFile(filePath)
  const rows: OracleRow[] = []

  try {
    const cursor = reader.getCursor()

    // Values stay as the reader returns them (INT64 as bigint), so the oracle diff is exact.
    for (let raw = await cursor.next(); raw; raw = await cursor.next()) {
      const value = raw as Record<string, unknown>
      rows.push({ table, block: blockNumberOf(value[blockColumn], filePath), value })
    }
  } finally {
    await reader.close()
  }

  return rows
}

/** A bigint past 2^53 converts to an unsafe number, so the height check also catches lost precision. */
function blockNumberOf(value: unknown, filePath: string): number {
  const number = typeof value === 'bigint' ? Number(value) : value

  if (typeof number !== 'number' || !isHeight(number)) {
    throw new Error(`'${filePath}' holds a row whose block column is not a block: ${String(value)}`)
  }

  return number
}
