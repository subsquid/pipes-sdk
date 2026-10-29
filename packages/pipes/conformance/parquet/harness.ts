import { mkdtemp, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { expect } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { killChild } from '../kill/spawn.js'
import type { ReferenceModel } from '../oracle/reference-model.js'
import { type ParquetTableSnapshot, validateParquetSnapshot, validateRouteMode } from '../oracle/validators.js'
import { RANGES, S1, type S1Simulator, S1_ROWS, endStateOf, follow, s1Oracle } from '../scenarios/backfill.js'
import { PIPE_ID, TABLE_NAMES, type TableName, inStoreOrder } from '../scenarios/erc20.js'
import { it as scenarios } from '../scenarios/fixtures.js'
import type { JournalEntry, KillPoint } from './kill-switch.js'
import { streamParquet } from './pipe.js'
import { type ParquetRead, type ParquetUnit, readParquetSnapshot, rowsOf } from './probe.js'

const KILLED_RUN = fileURLToPath(new URL('./killed-run.ts', import.meta.url))

/** The files of the class-K protocol a kill point names, as path patterns relative to the store. */
export const STATE_FILE = String.raw`^_sqd_parquet_state\.${PIPE_ID}\.json$`
export const TEMP_STATE = String.raw`^\.tmp-state-`
export const ANY_UNIT = String.raw`^[a-z]+/\d{12}-\d{12}\.parquet$`
export const ANY_TEMP_UNIT = String.raw`^[a-z]+/\.tmp-\d+\.parquet$`
export const unitOf = (table: TableName) => String.raw`^${table}/\d{12}-\d{12}\.parquet$`
export const tempUnitOf = (table: TableName) => String.raw`^${table}/\.tmp-\d+\.parquet$`

/** The scenarios' portals, and a store directory of the test's own. */
export const it = scenarios.extend<{ dir: string }>({
  dir: async ({}, use) => {
    const dir = await mkdtemp(path.join(tmpdir(), 'sqd-ct-'))
    await use(dir)
    await rm(dir, { recursive: true, force: true })
  },
})

/** A read of a class-K store of the ERC-20 tables. */
export type ParquetStore = ParquetRead<TableName>

export function readParquetStore(dir: string): Promise<ParquetStore> {
  return readParquetSnapshot({ dir, id: PIPE_ID, tables: TABLE_NAMES })
}

/** The store's cursor, -1 when it holds none. */
export function cursorOf(store: ParquetStore): number {
  return store.state?.current?.number ?? -1
}

/**
 * Streams S1 into a Parquet store and returns the store as the run leaves it. It fails unless the
 * run ends in S1's end state:
 *
 * - the store passes every structural validator, and every request takes the route class K needs;
 * - the store holds exactly S1's rows, the cursor at the range end and the last floor reported;
 * - so does the oracle, which recovers from the store as the pipe does and follows every batch.
 *
 * The end state comes from the simulator, not the pipe: the oracle follows the pipe, so it agrees
 * with a pipe that stopped early.
 */
export async function runToEnd({
  simulator,
  dir,
  model = s1Oracle(),
}: {
  simulator: S1Simulator
  dir: string
  /** Pass one to watch it during the run, or to carry it across a restart or a crash. */
  model?: ReferenceModel
}): Promise<ParquetStore> {
  const before = await readParquetStore(dir)
  model.recover(before.state)

  await streamParquet({
    range: { from: S1.from, to: S1.to },
    url: simulator.chain.url,
    dir,
    logger: testLogger(),
    observe: (observation) => follow(model, simulator, observation),
  })

  const after = await readParquetStore(dir)
  const end = endStateOf(simulator)
  const violations = validateParquetSnapshot(after, RANGES)
  const offRoute = validateRouteMode('K', simulator.chain.requests)
  const rows = rowsOf(after)
  const oracleRows = inStoreOrder(model.data)

  expect(violations, 'the structural validators').toEqual([])
  expect(offRoute, 'route-mode coherence').toEqual([])
  expect(after.state, 'the stored state').toStrictEqual(end)
  expect(rows, 'the stored rows').toEqual(S1_ROWS)
  expect(model.readState(), 'the oracle state').toStrictEqual(end)
  expect(oracleRows, 'the oracle rows').toEqual(S1_ROWS)

  return after
}

/**
 * Runs S1 in a child process that kills itself with SIGKILL at `point`, and returns the store as
 * the kill left it.
 *
 * The oracle recovers from the store first, as the child does, then follows every batch the child
 * journaled before it died.
 */
export async function runKilled({
  simulator,
  dir,
  model,
  point,
}: {
  simulator: S1Simulator
  dir: string
  model: ReferenceModel
  point: KillPoint
}): Promise<ParquetStore> {
  const before = await readParquetStore(dir)
  model.recover(before.state)

  const state = `_sqd_parquet_state.${PIPE_ID}.json`
  const entries = await killChild<JournalEntry>({
    script: KILLED_RUN,
    point,
    options: { range: { from: S1.from, to: S1.to }, url: simulator.chain.url, dir, state },
  })

  for (const entry of entries) {
    if ('batch' in entry) {
      follow(model, simulator, entry.batch)
    }
  }

  return readParquetStore(dir)
}

/** What a crash left beside its commit point. */
export type CrashWindow = {
  /** Whether the store holds a commit. */
  state: boolean
  /** The stream start its start state names, when the store holds one instead of a commit. */
  start: number | undefined
  /** Per table, the units above the cursor (every unit, with no cursor), by name. */
  above: Record<TableName, string[]>
  /** The window ends of the units above the cursor: one checkpoint publishes them all at one end. */
  aboveEnds: number[]
  /** Per table, the size of each temp unit left. */
  temp: Record<TableName, number[]>
  /** How many temp state files are left. */
  tempStates: number
  /** Residue that is neither a temp unit nor a temp state. */
  unknown: string[]
}

async function crashWindowOf(dir: string, store: ParquetStore): Promise<CrashWindow> {
  const cursor = cursorOf(store)
  const matching = (pattern: string) => store.residue.filter((file) => new RegExp(pattern).test(file))
  const above = {} as Record<TableName, string[]>
  const temp = {} as Record<TableName, number[]>

  for (const table of TABLE_NAMES) {
    const units = store.tables[table].units.filter((unit) => unit.to > cursor)
    const sizes = matching(tempUnitOf(table)).map(async (file) => (await stat(path.join(dir, file))).size)

    above[table] = units.map((unit) => unit.name)
    temp[table] = await Promise.all(sizes)
  }

  const ends = TABLE_NAMES.flatMap((table) => store.tables[table].units.filter((unit) => unit.to > cursor))
  const known = new Set([...matching(ANY_TEMP_UNIT), ...matching(TEMP_STATE)])

  return {
    state: store.state !== undefined,
    start: store.start,
    above,
    aboveEnds: [...new Set(ends.map((unit) => unit.to))],
    temp,
    tempStates: matching(TEMP_STATE).length,
    unknown: store.residue.filter((file) => !known.has(file)),
  }
}

/** The store as its commit point left it: the units at or below the cursor, and no residue. */
function committedPart(store: ParquetStore): ParquetStore {
  const cursor = cursorOf(store)
  const tables = {} as Record<TableName, ParquetTableSnapshot<ParquetUnit>>

  for (const table of TABLE_NAMES) {
    const { units, next } = store.tables[table]

    tables[table] = { units: units.filter((unit) => unit.to <= cursor), next }
  }

  return { state: store.state, start: store.start, tables, residue: [] }
}

/**
 * Kills S1 at `point`, and returns the store with its crash window: what lies beside the commit
 * point. It fails unless the crash left a commit point intact (CN-40): with the window set apart,
 * the store passes every structural validator and holds exactly S1's rows up to its cursor.
 */
export async function crashAt({
  simulator,
  dir,
  model,
  point,
}: {
  simulator: S1Simulator
  dir: string
  model: ReferenceModel
  point: KillPoint
}): Promise<{ store: ParquetStore; window: CrashWindow }> {
  const store = await runKilled({ simulator, dir, model, point })
  const committed = committedPart(store)
  const cursor = cursorOf(store)
  const violations = validateParquetSnapshot(committed, RANGES)
  const rows = rowsOf(committed)
  const expected = S1_ROWS.filter((row) => row.block <= cursor)

  expect(violations, 'the committed part').toEqual([])
  expect(rows, 'the committed rows').toEqual(expected)

  return { store, window: await crashWindowOf(dir, store) }
}

/**
 * Restarts S1 in-process over what a crash left. It fails unless the first request starts right
 * after the recovered cursor and anchors on it (WP-1, IB-3), and the run ends as `runToEnd` demands.
 */
export async function resumeToEnd({
  simulator,
  dir,
  model,
  store,
}: {
  simulator: S1Simulator
  dir: string
  model: ReferenceModel
  /** The store as the restart finds it. */
  store: ParquetStore
}): Promise<void> {
  const first = simulator.chain.requests.length
  const cursor = store.state?.current

  await runToEnd({ simulator, dir, model })

  const resumed = simulator.chain.requests[first]?.query

  expect(resumed?.fromBlock, 'the first block the restart asks for').toBe(
    cursor === undefined ? S1.from : cursor.number + 1,
  )
  expect(resumed?.parentBlockHash, 'the anchor of the restart').toBe(cursor?.hash)
}
