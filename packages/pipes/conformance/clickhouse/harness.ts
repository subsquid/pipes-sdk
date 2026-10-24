import { randomBytes } from 'node:crypto'
import { Writable } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'

import { type ClickHouseClient, createClient } from '@clickhouse/client'
import { pino } from 'pino'
import { expect } from 'vitest'

import type { BlockCursor, Logger } from '~/core/index.js'
import { type MockChainPortal, testLogger } from '~/testing/index.js'

import { killChild } from '../kill/spawn.js'
import type { Cursor, OracleRow, PersistedState, ReferenceModel } from '../oracle/reference-model.js'
import { type ClickhouseSnapshot, validateClickhouseSnapshot } from '../oracle/validators.js'
import { type Observation, PIPE_ID, TABLE_NAMES, type TableName, inStoreOrder } from '../scenarios/erc20.js'
import { it as scenarios } from '../scenarios/fixtures.js'
import { type FullStreamScenario, s1FullStream, s3FullStream } from '../scenarios/full-stream.js'
import type { KillEvidence, RequestJournalEntry, RequestKillPoint } from './kill-switch.js'
import { type ClickhouseServer, type Repair, clickhouseClient, clickhouseDdl, streamClickhouse } from './pipe.js'
import { readClickhouseSnapshot, rowsOfClickhouse } from './probe.js'

const SERVER: ClickhouseServer = {
  url: process.env['TEST_CLICKHOUSE_URL'] || 'http://localhost:10123',
  username: process.env['TEST_CLICKHOUSE_USERNAME'] || 'default',
  password: process.env['TEST_CLICKHOUSE_PASSWORD'] || 'default',
}
const KILLED_RUN = fileURLToPath(new URL('./killed-run.ts', import.meta.url))

/** How long the server may take to finish the queries a killed child left. */
const QUIET_TIMEOUT_MS = 5_000

/** A database of one test's own: the tables and the pipe's sync table live in it. */
export type Database = { server: ClickhouseServer; name: string; admin: ClickHouseClient }

/**
 * The scenarios' portals, and a database of the test's own. Merges are stopped on the tables: a
 * merge folds rows that share a sorting key at a time of its own, so a row the pipe inserted twice
 * would read as once or twice depending on when the probe came. The pipe's client does not fold
 * on insert either (`clickhouseClient`).
 */
export const it = scenarios.extend<{ database: Database }>({
  database: async ({}, use) => {
    const admin = createClient(SERVER)
    const name = `ct_${randomBytes(6).toString('hex')}`
    await admin.command({ query: `CREATE DATABASE "${name}"` })

    for (const ddl of clickhouseDdl(name)) {
      await admin.command({ query: ddl })
    }

    for (const table of TABLE_NAMES) {
      await admin.command({ query: `SYSTEM STOP MERGES "${name}"."${table}"` })
    }

    await use({ server: SERVER, name, admin })

    await admin.command({ query: `DROP DATABASE "${name}" SYNC` })
    await admin.close()
  },
})

/** S1 as class A: the full stream, a cursor row after each batch's data (CN-13), no fork. */
export const S1_CLICKHOUSE = s1FullStream('A')

/** S3 as class A: an unfinalized tail, a floor that rises once, one fork. */
export const S3_CLICKHOUSE = s3FullStream('A')

/** A read of a class-A store of the ERC-20 tables. */
export type ClickhouseStore = ClickhouseSnapshot<TableName>

export function readClickhouseStore({ admin, name }: Database): Promise<ClickhouseStore> {
  return readClickhouseSnapshot({ client: admin, database: name, id: PIPE_ID, tables: TABLE_NAMES })
}

export function cursorOf(store: ClickhouseStore): Cursor | undefined {
  return store.state?.current
}

/**
 * The store as the oracle recovers from it: ⟨C, F, RC⟩, the fields CN-40 names. The rollback
 * records stay out: a fork writes no record, while the model writes one, so the two lists part at
 * the first fork whatever the store does right.
 */
function persistedOf(store: ClickhouseStore): PersistedState | undefined {
  return store.state
}

/**
 * The scenario's rows of the blocks above `from`, up to `to`: what the batches between them write.
 * `to` is a batch cursor, whose hash names the branch on a scenario that forks.
 */
export function rowsBetween<Sim extends { chain: MockChainPortal }>(
  scenario: FullStreamScenario<Sim>,
  from: BlockCursor | undefined,
  to: BlockCursor,
): OracleRow[] {
  const { hash } = to
  if (hash === undefined) {
    throw new Error(`the cursor ${to.number} has no hash, so it names no branch`)
  }

  const above = from?.number ?? -1

  return scenario.rowsUpTo({ ...to, hash }).filter((row) => row.block > above)
}

/** Names a run's requests, so the server can be watched for them. */
function applicationName(): string {
  return `ct-${randomBytes(6).toString('hex')}`
}

/**
 * Streams a scenario into the ClickHouse store, with the repair hook, and returns the store as the
 * run leaves it. It fails unless the run ends in the scenario's end state:
 *
 * - the store passes every structural validator;
 * - the store holds exactly the scenario's rows and end state;
 * - so does the oracle, which recovers from the store as the pipe does and follows every batch and
 *   fork.
 *
 * The end state comes from the scenario and the simulator, not the pipe: the oracle follows the
 * pipe, so it agrees with a pipe that stopped early.
 */
export async function runToEnd<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  database,
  model = scenario.oracle(),
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  /** Pass one to watch it during the run, or to carry it across a restart or a crash. */
  model?: ReferenceModel
}): Promise<ClickhouseStore> {
  const before = await readClickhouseStore(database)
  model.recover(persistedOf(before))

  const follower = scenario.follower(model, simulator, simulator.chain.requests.length)
  const client = clickhouseClient({ server: database.server, database: database.name, application: applicationName() })
  try {
    await streamClickhouse({
      range: scenario.range,
      url: simulator.chain.url,
      client,
      repair: 'hook',
      logger: testLogger(),
      observe: (observation) => follower.batch(observation),
      onFork: (canonical) => follower.fork(canonical),
    })
  } finally {
    await client.close()
  }

  const after = await readClickhouseStore(database)
  const end = scenario.end(simulator)
  const violations = validateClickhouseSnapshot(after, scenario.ranges)
  const rows = rowsOfClickhouse(after)
  const oracleRows = inStoreOrder(model.data)

  expect(violations, 'the structural validators').toEqual([])
  expect(after.state, 'the stored state').toStrictEqual(end.state)
  expect(rows, 'the stored rows').toEqual(end.rows)
  expect(model.readState(), 'the oracle state').toStrictEqual(end.state)
  expect(oracleRows, 'the oracle rows').toEqual(end.rows)

  return after
}

/**
 * Streams a scenario into the ClickHouse store with no oracle watching, and returns the store as
 * the run leaves it: for a run the oracle refuses, to see where the pipe still ends.
 */
export async function runUnwatched<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  database,
  repair = 'hook',
  logger = testLogger(),
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  repair?: Repair
  logger?: Logger
}): Promise<ClickhouseStore> {
  const client = clickhouseClient({ server: database.server, database: database.name, application: applicationName() })
  try {
    await streamClickhouse({
      range: scenario.range,
      url: simulator.chain.url,
      client,
      repair,
      logger,
      observe: () => {},
      onFork: () => {},
    })
  } finally {
    await client.close()
  }

  return readClickhouseStore(database)
}

/** A logger that keeps the message of every warning it is given. */
export function warningsLogger(): { logger: Logger; warnings: string[] } {
  const warnings: string[] = []
  const sink = new Writable({
    write(line, _encoding, done) {
      warnings.push(JSON.parse(String(line)).message)
      done()
    },
  })

  return { logger: pino({ level: 'warn', messageKey: 'message', base: {} }, sink), warnings }
}

/** What a killed run left. */
export type KilledRun = {
  store: ClickhouseStore
  /** The batches the child delivered before it died, in order. */
  batches: Observation[]
  /** The canonical chain of every fork the child began to roll back to, in order. */
  forks: BlockCursor[][]
  /** Where the kill landed. */
  killed: KillEvidence
}

/**
 * Runs a scenario into the ClickHouse store in a child process that kills itself with SIGKILL at
 * `point`, and returns the store once the server runs none of the child's queries: one the kill
 * left on the wire reaches the server long before the parent asks.
 *
 * The oracle recovers from the store first, as the child does, then follows every batch and fork
 * the child journaled before it died.
 */
export async function runKilled<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  database,
  model,
  point,
  repair = 'hook',
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  model: ReferenceModel
  point: RequestKillPoint
  repair?: Repair
}): Promise<KilledRun> {
  const before = await readClickhouseStore(database)
  model.recover(persistedOf(before))

  const follower = scenario.follower(model, simulator, simulator.chain.requests.length)
  const application = applicationName()
  const entries = await killChild<RequestJournalEntry>({
    script: KILLED_RUN,
    point,
    options: {
      range: scenario.range,
      url: simulator.chain.url,
      server: database.server,
      database: database.name,
      application,
      repair,
    },
    diagnose: () => serverView(database.admin, application),
  })
  await quiet(database.admin, application)

  const batches: Observation[] = []
  const forks: BlockCursor[][] = []
  for (const entry of entries) {
    if ('batch' in entry) {
      follower.batch(entry.batch)
      batches.push(entry.batch)
    }

    if ('fork' in entry) {
      follower.fork(entry.fork)
      forks.push(entry.fork)
    }
  }

  // `killChild` made sure the kill is the last line.
  const { killed, ...evidence } = entries.at(-1) as Extract<RequestJournalEntry, { killed: string }>

  return { store: await readClickhouseStore(database), batches, forks, killed: evidence }
}

/** The store as its commit point left it: the rows at or below the cursor. */
function committedPart(store: ClickhouseStore): ClickhouseStore {
  const cursor = cursorOf(store)?.number ?? -1
  const tables = {} as ClickhouseStore['tables']

  for (const table of TABLE_NAMES) {
    tables[table] = { rows: store.tables[table].rows.filter((row) => row.block <= cursor) }
  }

  return { ...store, tables }
}

/** The rows above the cursor: the class-A crash window (CN-13), table by table, each in block order. */
function windowOf(store: ClickhouseStore): OracleRow[] {
  const cursor = cursorOf(store)?.number ?? -1

  return rowsOfClickhouse(store).filter((row) => row.block > cursor)
}

/**
 * Kills a scenario's run at `point`, and returns what the kill left with its crash window: the rows
 * above the cursor. It fails unless the crash left a commit point intact (CN-40): with the window set
 * apart, the store passes every structural validator and holds exactly the scenario's rows up to
 * its cursor. Where the window lies is for each test to say: it proves the kill landed on the step
 * the test names.
 */
export async function crashAt<Sim extends { chain: MockChainPortal }>(options: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  model: ReferenceModel
  point: RequestKillPoint
  repair?: Repair
}): Promise<KilledRun & { window: OracleRow[] }> {
  const run = await runKilled(options)
  const committed = committedPart(run.store)
  const violations = validateClickhouseSnapshot(committed, options.scenario.ranges)
  const rows = rowsOfClickhouse(committed)
  const expected = options.scenario.rowsUpTo(cursorOf(run.store))

  expect(violations, 'the committed part').toEqual([])
  expect(rows, 'the committed rows').toEqual(expected)

  return { ...run, window: windowOf(run.store) }
}

/**
 * Restarts a scenario in-process over what a crash left, with the repair hook. It fails unless the
 * first request starts right after the recovered cursor and anchors on it (WP-1, IB-3), and the run
 * ends as `runToEnd` demands.
 */
export async function resumeToEnd<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  database,
  model,
  store,
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  model: ReferenceModel
  /** The store as the restart finds it. */
  store: ClickhouseStore
}): Promise<void> {
  const first = simulator.chain.requests.length
  const cursor = cursorOf(store)

  await runToEnd({ scenario, simulator, database, model })

  const resumed = simulator.chain.requests[first]?.query

  expect(resumed?.fromBlock, 'the first block the restart asks for').toBe(
    cursor === undefined ? scenario.range.from : cursor.number + 1,
  )
  expect(resumed?.parentBlockHash, 'the anchor of the restart').toBe(cursor?.hash)
}

/** The queries of `application` the server still runs. */
async function running(admin: ClickHouseClient, application: string): Promise<{ query: string; elapsed: number }[]> {
  const result = await admin.query({
    query: `SELECT query, elapsed FROM system.processes WHERE startsWith(http_user_agent, {application:String})`,
    query_params: { application: `${application} ` },
    format: 'JSONEachRow',
  })

  return result.json()
}

async function serverView(admin: ClickHouseClient, application: string): Promise<string> {
  return `the server's view of '${application}': ${JSON.stringify(await running(admin, application))}`
}

/** Waits until the server runs no query of `application`, so none of its writes is still landing. */
export async function quiet(admin: ClickHouseClient, application: string): Promise<void> {
  const deadline = Date.now() + QUIET_TIMEOUT_MS

  while (true) {
    const open = await running(admin, application)
    if (!open.length) {
      return
    }

    if (Date.now() > deadline) {
      throw new Error(`the server still runs ${open.length} queries of the killed run after ${QUIET_TIMEOUT_MS} ms`)
    }

    await sleep(20)
  }
}
