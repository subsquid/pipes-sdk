import { randomBytes } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'

import pg from 'pg'
import { expect } from 'vitest'

import type { BlockCursor } from '~/core/index.js'
import { type MockChainPortal, testLogger } from '~/testing/index.js'

import { killChild } from '../kill/spawn.js'
import type { PersistedState, ReferenceModel } from '../oracle/reference-model.js'
import { type PostgresSnapshot, validatePostgresSnapshot } from '../oracle/validators.js'
import { type Observation, PIPE_ID, TABLE_NAMES, type TableName, inStoreOrder } from '../scenarios/erc20.js'
import { it as scenarios } from '../scenarios/fixtures.js'
import { type FullStreamScenario, s1FullStream, s3FullStream } from '../scenarios/full-stream.js'
import type { KillEvidence, StatementJournalEntry, StatementKillPoint } from './kill-switch.js'
import { postgresDdl, postgresPool, streamPostgres } from './pipe.js'
import { readPostgresSnapshot, rowsOfPostgres } from './probe.js'

const DSN = process.env['TEST_POSTGRES_DSN'] || 'postgresql://postgres:postgres@localhost:5432/postgres'
const KILLED_RUN = fileURLToPath(new URL('./killed-run.ts', import.meta.url))

/** How long the server may take to see a killed child's connections drop. */
const DISCONNECT_TIMEOUT_MS = 5_000

/** The statements of the class-T protocol a kill point names, as SQL patterns. */
export const COMMIT = String.raw`^commit$`
export const CURSOR_WRITE = String.raw`^\s*insert into "[^"]+"\."sync"`
/** The fork drops the sync rows above the ancestor; retention drops those at or below a safe block. */
export const FORKED_ROWS = String.raw`^\s*delete from "[^"]+"\."sync"\s+where "id" = \$1 and "current_number" > `
/** The undo-log set-up takes this lock. */
export const TRIGGER_SET_UP = 'sqd_drizzle_triggers'

/** The batch transaction whose batch ends at `block`: it tags its undo records with that block. */
export const batchAt = (block: number) => String.raw`snapshot_block_number = ${block}\b`

/** A schema of one test's own: the tables, the pipe's sync table and the undo logs all live in it. */
export type Database = { dsn: string; schema: string; admin: pg.Pool }

/** The scenarios' portals, and a schema of the test's own. */
export const it = scenarios.extend<{ database: Database }>({
  database: async ({}, use) => {
    const admin = new pg.Pool({ connectionString: DSN })
    const schema = `ct_${randomBytes(6).toString('hex')}`
    await admin.query(`CREATE SCHEMA "${schema}";\n${postgresDdl(schema)}`)

    await use({ dsn: DSN, schema, admin })

    await admin.query(`DROP SCHEMA "${schema}" CASCADE`)
    await admin.end()
  },
})

/** S1 as class T: the full stream, one commit per batch (CN-10), no fork. */
export const S1_POSTGRES = s1FullStream('T')

/** S3 as class T: an unfinalized tail, a floor that rises once, one fork. */
export const S3_POSTGRES = s3FullStream('T')

/** A read of a class-T store of the ERC-20 tables. */
export type PostgresStore = PostgresSnapshot<TableName>

export function readPostgresStore({ admin, schema }: Database): Promise<PostgresStore> {
  return readPostgresSnapshot({ pool: admin, schema, id: PIPE_ID, tables: TABLE_NAMES })
}

/**
 * The store as the oracle recovers from it: ⟨C, F, RC⟩, the fields CN-40 names. The rollback
 * records stay out: a fork drops the forked ones from the store, while the model keeps every
 * record it wrote, so the two lists part at the first fork whatever the store does right.
 */
function persistedOf(store: PostgresStore): PersistedState | undefined {
  return store.state
}

/** Names a run's connections, so the server can be watched for them. */
function applicationName(): string {
  return `ct-${randomBytes(6).toString('hex')}`
}

/**
 * Streams a scenario into the Postgres store and returns the store as the run leaves it. It fails
 * unless the run ends in the scenario's end state:
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
}): Promise<PostgresStore> {
  const before = await readPostgresStore(database)
  model.recover(persistedOf(before))

  const follower = scenario.follower(model, simulator, simulator.chain.requests.length)
  const pool = postgresPool({ dsn: database.dsn, schema: database.schema, application: applicationName() })
  try {
    await streamPostgres({
      range: scenario.range,
      url: simulator.chain.url,
      pool,
      schema: database.schema,
      logger: testLogger(),
      observe: (observation) => follower.batch(observation),
      onFork: (canonical) => follower.fork(canonical),
    })
  } finally {
    await pool.end()
  }

  const after = await readPostgresStore(database)
  const end = scenario.end(simulator)
  const violations = validatePostgresSnapshot(after, scenario.ranges)
  const rows = rowsOfPostgres(after)
  const oracleRows = inStoreOrder(model.data)

  expect(violations, 'the structural validators').toEqual([])
  expect(after.state, 'the stored state').toStrictEqual(end.state)
  expect(rows, 'the stored rows').toEqual(end.rows)
  expect(model.readState(), 'the oracle state').toStrictEqual(end.state)
  expect(oracleRows, 'the oracle rows').toEqual(end.rows)

  return after
}

/**
 * Streams a scenario into the Postgres store with no oracle watching, and returns the store as the
 * run leaves it: for a restart the oracle refuses, to see where the pipe still ends.
 */
export async function runUnwatched<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  database,
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
}): Promise<PostgresStore> {
  const pool = postgresPool({ dsn: database.dsn, schema: database.schema, application: applicationName() })
  try {
    await streamPostgres({
      range: scenario.range,
      url: simulator.chain.url,
      pool,
      schema: database.schema,
      logger: testLogger(),
      observe: () => {},
      onFork: () => {},
    })
  } finally {
    await pool.end()
  }

  return readPostgresStore(database)
}

/** What a killed run left. */
export type KilledRun = {
  store: PostgresStore
  /** The batches the child delivered before it died, in order. */
  batches: Observation[]
  /** The canonical chain of every fork the child began to roll back to, in order. */
  forks: BlockCursor[][]
  /** Where the kill landed. */
  killed: KillEvidence
}

/**
 * Runs a scenario into the Postgres store in a child process that kills itself with SIGKILL at
 * `point`, and returns the store once the server has dropped the child's connections, and with
 * them any transaction the child held open.
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
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  model: ReferenceModel
  point: StatementKillPoint
}): Promise<KilledRun> {
  const before = await readPostgresStore(database)
  model.recover(persistedOf(before))

  const follower = scenario.follower(model, simulator, simulator.chain.requests.length)
  const application = applicationName()
  const entries = await killChild<StatementJournalEntry>({
    script: KILLED_RUN,
    point,
    options: {
      range: scenario.range,
      url: simulator.chain.url,
      dsn: database.dsn,
      schema: database.schema,
      application,
    },
    diagnose: () => serverView(database.admin, application),
  })
  await disconnected(database.admin, application)

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
  const { killed, ...evidence } = entries.at(-1) as Extract<StatementJournalEntry, { killed: string }>

  return { store: await readPostgresStore(database), batches, forks, killed: evidence }
}

/**
 * Kills a scenario's run at `point`, and returns what the kill left. It fails unless the crash left
 * a commit point intact (CN-40). Class T has no crash window, so the whole store must pass every
 * structural validator and hold exactly the scenario's rows up to its cursor.
 */
export async function crashAt<Sim extends { chain: MockChainPortal }>(options: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  model: ReferenceModel
  point: StatementKillPoint
}): Promise<KilledRun> {
  const run = await runKilled(options)
  const violations = validatePostgresSnapshot(run.store, options.scenario.ranges)
  const rows = rowsOfPostgres(run.store)
  const expected = options.scenario.rowsUpTo(run.store.state?.current)

  expect(violations, 'the store').toEqual([])
  expect(rows, 'the committed rows').toEqual(expected)

  return run
}

/**
 * Restarts a scenario in-process over what a crash left. It fails unless the first request starts
 * right after the recovered cursor and anchors on it (WP-1, IB-3), and the run ends as `runToEnd`
 * demands.
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
  store: PostgresStore
}): Promise<void> {
  const first = simulator.chain.requests.length
  const cursor = store.state?.current

  await runToEnd({ scenario, simulator, database, model })

  const resumed = simulator.chain.requests[first]?.query

  expect(resumed?.fromBlock, 'the first block the restart asks for').toBe(
    cursor === undefined ? scenario.range.from : cursor.number + 1,
  )
  expect(resumed?.parentBlockHash, 'the anchor of the restart').toBe(cursor?.hash)
}

/** What the server shows of `application`'s connections, and of every advisory lock held or awaited. */
async function serverView(admin: pg.Pool, application: string): Promise<string> {
  const { rows: connections } = await admin.query(
    `SELECT pid, state, wait_event_type, wait_event, now() - xact_start AS in_transaction, left(query, 120) AS query
     FROM pg_stat_activity WHERE application_name = $1`,
    [application],
  )
  const { rows: locks } = await admin.query(
    `SELECT l.pid, a.application_name, l.granted, l.objid, a.state, left(a.query, 120) AS query
     FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory'`,
  )

  return `the server's view of '${application}': ${JSON.stringify(connections)}\nadvisory locks: ${JSON.stringify(locks)}`
}

/** Waits until the server holds no connection of `application`, so none of its transactions is open. */
export async function disconnected(admin: pg.Pool, application: string): Promise<void> {
  const deadline = Date.now() + DISCONNECT_TIMEOUT_MS

  while (true) {
    const { rows } = await admin.query<{ open: number }>(
      'SELECT count(*)::int AS open FROM pg_stat_activity WHERE application_name = $1',
      [application],
    )
    const open = rows[0].open
    if (open === 0) {
      return
    }

    if (Date.now() > deadline) {
      throw new Error(`the server still holds ${open} connections of the killed run after ${DISCONNECT_TIMEOUT_MS} ms`)
    }

    await sleep(20)
  }
}
