import { randomBytes } from 'node:crypto'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'

import { expect } from 'vitest'

import type { BlockCursor } from '~/core/index.js'
import { type MockChainPortal, testLogger } from '~/testing/index.js'

import { killChild } from '../kill/spawn.js'
import type { Cursor, OracleRow, PersistedState, ReferenceModel } from '../oracle/reference-model.js'
import { type BigquerySnapshot, validateBigquerySnapshot, validateBigqueryWindow } from '../oracle/validators.js'
import { type Observation, PIPE_ID, TABLE_NAMES, type TableName, inStoreOrder } from '../scenarios/erc20.js'
import { it as scenarios } from '../scenarios/fixtures.js'
import { type FullStreamScenario, type ScenarioPortal, s1FullStream, s3FullStream } from '../scenarios/full-stream.js'
import type { CallJournalEntry, CallKillPoint, KillEvidence } from './kill-switch.js'
import { bigqueryAdmin, streamBigquery } from './pipe.js'
import { readBigquerySnapshot, rowsOfBigquery } from './probe.js'

/** Set to the project the harness may write; without it the whole class-W suite skips. */
const PROJECT = process.env['BIGQUERY_TEST_PROJECT']
const KILLED_RUN = fileURLToPath(new URL('./killed-run.ts', import.meta.url))

/** A killed run may have to cross a wide network before it reaches its point. */
const KILL_TIMEOUT_MS = 180_000
/** How long a created DML job may take to finish once its creator is dead. */
const JOB_TIMEOUT_MS = 60_000
/** How long the store may take to stop changing after the last job finished. */
const SETTLE_TIMEOUT_MS = 30_000

/** A dataset of one test's own: the pipe's tracked tables and its sync table live in it. */
export type Database = { projectId: string; dataset: string; admin: ReturnType<typeof bigqueryAdmin> }

/**
 * The scenarios' portals, and a dataset of one test's own. The pipe creates its tables inside the
 * dataset; the fixture drops the dataset with them, so a failed run leaves nothing behind.
 */
export const it = scenarios.extend<{ database: Database }>({
  database: async ({}, use) => {
    const projectId = PROJECT as string
    const admin = bigqueryAdmin(projectId)
    const dataset = `ctw_${randomBytes(6).toString('hex')}`

    await admin.createDataset(dataset)

    await use({ projectId, dataset, admin })

    // `force` drops the tables with the dataset; the BigQuery client keeps no pool to close.
    await admin.dataset(dataset).delete({ force: true })
  },
})

/** S1 as class W: the full stream, a WAL bracket around every batch (CN-14), no fork. */
export const S1_BIGQUERY = s1FullStream('W')

/** S3 as class W: an unfinalized tail, a floor that rises once, one fork. */
export const S3_BIGQUERY = s3FullStream('W')

/** A read of a class-W store of the ERC-20 tables. */
export type BigqueryStore = BigquerySnapshot<TableName>

export function readBigqueryStore({ admin, projectId, dataset }: Database): Promise<BigqueryStore> {
  return readBigquerySnapshot({ client: admin, dataset, id: PIPE_ID, tables: TABLE_NAMES })
}

export function cursorOf(store: BigqueryStore): Cursor | undefined {
  return store.state?.current
}

/**
 * The store as the oracle recovers from it: ⟨C, F, RC⟩, the fields CN-40 names. The rollback records
 * stay out: the store's WAL holds a rollback marker where the model keeps the record itself, so the
 * two lists part at the first fork whatever the store does right.
 */
function persistedOf(store: BigqueryStore): PersistedState | undefined {
  return store.state
}

/**
 * Streams a scenario into the BigQuery store and returns the store as the run leaves it. It fails
 * unless the run ends in the scenario's end state:
 *
 * - the store passes every structural validator, and no WAL row is in flight;
 * - the store holds exactly the scenario's rows and end state;
 * - so does the oracle, which recovers from the store as the pipe does and follows every batch and
 *   fork.
 *
 * The end state comes from the scenario and the simulator, not the pipe: the oracle follows the
 * pipe, so it agrees with a pipe that stopped early.
 */
export async function runToEnd<Sim extends { chain: ScenarioPortal }>({
  scenario,
  simulator,
  database,
  model = scenario.oracle(),
  onRollback,
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  /** Pass one to watch it during a run, or to carry it across a restart or a crash. */
  model?: ReferenceModel
  onRollback?: (cursor: BlockCursor) => void
}): Promise<BigqueryStore> {
  const before = await readBigqueryStore(database)
  model.recover(persistedOf(before))

  const follower = scenario.follower(model, simulator, simulator.chain.requests.length)
  await streamBigquery({
    range: scenario.range,
    url: simulator.chain.url,
    projectId: database.projectId,
    dataset: database.dataset,
    logger: testLogger(),
    observe: (observation) => follower.batch(observation),
    onFork: (canonical) => follower.fork(canonical),
    onRollback,
  })

  const after = await readBigqueryStore(database)
  const end = scenario.end(simulator)
  const violations = validateBigquerySnapshot(after, scenario.ranges)
  const rows = rowsOfBigquery(after)
  const oracleRows = inStoreOrder(model.data)

  expect(violations, 'the structural validators').toEqual([])
  expect(after.inFlight, 'the in-flight WAL row').toBeUndefined()
  expect(after.state, 'the stored state').toStrictEqual(end.state)
  expect(rows, 'the stored rows').toEqual(end.rows)
  expect(model.readState(), 'the oracle state').toStrictEqual(end.state)
  expect(oracleRows, 'the oracle rows').toEqual(end.rows)

  return after
}

/** What a killed run left. */
export type KilledRun = {
  store: BigqueryStore
  /** The batches the child delivered before it died, in order. */
  batches: Observation[]
  /** The canonical chain of every fork the child began to roll back to, in order. */
  forks: BlockCursor[][]
  /** Where the kill landed. */
  killed: KillEvidence
}

/**
 * Runs a scenario into the BigQuery store in a child process that kills itself with SIGKILL at
 * `point`, and returns the store once nothing of the child's is still landing: every DML job the
 * child created has finished, and the store has stopped changing.
 *
 * The oracle recovers from the store first, as the child does, then follows every batch and fork the
 * child journaled before it died.
 */
export async function runKilled<Sim extends { chain: MockChainPortal }>(options: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  model: ReferenceModel
  point: CallKillPoint
}): Promise<KilledRun> {
  const journal = await runKilledOverlapping(options)

  await settled(options.database, journal.jobs)

  return { ...journal, store: await readBigqueryStore(options.database) }
}

/** The journal of a killed run, handed over at once: what it delivered, and the jobs it left running. */
export type KilledJournal = {
  batches: Observation[]
  forks: BlockCursor[][]
  killed: KillEvidence
  /** Every DML job the killed run created, in order; it may still be running. */
  jobs: string[]
}

/**
 * Runs a scenario into the store in a child that kills itself at `point`, and hands its journal over
 * at once: no read of the store, no wait for the killed run's DML jobs. The caller restarts while
 * those jobs still run — the way a supervisor restarts the process — and must settle them before it
 * trusts the store. Use `runKilled` unless the restart must overlap them.
 */
export async function runKilledOverlapping<Sim extends { chain: MockChainPortal }>({
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
  point: CallKillPoint
}): Promise<KilledJournal> {
  const before = await readBigqueryStore(database)
  model.recover(persistedOf(before))

  const follower = scenario.follower(model, simulator, simulator.chain.requests.length)
  const scratch = await mkdtemp(path.join(tmpdir(), 'sqd-ct2-bigquery-'))
  const jobs = path.join(scratch, 'jobs.txt')

  let entries: CallJournalEntry[]
  let ids: string[] = []
  try {
    entries = await killChild<CallJournalEntry>({
      script: KILLED_RUN,
      point,
      options: {
        range: scenario.range,
        url: simulator.chain.url,
        projectId: database.projectId,
        dataset: database.dataset,
        jobs,
      },
      timeoutMs: KILL_TIMEOUT_MS,
    })

    ids = await readJobIds(jobs)
  } finally {
    await rm(scratch, { recursive: true, force: true })
  }

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
  const { killed, ...evidence } = entries.at(-1) as Extract<CallJournalEntry, { killed: string }>

  return { batches, forks, killed: evidence, jobs: ids }
}

/** The store as its commit point left it: the rows at or below the recovery cursor. */
function committedPart(store: BigqueryStore): BigqueryStore {
  const cursor = cursorOf(store)?.number ?? -1
  const tables = {} as BigqueryStore['tables']

  for (const table of TABLE_NAMES) {
    tables[table] = { rows: store.tables[table].rows.filter((row) => row.block <= cursor) }
  }

  return { ...store, tables }
}

/** The rows above the recovery cursor: the class-W crash window (CN-14), table by table, in block order. */
function windowOf(store: BigqueryStore): OracleRow[] {
  const cursor = cursorOf(store)?.number ?? -1

  return rowsOfBigquery(store).filter((row) => row.block > cursor)
}

/**
 * Kills a scenario's run at `point`, and returns what the kill left with its crash window: the rows
 * above the recovery cursor. It fails unless the crash left a commit point intact (CN-40) — with the
 * window set apart, the store passes every structural validator and holds exactly the scenario's
 * rows up to its cursor — and unless every row above the cursor lies inside the range the in-flight
 * WAL row names (INV-42): a row outside it is residue the store cannot explain. Which rows the
 * window holds is for each test to say: it proves the kill landed on the step the test names.
 */
export async function crashAt<Sim extends { chain: MockChainPortal }>(options: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
  model: ReferenceModel
  point: CallKillPoint
}): Promise<KilledRun & { window: OracleRow[] }> {
  const run = await runKilled(options)
  const committed = committedPart(run.store)
  const violations = validateBigquerySnapshot(committed, options.scenario.ranges)
  const rows = rowsOfBigquery(committed)
  const expected = options.scenario.rowsUpTo(cursorOf(run.store))

  expect(violations, 'the committed part').toEqual([])
  expect(rows, 'the committed rows').toEqual(expected)
  expect(validateBigqueryWindow(run.store), 'the crash window').toEqual([])

  return { ...run, window: windowOf(run.store) }
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
  store: BigqueryStore
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

/**
 * Waits until nothing of the killed child is still landing: first every DML job it created — a job
 * runs on once its creator dies — then the store itself, which stops changing only after the last of
 * them finished and any append still on the wire arrived.
 */
async function settled(database: Database, jobs: string[]): Promise<void> {
  await settledJobs(database, jobs)

  const deadline = Date.now() + SETTLE_TIMEOUT_MS
  let counts = await rowCounts(database)

  while (true) {
    await sleep(1_500)
    const again = await rowCounts(database)

    if (counts === again) {
      return
    }

    if (Date.now() > deadline) {
      throw new Error(`the store still changes after ${SETTLE_TIMEOUT_MS} ms: ${counts} became ${again}`)
    }

    counts = again
  }
}

/** The ids the killed child noted, one per DML job whose creation resolved before it died. */
async function readJobIds(jobs: string): Promise<string[]> {
  return readFile(jobs, 'utf-8')
    .then((text) => text.split('\n').filter((line) => line.length > 0))
    .catch(() => [] as string[])
}

/** Waits until BigQuery runs none of these jobs: the killed run's last writes have landed. */
export async function settledJobs(database: Database, jobs: string[]): Promise<void> {
  for (const jobId of jobs) {
    await jobDone(database, jobId)
  }
}

/**
 * Runs one DELETE of `range` on `table` — the statement a killed run's outstanding rollback job
 * holds — and returns how many rows it removed: what that job's late completion takes.
 */
export async function lateRollbackDelete(
  { admin, dataset }: Database,
  table: string,
  range: { low: number; high: number },
): Promise<number> {
  const [job] = await admin.createQueryJob({
    query: `DELETE FROM \`${dataset}.${table}\` WHERE \`blockNumber\` >= @low AND \`blockNumber\` <= @high`,
    params: range,
  })
  await job.getQueryResults()

  const affected = (job.metadata?.statistics?.query?.numDmlAffectedRows ?? '0') as string

  return Number.parseInt(affected, 10)
}

/**
 * Asserts the store stands at the scenario's end: what a restart must leave behind, checked again
 * after anything it overlapped has landed.
 */
export async function expectEndState<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  database,
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  database: Database
}): Promise<void> {
  const after = await readBigqueryStore(database)
  const end = scenario.end(simulator)

  expect(validateBigquerySnapshot(after, scenario.ranges)).toEqual([])
  expect(after.inFlight, 'the in-flight WAL row').toBeUndefined()
  expect(after.state, 'the stored state').toStrictEqual(end.state)
  expect(rowsOfBigquery(after), 'the stored rows').toEqual(end.rows)
}

/** Waits until the job BigQuery still runs for the killed child is DONE. */
async function jobDone({ admin }: Database, jobId: string): Promise<void> {
  const deadline = Date.now() + JOB_TIMEOUT_MS
  const job = admin.job(jobId)

  while (true) {
    const [metadata] = await job.getMetadata()
    if (metadata.status?.state === 'DONE') {
      return
    }

    if (Date.now() > deadline) {
      throw new Error(`the job ${jobId} the killed child created is still ${metadata.status?.state}`)
    }

    await sleep(1_000)
  }
}

/** One cheap read of every table's row count: the store's changes show in it, whatever they were. */
async function rowCounts({ admin, dataset }: Database): Promise<string> {
  const counts = [`(SELECT COUNT(*) FROM \`${dataset}.sync\`) AS sync`]
  for (const table of TABLE_NAMES) {
    counts.push(`(SELECT COUNT(*) FROM \`${dataset}.${table}\`) AS ${table}`)
  }

  // A child that died before its first DDL leaves no table behind; that is a stable store too.
  try {
    const [rows] = await admin.query({ query: `SELECT ${counts.join(', ')}` })

    return JSON.stringify(rows[0] ?? {})
  } catch (error) {
    if (/not found: table|does not exist/i.test(String(error))) {
      return 'no tables'
    }

    throw error
  }
}
