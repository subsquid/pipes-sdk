import { existsSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { expect } from 'vitest'

import type { BlockCursor } from '~/core/index.js'
import { type MockChainPortal, testLogger } from '~/testing/index.js'

import { killChild } from '../kill/spawn.js'
import type { ReferenceModel } from '../oracle/reference-model.js'
import { type ChangelogOperation, type PubsubSnapshot, validatePubsubSnapshot } from '../oracle/validators.js'
import { type Observation, PIPE_ID, inStoreOrder } from '../scenarios/erc20.js'
import { it as scenarios } from '../scenarios/fixtures.js'
import { type FullStreamScenario, s1FullStream, s3FullStream } from '../scenarios/full-stream.js'
import { type Bus, type BusMessage, startBus } from './bus.js'
import type { KillEvidence, StepJournalEntry, StepKillPoint } from './kill-switch.js'
import { PROJECT_ID, TOPIC, streamPubsub } from './pipe.js'
import { readPubsubSnapshot, wireOperations } from './probe.js'

const KILLED_RUN = fileURLToPath(new URL('./killed-run.ts', import.meta.url))

/** What a class-C pipe writes to: the bus it publishes to, and its state file. */
export type Pubsub = { bus: Bus; statePath: string }

/** The scenarios' portals, a bus of the test's own, and a directory for the state file. */
export const it = scenarios.extend<{ pubsub: Pubsub }>({
  pubsub: async ({}, use) => {
    const bus = await startBus({ projectId: PROJECT_ID, topics: [TOPIC] })
    const directory = await mkdtemp(path.join(tmpdir(), 'sqd-ct-pubsub-'))

    await use({ bus, statePath: path.join(directory, 'state.sqlite') })

    await rm(directory, { recursive: true, force: true })
    await bus.close()
  },
})

/** S1 as class C: the full stream, one commit and one publish per batch (CN-17), no fork. */
export const S1_PUBSUB = s1FullStream('C')

/** S3 as class C: an unfinalized tail, a floor that rises once, one fork the sink compensates (CN-35). */
export const S3_PUBSUB = s3FullStream('C')

/** A read of a class-C store: the state file and the bus, the consumer's rows in store order. */
export type PubsubStore = PubsubSnapshot

export function readPubsubStore({ bus, statePath }: Pubsub): PubsubStore {
  const snapshot = readPubsubSnapshot({ path: statePath, id: PIPE_ID, messages: bus.messages })

  return { ...snapshot, rows: inStoreOrder(snapshot.rows) }
}

/**
 * Only a run that finds no state file declares the bootstrap (CN-46): a restart over a file a crash
 * left must start without one, as an operator's would.
 */
function coldStart({ statePath }: Pubsub): boolean {
  return !existsSync(statePath)
}

/**
 * Streams a scenario to the bus, and returns the store as the run leaves it. It fails unless the
 * run ends in the scenario's end state:
 *
 * - the store passes every structural validator: among them, every republish repeats its message
 *   byte for byte, and the bus and the outbox hold every number the state handed out (CN-17, CN-46);
 * - the state is the scenario's end state, the outbox is empty, and a consumer of the bus holds
 *   exactly the scenario's rows;
 * - every message carries a finalized head (RP-45);
 * - so does the oracle, which recovers from the store as the pipe does and follows every batch and
 *   fork.
 */
export async function runToEnd<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  pubsub,
  model = scenario.oracle(),
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  pubsub: Pubsub
  /** Pass one to watch it during the run, or to carry it across a restart or a crash. */
  model?: ReferenceModel
}): Promise<PubsubStore> {
  const before = readPubsubStore(pubsub)
  model.recover(before.state)

  const follower = scenario.follower(model, simulator, simulator.chain.requests.length)
  await streamPubsub({
    range: scenario.range,
    url: simulator.chain.url,
    bus: pubsub.bus,
    statePath: pubsub.statePath,
    coldStart: coldStart(pubsub),
    logger: testLogger(),
    observe: (observation) => follower.batch(observation),
    onFork: (canonical) => follower.fork(canonical),
  })

  const after = readPubsubStore(pubsub)
  const end = scenario.end(simulator)
  const violations = validatePubsubSnapshot(after, scenario.ranges)
  const unstamped = after.wire.filter((operation) => operation.finalized === undefined).map(({ seq }) => seq)

  expect(violations, 'the structural validators').toEqual([])
  expect(after.state, 'the stored state').toStrictEqual(end.state)
  expect(after.outbox, 'the outbox').toEqual([])
  expect(after.rows, 'the rows a consumer of the bus holds').toEqual(end.rows)
  expect(unstamped, 'the messages with no finalized head').toEqual([])
  expect(model.readState(), 'the oracle state').toStrictEqual(end.state)
  expect(inStoreOrder(model.data), 'the oracle rows').toEqual(end.rows)

  return after
}

/** What a killed run left. */
export type KilledRun = {
  store: PubsubStore
  /** The batches the child delivered before it died, in order. */
  batches: Observation[]
  /** The canonical chain of every fork the child began to roll back to, in order. */
  forks: BlockCursor[][]
  /** Where the kill landed. */
  killed: KillEvidence
}

/**
 * Runs a scenario to the bus in a child process that kills itself with SIGKILL at `point`, and
 * returns the store as the kill left it. No point lands while a publish call is open, so the bus
 * has taken every message it will take from the child by the time it is dead.
 *
 * The oracle recovers from the store first, as the child does, then follows every batch and fork
 * the child journaled before it died.
 */
export async function runKilled<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  pubsub,
  model,
  point,
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  pubsub: Pubsub
  model: ReferenceModel
  point: StepKillPoint
}): Promise<KilledRun> {
  const before = readPubsubStore(pubsub)
  model.recover(before.state)

  const follower = scenario.follower(model, simulator, simulator.chain.requests.length)
  const entries = await killChild<StepJournalEntry>({
    script: KILLED_RUN,
    point,
    options: {
      range: scenario.range,
      url: simulator.chain.url,
      bus: { endpoint: pubsub.bus.endpoint, projectId: pubsub.bus.projectId },
      statePath: pubsub.statePath,
      coldStart: coldStart(pubsub),
    },
  })

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
  const { killed, ...evidence } = entries.at(-1) as Extract<StepJournalEntry, { killed: string }>

  return { store: readPubsubStore(pubsub), batches, forks, killed: evidence }
}

/** The sequence numbers of a list of operations, in its order. */
export function seqsOf(operations: readonly Pick<ChangelogOperation, 'seq'>[]): number[] {
  return operations.map(({ seq }) => seq)
}

/**
 * Kills a scenario's run at `point`, and returns what the kill left. It fails unless the crash left
 * a commit point intact (CN-40): the store passes every structural validator, so the bus holds
 * nothing the state did not record and the outbox holds what the bus lacks (CN-17), and the
 * changelog the two hold leaves a consumer exactly the scenario's rows up to the cursor. Which
 * numbers the bus and the outbox hold is for each test to say: it proves the kill landed on the
 * step the test names.
 */
export async function crashAt<Sim extends { chain: MockChainPortal }>(options: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  pubsub: Pubsub
  model: ReferenceModel
  point: StepKillPoint
}): Promise<KilledRun> {
  const run = await runKilled(options)
  const violations = validatePubsubSnapshot(run.store, options.scenario.ranges)
  const expected = options.scenario.rowsUpTo(run.store.state?.current)

  expect(violations, 'the store the crash left').toEqual([])
  expect(run.store.rows, 'the rows the recorded changelog leaves').toEqual(expected)

  return run
}

/**
 * How many of `messages` share a Publish call with one of the first `count`: none, when those go
 * out on their own before anything after them is published.
 */
export function sharingCalls(messages: readonly BusMessage[], count: number): number {
  const calls = new Set(messages.slice(0, count).map(({ call }) => call))

  return messages.slice(count).filter(({ call }) => calls.has(call)).length
}

/**
 * Restarts a scenario in-process over what a crash left. It fails unless the restart first
 * publishes the outbox the crash left, operation for operation, and has it confirmed before it
 * streams: no message of a later batch goes out in the same call (CN-46); then asks for the block
 * right after the recovered cursor, anchored on it (WP-1, IB-3); and the run ends as `runToEnd`
 * demands.
 */
export async function resumeToEnd<Sim extends { chain: MockChainPortal }>({
  scenario,
  simulator,
  pubsub,
  model,
  store,
}: {
  scenario: FullStreamScenario<Sim>
  simulator: Sim
  pubsub: Pubsub
  model: ReferenceModel
  /** The store as the restart finds it. */
  store: PubsubStore
}): Promise<void> {
  const first = simulator.chain.requests.length
  const published = pubsub.bus.messages.length
  const cursor = store.state?.current

  await runToEnd({ scenario, simulator, pubsub, model })

  const restart = pubsub.bus.messages.slice(published)
  const drain = wireOperations(restart.slice(0, store.outbox.length))
  const drained = drain.map(({ topic, seq, op, id, attributes, row }) => ({ topic, seq, op, id, attributes, row }))
  const resumed = simulator.chain.requests[first]?.query

  expect(drained, 'what the restart publishes first').toEqual(store.outbox)
  expect(sharingCalls(restart, store.outbox.length), 'later messages in the calls that publish the outbox').toBe(0)
  expect(resumed?.fromBlock, 'the first block the restart asks for').toBe(
    cursor === undefined ? scenario.range.from : cursor.number + 1,
  )
  expect(resumed?.parentBlockHash, 'the anchor of the restart').toBe(cursor?.hash)
}
