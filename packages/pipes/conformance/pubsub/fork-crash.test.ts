import { describe, expect } from 'vitest'

import { numbers } from '../scenarios/erc20.js'
import { S3 } from '../scenarios/fork.js'
import { type PubsubStore, S3_PUBSUB, crashAt, it, readPubsubStore, resumeToEnd, seqsOf } from './harness.js'
import { PUBLISH, type StepKillPoint } from './kill-switch.js'

/**
 * The steps of S3 that S1 never reaches: a batch above the floor, whose transaction also records
 * its operations in the rollback manifest and its blocks in the ledger; and the fork, whose
 * transaction enqueues a compensation for each operation above the ancestor, drops the manifest
 * and ledger above it and writes the ancestor as the cursor, before the compensations are
 * published (CN-35). The batch that ends at 27 holds blocks 21 to 27, all above the floor at 19.
 */
const POINTS = {
  aboveFloorCommit: {
    name: 'inside a batch above the floor, before its cursor write',
    step: '^insert cursor$',
    inBatch: 27,
    at: 1,
    when: 'before',
  },
  aboveFloorPublished: {
    name: 'a batch above the floor, the bus took it, its answer unread',
    step: PUBLISH,
    inBatch: 27,
    at: 1,
    when: 'after',
  },
  forkCommit: {
    name: 'inside the fork transaction, before the ancestor is written',
    step: '^insert cursor$',
    inFork: true,
    at: 1,
    when: 'before',
  },
  forkCommitted: {
    name: 'after the fork transaction, before the compensations go out',
    step: '^commit$',
    inTransaction: '^delete ledger_blocks$',
    inFork: true,
    at: 1,
    when: 'after',
  },
  forkPublished: {
    name: 'the bus took the compensations, their answer unread',
    step: PUBLISH,
    inFork: true,
    at: 1,
    when: 'after',
  },
} satisfies Record<string, StepKillPoint>

/** The rows on the forked branch, above the ancestor and up to the head the run starts at. */
const FORKED_IDS = ['approvals:28', 'transfers:27', 'transfers:28', 'transfers:29']

/** The rows of the batch that ends at 27, in the order the pipe maps them: a transfer in each block, an approval in each even one. */
const BATCH_27_IDS = numbers(21, 27).flatMap((block) =>
  block % 2 === 0 ? [`transfers:${block}`, `approvals:${block}`] : [`transfers:${block}`],
)

const scenario = S3_PUBSUB

/** The ids a list of operations deletes, sorted: the order of one fork's compensations is free. */
function deletedIds(operations: PubsubStore['outbox']): string[] {
  return operations
    .filter((operation) => operation.op === 'delete')
    .map(({ id }) => id)
    .sort()
}

/**
 * S3 to the bus, killed by SIGKILL in a batch above the floor and at each step of the fork (CT-2).
 * The bus cannot take back what it published, so the fork's commit point is the ancestor with the
 * compensations recorded in the outbox: the state may be ahead of the bus, never behind it. Every
 * test runs the same two checks as on S1: `crashAt` fails unless the bus and the outbox hold exactly
 * the changelog up to the recovered cursor (CN-40), and `resumeToEnd` fails unless the restart
 * publishes the outbox first and ends in S3's end state.
 */
describe('Pub/Sub (class C) · S3 one fork · killed at each commit step (CT-2)', () => {
  it('inside a batch above the floor: neither its numbers nor its manifest rows nor its blocks are kept', async ({
    simulateS3,
    pubsub,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      pubsub,
      model,
      point: POINTS.aboveFloorCommit,
    })

    expect(killed.sent).toEqual(expect.arrayContaining(['insert outbox', 'insert manifest', 'insert ledger_blocks']))
    expect(batches.at(-1)?.current.number).toBe(27)
    expect(store.state?.current?.number).toBe(20)
    expect(store.state?.rollbackChain.map((block) => block.number)).toEqual([20])
    expect(store.outbox).toEqual([])

    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })

  it('a batch above the floor, the bus took it: the restart publishes it again, byte for byte', async ({
    simulateS3,
    pubsub,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.aboveFloorPublished })
    const published = seqsOf(store.wire)

    expect(killed.step).toBe(PUBLISH)
    expect(store.state?.current?.number).toBe(27)
    expect(store.state?.rollbackChain.map((block) => block.number)).toEqual(numbers(20, 27))
    expect(store.outbox.map(({ id }) => id)).toEqual(BATCH_27_IDS)
    expect(seqsOf(store.outbox)).toEqual(published.slice(-BATCH_27_IDS.length))

    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })

  it('inside the fork transaction: the state keeps the forked branch, and the restart forks again', async ({
    simulateS3,
    pubsub,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, forks, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.forkCommit })

    expect(killed.sent).toEqual(expect.arrayContaining(['insert outbox', 'delete manifest', 'delete ledger_blocks']))
    expect(forks).toHaveLength(1)
    expect(store.state?.current?.number).toBe(S3.latest)
    expect(store.outbox).toEqual([])

    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })

  it('a second crash inside the fork transaction: the fork runs again and converges (WP-46)', async ({
    simulateS3,
    pubsub,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.forkCommit })
    const second = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.forkCommit })

    expect(second.forks).toHaveLength(1)
    expect(second.batches).toEqual([])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, pubsub, model, store: second.store })
  })

  it('after the fork transaction: the ancestor is the cursor, the compensations wait in the outbox (CN-34)', async ({
    simulateS3,
    pubsub,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.forkCommitted })

    expect(killed.sent.at(-1)).toBe('commit')
    expect(store.state?.current?.number).toBe(S3.ancestor)
    expect(store.state?.finalized?.number).toBe(S3.raisedTo)
    expect(store.state?.rollbackChain.map((block) => block.number)).toEqual([24, 25, 26])
    expect(deletedIds(store.outbox)).toEqual(FORKED_IDS)
    expect(deletedIds(store.wire)).toEqual([])

    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })

  it('the bus took the compensations: the restart publishes them again, before the new branch', async ({
    simulateS3,
    pubsub,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.forkPublished })

    expect(killed.step).toBe(PUBLISH)
    expect(store.state?.current?.number).toBe(S3.ancestor)
    expect(deletedIds(store.outbox)).toEqual(FORKED_IDS)
    expect(deletedIds(store.wire)).toEqual(FORKED_IDS)

    await resumeToEnd({ scenario, simulator, pubsub, model, store })

    expect(deletedIds(readPubsubStore(pubsub).wire), 'the bus').toEqual(FORKED_IDS.flatMap((id) => [id, id]))
  })
})
