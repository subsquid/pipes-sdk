import { describe, expect } from 'vitest'

import { S1_ROWS } from '../scenarios/backfill.js'
import { numbers } from '../scenarios/erc20.js'
import { S1_PUBSUB, crashAt, it, readPubsubStore, resumeToEnd, seqsOf } from './harness.js'
import { PUBLISH, type StepKillPoint } from './kill-switch.js'

/**
 * The class-C commit protocol (CN-17) step by step. Each batch runs one transaction on the state
 * file that takes a sequence number for each operation and enqueues it in the outbox, then writes
 * the cursor; then publishes the outbox; then deletes the confirmed rows from the outbox in a
 * second transaction. A restart publishes the outbox before it reads (CN-46). A point names its
 * batch by its last block: S1's first batch ends at 6, its second at 13.
 */
const POINTS = {
  insideCommit: {
    name: 'inside the batch transaction, before its cursor write',
    step: '^insert cursor$',
    inBatch: 13,
    at: 1,
    when: 'before',
  },
  afterCommit: {
    name: 'after the batch transaction, before the publish',
    step: '^commit$',
    inTransaction: '^insert cursor$',
    inBatch: 13,
    at: 1,
    when: 'after',
  },
  published: { name: 'the bus took the batch, its answer unread', step: PUBLISH, inBatch: 13, at: 1, when: 'after' },
  insideConfirm: {
    name: 'inside the confirmation, after one outbox row is deleted',
    step: '^delete outbox$',
    inBatch: 13,
    at: 1,
    when: 'after',
  },
  afterConfirm: {
    name: 'after the confirmation',
    step: '^commit$',
    inTransaction: '^delete outbox$',
    inBatch: 13,
    at: 1,
    when: 'after',
  },
  firstCommit: {
    name: 'the first batch, inside its transaction',
    step: '^insert cursor$',
    inBatch: 6,
    at: 1,
    when: 'before',
  },
  firstPublished: {
    name: 'the first batch, the bus took it, its answer unread',
    step: PUBLISH,
    inBatch: 6,
    at: 1,
    when: 'after',
  },
  recoveryPublished: {
    name: 'recovery, the bus took the outbox, its answer unread',
    step: PUBLISH,
    at: 1,
    when: 'after',
  },
} satisfies Record<string, StepKillPoint>

/** The transaction of the batch that ends at 13, up to its cursor write: a number and an outbox row for each of its three rows. */
const BATCH_13_WRITES = ['begin', ...Array.from({ length: 3 }, () => ['insert meta', 'insert outbox']).flat()]

/** S1's first batch holds the rows numbered 1 to 3, its second 4 to 6; S1 has a number for each of its rows. */
const FIRST = numbers(1, 3)
const SECOND = numbers(4, 6)
const ALL = numbers(1, S1_ROWS.length)

const scenario = S1_PUBSUB

/**
 * S1 to the bus, killed by SIGKILL at each step of the commit protocol and of recovery (CT-2). The
 * commit records every operation before the bus can take it, so a crash leaves operations the bus
 * may lack, never ones the state did not record: the outbox is the crash window, and the restart
 * publishes it again, number for number and byte for byte (CN-17, CN-46). Every test runs the same
 * two checks:
 *
 * - `crashAt` fails unless the crash left a commit point intact (CN-40): the bus and the outbox
 *   together hold exactly the changelog of S1 up to the cursor. It returns the store, whose bus and
 *   outbox each test pins to the step it names;
 * - `resumeToEnd` fails unless the restart publishes the outbox first, then starts right after the
 *   recovered cursor and ends in S1's end state, every copy of a number on the bus byte-identical
 *   to the first (INV-40…INV-42).
 */
describe('Pub/Sub (class C) · S1 backfill · killed at each commit step (CT-2)', () => {
  it('inside the batch transaction: the state holds the batch before, and none of its numbers', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      pubsub,
      model,
      point: POINTS.insideCommit,
    })

    expect(killed).toEqual({ step: 'insert cursor', sent: BATCH_13_WRITES })
    expect(store.state?.current).toStrictEqual(batches[0].current)
    expect(store.sequence).toBe(3)
    expect(seqsOf(store.outbox)).toEqual([])
    expect(seqsOf(store.wire)).toEqual(FIRST)

    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })

  it('after the batch transaction: the batch is recorded, not published, and the restart publishes it', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.afterCommit })

    expect(killed).toEqual({ step: 'commit', sent: [...BATCH_13_WRITES, 'insert cursor', 'commit'] })
    expect(store.state?.current).toStrictEqual(batches[1].current)
    expect(seqsOf(store.outbox)).toEqual(SECOND)
    expect(seqsOf(store.wire)).toEqual(FIRST)

    await resumeToEnd({ scenario, simulator, pubsub, model, store })

    expect(seqsOf(readPubsubStore(pubsub).wire), 'the bus').toEqual(ALL)
  })

  it('the bus took the batch, its answer unread: the restart publishes it again, byte for byte (GAP-14)', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.published })

    expect(killed).toEqual({ step: PUBLISH, sent: [...BATCH_13_WRITES, 'insert cursor', 'commit', PUBLISH] })
    expect(store.state?.current).toStrictEqual(batches[1].current)
    expect(seqsOf(store.outbox)).toEqual(SECOND)
    expect(seqsOf(store.wire)).toEqual([...FIRST, ...SECOND])

    await resumeToEnd({ scenario, simulator, pubsub, model, store })

    expect(seqsOf(readPubsubStore(pubsub).wire), 'the bus').toEqual([...FIRST, ...SECOND, ...SECOND, ...ALL.slice(6)])
  })

  it('inside the confirmation: the outbox keeps every row of the batch, and the restart publishes them again', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.insideConfirm })

    expect(killed.sent.slice(-3)).toEqual([PUBLISH, 'begin', 'delete outbox'])
    expect(seqsOf(store.outbox)).toEqual(SECOND)
    expect(seqsOf(store.wire)).toEqual([...FIRST, ...SECOND])

    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })

  it('after the confirmation: the outbox is empty, and the restart publishes nothing twice', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.afterConfirm })

    expect(killed.sent.slice(-6)).toEqual([
      PUBLISH,
      'begin',
      'delete outbox',
      'delete outbox',
      'delete outbox',
      'commit',
    ])
    expect(store.state?.current).toStrictEqual(batches[1].current)
    expect(seqsOf(store.outbox)).toEqual([])
    expect(seqsOf(store.wire)).toEqual([...FIRST, ...SECOND])

    await resumeToEnd({ scenario, simulator, pubsub, model, store })

    expect(seqsOf(readPubsubStore(pubsub).wire), 'the bus').toEqual(ALL)
  })

  it('the first batch, inside its transaction: no cursor, no number, and the restart needs no bootstrap', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, killed } = await crashAt({ scenario, simulator, pubsub, model, point: POINTS.firstCommit })

    expect(killed.step).toBe('insert cursor')
    expect(killed.sent).toContain('insert outbox')
    expect(store.state).toBeUndefined()
    expect(store.sequence).toBe(0)
    expect(seqsOf(store.outbox)).toEqual([])
    expect(seqsOf(store.wire)).toEqual([])

    // The state file is there, so the restart declares no cold start (CN-46), and the target needs none.
    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })

  it('the first batch, the bus took it, its answer unread: the restart publishes it again', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      pubsub,
      model,
      point: POINTS.firstPublished,
    })

    expect(killed.step).toBe(PUBLISH)
    expect(killed.sent).toContain('insert cursor')
    expect(store.state?.current).toStrictEqual(batches[0].current)
    expect(seqsOf(store.outbox)).toEqual(FIRST)
    expect(seqsOf(store.wire)).toEqual(FIRST)

    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })

  it('a second crash, once the bus took the recovery publish: the third run publishes the outbox once more', async ({
    simulateS1,
    pubsub,
  }) => {
    const simulator = await simulateS1()
    const model = scenario.oracle()

    await crashAt({ scenario, simulator, pubsub, model, point: POINTS.published })
    const { store, batches, killed } = await crashAt({
      scenario,
      simulator,
      pubsub,
      model,
      point: POINTS.recoveryPublished,
    })

    expect(killed.step).toBe(PUBLISH)
    // The kill landed on recovery's publish, before the pipe delivered a batch.
    expect(batches).toEqual([])
    expect(seqsOf(store.outbox)).toEqual(SECOND)
    expect(seqsOf(store.wire)).toEqual([...FIRST, ...SECOND, ...SECOND])

    await resumeToEnd({ scenario, simulator, pubsub, model, store })
  })
})
