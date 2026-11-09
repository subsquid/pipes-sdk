import { describe, expect } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { S3, s3RowsUpTo } from '../scenarios/fork.js'
import { s3FullStream } from '../scenarios/full-stream.js'
import { crashAt, it, readPubsubStore, resumeToEnd, runToEnd } from './harness.js'
import { streamPubsub } from './pipe.js'

const POINTS = [
  { name: 'before ancestor write', step: '^insert cursor$', inFork: true, at: 1, when: 'before' },
  {
    name: 'after compensation commit',
    step: '^commit$',
    inTransaction: '^delete ledger_blocks$',
    inFork: true,
    at: 1,
    when: 'after',
  },
] as const

describe('pubsub · fork depth and partitioning (CT-2/CT-3)', () => {
  for (const ancestor of [S3.latest - 1, S3.ancestor, S3.raisedTo]) {
    for (const blocksPerResponse of [1, S3.blocksPerResponse]) {
      // The original combination, including target-specific assertions, lives in fork.test.ts.
      if (ancestor === S3.ancestor && blocksPerResponse === S3.blocksPerResponse) {
        continue
      }

      it(`rolls back ${S3.latest - ancestor} blocks with responses of ${blocksPerResponse} blocks`, {
        timeout: 30_000,
      }, async ({ simulateS3, pubsub }) => {
        const options = { ancestor, blocksPerResponse }
        const scenario = s3FullStream('C', options)
        const simulator = await simulateS3(options)

        await runToEnd({ scenario, simulator, pubsub })

        const requests = simulator.chain.requests
        const forks = requests.filter((request) => request.answer?.kind === 'fork')
        expect(forks).toHaveLength(1)
        expect(requests[requests.indexOf(forks[0]) + 1]?.query.fromBlock).toBe(ancestor + 1)
      })
    }
  }

  it('recovers after two crashes before the rollback to finality commits', { timeout: 30_000 }, async ({
    simulateS3,
    pubsub,
  }) => {
    const options = { ancestor: S3.raisedTo }
    const scenario = s3FullStream('C', options)
    const simulator = await simulateS3(options)
    const model = scenario.oracle()
    const point = POINTS[0]

    const first = await crashAt({ scenario, simulator, pubsub, model, point })
    const second = await crashAt({ scenario, simulator, pubsub, model, point })

    expect(first.forks).toHaveLength(1)
    expect(second.forks).toHaveLength(1)
    expect(second.batches).toEqual([])
    expect(second.store.state).toStrictEqual(first.store.state)
    await resumeToEnd({ scenario, simulator, pubsub, model, store: second.store })
  })

  for (const point of POINTS) {
    it(`recovers a rollback to the finalized block: ${point.name}`, { timeout: 30_000 }, async ({
      simulateS3,
      pubsub,
    }) => {
      const options = { ancestor: S3.raisedTo }
      const scenario = s3FullStream('C', options)
      const simulator = await simulateS3(options)
      const model = scenario.oracle()

      const { store, forks } = await crashAt({ scenario, simulator, pubsub, model, point })
      expect(forks).toHaveLength(1)

      await resumeToEnd({ scenario, simulator, pubsub, model, store })
    })
  }
})

describe('pubsub · finality conflict (CT-3)', () => {
  it('compensates to the finalized baseline before halting on a finality conflict', async ({ simulateS3, pubsub }) => {
    const options = { ancestor: S3.raisedTo - 1 }
    const simulator = await simulateS3(options)
    let before: ReturnType<typeof readPubsubStore> | undefined
    await expect(
      streamPubsub({
        range: { from: S3.from, to: S3.to },
        url: simulator.chain.url,
        bus: pubsub.bus,
        statePath: pubsub.statePath,
        coldStart: true,
        logger: testLogger(),
        observe: () => {},
        onFork: () => {
          before = readPubsubStore(pubsub)
        },
      }),
    ).rejects.toMatchObject({ code: 'E1003' })
    const after = readPubsubStore(pubsub)
    expect(before).toBeDefined()
    expect(after.state?.current?.number).toBe(S3.raisedTo)
    expect(after.state?.finalized).toEqual(before?.state?.finalized)
    expect(after.rows).toEqual(s3RowsUpTo(before?.state?.current, options).filter((row) => row.block <= S3.raisedTo))
    expect(after.wire.slice(0, before?.wire.length)).toEqual(before?.wire)
    const compensations = after.wire.slice(before?.wire.length)
    expect(compensations.length).toBeGreaterThan(0)
    expect(compensations.every((operation) => operation.op === 'delete')).toBe(true)
    expect(after.outbox).toEqual([])
    expect(simulator.chain.requests.filter((request) => request.answer?.kind === 'fork')).toHaveLength(1)
  })
})
