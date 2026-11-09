import { describe, expect } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { S3, s3RowsUpTo } from '../scenarios/fork.js'
import { s3FullStream } from '../scenarios/full-stream.js'
import { crashAt, it, readBigqueryStore, resumeToEnd, runToEnd } from './harness.js'
import { streamBigquery } from './pipe.js'
import { rowsOfBigquery } from './probe.js'

const POINTS = [
  { name: 'before rollback WAL', call: '^append sync$', inFork: true, at: 1, when: 'before' },
  { name: 'after first rollback DELETE', call: '^delete ', inFork: true, at: 1, when: 'after' },
] as const

describe.skipIf(!process.env['BIGQUERY_TEST_PROJECT'])('bigquery · fork depth and partitioning (CT-2/CT-3)', () => {
  for (const ancestor of [S3.latest - 1, S3.ancestor, S3.raisedTo]) {
    for (const blocksPerResponse of [1, S3.blocksPerResponse]) {
      // The original combination, including target-specific assertions, lives in fork.test.ts.
      if (ancestor === S3.ancestor && blocksPerResponse === S3.blocksPerResponse) {
        continue
      }

      it(`rolls back ${S3.latest - ancestor} blocks with responses of ${blocksPerResponse} blocks`, {
        timeout: 300_000,
      }, async ({ simulateS3, database }) => {
        const options = { ancestor, blocksPerResponse }
        const scenario = s3FullStream('W', options)
        const simulator = await simulateS3(options)

        await runToEnd({ scenario, simulator, database })

        const requests = simulator.chain.requests
        const forks = requests.filter((request) => request.answer?.kind === 'fork')
        expect(forks).toHaveLength(1)
        expect(requests[requests.indexOf(forks[0]) + 1]?.query.fromBlock).toBe(ancestor + 1)
      })
    }
  }

  it('recovers after two crashes before the rollback to finality commits', { timeout: 300_000 }, async ({
    simulateS3,
    database,
  }) => {
    const options = { ancestor: S3.raisedTo }
    const scenario = s3FullStream('W', options)
    const simulator = await simulateS3(options)
    const model = scenario.oracle()
    const point = POINTS[0]

    const first = await crashAt({ scenario, simulator, database, model, point })
    const second = await crashAt({ scenario, simulator, database, model, point })

    expect(first.forks).toHaveLength(1)
    expect(second.forks).toHaveLength(1)
    expect(second.batches).toEqual([])
    expect(second.store.state).toStrictEqual(first.store.state)
    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  for (const point of POINTS) {
    it(`recovers a rollback to the finalized block: ${point.name}`, { timeout: 300_000 }, async ({
      simulateS3,
      database,
    }) => {
      const options = { ancestor: S3.raisedTo }
      const scenario = s3FullStream('W', options)
      const simulator = await simulateS3(options)
      const model = scenario.oracle()

      const { store, forks } = await crashAt({ scenario, simulator, database, model, point })
      expect(forks).toHaveLength(1)

      await resumeToEnd({ scenario, simulator, database, model, store })
    })
  }
})

describe.skipIf(!process.env['BIGQUERY_TEST_PROJECT'])('bigquery · finality conflict (CT-3)', () => {
  it('refuses a fork below finality without removing stored rows', { timeout: 300_000 }, async ({
    simulateS3,
    database,
  }) => {
    const options = { ancestor: S3.raisedTo - 1 }
    const simulator = await simulateS3(options)
    await expect(
      streamBigquery({
        range: { from: S3.from, to: S3.to },
        url: simulator.chain.url,
        projectId: database.projectId,
        dataset: database.dataset,
        logger: testLogger(),
        observe: () => {},
        onFork: () => {},
      }),
    ).rejects.toMatchObject({ code: 'E1003' })
    const store = await readBigqueryStore(database)
    expect(store.state?.current?.number).toBe(S3.latest)
    expect(store.state?.finalized?.number).toBe(S3.raisedTo)
    expect(store.inFlight).toBeUndefined()
    expect(rowsOfBigquery(store)).toEqual(s3RowsUpTo(store.state?.current, options))
    expect(simulator.chain.requests.filter((request) => request.answer?.kind === 'fork')).toHaveLength(1)
  })
})
