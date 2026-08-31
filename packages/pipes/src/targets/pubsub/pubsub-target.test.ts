import { afterEach, describe, expect, it, vi } from 'vitest'

import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { MockPortal, mockMetricsServer, mockPortal, testLogger } from '~/testing/index.js'

import { PUBSUB_ERROR_CODES } from './errors.js'
import { PUBSUB_LIMITS } from './protocol.js'
import { SqlitePubsubState } from './pubsub-state.js'
import { pubsubTarget } from './pubsub-target.js'
import {
  FakePublisher,
  cleanupTempState,
  keyedBlockDecoder,
  makeBatchContext,
  portalBlocks,
  tempStatePath,
} from './test-support.js'
import { windowTopic } from './window-topic.js'

type Blocks = { blocks: { number: number; hash: string; timestamp: number }[] }

const body = (message: FakePublisher['published'][number]) => JSON.parse(message.payload)

let portal: MockPortal | undefined

afterEach(async () => {
  await portal?.close()
  portal = undefined
  cleanupTempState()
})

function blocksRoute(topic = 'blocks') {
  return {
    topic,
    map: ({ data }: { data: Blocks['blocks'] }) =>
      data.map((header) => ({
        data: { number: header.number },
        block: header,
        attributes: { chain: 'mock' },
      })),
  }
}

/** A chain whose block N is stamped 12s apart from a realistic epoch, as a real cursor would be. */
const CHAIN_GENESIS_SECONDS = 1_700_000_000
const chainTime = (blockNumber: number) => CHAIN_GENESIS_SECONDS + blockNumber * 12

/** One block header. Single-argument on purpose, so `numbers.map(header)` is safe. */
const header = (number: number) => ({ number, hash: `0x${number}`, timestamp: chainTime(number) })

/** Drives `target.write()` one block per batch, so batch boundaries are the test's to choose. */
async function driveBatches({
  statePath,
  publisher,
  blocks,
  finalized,
  finalizedAt = () => finalized,
  targetOptions = {},
  metrics,
  lastBlockReceivedAt,
  blockTimestamp = chainTime,
}: {
  statePath: string
  publisher: FakePublisher
  blocks: number[]
  finalized: number
  /** Finalized head per block, for the cases that watch the watermark move. */
  finalizedAt?: (blockNumber: number) => number | undefined
  targetOptions?: Partial<Parameters<typeof pubsubTarget<Blocks>>[0]>
  metrics?: ReturnType<typeof mockMetricsServer>
  lastBlockReceivedAt?: Date
  /** Cursor timestamp per block; `() => undefined` reproduces a query that selects no timestamp. */
  blockTimestamp?: (blockNumber: number) => number | undefined
}) {
  const target = pubsubTarget<Blocks>({
    pubsub: {} as never,
    publisher,
    state: { path: statePath },
    publishFrom: 0,
    allowColdStart: true,
    topics: { blocks: blocksRoute() },
    ...targetOptions,
  })

  async function* read() {
    for (const number of blocks) {
      const head = finalizedAt(number)

      yield {
        data: { blocks: [{ number, hash: `0x${number}`, timestamp: number }] },
        ctx: makeBatchContext({
          current: { number, hash: `0x${number}`, timestamp: blockTimestamp(number) },
          finalized: head === undefined ? undefined : { number: head, hash: `0x${head}` },
          rollbackChain: [{ number, hash: `0x${number}` }],
          metrics: metrics?.server.metrics,
          lastBlockReceivedAt,
        }),
      }
    }
  }

  await target.write({ read: read as never, logger: testLogger(), id: 'test-pipe' })
}

async function runPipe({
  publisher,
  statePath,
  responses,
  targetOptions = {},
  id = 'test-pipe',
  to = 5,
  metrics,
}: {
  publisher: FakePublisher
  statePath: string
  responses: Parameters<typeof mockPortal>[0]
  targetOptions?: Partial<Parameters<typeof pubsubTarget<Blocks>>[0]>
  id?: string
  to?: number
  metrics?: ReturnType<typeof mockMetricsServer>
}) {
  portal = await mockPortal(responses)

  await evmPortalStream({
    id,
    portal: portal.url,
    outputs: keyedBlockDecoder({ from: 0, to }),
    metrics: metrics?.server,
  }).pipeTo(
    pubsubTarget<Blocks>({
      pubsub: {} as never,
      publisher,
      state: { path: statePath },
      publishFrom: 0,
      allowColdStart: true,
      topics: { blocks: blocksRoute() },
      ...targetOptions,
    }),
  )
}

describe('pubsubTarget', () => {
  it('publishes one CDC row per draft and keeps the filter attributes', async () => {
    const publisher = new FakePublisher()

    await runPipe({
      publisher,
      statePath: tempStatePath(),
      responses: [{ statusCode: 200, data: portalBlocks([1, 2]), head: { finalized: { number: 2, hash: '0x2' } } }],
      to: 2,
    })

    expect(publisher.published).toHaveLength(2)
    expect(publisher.published[0]).toEqual({
      topic: 'blocks',
      orderingKey: '',
      attributes: { _finalized: '2', chain: 'mock' },
      payload:
        '{"_CHANGE_SEQUENCE_NUMBER":"1","_CHANGE_TYPE":"UPSERT",' + '"_id":"test-pipe:blocks:1:0x1:0","number":1}',
    })
    expect(body(publisher.published[1])._CHANGE_SEQUENCE_NUMBER).toBe('2')
  })

  it('passes the complete CDC row to a route encoder', async () => {
    const publisher = new FakePublisher()

    await runPipe({
      publisher,
      statePath: tempStatePath(),
      responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
      to: 1,
      targetOptions: {
        topics: {
          blocks: {
            ...blocksRoute(),
            encode: (message) => JSON.stringify({ record: message }),
          },
        },
      },
    })

    expect(body(publisher.published[0]).record).toMatchObject({
      number: 1,
      _id: 'test-pipe:blocks:1:0x1:0',
      _CHANGE_TYPE: 'UPSERT',
      _CHANGE_SEQUENCE_NUMBER: '1',
    })
  })

  it('validates topics before accepting any data', async () => {
    const publisher = new FakePublisher()

    await runPipe({
      publisher,
      statePath: tempStatePath(),
      responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
      to: 1,
    })

    expect(publisher.setupCalls).toEqual([['blocks']])
  })

  it('namespaces generated ids by the producer, so a fan-in topic cannot collide', async () => {
    const publisher = new FakePublisher()

    await runPipe({
      publisher,
      statePath: tempStatePath(),
      responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
      to: 1,
      targetOptions: { namespace: 'eth-stables' },
    })

    expect(body(publisher.published[0])._id).toBe('eth-stables:blocks:1:0x1:0')
  })

  it('uses an explicit draft id verbatim', async () => {
    const publisher = new FakePublisher()

    await runPipe({
      publisher,
      statePath: tempStatePath(),
      responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
      to: 1,
      targetOptions: {
        topics: {
          blocks: {
            topic: 'blocks',
            map: ({ data }) => data.map((header) => ({ data: header, block: header, id: `custom-${header.number}` })),
          },
        },
      },
    })

    expect(body(publisher.published[0])._id).toBe('custom-1')
  })

  it('adds _uid only when the option asks for it', async () => {
    const publisher = new FakePublisher()

    await runPipe({
      publisher,
      statePath: tempStatePath(),
      responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
      to: 1,
      targetOptions: { publish: { uidAttribute: true } },
    })

    expect(publisher.published[0].attributes['_uid']).toBe('["test-pipe","blocks","","1"]')
  })

  it('rejects a reserved user attribute', async () => {
    const publisher = new FakePublisher()

    await expect(
      runPipe({
        publisher,
        statePath: tempStatePath(),
        responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
        to: 1,
        targetOptions: {
          topics: {
            blocks: {
              topic: 'blocks',
              map: ({ data }) => data.map((header) => ({ data: header, block: header, attributes: { _id: 'x' } })),
            },
          },
        },
      }),
    ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.RESERVED_ATTRIBUTE })
  })

  it('rejects a per-draft ordering key when message ordering is disabled', async () => {
    const publisher = new FakePublisher()

    await expect(
      runPipe({
        publisher,
        statePath: tempStatePath(),
        responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
        to: 1,
        targetOptions: {
          topics: {
            blocks: {
              topic: 'blocks',
              map: ({ data }) => data.map((header) => ({ data: header, block: header, orderingKey: 'pool-a' })),
            },
          },
        },
      }),
    ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.ORDERING_KEY_NOT_SUPPORTED })
  })

  it('gives every topic a constant key when message ordering is enabled', async () => {
    const publisher = new FakePublisher()

    await runPipe({
      publisher,
      statePath: tempStatePath(),
      responses: [{ statusCode: 200, data: portalBlocks([1, 2]), head: { finalized: { number: 2, hash: '0x2' } } }],
      to: 2,
      targetOptions: { publish: { messageOrdering: true } },
    })

    expect(publisher.published.map((m) => [m.orderingKey, body(m)._CHANGE_SEQUENCE_NUMBER])).toEqual([
      ['blocks', '1'],
      ['blocks', '2'],
    ])
  })

  it('resumes from the persisted cursor instead of re-reading the whole range', async () => {
    const publisher = new FakePublisher()
    const statePath = tempStatePath()

    await runPipe({
      publisher,
      statePath,
      responses: [{ statusCode: 200, data: portalBlocks([1, 2]), head: { finalized: { number: 2, hash: '0x2' } } }],
      to: 2,
    })
    await portal?.close()

    const resumed = new FakePublisher()
    portal = await mockPortal([
      {
        statusCode: 200,
        data: portalBlocks([3]),
        head: { finalized: { number: 3, hash: '0x3' } },
        validateRequest: (request) => {
          expect(request.fromBlock).toBe(3)
        },
      },
    ])

    await evmPortalStream({
      id: 'test-pipe',
      portal: portal.url,
      outputs: keyedBlockDecoder({ from: 0, to: 3 }),
    }).pipeTo(
      pubsubTarget<Blocks>({
        pubsub: {} as never,
        publisher: resumed,
        state: { path: statePath },
        publishFrom: 0,
        topics: { blocks: blocksRoute() },
      }),
    )

    expect(resumed.published.map((message) => body(message)._id)).toEqual(['test-pipe:blocks:3:0x3:0'])
    // The sequence is the producer's, not the run's: it continues where the last run stopped.
    expect(body(resumed.published[0])._CHANGE_SEQUENCE_NUMBER).toBe('3')
  })

  describe('publishFrom', () => {
    it('skips everything below the go-live block, including inside a straddling batch', async () => {
      const publisher = new FakePublisher()

      await runPipe({
        publisher,
        statePath: tempStatePath(),
        responses: [
          { statusCode: 200, data: portalBlocks([1, 2, 3, 4]), head: { finalized: { number: 4, hash: '0x4' } } },
        ],
        to: 4,
        targetOptions: { publishFrom: 3 },
      })

      expect(publisher.published.map((message) => body(message).number)).toEqual([3, 4])
    })

    it('resolves "latest" once and keeps the same go-live block across restarts', async () => {
      const publisher = new FakePublisher()
      const statePath = tempStatePath()

      await runPipe({
        publisher,
        statePath,
        responses: [
          {
            statusCode: 200,
            data: portalBlocks([1, 2]),
            head: { finalized: { number: 2, hash: '0x2' }, latest: { number: 2 } },
          },
        ],
        to: 2,
        targetOptions: { publishFrom: 'latest' },
      })

      // Go-live resolved to the head (2), so block 1 was below it.
      expect(publisher.published.map((message) => body(message).number)).toEqual([2])
      await portal?.close()

      const resumed = new FakePublisher()
      portal = await mockPortal([
        {
          statusCode: 200,
          data: portalBlocks([3, 4]),
          head: { finalized: { number: 4, hash: '0x4' }, latest: { number: 4 } },
        },
      ])

      await evmPortalStream({
        id: 'test-pipe',
        portal: portal.url,
        outputs: keyedBlockDecoder({ from: 0, to: 4 }),
      }).pipeTo(
        pubsubTarget<Blocks>({
          pubsub: {} as never,
          publisher: resumed,
          state: { path: statePath },
          publishFrom: 'latest',
          topics: { blocks: blocksRoute() },
        }),
      )

      // A re-resolved 'latest' would have skipped block 3; the persisted go-live keeps it.
      expect(resumed.published.map((message) => body(message).number)).toEqual([3, 4])
    })
  })

  describe('finality', () => {
    it('refuses a dataset that reports no finalized head', async () => {
      const publisher = new FakePublisher()

      await expect(
        runPipe({
          publisher,
          statePath: tempStatePath(),
          responses: [{ statusCode: 200, data: portalBlocks([1]) }],
          to: 1,
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.NO_FINALITY_HEAD })

      expect(publisher.published).toHaveLength(0)
    })

    it('publishes upserts only once the operator declares the dataset fork-free', async () => {
      const publisher = new FakePublisher()

      await runPipe({
        publisher,
        statePath: tempStatePath(),
        responses: [{ statusCode: 200, data: portalBlocks([1]) }],
        to: 1,
        targetOptions: { assumeNoForks: true },
      })

      expect(publisher.operations().map((operation) => operation.op)).toEqual(['upsert'])
    })

    it('refuses to resolve a fork under assumeNoForks — the data has already left', async () => {
      const target = pubsubTarget<Blocks>({
        pubsub: {} as never,
        publisher: new FakePublisher(),
        state: { path: tempStatePath() },
        assumeNoForks: true,
        allowColdStart: true,
        topics: { blocks: blocksRoute() },
      })

      await expect(target.resolveFork!([{ number: 1, hash: '0x1' }])).rejects.toMatchObject({
        code: PUBSUB_ERROR_CODES.FORK_UNDER_ASSUME_NO_FORKS,
      })
    })
  })

  describe('observability', () => {
    it('raises the cold-start gauge on a fresh state and drops it on the next run', async () => {
      const statePath = tempStatePath()
      const cold = mockMetricsServer()

      await runPipe({
        publisher: new FakePublisher(),
        statePath,
        responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
        to: 1,
        metrics: cold,
      })
      expect(cold.gauge('sqd_pubsub_cold_start').lastValue).toBe(1)
      await portal?.close()

      const warm = mockMetricsServer()
      await runPipe({
        publisher: new FakePublisher(),
        statePath,
        responses: [{ statusCode: 200, data: portalBlocks([2]), head: { finalized: { number: 2, hash: '0x2' } } }],
        to: 2,
        metrics: warm,
      })
      expect(warm.gauge('sqd_pubsub_cold_start').lastValue).toBe(0)
    })

    it('counts operations by topic and operation', async () => {
      const metrics = mockMetricsServer()

      await runPipe({
        publisher: new FakePublisher(),
        statePath: tempStatePath(),
        responses: [{ statusCode: 200, data: portalBlocks([1, 2]), head: { finalized: { number: 2, hash: '0x2' } } }],
        to: 2,
        metrics,
      })

      expect(metrics.counter('sqd_pubsub_operations_total').total).toBe(2)
      expect(metrics.counter('sqd_pubsub_operations_total').calls[0].labels).toMatchObject({
        topic: 'blocks',
        operation: 'upsert',
      })
    })

    describe('commit-lag histograms', () => {
      /** Block 5 on the fixture chain, published 90s of wall clock later. */
      const BLOCK = 5
      const PUBLISHED_AT = chainTime(BLOCK) + 90

      afterEach(() => {
        vi.useRealTimers()
      })

      /** Freezes the clock at `PUBLISHED_AT`, minus a publish window the drain then consumes. */
      function atPublishTime(publishSeconds = 0) {
        vi.useFakeTimers()
        vi.setSystemTime(new Date((PUBLISHED_AT - publishSeconds) * 1000))

        const publisher = new FakePublisher()
        publisher.onDrain = () => vi.advanceTimersByTime(publishSeconds * 1000)

        return publisher
      }

      it('observes block_to_commit_lag as wall-clock ack minus the block’s chain time', async () => {
        const metrics = mockMetricsServer()

        await driveBatches({
          statePath: tempStatePath(),
          publisher: atPublishTime(),
          blocks: [BLOCK],
          finalized: BLOCK,
          metrics,
        })

        expect(metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').observations).toEqual([90])
      })

      it('observes portal_to_commit_lag as wall-clock ack minus batch.lastBlockReceivedAt', async () => {
        const metrics = mockMetricsServer()

        await driveBatches({
          statePath: tempStatePath(),
          publisher: atPublishTime(),
          blocks: [BLOCK],
          finalized: BLOCK,
          metrics,
          lastBlockReceivedAt: new Date((PUBLISHED_AT - 6) * 1000),
        })

        expect(metrics.histogram('sqd_pubsub_portal_to_commit_lag_seconds').observations).toEqual([6])
      })

      it('labels both histograms with the pipe id', async () => {
        const metrics = mockMetricsServer()

        await driveBatches({
          statePath: tempStatePath(),
          publisher: atPublishTime(),
          blocks: [BLOCK],
          finalized: BLOCK,
          metrics,
        })

        // An undeclared label name throws in prom-client and an unlabelled observe on a
        // labelled histogram silently creates a series no dashboard query matches.
        expect(metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').calls[0].labels).toEqual({ id: 'test-pipe' })
        expect(metrics.histogram('sqd_pubsub_portal_to_commit_lag_seconds').calls[0].labels).toEqual({
          id: 'test-pipe',
        })
      })

      it('anchors on the publish ack, not on the outbox ack-delete that follows it', async () => {
        const metrics = mockMetricsServer()

        // The drain burns 4s publishing; the state.confirm() after it must not be inside the
        // measured quantity, so the lag stays the block's own 90s.
        await driveBatches({
          statePath: tempStatePath(),
          publisher: atPublishTime(4),
          blocks: [BLOCK],
          finalized: BLOCK,
          metrics,
        })

        expect(metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').observations).toEqual([90])
        expect(metrics.histogram('sqd_pubsub_publish_duration_seconds').observations).toEqual([4])
      })

      it('still observes when the publish fails, so an outage climbs instead of going silent', async () => {
        const metrics = mockMetricsServer()
        const publisher = atPublishTime()
        publisher.failOn = () => new Error('publish boom')

        await expect(
          driveBatches({
            statePath: tempStatePath(),
            publisher,
            blocks: [BLOCK],
            finalized: BLOCK,
            metrics,
            lastBlockReceivedAt: new Date((PUBLISHED_AT - 6) * 1000),
          }),
        ).rejects.toThrow('publish boom')

        expect(metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').observations).toEqual([90])
        expect(metrics.histogram('sqd_pubsub_portal_to_commit_lag_seconds').observations).toEqual([6])
      })

      it('normalizes millisecond chain timestamps instead of reporting them 1000x off', async () => {
        const metrics = mockMetricsServer()

        // tron and substrate both declare epoch ms; verbatim subtraction yields ~-1.7e12.
        await driveBatches({
          statePath: tempStatePath(),
          publisher: atPublishTime(),
          blocks: [BLOCK],
          finalized: BLOCK,
          metrics,
          blockTimestamp: (n) => chainTime(n) * 1000,
        })

        expect(metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').observations).toEqual([90])
      })

      it('drops the above-2^53 timestamps tron emits rather than poisoning the histogram sum', async () => {
        const metrics = mockMetricsServer()

        await driveBatches({
          statePath: tempStatePath(),
          publisher: atPublishTime(),
          blocks: [BLOCK],
          finalized: BLOCK,
          metrics,
          // Parsed, not spelled: the literal cannot be written at full precision.
          blockTimestamp: () => Number('639208360527210660'),
        })

        // One such observation would move `_sum` to a magnitude where the float64 ULP exceeds
        // every later sample, freezing the series for the life of the process.
        expect(metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').observations).toEqual([])
      })

      it('skips block_to_commit_lag, but not portal_to_commit_lag, when the cursor has no timestamp', async () => {
        const metrics = mockMetricsServer()

        await driveBatches({
          statePath: tempStatePath(),
          publisher: atPublishTime(),
          blocks: [BLOCK],
          finalized: BLOCK,
          metrics,
          lastBlockReceivedAt: new Date((PUBLISHED_AT - 6) * 1000),
          blockTimestamp: () => undefined,
        })

        expect(metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').observations).toEqual([])
        expect(metrics.histogram('sqd_pubsub_portal_to_commit_lag_seconds').observations).toEqual([6])
      })

      it('observes neither histogram below the go-live block', async () => {
        const metrics = mockMetricsServer()
        const publisher = atPublishTime()

        await driveBatches({
          statePath: tempStatePath(),
          publisher,
          blocks: [BLOCK],
          finalized: BLOCK,
          metrics,
          targetOptions: { publishFrom: 1_000_000 },
        })

        // Nothing was published, so a reading here would describe a stage that never ran.
        expect(publisher.published).toEqual([])
        expect(metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').observations).toEqual([])
        expect(metrics.histogram('sqd_pubsub_portal_to_commit_lag_seconds').observations).toEqual([])
      })

      it('observes once per batch across a multi-batch run', async () => {
        const metrics = mockMetricsServer()

        await driveBatches({
          statePath: tempStatePath(),
          publisher: atPublishTime(),
          blocks: [1, 2, 3],
          finalized: 3,
          metrics,
        })

        const observations = metrics.histogram('sqd_pubsub_block_to_commit_lag_seconds').observations
        expect(observations).toHaveLength(3)

        // Each batch is measured against its own block, 12s apart on the fixture chain.
        expect(observations[0] - observations[1]).toBe(12)
        expect(observations[1] - observations[2]).toBe(12)
      })
    })

    describe('recovery drain metrics', () => {
      it('reports the restart backlog once the first batch registers the metrics', async () => {
        const statePath = tempStatePath()
        const metrics = mockMetricsServer()

        // Leave a full outbox behind: the publish fails, so the rows stay queued.
        const failing = new FakePublisher()
        failing.failOn = () => new Error('publish boom')
        await expect(
          driveBatches({ statePath, publisher: failing, blocks: [1], finalized: 1, metrics: mockMetricsServer() }),
        ).rejects.toThrow('publish boom')

        // The restart drains that backlog before any batch has brought ctx.metrics.
        const recovered = new FakePublisher()
        await driveBatches({ statePath, publisher: recovered, blocks: [2], finalized: 2, metrics })

        expect(recovered.published.length).toBeGreaterThan(1)

        // Two drains: the deferred recovery one, then the batch's own.
        expect(metrics.histogram('sqd_pubsub_publish_duration_seconds').observations).toHaveLength(2)
        expect(metrics.counter('sqd_pubsub_published_bytes_total').total).toBeGreaterThan(0)
      })
    })
  })

  describe('route validation', () => {
    it('refuses generated ids on a fork-capable dataset without block hashes', async () => {
      const target = pubsubTarget<Blocks>({
        pubsub: {} as never,
        publisher: new FakePublisher(),
        state: { path: tempStatePath() },
        publishFrom: 0,
        allowColdStart: true,
        topics: {
          blocks: {
            topic: 'blocks',
            map: ({ data }) => data.map((header) => ({ data: header, block: { number: header.number } })),
          },
        },
      })

      async function* read() {
        yield {
          data: { blocks: [{ number: 1, hash: '0x1', timestamp: 1 }] },
          ctx: makeBatchContext({ current: { number: 1, hash: '0x1' }, finalized: { number: 0, hash: '0x0' } }),
        }
      }

      await expect(target.write({ read: read as never, logger: testLogger() })).rejects.toMatchObject({
        code: PUBSUB_ERROR_CODES.MISSING_BLOCK_HASH,
      })
    })

    it('refuses two drafts claiming the same id in one batch', async () => {
      const publisher = new FakePublisher()

      await expect(
        runPipe({
          publisher,
          statePath: tempStatePath(),
          responses: [{ statusCode: 200, data: portalBlocks([1, 2]), head: { finalized: { number: 2, hash: '0x2' } } }],
          to: 2,
          targetOptions: {
            topics: {
              blocks: {
                topic: 'blocks',
                map: ({ data }) => data.map((header) => ({ data: header, block: header, id: 'same' })),
              },
            },
          },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.DUPLICATE_DRAFT_ID })
    })

    it('uses data._id as the row identity', async () => {
      const publisher = new FakePublisher()

      await runPipe({
        publisher,
        statePath: tempStatePath(),
        responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
        to: 1,
        targetOptions: {
          topics: {
            blocks: {
              topic: 'blocks',
              map: ({ data }) =>
                data.map((header) => ({ data: { ...header, _id: 'owned' }, block: header, id: 'fallback' })),
            },
          },
        },
      })

      expect(body(publisher.published[0])._id).toBe('owned')
    })

    it('refuses a materialized route that switches id sources between revisions', async () => {
      await expect(
        driveBatches({
          publisher: new FakePublisher(),
          statePath: tempStatePath(),
          blocks: [1, 2],
          finalized: 0,
          targetOptions: {
            topics: {
              blocks: {
                topic: 'blocks',
                mode: 'materialized',
                map: ({ data }) =>
                  data.map((header) => ({
                    data: header.number === 1 ? { _id: 'row', number: header.number } : { number: header.number },
                    block: header,
                    id: 'row',
                  })),
              },
            },
          },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED })
    })

    it('refuses a non-string data._id', async () => {
      const publisher = new FakePublisher()

      await expect(
        runPipe({
          publisher,
          statePath: tempStatePath(),
          responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
          to: 1,
          targetOptions: {
            topics: {
              blocks: {
                topic: 'blocks',
                map: ({ data }) => data.map((header) => ({ data: { ...header, _id: 42 }, block: header })),
              },
            },
          },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.INVALID_CDC_ROW })

      expect(publisher.published).toHaveLength(0)
    })

    it('refuses an oversized ordering key', async () => {
      await expect(
        runPipe({
          publisher: new FakePublisher(),
          statePath: tempStatePath(),
          responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
          to: 1,
          targetOptions: {
            publish: { messageOrdering: true },
            topics: {
              blocks: {
                topic: 'blocks',
                map: ({ data }) =>
                  data.map((header) => ({ data: header, block: header, orderingKey: 'k'.repeat(1025) })),
              },
            },
          },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET })
    })

    it('refuses a _uid that could not fit once its sequence is appended', async () => {
      await expect(
        runPipe({
          publisher: new FakePublisher(),
          statePath: tempStatePath(),
          responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
          to: 1,
          targetOptions: { namespace: 'n'.repeat(1010), publish: { uidAttribute: true } },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET })
    })

    it('refuses an oversized rollback inverse — it is published with no draft left to fix', async () => {
      await expect(
        runPipe({
          publisher: new FakePublisher(),
          statePath: tempStatePath(),
          responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 0, hash: '0x0' } } }],
          to: 1,
          targetOptions: {
            topics: {
              blocks: {
                topic: 'blocks',
                mode: 'materialized',
                map: ({ data }) => data.map((header) => ({ data: header, block: header, id: 'row' })),
                rollbackWhenMissing: () => ({ op: 'upsert', data: { value: 'x'.repeat(11 * 1024 * 1024) } }),
              },
            },
          },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.MESSAGE_TOO_LARGE })
    })

    it('refuses a payload that leaves no room for the publish-request envelope', async () => {
      const publisher = new FakePublisher()

      await expect(
        driveBatches({
          publisher,
          statePath: tempStatePath(),
          blocks: [1],
          finalized: 1,
          targetOptions: {
            topics: {
              blocks: {
                topic: 'blocks',
                map: ({ data }) =>
                  data.map((header) => ({
                    data: { value: 'x'.repeat(PUBSUB_LIMITS.maxMessageBytes) },
                    block: header,
                    id: 'row',
                  })),
              },
            },
          },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.MESSAGE_TOO_LARGE })

      expect(publisher.published).toHaveLength(0)
    })

    it('refuses a delete-free window route declared without its empty value', () => {
      expect(() => windowTopic({ topic: 'candles', emptyWindows: 'upsert' })).toThrow(
        PUBSUB_ERROR_CODES.MISSING_EMPTY_VALUES,
      )
    })
  })

  describe('the sequence barrier', () => {
    const secondTopic = { blocks: blocksRoute(), extra: blocksRoute('other-topic') } as never

    it('refuses a producer that would split its counter across topics', () => {
      expect(() =>
        pubsubTarget<Blocks>({
          pubsub: {} as never,
          publisher: new FakePublisher(),
          state: { path: tempStatePath() },
          topics: secondTopic,
        }),
      ).toThrowError(expect.objectContaining({ code: PUBSUB_ERROR_CODES.MULTIPLE_TOPICS }))
    })

    it('accepts several topics once the barrier is declared off', () => {
      expect(() =>
        pubsubTarget<Blocks>({
          pubsub: {} as never,
          publisher: new FakePublisher(),
          state: { path: tempStatePath() },
          sequenceBarrier: false,
          topics: secondTopic,
        }),
      ).not.toThrow()
    })

    /** Descending drafts: the second operation would take a higher number than the first. */
    function descendingRoute(topic = 'blocks') {
      return {
        topic,
        map: ({ data }: { data: Blocks['blocks'] }) =>
          [...data, ...data].map((header, index) => ({
            data: { number: header.number - index },
            block: { ...header, number: header.number - index },
          })),
      }
    }

    it('refuses a batch whose operations step backwards in block order', async () => {
      await expect(
        driveBatches({
          statePath: tempStatePath(),
          publisher: new FakePublisher(),
          blocks: [5],
          finalized: 0,
          targetOptions: { topics: { blocks: descendingRoute() } },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.BLOCK_ORDER })
    })

    it('refuses a step backwards between batches, not only inside one', async () => {
      await expect(
        driveBatches({
          statePath: tempStatePath(),
          publisher: new FakePublisher(),
          blocks: [5, 4],
          finalized: 0,
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.BLOCK_ORDER })
    })

    it('checks no block order once the barrier is declared off', async () => {
      const publisher = new FakePublisher()

      await driveBatches({
        statePath: tempStatePath(),
        publisher,
        blocks: [5, 4],
        finalized: 0,
        targetOptions: { sequenceBarrier: false },
      })

      expect(publisher.operations()).toHaveLength(2)
    })

    /** Two routes onto one topic, over a single batch spanning two blocks. */
    async function writeSharedTopic(publisher: FakePublisher, sequenceBarrier?: boolean) {
      const headers = [100, 101].map(header)

      const target = pubsubTarget<Blocks & { approvals: Blocks['blocks'] }>({
        pubsub: {} as never,
        publisher,
        state: { path: tempStatePath() },
        publishFrom: 0,
        allowColdStart: true,
        sequenceBarrier,
        topics: { blocks: blocksRoute(), approvals: blocksRoute() },
      })

      async function* read() {
        yield {
          data: { blocks: headers, approvals: headers },
          ctx: makeBatchContext({
            current: headers[1],
            finalized: header(90),
            rollbackChain: headers,
          }),
        }
      }

      await target.write({ read: read as never, logger: testLogger(), id: 'test-pipe' })
    }

    /**
     * The barrier admits one multi-block route per producer (GAP-40): operations are sequenced
     * route after route, so the second route restarts at the batch's first block. No draft order
     * the mapper can supply avoids it — this pins the limit the docs state.
     */
    it('refuses a second route on the same topic once a batch spans several blocks', async () => {
      await expect(writeSharedTopic(new FakePublisher())).rejects.toMatchObject({
        code: PUBSUB_ERROR_CODES.BLOCK_ORDER,
      })
    })

    it('takes a second route on the same topic once the barrier is declared off', async () => {
      const publisher = new FakePublisher()

      await writeSharedTopic(publisher, false)

      expect(publisher.operations()).toHaveLength(4)
    })
  })

  describe('the finality watermark', () => {
    const finalizedOf = (publisher: FakePublisher) =>
      publisher.published.map((message) => message.attributes['_finalized'])

    it('stamps the source finalized head on every message', async () => {
      const publisher = new FakePublisher()

      await driveBatches({ statePath: tempStatePath(), publisher, blocks: [1, 2], finalized: 5 })

      // Not a per-row reading: the same reference value rides every message of the batch.
      expect(finalizedOf(publisher)).toEqual(['5', '5'])
    })

    it('advances the stamp as the source finalizes more blocks', async () => {
      const publisher = new FakePublisher()

      await driveBatches({
        statePath: tempStatePath(),
        publisher,
        blocks: [1, 2, 3],
        finalized: 0,
        finalizedAt: (block) => block * 100,
      })

      expect(finalizedOf(publisher)).toEqual(['100', '200', '300'])
    })

    it('stalls while the table is quiet, and the next row carries the head it reached', async () => {
      const publisher = new FakePublisher()

      await driveBatches({
        statePath: tempStatePath(),
        publisher,
        blocks: [1, 2, 3],
        finalized: 0,
        finalizedAt: (block) => block * 100,
        // Riding rows means a stretch that publishes nothing publishes no watermark either. That
        // only delays compaction: nothing accrues downstream while nothing arrives.
        targetOptions: {
          topics: {
            blocks: {
              ...blocksRoute(),
              map: ({ data }) => (data[0].number < 3 ? [] : [{ data: { number: 3 }, block: data[0] }]),
            },
          },
        },
      })

      expect(finalizedOf(publisher)).toEqual(['300'])
    })

    it('keeps the last known head when a batch reports none', async () => {
      const publisher = new FakePublisher()

      await driveBatches({
        statePath: tempStatePath(),
        publisher,
        blocks: [1, 2],
        finalized: 0,
        finalizedAt: (block) => (block === 1 ? 50 : undefined),
      })

      expect(finalizedOf(publisher)).toEqual(['50', '50'])
    })

    it('stamps the persisted head on a restart’s recovery drain, before any batch arrives', async () => {
      const statePath = tempStatePath()

      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')
      await expect(driveBatches({ statePath, publisher: failing, blocks: [1], finalized: 50 })).rejects.toThrow(
        'network down',
      )

      // No batches at all: everything published here comes from the recovery drain, and a cold
      // consumer still gets a floor to reason against on its very first message.
      const restarted = new FakePublisher()
      await driveBatches({ statePath, publisher: restarted, blocks: [], finalized: 0 })

      expect(finalizedOf(restarted)).toEqual(['50'])
    })

    it('rides the producer-wide attributes rather than replacing them', async () => {
      const publisher = new FakePublisher()

      await driveBatches({
        statePath: tempStatePath(),
        publisher,
        blocks: [1],
        finalized: 5,
        targetOptions: { attributes: { chain: 'mock', table: 'blocks' } },
      })

      expect(publisher.published[0].attributes).toEqual({
        _finalized: '5',
        chain: 'mock',
        table: 'blocks',
      })
    })

    it('omits it on a dataset that reports no finalized head at all', async () => {
      const publisher = new FakePublisher()

      await runPipe({
        publisher,
        statePath: tempStatePath(),
        responses: [{ statusCode: 200, data: portalBlocks([1]) }],
        to: 1,
        targetOptions: { assumeNoForks: true },
      })

      expect(publisher.published[0].attributes).not.toHaveProperty('_finalized')
    })
  })

  describe('cold start', () => {
    it('refuses a run that would restart the change sequence', async () => {
      await expect(
        driveBatches({
          statePath: tempStatePath(),
          publisher: new FakePublisher(),
          blocks: [1],
          finalized: 1,
          targetOptions: { allowColdStart: false },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.COLD_START_REFUSED })
    })

    it('publishes nothing at all when it refuses', async () => {
      const publisher = new FakePublisher()

      await expect(
        driveBatches({
          statePath: tempStatePath(),
          publisher,
          blocks: [1],
          finalized: 1,
          targetOptions: { allowColdStart: false },
        }),
      ).rejects.toThrow()

      expect(publisher.published).toHaveLength(0)
      expect(publisher.setupCalls).toHaveLength(0)
    })

    /**
     * The refusal is worthless if it disarms itself: a state file stamped on the way out reads
     * as a warm start, so the retry would publish from sequence zero with no error, no warning
     * and a `cold_start` gauge of 0 — quieter than the warning this replaced.
     */
    it('leaves the state untouched, so a retry meets the same refusal', async () => {
      const statePath = tempStatePath()

      for (const _ of [1, 2, 3]) {
        await expect(
          driveBatches({
            statePath,
            publisher: new FakePublisher(),
            blocks: [1],
            finalized: 1,
            targetOptions: { allowColdStart: false },
          }),
        ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.COLD_START_REFUSED })
      }
    })

    it('still bootstraps once the declaration arrives, sequencing from the start', async () => {
      const statePath = tempStatePath()

      await expect(
        driveBatches({
          statePath,
          publisher: new FakePublisher(),
          blocks: [1],
          finalized: 1,
          targetOptions: { allowColdStart: false },
        }),
      ).rejects.toThrow()

      const declared = new FakePublisher()
      await driveBatches({ statePath, publisher: declared, blocks: [1], finalized: 1 })

      expect(declared.operations().map((operation) => operation.seq)).toEqual([1])
    })

    it('needs no declaration on a warm restart', async () => {
      const statePath = tempStatePath()
      await driveBatches({ statePath, publisher: new FakePublisher(), blocks: [1], finalized: 1 })

      const restarted = new FakePublisher()
      await driveBatches({
        statePath,
        publisher: restarted,
        blocks: [2],
        finalized: 1,
        targetOptions: { allowColdStart: false },
      })

      expect(restarted.operations()).toHaveLength(1)
    })
  })

  describe('startup failures', () => {
    it('requires an outbox route to remain configured during recovery', async () => {
      const statePath = tempStatePath()
      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')

      await expect(driveBatches({ statePath, publisher: failing, blocks: [1], finalized: 0 })).rejects.toThrow(
        'network down',
      )

      // The target is valid on its own terms — it just no longer configures the route the
      // pending row belongs to, so recovery cannot encode it.
      await expect(
        driveBatches({
          statePath,
          publisher: new FakePublisher(),
          blocks: [],
          finalized: 0,
          targetOptions: { topics: { renamed: blocksRoute() } as never },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.ROUTE_NOT_CONFIGURED })
    })

    it('releases the state lock when topic validation fails', async () => {
      const statePath = tempStatePath()
      const failing = new FakePublisher()
      failing.setup = async () => {
        throw new Error('topic does not exist')
      }

      await expect(
        runPipe({
          publisher: failing,
          statePath,
          responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
          to: 1,
        }),
      ).rejects.toThrow('topic does not exist')
      await portal?.close()

      // The state file holds an exclusive lock; a startup failure that kept it would make the
      // corrected retry fail as a second producer instead of starting.
      const retry = new FakePublisher()
      await runPipe({
        publisher: retry,
        statePath,
        responses: [{ statusCode: 200, data: portalBlocks([1]), head: { finalized: { number: 1, hash: '0x1' } } }],
        to: 1,
      })

      expect(retry.published).toHaveLength(1)
    })

    it('releases the state lock when the recovery drain fails', async () => {
      const statePath = tempStatePath()

      const first = new FakePublisher()
      await driveBatches({ statePath, publisher: first, blocks: [1], finalized: 0 })

      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')
      // Leaves an unconfirmed row behind, so the next start drains before it reads anything.
      await expect(driveBatches({ statePath, publisher: failing, blocks: [2], finalized: 0 })).rejects.toThrow(
        'network down',
      )

      const retry = new FakePublisher()
      await driveBatches({ statePath, publisher: retry, blocks: [], finalized: 0 })

      expect(retry.published.map((message) => body(message).number)).toEqual([2])
    })
  })

  describe('crash recovery (CN-17, CN-46)', () => {
    it('republishes an unconfirmed operation with the same seq and bytes', async () => {
      const statePath = tempStatePath()

      const failing = new FakePublisher()
      const decode = (row: { payload: Uint8Array }) => JSON.parse(new TextDecoder().decode(row.payload))
      failing.failOn = (row) => (decode(row).number === 2 ? new Error('publish timed out') : undefined)

      await expect(driveBatches({ statePath, publisher: failing, blocks: [1, 2], finalized: 0 })).rejects.toThrow(
        'publish timed out',
      )
      expect(failing.published.map((message) => body(message).number)).toEqual([1])

      const restarted = new FakePublisher()
      await driveBatches({ statePath, publisher: restarted, blocks: [], finalized: 0 })

      // The row that never confirmed comes back byte- and seq-identical, so a consumer drops it.
      expect(restarted.published).toHaveLength(1)
      expect(body(restarted.published[0])).toMatchObject({ number: 2, _CHANGE_SEQUENCE_NUMBER: '2' })
    })

    it('recovers an outbox row through its route custom encoder', async () => {
      const statePath = tempStatePath()
      const encode = (message: object) => JSON.stringify({ record: message })
      const topics = { blocks: { ...blocksRoute(), encode } }

      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')
      await expect(
        driveBatches({ statePath, publisher: failing, blocks: [1], finalized: 0, targetOptions: { topics } }),
      ).rejects.toThrow('network down')

      const restarted = new FakePublisher()
      await driveBatches({ statePath, publisher: restarted, blocks: [], finalized: 0, targetOptions: { topics } })

      expect(body(restarted.published[0]).record).toMatchObject({
        number: 1,
        _CHANGE_TYPE: 'UPSERT',
        _CHANGE_SEQUENCE_NUMBER: '1',
      })
    })

    it.each([
      ['adding', undefined, (message: object) => JSON.stringify({ record: message })],
      ['removing', (message: object) => JSON.stringify({ record: message }), undefined],
    ])('refuses %s a route encoder while that route has a pending outbox row', async (_name, first, second) => {
      const statePath = tempStatePath()
      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')

      await expect(
        driveBatches({
          statePath,
          publisher: failing,
          blocks: [1],
          finalized: 0,
          targetOptions: { topics: { blocks: { ...blocksRoute(), encode: first } } },
        }),
      ).rejects.toThrow('network down')

      await expect(
        driveBatches({
          statePath,
          publisher: new FakePublisher(),
          blocks: [],
          finalized: 0,
          targetOptions: { topics: { blocks: { ...blocksRoute(), encode: second } } },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.STATE_WIRE_CONFIG_MISMATCH })
    })

    it('refuses a wire config it cannot compare, while the outbox is pending', async () => {
      const statePath = tempStatePath()
      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')

      await expect(driveBatches({ statePath, publisher: failing, blocks: [1], finalized: 0 })).rejects.toThrow(
        'network down',
      )

      const state = new SqlitePubsubState({ path: statePath })
      await state.open({ cursorKey: 'test-pipe', logger: testLogger() })
      await state.setMeta('wire_config', JSON.stringify(['test-pipe', false, false]))
      await state.close()

      await expect(
        driveBatches({ statePath, publisher: new FakePublisher(), blocks: [], finalized: 0 }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.STATE_WIRE_CONFIG_MISMATCH })
    })

    it('rechecks a custom encoder output size during recovery', async () => {
      const statePath = tempStatePath()
      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')

      await expect(
        driveBatches({
          statePath,
          publisher: failing,
          blocks: [1],
          finalized: 0,
          targetOptions: {
            topics: { blocks: { ...blocksRoute(), encode: (message) => JSON.stringify({ record: message }) } },
          },
        }),
      ).rejects.toThrow('network down')

      const restarted = new FakePublisher()
      await expect(
        driveBatches({
          statePath,
          publisher: restarted,
          blocks: [],
          finalized: 0,
          targetOptions: {
            topics: {
              blocks: {
                ...blocksRoute(),
                encode: () => 'x'.repeat(PUBSUB_LIMITS.maxMessageBytes + 1),
              },
            },
          },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.MESSAGE_TOO_LARGE })

      expect(restarted.published).toHaveLength(0)
    })

    it('refuses a uidAttribute change before recovery can mutate the envelope', async () => {
      const statePath = tempStatePath()

      class AmbiguousPublisher extends FakePublisher {
        override async drain(rows: Parameters<FakePublisher['drain']>[0]) {
          const result = await super.drain(rows)

          return { ...result, confirmed: [], error: new Error('publish outcome unknown') }
        }
      }

      const first = new AmbiguousPublisher()
      await expect(
        driveBatches({
          statePath,
          publisher: first,
          blocks: [1],
          finalized: 0,
          targetOptions: { publish: { uidAttribute: true } },
        }),
      ).rejects.toThrow('publish outcome unknown')

      const restarted = new FakePublisher()
      await expect(
        driveBatches({
          statePath,
          publisher: restarted,
          blocks: [],
          finalized: 0,
          targetOptions: { publish: { uidAttribute: false } },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.STATE_WIRE_CONFIG_MISMATCH })

      expect(restarted.published).toHaveLength(0)
    })

    it('never loses an operation whose batch committed before the crash', async () => {
      const statePath = tempStatePath()

      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')

      await expect(driveBatches({ statePath, publisher: failing, blocks: [1], finalized: 0 })).rejects.toThrow(
        'network down',
      )
      expect(failing.published).toHaveLength(0)

      const restarted = new FakePublisher()
      await driveBatches({ statePath, publisher: restarted, blocks: [], finalized: 0 })

      expect(restarted.published.map((message) => body(message).number)).toEqual([1])
    })
  })
})
