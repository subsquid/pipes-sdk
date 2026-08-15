import { afterEach, describe, expect, it } from 'vitest'

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

/** Drives `target.write()` one block per batch, so batch boundaries are the test's to choose. */
async function driveBatches({
  statePath,
  publisher,
  blocks,
  finalized,
  targetOptions = {},
}: {
  statePath: string
  publisher: FakePublisher
  blocks: number[]
  finalized: number
  targetOptions?: Partial<Parameters<typeof pubsubTarget<Blocks>>[0]>
}) {
  const target = pubsubTarget<Blocks>({
    pubsub: {} as never,
    publisher,
    state: { path: statePath },
    publishFrom: 0,
    topics: { blocks: blocksRoute() },
    ...targetOptions,
  })

  async function* read() {
    for (const number of blocks) {
      yield {
        data: { blocks: [{ number, hash: `0x${number}`, timestamp: number }] },
        ctx: makeBatchContext({
          current: { number, hash: `0x${number}` },
          finalized: { number: finalized, hash: `0x${finalized}` },
          rollbackChain: [{ number, hash: `0x${number}` }],
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
      attributes: { chain: 'mock' },
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

      expect(metrics.counter('sqd_pubsub_ops_total').total).toBe(2)
      expect(metrics.counter('sqd_pubsub_ops_total').calls[0].labels).toMatchObject({ topic: 'blocks', op: 'upsert' })
    })
  })

  describe('route validation', () => {
    it('refuses generated ids on a fork-capable dataset without block hashes', async () => {
      const target = pubsubTarget<Blocks>({
        pubsub: {} as never,
        publisher: new FakePublisher(),
        state: { path: tempStatePath() },
        publishFrom: 0,
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

  describe('startup failures', () => {
    it('requires an outbox route to remain configured during recovery', async () => {
      const statePath = tempStatePath()
      const failing = new FakePublisher()
      failing.failOn = () => new Error('network down')

      await expect(driveBatches({ statePath, publisher: failing, blocks: [1], finalized: 0 })).rejects.toThrow(
        'network down',
      )

      await expect(
        driveBatches({
          statePath,
          publisher: new FakePublisher(),
          blocks: [],
          finalized: 0,
          targetOptions: { topics: {} },
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

    it('refuses ambiguous legacy encoder metadata while the outbox is pending', async () => {
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
