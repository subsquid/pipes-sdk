import { afterEach, beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest'

import { defaultLogger } from '~/core/logger.js'
import { PortalCache } from '~/core/portal-source.js'
import { SpanHooks } from '~/core/profiling.js'
import { Target, createTarget } from '~/core/target.js'
import { TransformerArgs, createTransformer } from '~/core/transformer.js'
import { evmPortalStream, evmQuery } from '~/evm/index.js'
import { PortalBlockStreamOptions, PortalClient, PortalRequestOptions, Query } from '~/portal-client/index.js'
import { MockPortal, blockDecoder, finalizedMockPortal, mockPortal, readAll } from '~/testing/index.js'

/**
 * Tallies span starts and ends per name. A single global total hides the interesting cases — a
 * double-end on one span cancels out a leak on another.
 */
function spanCounter() {
  const started: Record<string, number> = {}
  const ended: Record<string, number> = {}

  const track = (name: string): SpanHooks => ({
    onStart: (child) => {
      started[child] = (started[child] ?? 0) + 1

      return track(child)
    },
    onEnd: () => {
      ended[name] = (ended[name] ?? 0) + 1
    },
  })

  return {
    hooks: track('<root>'),
    started,
    ended,
    unbalanced: () =>
      [...new Set([...Object.keys(started), ...Object.keys(ended)])]
        .filter((name) => (started[name] ?? 0) !== (ended[name] ?? 0))
        .map((name) => `${name}: ${started[name] ?? 0} started, ${ended[name] ?? 0} ended`),
  }
}

function threeBlockPortal() {
  return mockPortal(
    [1, 2, 3].map((number) => ({
      statusCode: 200,
      data: [{ header: { number, hash: `0x${number}`, timestamp: number * 1000 } }],
      head: { finalized: { number: 0, hash: '0x0' } },
    })),
  )
}

describe('Portal abstract stream', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  describe('common', () => {
    it('rejects an empty source id', async () => {
      // Targets key their persisted cursor by the source id, and an empty id would silently fall
      // back to the shared legacy "stream" key — reintroducing cross-pipe cursor collisions.
      expect(() =>
        evmPortalStream({
          id: '  ',
          portal: 'http://localhost:1',
          outputs: blockDecoder({ from: 0, to: 1 }),
        }),
      ).toThrow(/non-empty "id"/)
    })

    it('should expose finalization headers', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 2, hash: '0x456', timestamp: 2000 } }],
          head: {
            finalized: { number: 10, hash: '0xfinalized' },
            latest: { number: 12 },
          },
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 2 }),
      })

      let firstCtx
      for await (const { ctx } of stream) {
        firstCtx = {
          head: ctx.stream.head,
          progress_state: ctx.stream.progress?.state,
        }
      }

      expect(firstCtx).toMatchInlineSnapshot(`
        {
          "head": {
            "finalized": {
              "hash": "0xfinalized",
              "number": 10,
            },
            "latest": {
              "number": 12,
            },
          },
          "progress_state": {
            "current": 2,
            "etaSeconds": 0,
            "from": 0,
            "percent": 100,
            "to": 2,
          },
        }
      `)
    })

    it('should adjust latest block number from data over header', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 14, hash: '0x456', timestamp: 14000 } }], // latest block is 14 in data
          head: {
            finalized: { number: 10, hash: '0xfinalized' },
            latest: { number: 12 }, // but 12 in header
          },
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 2 }),
      })

      let firstCtx
      for await (const { ctx } of stream) {
        firstCtx = {
          head: ctx.stream.head,
          progress_state: ctx.stream.progress?.state,
        }
      }

      expect(firstCtx).toMatchInlineSnapshot(`
        {
          "head": {
            "finalized": {
              "hash": "0xfinalized",
              "number": 10,
            },
            "latest": {
              "number": 12,
            },
          },
          "progress_state": {
            "current": 14,
            "etaSeconds": 0,
            "from": 0,
            "percent": 100,
            "to": 14,
          },
        }
      `)
    })

    it('should keep requesting data on head', async () => {
      portal = await mockPortal([
        {
          statusCode: 204,
        },
        {
          statusCode: 204,
        },
        {
          statusCode: 204,
        },
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x123', timestamp: 1000 } }],
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 1 }),
      })

      expect(await readAll(stream)).toMatchInlineSnapshot(`
        [
          {
            "hash": "0x123",
            "number": 1,
            "timestamp": 1000,
          },
        ]
      `)
    })
  })

  describe('unfinalized', () => {
    it('should receive all stream data and stop', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x123', timestamp: 1000 } },
            { header: { number: 2, hash: '0x456', timestamp: 2000 } },
          ],
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 2 }),
      })

      const res = await readAll(stream)

      expect(res).toMatchInlineSnapshot(`
        [
          {
            "hash": "0x123",
            "number": 1,
            "timestamp": 1000,
          },
          {
            "hash": "0x456",
            "number": 2,
            "timestamp": 2000,
          },
        ]
      `)
    })

    it('should retries 10 by default', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x123', timestamp: 1000 } }],
        },
        ...new Array(10).fill({ statusCode: 503 }),
        {
          statusCode: 200,
          data: [{ header: { number: 2, hash: '0x456', timestamp: 2000 } }],
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: {
          url: portal.url,
          http: { retrySchedule: [0] },
        },
        outputs: blockDecoder({ from: 0, to: 2 }),
      })

      const res = await readAll(stream)

      expect(res).toMatchInlineSnapshot(`
        [
          {
            "hash": "0x123",
            "number": 1,
            "timestamp": 1000,
          },
          {
            "hash": "0x456",
            "number": 2,
            "timestamp": 2000,
          },
        ]
      `)
    })

    it('should throw an error after max retries', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x123', timestamp: 1000 } }],
        },
        ...new Array(2).fill({ statusCode: 503 }),
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: {
          url: portal.url,
          http: {
            retryAttempts: 1,
            retrySchedule: [0],
          },
        },
        outputs: blockDecoder({ from: 0, to: 2 }),
      })

      await expect(readAll(stream)).rejects.toThrow(`Got 503 from ${portal.url}`)
      await stream.stop()
    })

    it('should throw fork exception', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [
            {
              header: {
                number: 100_000_000,
                hash: '0x100000000',
                timestamp: 100_000_000_000,
              },
            },
          ],
        },
        {
          statusCode: 409,
          data: {
            previousBlocks: [
              {
                number: 99_999_999,
                hash: '0x99999999__1',
              },
              {
                number: 100_000_000,
                hash: '0x100000000__1',
              },
            ],
          },
          validateRequest: (req) => {
            expect(req).toMatchObject({
              type: 'evm',
              fromBlock: 100_000_001,
              parentBlockHash: '0x100000000',
            })
          },
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: {
          url: portal.url,
          http: { retryAttempts: 0, retrySchedule: [0] },
        },
        outputs: blockDecoder({ from: 0, to: 100_000_001 }),
      })

      await expect(readAll(stream)).rejects.toThrow(
        [
          `A blockchain fork was detected at 100,000,001 block.`,
          `-----------------------------------------`,
          `The correct hash:        "0x100000000__1".`,
          `But the client provided: "0x100000000".`,
          `-----------------------------------------`,
          // TODO add a link to the docs
          `Please refer to the documentation on how to handle forks.`,
        ].join('\n'),
      )
    })
  })

  describe('resume anchor (multi-range)', () => {
    // ADR-20: the cursor's hash anchors only the range that continues from it; carried into a
    // later disjoint range it faults a spurious 409/fork against a block it doesn't border.
    it('anchors only the range continuing from the resume cursor, not disjoint later ranges', async () => {
      const requests: any[] = []

      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 200, hash: '0xA200', timestamp: 200_000 } }],
          head: { finalized: { number: 200, hash: '0xA200' }, latest: { number: 200 } },
          validateRequest: (r) => requests.push(r),
        },
        {
          statusCode: 200,
          data: [{ header: { number: 5001, hash: '0xB5001', timestamp: 5_001_000 } }],
          head: { finalized: { number: 5001, hash: '0xB5001' }, latest: { number: 5001 } },
          validateRequest: (r) => requests.push(r),
        },
      ])

      const outputs = evmQuery()
        .addRange({ from: 100, to: 200 })
        .addRange({ from: 5000, to: 5001 })
        .addFields({ block: { number: true, hash: true, timestamp: true } })
        .build()
        .pipe((d) => d.flatMap((b) => b.header))

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs,
      })

      const target = createTarget({
        write: async ({ read }) => {
          for await (const _ of read({ latest: { number: 150, hash: '0xresume' }, finalized: null })) {
          }
        },
      })

      await stream.pipeTo(target as any)

      expect(requests.map((r) => ({ fromBlock: r.fromBlock, parentBlockHash: r.parentBlockHash }))).toEqual([
        { fromBlock: 151, parentBlockHash: '0xresume' },
        { fromBlock: 5000, parentBlockHash: undefined },
      ])
    })
  })

  describe('pipe/pipeTo', () => {
    it('should not throw when a transform function is passed to .pipe()', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x123', timestamp: 1000 } }],
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 1 }),
      })

      expect(() => stream.pipe((data: any) => data)).not.toThrow()
    })

    it('should not throw when a target is passed to .pipeTo()', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x123', timestamp: 1000 } }],
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 1 }),
      })

      const target = createTarget({
        write: async () => {},
      })

      expect(() => stream.pipeTo(target as any)).not.toThrow()
    })
  })

  describe('finalized', () => {
    it('should receive all finalized data and stop', async () => {
      portal = await finalizedMockPortal([
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x123', timestamp: 1000 } },
            { header: { number: 2, hash: '0x456', timestamp: 2000 } },
          ],
        },
      ])

      const stream = evmPortalStream({
        id: 'test',
        portal: {
          url: portal.url,
          finalized: true,
        },
        outputs: blockDecoder({ from: 0, to: 2 }),
      })

      const res = await readAll(stream)

      expect(res).toMatchInlineSnapshot(`
        [
          {
            "hash": "0x123",
            "number": 1,
            "timestamp": 1000,
          },
          {
            "hash": "0x456",
            "number": 2,
            "timestamp": 2000,
          },
        ]
      `)
    })
  })

  describe('finalized watermark (centralized clamp)', () => {
    it('clamps a transient missing finalized head up to the persisted floor', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 6, hash: '0x6', timestamp: 6000 } }],
          // finalized header dropped on this batch
        },
      ])

      const stream = evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 6 }) })

      const seen: unknown[] = []
      const target = createTarget({
        write: async ({ read }) => {
          for await (const { ctx } of read({
            latest: { number: 5, hash: '0x5' },
            finalized: { number: 5, hash: '0x5f' },
          })) {
            seen.push(ctx.stream.head.finalized)
          }
        },
      })
      await stream.pipeTo(target as any)

      // The dropped header must not leak as `undefined` (which would collapse the buffer threshold
      // to Infinity and release unfinalized rows); it clamps back up to the persisted floor (5).
      expect(seen).toEqual([{ number: 5, hash: '0x5f' }])
    })

    it('seeds the floor from the target resume state and clamps a regression below it (restart-mid-fork)', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 6, hash: '0x6', timestamp: 6000 } }],
          // first batch after restart reports a finalized head (3) below the persisted floor (5)
          head: { finalized: { number: 3, hash: '0x3f' }, latest: { number: 10 } },
        },
      ])

      const stream = evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 6 }) })

      const seen: unknown[] = []
      const target = createTarget({
        write: async ({ read }) => {
          for await (const { ctx } of read({
            latest: { number: 5, hash: '0x5' },
            finalized: { number: 5, hash: '0x5f' },
          })) {
            seen.push(ctx.stream.head.finalized)
          }
        },
      })
      await stream.pipeTo(target as any)

      // The persisted floor (5) survives the restart and clamps the lower reported head (3).
      expect(seen).toEqual([{ number: 5, hash: '0x5f' }])
    })

    it('leaves finalized undefined for a no-finality dataset (passthrough)', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
          ],
          // no head at all → no finality
        },
      ])

      const stream = evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 2 }) })

      const finalizedPerBatch = []
      for await (const { ctx } of stream) {
        finalizedPerBatch.push(ctx.stream.head.finalized)
      }

      // Floor is never seeded (only from a real finalized head), so it stays undefined.
      expect(finalizedPerBatch).toEqual([undefined])
    })
  })
})

describe('stop lifecycle', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  it('invokes transformer stop hook exactly once on normal completion', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0x123', timestamp: 1000 } },
          { header: { number: 2, hash: '0x456', timestamp: 2000 } },
        ],
      },
    ])

    const stopSpy = vi.fn()

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockDecoder({ from: 0, to: 2 }),
    }).pipe(
      createTransformer({
        profiler: { name: 'spy' },
        transform: (data) => data,
        stop: stopSpy,
      }),
    )

    await readAll(stream)

    expect(stopSpy).toHaveBeenCalledTimes(1)
  })

  it('runs stop hook cleanup when a transformer start hook fails', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [{ header: { number: 1, hash: '0x123', timestamp: 1000 } }],
      },
    ])

    const stopSpy = vi.fn()

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockDecoder({ from: 0, to: 1 }),
    })
      .pipe(
        createTransformer({
          profiler: { name: 'cleanup' },
          transform: (data) => data,
          stop: stopSpy,
        }),
      )
      .pipe(
        createTransformer({
          profiler: { name: 'boom' },
          transform: (data) => data,
          start: () => {
            throw new Error('start failed')
          },
        }),
      )

    await expect(readAll(stream)).rejects.toThrow('start failed')

    expect(stopSpy).toHaveBeenCalledTimes(1)
  })

  // The read loop arms a batch/fetch span pair up front, so the last one is always for a batch
  // that never arrives. It leaks a pair per stream, and read() restarts on every retry.
  it('ends the span pair armed for a batch that never arrives', async () => {
    portal = await mockPortal([
      { statusCode: 200, data: [], head: { finalized: { number: 0, hash: '0x0' } } },
      {
        statusCode: 200,
        data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
        head: { finalized: { number: 0, hash: '0x0' } },
      },
    ])

    const spans = spanCounter()

    await readAll(
      evmPortalStream({
        id: 'test',
        portal: portal.url,
        profiler: spans.hooks,
        outputs: blockDecoder({ from: 0, to: 1 }),
      }),
    )

    expect(spans.started['fetch data']).toBeGreaterThan(0)
    expect(spans.unbalanced()).toEqual([])
  })

  // A 204 head poll puts a zero-block batch and flushes it, so the read loop sees a batch it
  // never yields and nothing downstream closes its span.
  it('ends the batch span for the empty batch a 204 head poll delivers', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
        head: { finalized: { number: 0, hash: '0x0' } },
      },
      { statusCode: 204 },
      {
        statusCode: 200,
        data: [{ header: { number: 2, hash: '0x2', timestamp: 2000 } }],
        head: { finalized: { number: 0, hash: '0x0' } },
      },
    ])

    const spans = spanCounter()

    const blocks = await readAll(
      evmPortalStream({
        id: 'test',
        portal: portal.url,
        profiler: spans.hooks,
        outputs: blockDecoder({ from: 0, to: 2 }),
      }),
    )

    // Two yielded, one dropped from the 204, one armed for the batch that never arrives.
    expect(blocks.length).toBe(2)
    expect(spans.started['batch']).toBe(4)
    expect(spans.unbalanced()).toEqual([])
  })

  // Unwinding through the yield leaves readSpan already ended at the top of the loop body, so the
  // finally would end it a second time.
  it('does not double-end the fetch span when the consumer breaks early', async () => {
    portal = await threeBlockPortal()

    const spans = spanCounter()

    for await (const _ of evmPortalStream({
      id: 'test',
      portal: portal.url,
      profiler: spans.hooks,
      outputs: blockDecoder({ from: 0, to: 3 }),
    })) {
      break
    }

    expect(spans.started['fetch data']).toBeGreaterThan(0)
    expect(spans.unbalanced()).toEqual([])
  })

  it('does not double-end the fetch span when the consumer throws', async () => {
    portal = await threeBlockPortal()

    const spans = spanCounter()

    await expect(
      (async () => {
        for await (const _ of evmPortalStream({
          id: 'test',
          portal: portal.url,
          profiler: spans.hooks,
          outputs: blockDecoder({ from: 0, to: 3 }),
        })) {
          throw new Error('consumer boom')
        }
      })(),
    ).rejects.toThrow('consumer boom')

    expect(spans.unbalanced()).toEqual([])
  })

  // A throwing transformer skips its own span.end() and the enclosing 'apply transformers' one.
  it('ends transformer spans when a transformer throws mid-stream', async () => {
    portal = await threeBlockPortal()

    const spans = spanCounter()

    let seen = 0
    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      profiler: spans.hooks,
      outputs: blockDecoder({ from: 0, to: 3 }),
    }).pipe(
      createTransformer({
        profiler: { name: 'boom' },
        transform: (data) => {
          seen++
          if (seen === 2) {
            throw new Error('transformer boom')
          }

          return data
        },
      }),
    )

    await expect(readAll(stream)).rejects.toThrow('transformer boom')

    expect(spans.started['boom']).toBe(2)
    expect(spans.unbalanced()).toEqual([])
  })
})

describe('pipe/pipeTo type guards', () => {
  it('pipe() should not accept objects with a write() method (Target)', () => {
    type SinkLike = { write: () => void }
    expectTypeOf<SinkLike>().not.toMatchTypeOf<TransformerArgs<any, any>>()
  })

  it('pipeTo() should not accept plain functions', () => {
    type Fn = (data: any) => any
    expectTypeOf<Fn>().not.toMatchTypeOf<Target<any>>()
  })
})

describe('finalized-only targets', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  const response = {
    statusCode: 200 as const,
    data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
    head: { finalized: { number: 1, hash: '0x1' }, latest: { number: 1 } },
  }

  /** Records the `finalized` flag the source hands the target, and drains the stream. */
  function recordingTarget(requiresFinalizedStream: boolean, seen: { finalized?: boolean }) {
    return createTarget({
      requiresFinalizedStream,
      write: async ({ read, finalized }) => {
        seen.finalized = finalized
        for await (const _ of read()) {
          // drained
        }
      },
    })
  }

  it('switches a hot pipe to the finalized stream, and warns that it did', async () => {
    // The mock serves /finalized-stream ONLY, so a hot request would 404 — reaching the end of the
    // stream is itself the proof that the endpoint was switched.
    portal = await finalizedMockPortal([response])
    const logger = defaultLogger({ level: 'silent' })
    const warn = vi.spyOn(logger, 'warn')
    const seen: { finalized?: boolean } = {}

    await evmPortalStream({
      id: 'test',
      portal: { url: portal.url, finalized: false },
      logger,
      outputs: blockDecoder({ from: 1, to: 1 }),
    }).pipeTo(recordingTarget(true, seen) as any)

    // The flag the target is handed describes the stream it actually got, not the one configured.
    expect(seen.finalized).toBe(true)
    expect(warn).toHaveBeenCalledOnce()
  })

  it('refuses a fork on the finalized stream before invoking target rollback', async () => {
    portal = await finalizedMockPortal([
      {
        statusCode: 409,
        data: { previousBlocks: [{ number: 0, hash: '0x0' }] },
      },
    ])
    const resolveFork = vi.fn(async () => ({ number: 0, hash: '0x0' }))

    const run = evmPortalStream({
      id: 'test',
      portal: { url: portal.url, finalized: false },
      logger: 'silent',
      outputs: blockDecoder({ from: 1, to: 1 }),
    }).pipeTo(
      createTarget<any>({
        requiresFinalizedStream: true,
        write: async ({ read }) => {
          for await (const _ of read()) {
            // drained
          }
        },
        resolveFork,
      }),
    )

    await expect(run).rejects.toMatchObject({ code: 'E1005', name: 'ForkHandling' })
    expect(resolveFork).not.toHaveBeenCalled()
  })

  it("resolves 'latest' against the finalized head after switching the stream", async () => {
    portal = await finalizedMockPortal([
      {
        ...response,
        validateRequest: (query) => expect(query.fromBlock).toBe(1),
      },
    ])
    let requestedFinalizedHead: boolean | undefined

    class RecordingPortalClient extends PortalClient {
      override async getHead(options?: PortalRequestOptions & { finalized: boolean }) {
        requestedFinalizedHead = options?.finalized

        // A hot-head lookup would make the configured range invalid (`from: 10, to: 1`).
        return options?.finalized ? { number: 1, hash: '0x1' } : { number: 10, hash: '0x10' }
      }
    }

    const client = new RecordingPortalClient({ url: portal.url, finalized: false })

    await evmPortalStream({
      id: 'test',
      portal: client,
      logger: 'silent',
      outputs: blockDecoder({ from: 'latest', to: 1 }),
    }).pipeTo(recordingTarget(true, {}) as any)

    expect(requestedFinalizedHead).toBe(true)
  })

  it("falls back to the hot head for 'latest' on a dataset that finalizes nothing", async () => {
    // Such a dataset still streams (its output just isn't reorg-safe). Taking the absent finalized
    // head at face value would resolve 'latest' to 0 and silently backfill the whole chain.
    portal = await finalizedMockPortal([
      {
        ...response,
        validateRequest: (query) => expect(query.fromBlock).toBe(1),
      },
    ])

    class NoFinalityPortalClient extends PortalClient {
      override async getHead(options?: PortalRequestOptions & { finalized: boolean }) {
        return options?.finalized ? undefined : { number: 1, hash: '0x1' }
      }
    }

    await evmPortalStream({
      id: 'test',
      portal: new NoFinalityPortalClient({ url: portal.url, finalized: false }),
      logger: 'silent',
      outputs: blockDecoder({ from: 'latest', to: 1 }),
    }).pipeTo(recordingTarget(true, {}) as any)
  })

  it('gives a transformer the hot head when it explicitly asks for one', async () => {
    // `start` hooks get the finalized view, which pins the STREAM. A transformer measuring head lag
    // still has to see the block the chain is actually on.
    portal = await finalizedMockPortal([response])
    let seenHead: { number: number } | undefined

    class TwoHeadPortalClient extends PortalClient {
      override async getHead(options?: PortalRequestOptions & { finalized: boolean }) {
        return options?.finalized ? { number: 1, hash: '0x1' } : { number: 99, hash: '0x99' }
      }
    }

    await evmPortalStream({
      id: 'test',
      portal: new TwoHeadPortalClient({ url: portal.url, finalized: false }),
      logger: 'silent',
      outputs: blockDecoder({ from: 1, to: 1 }),
    })
      .pipe(
        createTransformer({
          profiler: { name: 'head-lag' },
          transform: (data) => data,
          start: async (ctx) => {
            seenHead = await ctx.portal.getHead({ finalized: false })
          },
        }),
      )
      .pipeTo(recordingTarget(true, {}) as any)

    expect(seenHead?.number).toBe(99)
  })

  it('does not pin later consumers of the same source to the finalized stream', async () => {
    // One source, two sinks. The endpoint switch belongs to the pipe that asked for it — leaking it
    // onto the next `pipeTo` would silently cost that sink every unfinalized block.
    portal = await finalizedMockPortal([])
    const requestedFinalized: (boolean | undefined)[] = []

    class RecordingPortalClient extends PortalClient {
      override async getMetadata(): Promise<any> {
        return { dataset: 'mock-dataset', aliases: [], real_time: true, start_block: 0, metadata: { kind: 'evm' } }
      }
      override getStream<Q extends Query>(query: Q, options?: PortalBlockStreamOptions): any {
        requestedFinalized.push(options?.finalized)

        return (async function* () {})()
      }
    }

    const stream = evmPortalStream({
      id: 'test',
      portal: new RecordingPortalClient({ url: portal.url, finalized: false }),
      logger: 'silent',
      outputs: blockDecoder({ from: 1, to: 1 }),
    })

    await stream.pipeTo(recordingTarget(true, {}) as any)
    const hot: { finalized?: boolean } = {}
    await stream.pipeTo(recordingTarget(false, hot) as any)

    expect(requestedFinalized).toEqual([true, false])
    expect(hot.finalized).toBe(false)
  })

  it('keeps a custom cache safe even when it ignores the finalized option', async () => {
    // The cache intentionally behaves like an implementation compiled against the old contract:
    // it ignores `finalized` and relies entirely on the client defaults. The mock serves only
    // /finalized-stream, so this completes only if the client handed to the cache is finalized.
    portal = await finalizedMockPortal([response])
    let cacheSawFinalizedClient: boolean | undefined
    const cache: PortalCache = {
      getStream({ portal: cachedPortal, query }) {
        cacheSawFinalizedClient = cachedPortal.finalized
        return cachedPortal.getStream(query)
      },
    }

    await evmPortalStream({
      id: 'test',
      portal: { url: portal.url, finalized: false },
      cache,
      logger: 'silent',
      outputs: blockDecoder({ from: 1, to: 1 }),
    }).pipeTo(recordingTarget(true, {}) as any)

    expect(cacheSawFinalizedClient).toBe(true)
  })

  it('stays quiet when the pipe already asked for the finalized stream', async () => {
    portal = await finalizedMockPortal([response])
    const logger = defaultLogger({ level: 'silent' })
    const warn = vi.spyOn(logger, 'warn')
    const seen: { finalized?: boolean } = {}

    await evmPortalStream({
      id: 'test',
      portal: { url: portal.url, finalized: true },
      logger,
      outputs: blockDecoder({ from: 1, to: 1 }),
    }).pipeTo(recordingTarget(true, seen) as any)

    expect(seen.finalized).toBe(true)
    expect(warn).not.toHaveBeenCalled()
  })

  it('leaves a target that does not require finality on the hot stream', async () => {
    // This mock serves /stream only, so the switch firing here would 404.
    portal = await mockPortal([response])
    const seen: { finalized?: boolean } = {}

    await evmPortalStream({
      id: 'test',
      portal: { url: portal.url, finalized: false },
      outputs: blockDecoder({ from: 1, to: 1 }),
    }).pipeTo(recordingTarget(false, seen) as any)

    expect(seen.finalized).toBe(false)
  })
})

describe('stall reporting', () => {
  const head = { number: 100, hash: '0x100' }

  /** Portal stand-in with no I/O at all, so the test owns every timer in the pipe. */
  class StubPortalClient extends PortalClient {
    constructor(private readonly blocks: () => AsyncIterable<any>) {
      super({ url: 'http://portal.invalid' })
    }

    override async getMetadata() {
      return { dataset: 'test', aliases: [], real_time: true, start_block: 0 } as any
    }

    override async getHead() {
      return head
    }

    override getStream(): any {
      return this.blocks()
    }
  }

  function batch(number: number) {
    return {
      blocks: [{ header: { number, hash: `0x${number}`, timestamp: number * 1000 } }],
      head: { latest: head },
      meta: { bytes: 1, requestedFromBlock: number, lastBlockReceivedAt: new Date(), requests: {} },
    }
  }

  function hangingTarget(onBatch: () => Promise<void>) {
    return createTarget({
      write: async ({ read }) => {
        for await (const _ of read()) {
          await onBatch()
        }
      },
    })
  }

  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('reports a portal that stops answering', async () => {
    const logger = defaultLogger({ level: 'silent' })
    const warn = vi.spyOn(logger, 'warn')

    const portalClient = new StubPortalClient(async function* () {
      await new Promise(() => {})
    })

    evmPortalStream({
      id: 'test',
      portal: portalClient,
      logger,
      progress: { interval: 0 },
      outputs: blockDecoder({ from: 1, to: 1 }),
    })
      .pipeTo(hangingTarget(async () => {}) as any)
      .catch(() => {})

    await vi.advanceTimersByTimeAsync(120_000)

    expect(warn).toHaveBeenCalledExactlyOnceWith({
      message: 'stalled: waiting for data from the portal for 2m 0s',
      phase: 'waiting for data from the portal',
      elapsedMs: 120_000,
    })
  })

  it('does not call a stall that ended in an error recovered', async () => {
    // The recovery line is the only evidence that the pipe came back. Printing it on the way
    // out of a failure points whoever reads the log afterwards at the wrong conclusion.
    const logger = defaultLogger({ level: 'silent' })
    const info = vi.spyOn(logger, 'info')

    const portalClient = new StubPortalClient(async function* () {
      await new Promise((_, reject) => {
        setTimeout(() => reject(new Error('portal is gone')), 180_000)
      })
    })

    evmPortalStream({
      id: 'test',
      portal: portalClient,
      logger,
      progress: { interval: 0 },
      outputs: blockDecoder({ from: 1, to: 1 }),
    })
      .pipeTo(hangingTarget(async () => {}) as any)
      .catch(() => {})

    await vi.advanceTimersByTimeAsync(180_000)

    expect(info).not.toHaveBeenCalledWith(expect.objectContaining({ message: expect.stringContaining('recovered') }))
  })

  it('names the batch a wedged target is sitting on, and repeats less and less often', async () => {
    const logger = defaultLogger({ level: 'silent' })
    const warn = vi.spyOn(logger, 'warn')
    const info = vi.spyOn(logger, 'info')

    let release = () => {}
    const wedged = new Promise<void>((resolve) => {
      release = resolve
    })

    const portalClient = new StubPortalClient(async function* () {
      yield batch(1)
      await new Promise(() => {})
    })

    evmPortalStream({
      id: 'test',
      portal: portalClient,
      logger,
      progress: { interval: 0 },
      outputs: blockDecoder({ from: 1, to: 10 }),
    })
      .pipeTo(hangingTarget(() => wedged) as any)
      .catch(() => {})

    await vi.advanceTimersByTimeAsync(120_000)
    expect(warn).toHaveBeenCalledExactlyOnceWith({
      message: 'stalled: processing block 1 for 2m 0s',
      phase: 'processing block 1',
      elapsedMs: 120_000,
    })

    // The second report comes 4 minutes after the first, so an hours-long wedge is a handful
    // of lines rather than one every couple of minutes.
    await vi.advanceTimersByTimeAsync(120_000)
    expect(warn).toHaveBeenCalledOnce()

    await vi.advanceTimersByTimeAsync(120_000)
    expect(warn).toHaveBeenCalledTimes(2)

    release()
    await vi.advanceTimersByTimeAsync(0)

    expect(info).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining('recovered: processing block 1 took'),
        phase: 'processing block 1',
      }),
    )
  })
})
