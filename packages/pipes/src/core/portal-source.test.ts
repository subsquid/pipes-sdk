import { afterEach, beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest'

import { DefaultPipeIdError } from '~/core/errors.js'
import { RollbackRecord, resolveForkCursor } from '~/core/fork.js'
import { defaultLogger } from '~/core/logger.js'
import { PortalCache } from '~/core/portal-source.js'
import { SpanHooks } from '~/core/profiling.js'
import { ReadOptions, Target, TargetState, createTarget } from '~/core/target.js'
import { TransformerArgs, createTransformer } from '~/core/transformer.js'
import type { BlockCursor } from '~/core/types.js'
import { evmPortalStream, evmQuery } from '~/evm/index.js'
import { PortalBlockStreamOptions, PortalClient, PortalClientOptions, Query } from '~/portal-client/index.js'
import {
  MockChainPortal,
  MockPortal,
  MockPortalOptions,
  MockResponse,
  blockDecoder,
  mockMetricsServer,
  mockPortal,
  readAll,
} from '~/testing/index.js'
import { withPortal } from '~/testing/portal-fixture.js'
import { STUB_HEAD, StubPortalClient } from '~/testing/stub-portal.js'

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

function threeBlockResponses(): MockResponse[] {
  return [1, 2, 3].map((number) => ({
    statusCode: 200,
    data: [{ header: { number, hash: `0x${number}`, timestamp: number * 1000 } }],
    head: { finalized: { number: 0, hash: '0x0' } },
  }))
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
      ).toThrow(DefaultPipeIdError)
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
        outputs: blockDecoder({ from: 0, to: 14 }),
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

      // An unawaited run keeps the shared shutdown listeners past this test (ADR-24).
      let run!: Promise<void>
      expect(() => {
        run = stream.pipeTo(target as any)
      }).not.toThrow()
      await run
    })
  })

  describe('finalized', () => {
    it('should receive all finalized data and stop', async () => {
      portal = await mockPortal(
        [
          {
            statusCode: 200,
            data: [
              { header: { number: 1, hash: '0x123', timestamp: 1000 } },
              { header: { number: 2, hash: '0x456', timestamp: 2000 } },
            ],
          },
        ],
        { finalized: true },
      )

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
  withPortal('invokes transformer stop hook exactly once on normal completion', async ({ portal: serve }) => {
    const portal = await serve([
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

  withPortal('runs stop hook cleanup when a transformer start hook fails', async ({ portal: serve }) => {
    const portal = await serve([
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

  withPortal('runs the start and stop hooks once across a fork', async ({ portal: serve }) => {
    let forked = false
    const chain: MockChainPortal = await serve({
      latest: 8,
      finalized: 2,
      blocksPerResponse: 2,
      // Once the pipe asks past 5, every block above 3 is replaced
      delay: ({ query }) => {
        if (query.fromBlock === 6 && !forked) {
          forked = true
          chain.fork(3)
        }
      },
    })
    const metrics = { ...mockMetricsServer().server, start: vi.fn(), stop: vi.fn(async () => {}) }
    const hooks = { start: vi.fn(), stop: vi.fn(), rollback: vi.fn() }
    const records: RollbackRecord[] = []
    const written: number[] = []

    await evmPortalStream({
      id: 'test',
      portal: chain.url,
      logger: 'silent',
      metrics,
      outputs: blockDecoder({ from: 0, to: 8 }),
    })
      .pipe(createTransformer({ profiler: { name: 'hooks' }, transform: (data) => data, ...hooks }))
      .pipeTo(
        createTarget({
          write: async ({ read }) => {
            for await (const { data, ctx } of read()) {
              written.push(...data.map((block) => block.number))
              // Newest first, as a store reads its records back.
              records.unshift({ rollbackChain: ctx.stream.state.rollbackChain, finalized: ctx.stream.head.finalized })
            }
          },
          resolveFork: (canonical) => resolveForkCursor(records, canonical),
        }),
      )

    expect(hooks.rollback).toHaveBeenCalledTimes(1)
    expect(written.at(-1)).toBe(8)
    expect(hooks.start).toHaveBeenCalledTimes(1)
    expect(hooks.stop).toHaveBeenCalledTimes(1)
    expect(metrics.start).toHaveBeenCalledTimes(1)
    expect(metrics.stop).toHaveBeenCalledTimes(1)
  })

  // The read loop arms a batch/fetch span pair up front, so the last one is always for a batch
  // that never arrives. It leaks a pair per stream, and read() restarts on every retry.
  withPortal('ends the span pair armed for a batch that never arrives', async ({ portal: serve }) => {
    const portal = await serve([
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
  withPortal('ends the batch span for the empty batch a 204 head poll delivers', async ({ portal: serve }) => {
    const portal = await serve([
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
  withPortal('does not double-end the fetch span when the consumer breaks early', async ({ portal: serve }) => {
    const portal = await serve(threeBlockResponses())

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

  withPortal('does not double-end the fetch span when the consumer throws', async ({ portal: serve }) => {
    const portal = await serve(threeBlockResponses())

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
  withPortal('ends transformer spans when a transformer throws mid-stream', async ({ portal: serve }) => {
    const portal = await serve(threeBlockResponses())

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

describe('pipe() transformer ids', () => {
  withPortal('gives each of 3+ same-id transformers its own id', async ({ portal: serve }) => {
    const portal = await serve({ latest: 1 })

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockDecoder({ from: 0, to: 1 }),
    })

    const duplicate = () => createTransformer<any, any>({ profiler: { name: 'dup' }, transform: (data) => data })
    const transformers = [duplicate(), duplicate(), duplicate(), duplicate()]
    const [t1, t2, t3, t4] = transformers

    stream.pipe(t1).pipe(t2).pipe(t3).pipe(t4)

    const ids = transformers.map((t) => t.id())
    expect(ids).toEqual(['dup', 'dup 2', 'dup 3', 'dup 4'])
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
    portal = await mockPortal([response], { finalized: true })
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
    portal = await mockPortal(
      [
        {
          statusCode: 409,
          data: { previousBlocks: [{ number: 0, hash: '0x0' }] },
        },
      ],
      { finalized: true },
    )
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
    // A hot-head lookup would make the configured range invalid (`from: 10, to: 1`).
    const chain = await mockPortal({ finalized: 1, latest: 10 })
    portal = chain

    await evmPortalStream({
      id: 'test',
      portal: { url: chain.url, finalized: false },
      logger: 'silent',
      outputs: blockDecoder({ from: 'latest', to: 1 }),
    }).pipeTo(recordingTarget(true, {}) as any)

    const requested = chain.requests.map((r) => ({ endpoint: r.endpoint, fromBlock: r.query.fromBlock }))
    expect(requested).toEqual([{ endpoint: 'finalized-stream', fromBlock: 1 }])
  })

  it("falls back to the hot head for 'latest' on a dataset that finalizes nothing", async () => {
    // Such a dataset still streams (its output just isn't reorg-safe). Taking the absent finalized
    // head at face value would resolve 'latest' to 0 and silently backfill the whole chain.
    const chain = await mockPortal({ finalized: null, latest: 1 })
    portal = chain

    await evmPortalStream({
      id: 'test',
      portal: { url: chain.url, finalized: false },
      logger: 'silent',
      outputs: blockDecoder({ from: 'latest', to: 1 }),
    }).pipeTo(recordingTarget(true, {}) as any)

    const requested = chain.requests.map((r) => ({ endpoint: r.endpoint, fromBlock: r.query.fromBlock }))
    expect(requested).toEqual([{ endpoint: 'finalized-stream', fromBlock: 1 }])
  })

  it('gives a transformer the hot head when it explicitly asks for one', async () => {
    portal = await mockPortal({ finalized: 1, latest: 99 })
    const client = new PortalClient({ url: portal.url, finalized: false })
    let seenHead: { number: number } | undefined

    await evmPortalStream({
      id: 'test',
      portal: client,
      logger: 'silent',
      outputs: blockDecoder({ from: 1, to: 1 }),
    })
      .pipe(
        createTransformer({
          profiler: { name: 'head-lag' },
          transform: (data) => data,
          start: async (ctx) => {
            expect(ctx.portal).toBe(client)
            expect(ctx.finalized).toBe(true)
            expect(ctx.portal.finalized).toBe(false)
            seenHead = await ctx.portal.getHead({ finalized: false, abort: ctx.signal })
          },
        }),
      )
      .pipeTo(recordingTarget(true, {}) as any)

    expect(seenHead?.number).toBe(99)
  })

  it('does not pin later consumers of the same source to the finalized stream', async () => {
    // One source, two sinks. The endpoint switch belongs to the pipe that asked for it — leaking it
    // onto the next `pipeTo` would silently cost that sink every unfinalized block.
    portal = await mockPortal([], { finalized: true })
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

  it('passes the effective finality and stop signal to a cache with the original client', async () => {
    portal = await mockPortal([response], { finalized: true })
    const client = new PortalClient({ url: portal.url, finalized: false })
    let cacheSawFinalized: boolean | undefined
    const cache: PortalCache = {
      getStream({ portal: cachedPortal, query, finalized, signal }) {
        expect(cachedPortal).toBe(client)
        expect(signal).toBeInstanceOf(AbortSignal)
        cacheSawFinalized = finalized
        return cachedPortal.getStream(query, { finalized, request: { abort: signal } })
      },
    }

    await evmPortalStream({
      id: 'test',
      portal: client,
      cache,
      logger: 'silent',
      outputs: blockDecoder({ from: 1, to: 1 }),
    }).pipeTo(recordingTarget(true, {}) as any)

    expect(cacheSawFinalized).toBe(true)
  })

  it('stays quiet when the pipe already asked for the finalized stream', async () => {
    portal = await mockPortal([response], { finalized: true })
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
  const head = STUB_HEAD

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

    let release = () => {}
    const paused = new Promise<void>((resolve) => {
      release = resolve
    })
    const portalClient = new StubPortalClient(async function* () {
      await paused
    })

    const run = evmPortalStream({
      id: 'test',
      portal: portalClient,
      logger,
      progress: { interval: 0 },
      outputs: blockDecoder({ from: 1, to: 1 }),
    }).pipeTo(hangingTarget(async () => {}) as any)

    try {
      await vi.advanceTimersByTimeAsync(120_000)

      expect(warn).toHaveBeenCalledExactlyOnceWith({
        message: 'stalled: waiting for data from the portal for 2m 0s',
        phase: 'waiting for data from the portal',
        elapsedMs: 120_000,
      })
    } finally {
      release()
      await run
    }
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

    const run = evmPortalStream({
      id: 'test',
      portal: portalClient,
      logger,
      progress: { interval: 0 },
      outputs: blockDecoder({ from: 1, to: 1 }),
    })
      .pipeTo(hangingTarget(async () => {}) as any)
      .catch((error) => error)

    await vi.advanceTimersByTimeAsync(180_000)
    expect(await run).toMatchObject({ message: 'portal is gone' })

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

    let finish = () => {}
    const paused = new Promise<void>((resolve) => {
      finish = resolve
    })
    const portalClient = new StubPortalClient(async function* () {
      yield batch(1)
      await paused
    })

    const run = evmPortalStream({
      id: 'test',
      portal: portalClient,
      logger,
      progress: { interval: 0 },
      outputs: blockDecoder({ from: 1, to: 10 }),
    }).pipeTo(hangingTarget(() => wedged) as any)

    try {
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
      await vi.waitFor(() => {
        expect(info).toHaveBeenCalledWith(
          expect.objectContaining({
            message: expect.stringContaining('recovered: processing block 1 took'),
            phase: 'processing block 1',
          }),
        )
      })
    } finally {
      release()
      finish()
      await run
    }
  })
})

describe('Portal stream per-block batches', () => {
  function blocks(numbers: number[], finalized?: number): MockResponse {
    return {
      statusCode: 200,
      data: numbers.map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
      head: finalized == null ? undefined : { finalized: { number: finalized, hash: `0x${finalized}` } },
    }
  }

  /** Block numbers and request counts per batch the target reads. */
  async function readBatches(
    portal: PortalClientOptions,
    { resume, to = 3, ...options }: ReadOptions & { resume?: TargetState; to?: number } = {},
  ) {
    const shapes: number[][] = []
    const requests: Record<number, number>[] = []
    const target = createTarget<{ number: number }[]>({
      write: async ({ read }) => {
        for await (const { data, ctx } of read(resume, options)) {
          shapes.push(data.map((b) => b.number))
          requests.push(ctx.batch.requests)
        }
      },
    })

    await evmPortalStream({ id: 'test', portal, logger: 'silent', outputs: blockDecoder({ from: 0, to }) }).pipeTo(
      target,
    )

    return { shapes, requests }
  }

  withPortal('keeps one batch per response unless the target asks for more', async ({ portal: serve }) => {
    const portal = await serve([blocks([1, 2, 3], 1)])

    const { shapes } = await readBatches({ url: portal.url })

    expect(shapes).toEqual([[1, 2, 3]])
  })

  // Postgres keys undo snapshots by the batch's last block.
  withPortal('splits every unfinalized block into its own batch when asked', async ({ portal: serve }) => {
    const portal = await serve([blocks([1, 2, 3], 1)])

    const { shapes } = await readBatches({ url: portal.url }, { perBlockUnfinalized: true })

    expect(shapes).toEqual([[1], [2], [3]])
  })

  // Read as "no finality", every hot block would be filed as finalized instead.
  withPortal('treats a finalized head of block 0 as finality, not as its absence', async ({ portal: serve }) => {
    const portal = await serve([blocks([1, 2, 3], 0)])

    const { shapes } = await readBatches({ url: portal.url }, { perBlockUnfinalized: true })

    expect(shapes).toEqual([[1], [2], [3]])
  })

  // maxIdleTimeMs is pinned high, so the client merges both responses into one batch.
  withPortal('never merges finalized blocks into an unfinalized block batch', async ({ portal: serve }) => {
    const portal = await serve([blocks([1], 1), blocks([2, 3], 1)])

    const { shapes } = await readBatches({ url: portal.url, maxIdleTimeMs: 60_000 }, { perBlockUnfinalized: true })

    expect(shapes).toEqual([[1], [2], [3]])
  })

  // The portal can drop the finalized header for a while. Read as "all final", the blocks would share
  // one batch, and the undo log could no longer roll back to the blocks inside it.
  withPortal(
    'splits blocks without a finalized head against the head an earlier response reported',
    async ({ portal: serve }) => {
      const portal = await serve([blocks([1], 1), blocks([2, 3])])

      const { shapes } = await readBatches({ url: portal.url, maxIdleTimeMs: 60_000 }, { perBlockUnfinalized: true })

      expect(shapes).toEqual([[1], [2], [3]])
    },
  )

  withPortal(
    'splits blocks without a finalized head against the persisted floor after a restart',
    async ({ portal: serve }) => {
      const portal = await serve([blocks([6, 7, 8])])

      const resume = { latest: { number: 5, hash: '0x5' }, finalized: { number: 5, hash: '0x5' } }
      const { shapes } = await readBatches({ url: portal.url }, { resume, to: 8, perBlockUnfinalized: true })

      expect(shapes).toEqual([[6], [7], [8]])
    },
  )

  // Passing the counters to each part would bill one response three times over.
  withPortal('counts one response as one request however many batches it becomes', async ({ portal: serve }) => {
    const portal = await serve([blocks([1, 2, 3], 0)])

    const { requests } = await readBatches({ url: portal.url }, { perBlockUnfinalized: true })

    expect(requests).toEqual([{ 200: 1 }, {}, {}])
  })
})

describe('Portal stream block order', () => {
  function blocks(numbers: number[]): MockResponse {
    return {
      statusCode: 200,
      data: numbers.map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
      head: { finalized: { number: 0, hash: '0x0' } },
    }
  }

  // Delivery timing decides how much of the first response the target drains first, so only a prefix is fixed.
  withPortal(
    'stops the pipe with E1006 before a block that steps back reaches the target',
    async ({ portal: serve }) => {
      const portal = await serve([blocks([1, 2, 3]), blocks([2])])
      const written: number[] = []

      const run = evmPortalStream({
        id: 'test',
        portal: portal.url,
        logger: 'silent',
        outputs: blockDecoder({ from: 1, to: 5 }),
      }).pipeTo(
        createTarget({
          write: async ({ read }) => {
            for await (const { data } of read()) {
              written.push(...data.map((header) => header.number))
            }
          },
        }),
      )

      await expect(run).rejects.toMatchObject({ code: 'E1006', name: 'PortalContract' })
      expect(written).toEqual([1, 2, 3].slice(0, written.length))
    },
  )
})

describe('Portal stream restart with a fork below the request start', () => {
  const block = (number: number, suffix = '') => ({ number, hash: `0x${number}${suffix}` })
  const finalized = block(2)
  const orphan = block(4, 'a')
  const replacements = [block(4, 'b'), block(5, 'b'), block(6, 'b')]

  /** Blocks 4–6 replaced the orphaned branch the saved cursor sits on. */
  const serveChain = (serve: (options: MockPortalOptions) => Promise<MockChainPortal>) =>
    serve({
      blocks: [1, 2, 3, 4, 5, 6].map((number) => ({
        header: { ...block(number, number >= 4 ? 'b' : ''), timestamp: number * 1000 },
      })),
      finalized: 2,
      latest: 6,
      blocksPerResponse: 1,
    })

  /** A durable target: the cursor and the rows outlive each run, as a store does across restarts. */
  function storeAt(cursor: BlockCursor) {
    const store = { cursor, seen: [] as BlockCursor[] }
    const records: RollbackRecord[] = [{ rollbackChain: [block(3), orphan], finalized }]

    const run = (portal: string | PortalClient, from: number | 'latest' | Date) =>
      evmPortalStream({
        id: 'test',
        portal,
        logger: 'silent',
        outputs: blockDecoder({ from, to: 6 }),
      }).pipeTo(
        createTarget({
          write: async ({ read }) => {
            for await (const { data, ctx } of read({ latest: store.cursor, finalized })) {
              store.seen.push(...data.map(({ number, hash }) => ({ number, hash })))
              store.cursor = ctx.stream.state.current
            }
          },
          resolveFork: async (canonical) => {
            const ancestor = await resolveForkCursor(records, canonical)
            if (ancestor) {
              store.cursor = ancestor
            }

            return ancestor
          },
        }),
      )

    return { store, run }
  }

  withPortal('halts when a fork rolls the cursor back below the range start', async ({ portal: serve }) => {
    const portal = await serveChain(serve)
    const { store, run } = storeAt(orphan)

    await expect(run(portal.url, 5)).rejects.toMatchObject({ code: 'E0002', name: 'PipeConfiguration' })

    expect(store.cursor).toEqual(block(3))
    expect(store.seen).toEqual([])
    expect(portal.requests).toHaveLength(1)
    expect(portal.requests[0]).toMatchObject({ status: 409, query: { fromBlock: 5, parentBlockHash: orphan.hash } })
  })

  withPortal('refuses a restart whose saved cursor is below the range start', async ({ portal: serve }) => {
    const portal = await serveChain(serve)
    const { store, run } = storeAt(block(3))

    await expect(run(portal.url, 5)).rejects.toMatchObject({ code: 'E0002', name: 'PipeConfiguration' })

    expect(store.cursor).toEqual(block(3))
    expect(portal.requests).toEqual([])
  })

  withPortal('recovers on restart once the start follows the saved cursor again', async ({ portal: serve }) => {
    const portal = await serveChain(serve)
    const { store, run } = storeAt(orphan)

    await expect(run(portal.url, store.cursor.number + 1)).rejects.toMatchObject({ code: 'E0002' })
    await run(portal.url, store.cursor.number + 1)

    expect(store.seen).toEqual(replacements)
    expect(store.cursor).toMatchObject(block(6, 'b'))
    expect(portal.requests[1]?.query).toMatchObject({ fromBlock: 4, parentBlockHash: block(3).hash })
  })

  for (const from of [1, 'latest'] as const) {
    withPortal(`replays the replacement blocks with a range start of ${from}`, async ({ portal: serve }) => {
      const portal = await serveChain(serve)
      const { store, run } = storeAt(orphan)

      await run(portal.url, from)

      expect(store.seen).toEqual(replacements)
      expect(store.cursor).toMatchObject(block(6, 'b'))
      expect(portal.requests[0]).toMatchObject({ status: 409, query: { fromBlock: 5, parentBlockHash: orphan.hash } })
      expect(portal.requests[1]?.query).toMatchObject({ fromBlock: 4, parentBlockHash: block(3).hash })
    })
  }

  for (const [label, from, lookup] of [
    ['latest', 'latest', 'getHead'],
    ['a date', new Date(1000 * 1000), 'resolveTimestamp'],
  ] as const) {
    withPortal(`resolves a range start of ${label} once across a fork`, async ({ portal: serve }) => {
      const portal = await serveChain(serve)
      const { store, run } = storeAt(orphan)
      const lookups: string[] = []

      class CountingPortalClient extends PortalClient {
        override getHead(...args: Parameters<PortalClient['getHead']>) {
          lookups.push('getHead')
          return super.getHead(...args)
        }
        override resolveTimestamp(...args: Parameters<PortalClient['resolveTimestamp']>) {
          lookups.push('resolveTimestamp')
          return super.resolveTimestamp(...args)
        }
        override getMetadata(...args: Parameters<PortalClient['getMetadata']>) {
          lookups.push('getMetadata')
          return super.getMetadata(...args)
        }
      }

      await run(new CountingPortalClient({ url: portal.url }), from)

      expect(portal.requests[0]).toMatchObject({ status: 409 })
      expect(store.seen).toEqual(replacements)
      expect(lookups.sort()).toEqual([lookup, 'getMetadata'].sort())
    })
  }

  withPortal('allows a resume block in a gap between configured ranges', async ({ portal: serve }) => {
    const portal = await serve({ latest: 6, finalized: 6, blocksPerResponse: 1 })
    const seen: number[] = []
    const outputs = evmQuery()
      .addRange({ from: 1, to: 1 })
      .addRange({ from: 5, to: 6 })
      .addFields({ block: { number: true } })
      .build()

    await evmPortalStream({ id: 'test', portal: portal.url, logger: 'silent', outputs }).pipeTo(
      createTarget({
        write: async ({ read }) => {
          for await (const { data } of read({ latest: { number: 3, hash: '0x3' }, finalized: null })) {
            seen.push(...data.map(({ header }) => header.number))
          }
        },
      }),
    )

    expect(seen).toEqual([5, 6])
    expect(portal.requests[0]?.query.parentBlockHash).toBeUndefined()
  })
})

describe('Portal stream fork below the finalized floor', () => {
  function blocks(numbers: number[], finalized: number): MockResponse {
    return {
      statusCode: 200,
      data: numbers.map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
      head: { finalized: { number: finalized, hash: `0x${finalized}` } },
    }
  }

  withPortal('stops the pipe with E1003 when the fork replaces the finalized block', async ({ portal: serve }) => {
    // The fork replaces the floor, block 3, so the only canonical match, block 2, lies below it.
    const replaced = [3, 4].map((number) => ({ number, hash: `0x${number}b` }))
    const portal = await serve([
      blocks([1, 2], 0),
      blocks([3, 4], 3),
      {
        statusCode: 409,
        data: { previousBlocks: [{ number: 1, hash: '0x1' }, { number: 2, hash: '0x2' }, ...replaced] },
      },
    ])
    const records: RollbackRecord[] = []

    const run = evmPortalStream({
      id: 'test',
      portal: portal.url,
      logger: 'silent',
      outputs: blockDecoder({ from: 1, to: 10 }),
    }).pipeTo(
      createTarget({
        write: async ({ read }) => {
          for await (const { ctx } of read()) {
            // Newest first, as a store reads its records back.
            records.unshift({ rollbackChain: ctx.stream.state.rollbackChain, finalized: ctx.stream.head.finalized })
          }
        },
        resolveFork: (canonical) => resolveForkCursor(records, canonical),
      }),
    )

    await expect(run).rejects.toMatchObject({ code: 'E1003', name: 'ForkHandling' })
  })
})

describe('Portal stream fork with a partial canonical window', () => {
  function blocks(numbers: number[]): MockResponse {
    return {
      statusCode: 200,
      data: numbers.map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
      head: { finalized: { number: 0, hash: '0x0' } },
    }
  }

  function forkedBlocks(numbers: number[], suffix: string): MockResponse {
    return {
      statusCode: 200,
      data: numbers.map((number) => ({ header: { number, hash: `0x${number}${suffix}`, timestamp: number * 1000 } })),
      head: { finalized: { number: 0, hash: '0x0' } },
    }
  }

  withPortal(
    'rolls the target back and continues when the window ends below the delivered block',
    async ({ portal: serve }) => {
      // Block 4 was delivered; the window ends at the forked block 3, one below it.
      const portal = await serve([
        blocks([1, 2, 3, 4]),
        {
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 1, hash: '0x1' },
              { number: 2, hash: '0x2' },
              { number: 3, hash: '0x3b' },
            ],
          },
        },
        forkedBlocks([3, 4, 5, 6], 'b'),
      ])

      const records: RollbackRecord[] = []
      const seen: number[][] = []
      const resolveFork = vi.fn((canonical: Parameters<typeof resolveForkCursor>[1]) =>
        resolveForkCursor(records, canonical),
      )

      const run = evmPortalStream({
        id: 'test',
        portal: portal.url,
        logger: 'silent',
        outputs: blockDecoder({ from: 1, to: 6 }),
      }).pipeTo(
        createTarget({
          write: async ({ read }) => {
            for await (const { data, ctx } of read()) {
              if (data.length > 0) seen.push(data.map((block: { number: number }) => block.number))
              // Newest first, as a store reads its records back.
              records.unshift({ rollbackChain: ctx.stream.state.rollbackChain, finalized: ctx.stream.head.finalized })
            }
          },
          resolveFork,
        }),
      )

      await expect(run).resolves.toBeUndefined()
      expect(resolveFork).toHaveBeenCalledOnce()
      expect(seen).toEqual([
        [1, 2, 3, 4],
        [3, 4, 5, 6],
      ])
    },
  )

  withPortal(
    'rolls a resumed target back when the window ends below the persisted cursor',
    async ({ portal: serve }) => {
      // No batch is delivered in this run: the cursor resumes at block 4, the window ends at 3.
      const portal = await serve([
        {
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 1, hash: '0x1' },
              { number: 2, hash: '0x2' },
              { number: 3, hash: '0x3b' },
            ],
          },
        },
        forkedBlocks([3, 4, 5, 6], 'b'),
      ])

      // Loaded from the store on resume, newest first.
      const chain = (top: number) =>
        Array.from({ length: top }, (_, i) => ({ number: i + 1, hash: `0x${i + 1}` })).reverse()
      const records: RollbackRecord[] = [{ rollbackChain: chain(4), finalized: undefined }]
      const seen: number[][] = []
      const resolveFork = vi.fn((canonical: Parameters<typeof resolveForkCursor>[1]) =>
        resolveForkCursor(records, canonical),
      )

      const run = evmPortalStream({
        id: 'test',
        portal: portal.url,
        logger: 'silent',
        outputs: blockDecoder({ from: 1, to: 6 }),
      }).pipeTo(
        createTarget({
          write: async ({ read }) => {
            for await (const { data } of read({ latest: { number: 4, hash: '0x4' }, finalized: null })) {
              if (data.length > 0) seen.push(data.map((block: { number: number }) => block.number))
            }
          },
          resolveFork,
        }),
      )

      await expect(run).resolves.toBeUndefined()
      expect(resolveFork).toHaveBeenCalledOnce()
      expect(seen).toEqual([[3, 4, 5, 6]])
    },
  )
})
