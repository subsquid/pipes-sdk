import { getEventListeners } from 'node:events'
import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'

import { describe, expect, vi } from 'vitest'

import { PipelineShutdownTimeoutError, PipelineStoppedError, ShutdownConfigurationError } from '~/core/errors.js'
import type { PortalCache } from '~/core/portal-source.js'
import { createTarget } from '~/core/target.js'
import { createTransformer } from '~/core/transformer.js'
import { evmPortalStream, evmQuery } from '~/evm/index.js'
import { mockBlock } from '~/testing/evm/index.js'
import { type MockResponse, blockDecoder } from '~/testing/index.js'
import { withPortal as it } from '~/testing/portal-fixture.js'

const never = new Promise<never>(() => {})

function deferred() {
  let resolve = () => {}
  const promise = new Promise<void>((res) => {
    resolve = res
  })

  return { promise, resolve }
}

const chainOf = (count: number) => Array.from({ length: count }, (_, i) => mockBlock({ number: i + 1 }))

const listeners = () => process.listeners('SIGTERM').length + process.listeners('SIGINT').length

/** `maxBytes: 1` hands each response over as its own batch. */
function streamFrom(url: string, range: { from: number | 'latest' | Date; to?: number } = { from: 1 }) {
  return evmPortalStream({
    id: 'shutdown',
    portal: { url, maxBytes: 1 },
    logger: 'silent',
    outputs: evmQuery()
      .addRange(range)
      .addFields({ block: { number: true, hash: true, timestamp: true } })
      .build()
      .pipe((blocks) => blocks.map((block) => block.header)),
  })
}

/** Reads to the end, keeping the cursor of each batch it saw and calling `onBatch` as its commit. */
function recordingTarget(onBatch: (block: number) => unknown = () => {}, order: string[] = []) {
  const committed: number[] = []

  const target = createTarget<any>({
    write: async ({ read }) => {
      for await (const { ctx } of read()) {
        const block = ctx.stream.state.current.number
        await onBatch(block)
        committed.push(block)
      }

      order.push('target closed')
    },
  })

  return { target, committed }
}

describe('pipeTo shutdown', () => {
  describe('options', () => {
    it('resolves on natural completion and does not run the callback', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(3) })
      const onShutdown = vi.fn()
      const before = listeners()
      const { target, committed } = recordingTarget()

      await streamFrom(chain.url, { from: 1, to: 3 }).pipeTo(target, { shutdown: onShutdown })

      expect(committed.at(-1)).toBe(3)
      expect(onShutdown).not.toHaveBeenCalled()
      expect(listeners()).toBe(before)
    })

    it.for([
      ['a bad timeout', { timeout: '0s' }],
      ['a timeout in milliseconds', { timeout: 30_000 }],
      ['a value it does not know', true],
    ] as const)('rejects %s before the pipe starts', async ([, shutdown], { portal }) => {
      const chain = await portal({ blocks: chainOf(1) })
      const write = vi.fn()

      const run = streamFrom(chain.url).pipeTo(createTarget({ write }), { shutdown: shutdown as never })

      await expect(run).rejects.toBeInstanceOf(ShutdownConfigurationError)
      expect(write).not.toHaveBeenCalled()
      expect(chain.requests).toEqual([])
    })

    it('leaves a pipe with `shutdown: false` out of it', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(3), blocksPerResponse: 1 })
      const { target, committed } = recordingTarget((block) => {
        if (block === 1) {
          process.emit('SIGTERM', 'SIGTERM')
        }
      })

      // The counts are read in one synchronous window around the call, so a pair some other
      // test is tearing down asynchronously cannot skew the comparison. Registration of a
      // managed run happens synchronously inside `pipeTo`, so this sees it if it happens.
      const before = listeners()
      const run = streamFrom(chain.url, { from: 1, to: 3 }).pipeTo(target, { shutdown: false })
      expect(listeners()).toBe(before)

      await run
      expect(committed).toEqual([1, 2, 3])
    })
  })

  describe('draining', () => {
    it('finishes the batch in flight, discards the rest, and cleans up before the callback', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(10), blocksPerResponse: 1 })
      const order: string[] = []
      const release = deferred()

      const { target, committed } = recordingTarget(async (block) => {
        if (block === 2) {
          process.emit('SIGTERM', 'SIGTERM')
          // The source has moved on by now, but the commit is not cancelled.
          await release.promise
          order.push('commit finished')
        }
      }, order)

      const run = streamFrom(chain.url, { from: 1, to: 10 })
        .pipe(
          createTransformer({
            transform: (data) => data,
            stop: () => {
              order.push('transformer stopped')
            },
          }),
        )
        .pipeTo(target, {
          shutdown: () => {
            order.push('callback')
          },
        })
        .catch((error) => error)

      await vi.waitFor(() => expect(committed).toEqual([1]))
      release.resolve()

      await expect(run).resolves.toBeInstanceOf(PipelineStoppedError)
      expect(committed).toEqual([1, 2])
      expect(order).toEqual(['commit finished', 'transformer stopped', 'target closed', 'callback'])
    })

    it('does not hand over a batch whose transformer saw the signal', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(3), blocksPerResponse: 1 })
      const { target, committed } = recordingTarget()

      const run = streamFrom(chain.url, { from: 1, to: 3 })
        .pipe(
          createTransformer({
            transform: (data: { number: number }[]) => {
              if (data[0].number === 2) {
                process.emit('SIGTERM', 'SIGTERM')
              }

              return data
            },
          }),
        )
        .pipeTo(target)

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
      expect(committed).toEqual([1])
    })

    it('discards the rest of a response split into per-block batches, untransformed', async ({ portal }) => {
      const blocks = [1, 2, 3, 4, 5].map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number } }))
      const chain = await portal([{ statusCode: 200, data: blocks, head: { finalized: { number: 1, hash: '0x1' } } }])
      const transformed: number[] = []
      const committed: number[] = []

      const target = createTarget<any>({
        write: async ({ read }) => {
          for await (const { ctx } of read(undefined, { perBlockUnfinalized: true })) {
            const block = ctx.stream.state.current.number

            if (block === 2) {
              process.emit('SIGTERM', 'SIGTERM')
            }

            committed.push(block)
          }
        },
      })

      const run = streamFrom(chain.url, { from: 1, to: 5 })
        .pipe(
          createTransformer({
            transform: (data: { number: number }[]) => {
              transformed.push(data[0].number)

              return data
            },
          }),
        )
        .pipeTo(target)

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
      expect(committed).toEqual([1, 2])
      expect(transformed).toEqual([1, 2])
    })

    it('cannot run completion-only code after an interrupted bounded run', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(5), blocksPerResponse: 1 })
      const completionOnly = vi.fn()

      const { target } = recordingTarget(() => {
        process.emit('SIGTERM', 'SIGTERM')
      })

      const main = async () => {
        await streamFrom(chain.url, { from: 1, to: 5 }).pipeTo(target)
        completionOnly()
      }

      await expect(main()).rejects.toBeInstanceOf(PipelineStoppedError)
      expect(completionOnly).not.toHaveBeenCalled()
    })

    it('lets a fork rollback in progress finish, and reads nothing after it', async ({ portal }) => {
      const responses: MockResponse[] = [
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1 } },
            { header: { number: 2, hash: '0x2', timestamp: 2 } },
          ],
        },
        { statusCode: 409, data: { previousBlocks: [{ number: 1, hash: '0x1' }] } },
      ]
      const script = await portal(responses)
      const reached = deferred()
      const release = deferred()
      let rolledBack = false
      let batches = 0

      const target = createTarget<any>({
        write: async ({ read }) => {
          for await (const _ of read()) {
            batches++
          }
        },
        resolveFork: async () => {
          process.emit('SIGTERM', 'SIGTERM')
          reached.resolve()
          await release.promise
          rolledBack = true

          return { number: 1, hash: '0x1' }
        },
      })

      const run = streamFrom(script.url, { from: 1, to: 5 })
        .pipeTo(target)
        .catch((error) => error)

      await reached.promise
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect(rolledBack).toBe(false)
      release.resolve()

      await expect(run).resolves.toBeInstanceOf(PipelineStoppedError)
      expect(rolledBack).toBe(true)
      expect(batches).toBe(1)
    })

    it('keeps a failure of a transformer stop hook, and still runs the callback', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(3), blocksPerResponse: 1 })
      const failure = new Error('stop failed')
      const onShutdown = vi.fn()

      const { target } = recordingTarget(() => {
        process.emit('SIGTERM', 'SIGTERM')
      })

      const run = streamFrom(chain.url, { from: 1, to: 3 })
        .pipe(
          createTransformer({
            transform: (data) => data,
            stop: () => {
              throw failure
            },
          }),
        )
        .pipeTo(target, { shutdown: onShutdown })

      await expect(run).rejects.toBe(failure)
      expect(onShutdown).toHaveBeenCalledTimes(1)
    })

    it('reports a commit that outlasts the budget, without cancelling it', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(2), blocksPerResponse: 1 })
      const onShutdown = vi.fn()
      const before = listeners()
      const release = deferred()

      const { target, committed } = recordingTarget(async () => {
        process.emit('SIGTERM', 'SIGTERM')
        await release.promise
      })

      const run = streamFrom(chain.url, { from: 1, to: 2 }).pipeTo(target, {
        shutdown: { timeout: '50ms', onShutdown },
      })

      await expect(run).rejects.toBeInstanceOf(PipelineShutdownTimeoutError)
      await expect(run).rejects.toMatchObject({ code: 'E3002', step: 'drain', timeout: '50ms' })
      expect(committed).toEqual([])
      expect(onShutdown).not.toHaveBeenCalled()
      expect(listeners()).toBe(before)

      release.resolve()
      await vi.waitFor(() => expect(onShutdown).toHaveBeenCalledTimes(1))
      expect(committed).toEqual([1])
    })

    it('stops every pipe on one signal, and runs each callback once', async ({ portal }) => {
      const chain = await portal({
        blocks: chainOf(1),
        delay: async () => {
          await never
        },
      })
      const first = vi.fn()
      const second = vi.fn()
      const { target: targetA } = recordingTarget()
      const { target: targetB } = recordingTarget()

      const runs = [
        streamFrom(chain.url).pipeTo(targetA, { shutdown: first }),
        streamFrom(chain.url).pipeTo(targetB, { shutdown: second }),
      ]

      await vi.waitFor(() => expect(chain.requests).toHaveLength(2))
      process.emit('SIGTERM', 'SIGTERM')

      await expect(runs[0]).rejects.toBeInstanceOf(PipelineStoppedError)
      await expect(runs[1]).rejects.toBeInstanceOf(PipelineStoppedError)
      expect(first).toHaveBeenCalledTimes(1)
      expect(second).toHaveBeenCalledTimes(1)
    })
  })

  describe('idle reads', () => {
    it('cancels a stream request the portal does not answer', async ({ portal }) => {
      const chain = await portal({
        blocks: chainOf(1),
        delay: async () => {
          await never
        },
      })
      const { target, committed } = recordingTarget()

      const run = streamFrom(chain.url).pipeTo(target)

      await vi.waitFor(() => expect(chain.requests).toHaveLength(1))
      process.emit('SIGTERM', 'SIGTERM')

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
      expect(committed).toEqual([])
    })

    it('stops a pipe that polls at the head', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(2), finalized: 0 })
      const { target, committed } = recordingTarget()

      const run = streamFrom(chain.url).pipeTo(target)

      await vi.waitFor(() => expect(committed.at(-1)).toBe(2))
      await vi.waitFor(() => expect(chain.requests.length).toBeGreaterThan(1))
      process.emit('SIGINT', 'SIGINT')

      await expect(run).rejects.toMatchObject({ code: 'E3001', signal: 'SIGINT' })
    })

    it('cancels the pause between retries of the stream request', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(1) })
      chain.fail({ status: 503, times: Number.POSITIVE_INFINITY })
      const { target } = recordingTarget()

      const run = streamFrom(chain.url).pipeTo(target)

      await vi.waitFor(() => expect(chain.requests.length).toBeGreaterThan(1))
      process.emit('SIGTERM', 'SIGTERM')

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)

      const requests = chain.requests.length
      await new Promise((resolve) => setTimeout(resolve, 50))
      expect(chain.requests).toHaveLength(requests)
    })

    it('stops a cache through its supplied signal', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(1) })
      const reading = deferred()
      const cache: PortalCache = {
        getStream: ({ signal }) => ({
          [Symbol.asyncIterator]: () => ({
            next: () =>
              new Promise((resolve) => {
                signal?.throwIfAborted()
                signal?.addEventListener('abort', () => resolve({ done: true, value: undefined }), { once: true })
                reading.resolve()
              }),
          }),
        }),
      }
      const { target } = recordingTarget()

      const run = evmPortalStream({
        id: 'shutdown',
        portal: chain.url,
        logger: 'silent',
        cache,
        outputs: blockDecoder({ from: 1 }),
      }).pipeTo(target)

      await reading.promise
      process.emit('SIGTERM', 'SIGTERM')

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
    })
  })

  describe('startup', () => {
    it.for([
      ['the head lookup', { from: 'latest' }],
      ['the metadata lookup', { from: 1 }],
      ['the timestamp lookup', { from: new Date('2026-01-01') }],
    ] as const)('cancels the retries of %s on a portal that is down', async ([, range]) => {
      const { target } = recordingTarget()

      const run = streamFrom('http://127.0.0.1:1', range).pipeTo(target)

      await new Promise((resolve) => setTimeout(resolve, 20))
      process.emit('SIGTERM', 'SIGTERM')

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
    })

    it('reads nothing when the signal comes while the target starts', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(3) })
      const onShutdown = vi.fn()
      let batches = 0

      const target = createTarget<any>({
        write: async ({ read }) => {
          process.emit('SIGTERM', 'SIGTERM')

          for await (const _ of read()) {
            batches++
          }
        },
      })

      const started = vi.fn()

      const run = streamFrom(chain.url)
        .pipe(createTransformer({ transform: (data) => data, start: started }))
        .pipeTo(target, { shutdown: onShutdown })

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
      expect(batches).toBe(0)
      expect(started).not.toHaveBeenCalled()
      expect(chain.requests).toEqual([])
      expect(onShutdown).toHaveBeenCalledTimes(1)
    })

    it('waits for a transformer start hook, stops it, and reads nothing', async ({ portal }) => {
      const chain = await portal({ blocks: chainOf(3) })
      const order: string[] = []

      const run = streamFrom(chain.url)
        .pipe(
          createTransformer({
            transform: (data) => data,
            start: async () => {
              process.emit('SIGTERM', 'SIGTERM')
              await new Promise((resolve) => setTimeout(resolve, 10))
              order.push('started')
            },
            stop: () => {
              order.push('stopped')
            },
          }),
        )
        .pipeTo(recordingTarget().target)

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
      expect(order).toEqual(['started', 'stopped'])
      expect(chain.requests).toEqual([])
    })

    it('cancels a portal call made with the signal supplied to a transformer start hook', async () => {
      const run = streamFrom('http://127.0.0.1:1')
        .pipe(
          createTransformer({
            transform: (data) => data,
            start: async ({ portal, signal }) => {
              setTimeout(() => process.emit('SIGTERM', 'SIGTERM'), 20)

              await portal.getHead({ abort: signal })
            },
          }),
        )
        .pipeTo(recordingTarget().target)

      await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
    })

    it.for(['start', 'transform'] as const)(
      'preserves a failure in %s after the stop signal',
      async (phase, { portal }) => {
        const chain = await portal({ blocks: chainOf(1) })
        const failure = new Error(`${phase} failed`)
        const fail = () => {
          process.emit('SIGTERM', 'SIGTERM')
          throw failure
        }
        const onShutdown = vi.fn()

        const run = streamFrom(chain.url)
          .pipe(
            createTransformer({
              start: phase === 'start' ? fail : undefined,
              transform: phase === 'transform' ? fail : (data) => data,
            }),
          )
          .pipeTo(recordingTarget().target, { shutdown: onShutdown })

        await expect(run).rejects.toBe(failure)
        expect(onShutdown).toHaveBeenCalledOnce()
      },
    )
  })

  describe('a long-running stream', () => {
    it('keeps no abort listener per batch', async ({ portal }) => {
      const blocks = 300
      const chain = await portal({ blocks: chainOf(blocks), blocksPerResponse: 1 })
      const controller = new AbortController()
      let most = 0

      const { target, committed } = recordingTarget(() => {
        most = Math.max(most, getEventListeners(controller.signal, 'abort').length)
      })

      // @ts-expect-error writeTo is private, and pipeTo keeps its signal to itself
      await streamFrom(chain.url, { from: 1, to: blocks }).writeTo(target, controller.signal)

      expect(committed.at(-1)).toBe(blocks)
      // The one link from the signal to the open stream.
      expect(most).toBe(1)
      expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0)
    })

    it('retains no consumed batch', async ({ portal }) => {
      setFlagsFromString('--expose-gc')
      const gc: () => void = runInNewContext('gc')

      const blocks = 50
      const chain = await portal({ blocks: chainOf(blocks), blocksPerResponse: 1 })
      const consumed: WeakRef<object>[] = []
      let retained = -1

      const target = createTarget<any>({
        write: async ({ read }) => {
          for await (const { data, ctx } of read()) {
            consumed.push(new WeakRef(data))

            if (ctx.stream.state.current.number === blocks) {
              await new Promise((resolve) => setImmediate(resolve))
              gc()
              retained = consumed.slice(0, -1).filter((batch) => batch.deref() !== undefined).length
            }
          }
        },
      })

      await streamFrom(chain.url, { from: 1, to: blocks }).pipeTo(target)

      expect(consumed.length).toBeGreaterThan(40)
      expect(retained).toBe(0)
    })
  })
})
