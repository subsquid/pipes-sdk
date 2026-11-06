import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, vi } from 'vitest'

import { PipelineShutdownTimeoutError, PipelineStoppedError } from '~/core/errors.js'
import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { mockBlock } from '~/testing/evm/index.js'
import { withPortal } from '~/testing/portal-fixture.js'

import type { PublishMessage } from './publisher.js'
import { type TopicRoute, pubsubTarget } from './pubsub-target.js'
import { FakePublisher, keyedBlockDecoder } from './test-support.js'

type Blocks = { blocks: { number: number; hash: string; timestamp: number }[] }

const it = withPortal.extend<{ statePath: string }>({
  statePath: async ({}, use) => {
    const dir = mkdtempSync(join(tmpdir(), 'pubsub-shutdown-'))
    await use(join(dir, 'state.sqlite'))
    rmSync(dir, { recursive: true, force: true })
  },
})

const route: TopicRoute<Blocks['blocks']> = {
  topic: 'blocks',
  map: ({ data }) => data.map((header) => ({ data: { number: header.number }, block: header })),
}

const chainOf = (count: number) => Array.from({ length: count }, (_, i) => mockBlock({ number: i + 1 }))

/** A publisher whose first publish waits for the test, as a publish on the network does. */
class HeldPublisher extends FakePublisher {
  #reached = Promise.withResolvers<void>()
  #release = Promise.withResolvers<void>()
  #held = false

  reached = this.#reached.promise

  release() {
    this.#release.resolve()
  }

  override async drain(rows: PublishMessage[]) {
    if (!this.#held && rows.length > 0) {
      this.#held = true
      this.#reached.resolve()
      await this.#release.promise
    }

    return super.drain(rows)
  }
}

function pipe({
  url,
  publisher,
  statePath,
  to,
  shutdown,
}: {
  url: string
  publisher: FakePublisher
  statePath: string
  to: number
  shutdown?: Parameters<ReturnType<typeof evmPortalStream>['pipeTo']>[1]
}) {
  return evmPortalStream({
    id: 'test-pipe',
    portal: { url, maxBytes: 1 },
    logger: 'silent',
    outputs: keyedBlockDecoder({ from: 1, to }),
  }).pipeTo(
    pubsubTarget<Blocks>({
      pubsub: {} as never,
      publisher,
      state: { path: statePath },
      publishFrom: 0,
      allowColdStart: true,
      topics: { blocks: route },
    }),
    shutdown,
  )
}

const numbersOf = (publisher: FakePublisher) => publisher.published.map(({ payload }) => JSON.parse(payload).number)

describe('pubsubTarget — graceful shutdown', () => {
  it('finishes the publish in flight, moves the cursor over it, and a restart resumes after it', async ({
    portal,
    statePath,
  }) => {
    const chain = await portal({ blocks: chainOf(5), blocksPerResponse: 1 })
    const publisher = new HeldPublisher()
    const onShutdown = vi.fn(() => {
      expect(publisher.closed).toBe(true)
    })

    const run = pipe({ url: chain.url, publisher, statePath, to: 5, shutdown: { shutdown: onShutdown } }).catch(
      (error) => error,
    )

    await publisher.reached
    process.emit('SIGTERM', 'SIGTERM')
    publisher.release()

    await expect(run).resolves.toBeInstanceOf(PipelineStoppedError)
    expect(numbersOf(publisher)).toEqual([1])
    expect(onShutdown).toHaveBeenCalledTimes(1)

    // The file is unlocked and the cursor is at the batch: a restart sends none of it again.
    const restarted = new FakePublisher()
    const requests = chain.requests.length
    await pipe({ url: chain.url, publisher: restarted, statePath, to: 5 })

    expect(numbersOf(restarted)).toEqual([2, 3, 4, 5])
    expect(chain.requests[requests].query.fromBlock).toBe(2)
    expect(chain.requests[requests].query.parentBlockHash).toBe(mockBlock({ number: 1 }).header.hash)
  })

  it('keeps a publish failure during the drain, and a restart publishes the outbox first', async ({
    portal,
    statePath,
  }) => {
    const chain = await portal({ blocks: chainOf(3), blocksPerResponse: 1 })
    const failure = new Error('publish failed')
    const publisher = new FakePublisher()
    const onShutdown = vi.fn()

    publisher.onDrain = () => {
      process.emit('SIGTERM', 'SIGTERM')
    }
    publisher.failOn = () => failure

    const run = pipe({ url: chain.url, publisher, statePath, to: 3, shutdown: { shutdown: onShutdown } })

    await expect(run).rejects.toThrow(failure)
    expect(publisher.published).toEqual([])
    expect(onShutdown).toHaveBeenCalledTimes(1)

    const restarted = new FakePublisher()
    await pipe({ url: chain.url, publisher: restarted, statePath, to: 3 })

    expect(numbersOf(restarted)).toEqual([1, 2, 3])
  })

  it('reports a publish that outlasts the budget, and the state unlocks once it settles', async ({
    portal,
    statePath,
  }) => {
    const chain = await portal({ blocks: chainOf(3), blocksPerResponse: 1 })
    const publisher = new HeldPublisher()

    const run = pipe({ url: chain.url, publisher, statePath, to: 3, shutdown: { shutdown: { timeout: '50ms' } } })

    await publisher.reached
    process.emit('SIGTERM', 'SIGTERM')

    await expect(run).rejects.toBeInstanceOf(PipelineShutdownTimeoutError)

    publisher.release()
    await vi.waitFor(() => expect(publisher.closed).toBe(true))

    const restarted = new FakePublisher()
    await pipe({ url: chain.url, publisher: restarted, statePath, to: 3 })

    expect(numbersOf(restarted)).toEqual([2, 3])
  })

  it('cancels an idle read, and leaves nothing to publish', async ({ portal, statePath }) => {
    const chain = await portal({
      blocks: chainOf(1),
      delay: () => new Promise<void>(() => {}),
    })
    const publisher = new FakePublisher()

    const run = pipe({ url: chain.url, publisher, statePath, to: 5 })

    await vi.waitFor(() => expect(chain.requests).toHaveLength(1))
    process.emit('SIGTERM', 'SIGTERM')

    await expect(run).rejects.toBeInstanceOf(PipelineStoppedError)
    expect(publisher.published).toEqual([])
    expect(publisher.closed).toBe(true)
  })

  it('leaves a client the caller passed in open', async ({ portal, statePath }) => {
    const chain = await portal({ blocks: chainOf(1) })
    const close = vi.fn(async () => {})
    const client = {
      projectId: 'test',
      close,
      topic: () => ({
        exists: async () => [true],
        flush: async () => {},
        publishMessage: async () => 'message-id',
        resumePublishing: () => {},
      }),
    }

    await evmPortalStream({
      id: 'test-pipe',
      portal: chain.url,
      logger: 'silent',
      outputs: keyedBlockDecoder({ from: 1, to: 1 }),
    }).pipeTo(
      pubsubTarget<Blocks>({
        pubsub: client as never,
        state: { path: statePath },
        publishFrom: 0,
        allowColdStart: true,
        topics: { blocks: route },
      }),
    )

    expect(close).not.toHaveBeenCalled()
  })

  it('stops during startup, before the first read', async ({ portal, statePath }) => {
    const chain = await portal({ blocks: chainOf(3) })
    const publisher = new FakePublisher()

    vi.spyOn(publisher, 'setup').mockImplementation(async () => {
      process.emit('SIGTERM', 'SIGTERM')
    })

    await expect(pipe({ url: chain.url, publisher, statePath, to: 3 })).rejects.toBeInstanceOf(PipelineStoppedError)

    expect(chain.requests).toEqual([])
    expect(publisher.closed).toBe(true)
  })
})
