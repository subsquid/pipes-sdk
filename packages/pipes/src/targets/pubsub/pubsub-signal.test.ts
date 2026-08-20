import { afterEach, describe, expect, it } from 'vitest'

import { evmPortalStream } from '~/evm/evm-stream.js'
import { MockPortal, mockMetricsServer, mockPortal, testLogger } from '~/testing/index.js'

import { PUBSUB_ERROR_CODES } from './errors.js'
import { SqlitePubsubState } from './pubsub-state.js'
import { SignalRoute, pubsubTarget } from './pubsub-target.js'
import {
  FakePublisher,
  cleanupTempState,
  keyedBlockDecoder,
  makeBatchContext,
  portalBlocks,
  tempStatePath,
} from './test-support.js'

type Output = { blocks: { number: number; hash: string }[] }

const body = (message: FakePublisher['published'][number]) => JSON.parse(message.payload)

let portal: MockPortal | undefined

afterEach(async () => {
  await portal?.close()
  portal = undefined
  cleanupTempState()
})

/** Drives one batch through a signal-only target, with the finalized head under the caller's control. */
async function driveSignals({
  route,
  current,
  finalized = current,
  publisher = new FakePublisher(),
  finalizedStream,
  publishFrom = 0,
}: {
  route: SignalRoute<Output['blocks']>
  current: { number: number; hash: string }
  finalized?: { number: number; hash: string }
  publisher?: FakePublisher
  finalizedStream?: boolean
  publishFrom?: number | 'latest'
}) {
  const target = pubsubTarget<Output>({
    pubsub: {} as never,
    publisher,
    state: { path: tempStatePath() },
    publishFrom,
    signals: { blocks: route },
  })

  async function* read() {
    yield {
      data: { blocks: [current] },
      ctx: makeBatchContext({ current, finalized, rollbackChain: [current] }),
    }
  }

  await target.write({ read: read as never, logger: testLogger(), id: 'test-pipe', finalized: finalizedStream })

  return publisher
}

const boundaryFork = { mode: 'boundary', map: () => ({ data: { type: 'fork' } }) } as const

describe('pubsubTarget signals', () => {
  it('publishes a signal-only route without CDC fields', async () => {
    const publisher = new FakePublisher()
    const target = pubsubTarget<Output>({
      pubsub: {} as never,
      publisher,
      state: { path: tempStatePath() },
      publishFrom: 0,
      signals: {
        blocks: {
          topic: 'raw-blocks',
          map: ({ data, epoch }) =>
            data.map((block) => ({ block, data: { type: 'block', epoch, _id: 'application-owned', block } })),
          fork: boundaryFork,
        },
      },
    })

    async function* read() {
      const current = { number: 10, hash: '0x10' }
      yield {
        data: { blocks: [current] },
        ctx: makeBatchContext({ current, finalized: current, rollbackChain: [current] }),
      }
    }

    await target.write({ read: read as never, logger: testLogger(), id: 'test-pipe', finalized: true })

    expect(publisher.published).toHaveLength(1)
    expect(publisher.published[0]).toMatchObject({ topic: 'raw-blocks' })
    expect(JSON.parse(publisher.published[0].payload)).toEqual({
      _id: 'application-owned',
      block: { hash: '0x10', number: 10 },
      epoch: 0,
      type: 'block',
    })
  })

  it('enqueues one boundary signal with the new durable epoch during a fork', async () => {
    const state = new SqlitePubsubState({ path: tempStatePath() })
    await state.open({ cursorKey: 'test-pipe', logger: testLogger() })
    await state.commit({
      operations: [],
      ledger: [{ number: 10, hash: '0x10' }],
      cursor: { number: 10, hash: '0x10' },
      finalized: { number: 9, hash: '0x9' },
      forkCapable: true,
    })

    const safe = await state.fork([{ number: 10, hash: '0x10b' }], ({ epoch, rollbackTo, deadEnd }) => [
      {
        kind: 'signal',
        route: 'blocks',
        topic: 'raw-blocks',
        orderingKey: '',
        attributes: {},
        payload: new TextEncoder().encode(JSON.stringify({ epoch, rollbackTo, deadEnd })),
        blockNumber: rollbackTo?.number ?? 0,
        signalType: 'fork',
      },
    ])

    expect(safe).toBeNull()
    expect(await state.getMeta('fork_epoch')).toBe('1')
    expect(new TextDecoder().decode((await state.pending())[0].payload)).toContain('"epoch":1')
    await state.close()
  })

  describe('fork boundary, end to end', () => {
    /**
     * Blocks 1–2 on branch `a` (1 finalized), then a 409 whose canonical view keeps only block 1 —
     * the same shallow fork the CDC fork suite uses.
     */
    const shallowFork = [
      {
        statusCode: 200,
        data: portalBlocks([1, 2]),
        head: { finalized: { number: 1, hash: '0x1' }, latest: { number: 4 } },
      },
      {
        statusCode: 409,
        data: {
          previousBlocks: [
            { number: 1, hash: '0x1' },
            { number: 2, hash: '0x2b' },
            { number: 3, hash: '0x3b' },
          ],
        },
      },
      {
        statusCode: 200,
        data: portalBlocks([2, 3], 'b'),
        head: { finalized: { number: 1, hash: '0x1' }, latest: { number: 4 } },
      },
    ]

    async function runFork(route: SignalRoute<Output['blocks']>, metrics?: ReturnType<typeof mockMetricsServer>) {
      const publisher = new FakePublisher()
      portal = await mockPortal(shallowFork as never)

      await evmPortalStream({
        id: 'test-pipe',
        portal: portal.url,
        outputs: keyedBlockDecoder({ from: 0, to: 3 }),
        metrics: metrics?.server,
      }).pipeTo(
        pubsubTarget<Output>({
          pubsub: {} as never,
          publisher,
          state: { path: tempStatePath() },
          publishFrom: 0,
          signals: { blocks: route },
        }),
      )

      return publisher
    }

    it('publishes one boundary message carrying the raised epoch and the rewind point', async () => {
      const publisher = await runFork({
        topic: 'raw-blocks',
        map: ({ data, epoch }) => data.map((block) => ({ block, data: { type: 'block', epoch, at: block.number } })),
        fork: {
          mode: 'boundary',
          map: ({ epoch, rollbackTo, deadEnd }) => ({
            data: { type: 'fork', epoch, rollbackTo: rollbackTo?.number ?? null, deadEnd },
          }),
        },
      })

      const forks = publisher.published.map(body).filter((message) => message.type === 'fork')

      expect(forks).toEqual([{ type: 'fork', epoch: 1, rollbackTo: 1, deadEnd: false }])
    })

    it('stamps data published after the fork with the raised epoch', async () => {
      const publisher = await runFork({
        topic: 'raw-blocks',
        map: ({ data, epoch }) => data.map((block) => ({ block, data: { type: 'block', epoch, at: block.number } })),
        fork: {
          mode: 'boundary',
          map: ({ epoch }) => ({ data: { type: 'fork', epoch } }),
        },
      })

      const messages = publisher.published.map(body)
      const forkIndex = messages.findIndex((message) => message.type === 'fork')

      expect(forkIndex).toBeGreaterThan(0)
      expect(forkIndex).toBeLessThan(messages.length - 1)

      // The re-streamed blocks carry epoch 1, so a consumer can drop the epoch-0 copies it
      // already received for the same block numbers.
      expect(messages.slice(0, forkIndex).map((message) => message.epoch)).toEqual([0, 0])
      expect(messages.slice(forkIndex + 1).map((message) => message.epoch)).toEqual([1, 1])
    })

    it('publishes no boundary message for a finalized-only route', async () => {
      const publisher = await runFork({
        topic: 'raw-blocks',
        // Only finalized blocks, so the fork cannot orphan anything this route published.
        map: ({ data, ctx }) =>
          data
            .filter((block) => block.number <= (ctx.stream.head.finalized?.number ?? -1))
            .map((block) => ({ block, data: { type: 'block', at: block.number } })),
        fork: { mode: 'finalized-only' },
      })

      const messages = publisher.published.map(body)

      // The route published block 1 (finalized) and the fork still resolved — it just produced
      // no boundary message, because nothing this route sent could have been orphaned.
      expect(messages).not.toHaveLength(0)
      expect(messages.filter((message) => message.type === 'fork')).toEqual([])
    })

    it('keeps the boundary message out of the compensations histogram', async () => {
      const metrics = mockMetricsServer()

      const publisher = await runFork(
        {
          topic: 'raw-blocks',
          map: ({ data }) => data.map((block) => ({ block, data: { type: 'block', at: block.number } })),
          fork: { mode: 'boundary', map: ({ epoch }) => ({ data: { type: 'fork', epoch } }) },
        },
        metrics,
      )

      // A signal route enters no manifest, so the fork repairs nothing — but it does publish one
      // boundary message on that drain. Counting it would report phantom fork blast radius.
      expect(publisher.published.map(body).filter((message) => message.type === 'fork')).toHaveLength(1)
      expect(metrics.histogram('sqd_pubsub_compensations_per_fork').observations).toEqual([0])
    })
  })

  describe('route validation', () => {
    it('refuses a target with neither topics nor signals', () => {
      expect(() =>
        pubsubTarget<Output>({
          pubsub: {} as never,
          publisher: new FakePublisher(),
          state: { path: tempStatePath() },
        }),
      ).toThrowError(expect.objectContaining({ code: PUBSUB_ERROR_CODES.NO_ROUTES }))
    })

    it('refuses a draft without a usable block', async () => {
      await expect(
        driveSignals({
          route: {
            topic: 'raw-blocks',
            map: () => [{ data: { type: 'block' } } as never],
            fork: boundaryFork,
          },
          current: { number: 10, hash: '0x10' },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.INVALID_SIGNAL_BLOCK })
    })

    it('refuses an ordering key while message ordering is disabled', async () => {
      await expect(
        driveSignals({
          route: {
            topic: 'raw-blocks',
            map: ({ data }) => data.map((block) => ({ block, data: { at: block.number }, orderingKey: 'shard-1' })),
            fork: boundaryFork,
          },
          current: { number: 10, hash: '0x10' },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.ORDERING_KEY_NOT_SUPPORTED })
    })
  })

  describe('finalized-only enforcement', () => {
    const finalizedOnlyRoute: SignalRoute<Output['blocks']> = {
      topic: 'raw-blocks',
      map: ({ data }) => data.map((block) => ({ block, data: { at: block.number } })),
      fork: { mode: 'finalized-only' },
    }

    it('refuses a draft above the finalized head', async () => {
      await expect(
        driveSignals({
          route: finalizedOnlyRoute,
          current: { number: 10, hash: '0x10' },
          finalized: { number: 9, hash: '0x9' },
        }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.SIGNAL_NOT_FINALIZED })
    })

    it('publishes a draft at the finalized head', async () => {
      const publisher = await driveSignals({
        route: finalizedOnlyRoute,
        current: { number: 9, hash: '0x9' },
        finalized: { number: 9, hash: '0x9' },
      })

      expect(publisher.published).toHaveLength(1)
    })

    it('accepts any block on the finalized stream, where nothing can be unfinalized', async () => {
      const publisher = await driveSignals({
        route: finalizedOnlyRoute,
        current: { number: 10, hash: '0x10' },
        finalized: { number: 9, hash: '0x9' },
        finalizedStream: true,
      })

      expect(publisher.published).toHaveLength(1)
    })

    it('checks finality after the go-live cut, so a skipped draft cannot fail the run', async () => {
      const publisher = await driveSignals({
        route: finalizedOnlyRoute,
        current: { number: 10, hash: '0x10' },
        finalized: { number: 9, hash: '0x9' },
        publishFrom: 1_000_000,
      })

      expect(publisher.published).toEqual([])
    })
  })
})
