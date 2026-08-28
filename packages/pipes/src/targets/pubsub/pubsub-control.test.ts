import { afterEach, describe, expect, it } from 'vitest'

import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { MockPortal, mockMetricsServer, mockPortal, testLogger } from '~/testing/index.js'

import { PUBSUB_ERROR_CODES } from './errors.js'
import { CDC_FIELDS, TYPE_ATTRIBUTE } from './protocol.js'
import { SqlitePubsubState } from './pubsub-state.js'
import { ControlRoute, pubsubTarget } from './pubsub-target.js'
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

const announceFork: ControlRoute = {
  fork: ({ epoch, rollbackTo, deadEnd }) => ({
    data: { type: 'fork', epoch, rollbackTo: rollbackTo?.number ?? null, deadEnd },
  }),
}

/** Forward-only, with the source's finalized head advancing between the two batches. */
const advancingFinality = [
  {
    statusCode: 200,
    data: portalBlocks([1, 2]),
    head: { finalized: { number: 1, hash: '0x1' }, latest: { number: 4 } },
  },
  {
    statusCode: 200,
    data: portalBlocks([3, 4]),
    head: { finalized: { number: 3, hash: '0x3' }, latest: { number: 4 } },
  },
]

const publishWatermark: ControlRoute = {
  finality: {
    everyBlocks: 1,
    map: ({ finalized, observedAt, epoch }) => ({
      data: { type: 'finality', finalBlock: finalized.number, observedAt: observedAt.number, epoch },
    }),
  },
}

/** The same target over the forward-only stream: watermarks, no fork. */
async function runFinality({
  control = publishWatermark,
  namespace,
  quiet = false,
}: {
  control?: ControlRoute
  namespace?: string
  quiet?: boolean
} = {}) {
  const publisher = new FakePublisher()
  portal = await mockPortal(advancingFinality as never)

  await evmPortalStream({
    id: 'test-pipe',
    portal: portal.url,
    outputs: keyedBlockDecoder({ from: 0, to: 4 }),
  }).pipeTo(
    pubsubTarget<Output>({
      pubsub: {} as never,
      publisher,
      state: { path: tempStatePath() },
      publishFrom: 0,
      ...(namespace ? { namespace } : {}),
      topics: {
        blocks: {
          topic: 'blocks',
          map: ({ data }) => (quiet ? [] : data.map((block) => ({ block, data: { at: block.number } }))),
        },
      },
      control,
    }),
  )

  return publisher
}

/** One data topic plus an optional control topic, driven through the shallow fork above. */
async function runFork({
  control = announceFork,
  metrics,
  namespace,
}: {
  control?: ControlRoute | null
  metrics?: ReturnType<typeof mockMetricsServer>
  namespace?: string
} = {}) {
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
      ...(namespace ? { namespace } : {}),
      topics: {
        blocks: {
          topic: 'blocks',
          map: ({ data, epoch }) => data.map((block) => ({ block, data: { at: block.number, epoch } })),
        },
      },
      ...(control ? { control } : {}),
    }),
  )

  return publisher
}

/** What a subscription filtering on `attributes._type = "control"` would receive. */
const controlMessages = (publisher: FakePublisher) =>
  publisher.published.filter((m) => m.attributes[TYPE_ATTRIBUTE] === 'control')

/** What a BigQuery subscription filtering on `attributes._type = "cdc"` would receive. */
const dataMessages = (publisher: FakePublisher, topic = 'blocks') =>
  publisher.published.filter((m) => m.topic === topic && !(m.attributes[TYPE_ATTRIBUTE] === 'control'))

describe('pubsubTarget control route', () => {
  describe('the announcement is a CDC row', () => {
    it('rides the data topic, tagged so a subscription can select on it', async () => {
      const publisher = await runFork()
      const announcements = controlMessages(publisher)

      expect(announcements).toHaveLength(1)
      expect(announcements[0].topic).toBe('blocks')
      expect(announcements[0].attributes[TYPE_ATTRIBUTE]).toBe('control')

      // A PubSub filter reads attributes and never the body, so every message says which kind
      // it is — neither side is recognised by an absence.
      expect(dataMessages(publisher).length).toBeGreaterThan(0)
      expect(publisher.published.every((m) => m.attributes[TYPE_ATTRIBUTE])).toBeTruthy()
    })

    it('carries the CDC envelope, so the announcement lands in BigQuery like any other row', async () => {
      const publisher = await runFork()
      const announcements = controlMessages(publisher)

      expect(body(announcements[0])).toMatchObject({
        [CDC_FIELDS.changeType]: 'UPSERT',
        type: 'fork',
        epoch: 1,
        rollbackTo: 1,
        deadEnd: false,
      })
      expect(body(announcements[0])[CDC_FIELDS.changeSequenceNumber]).toMatch(/^[0-9A-F]+$/)
    })

    it('defaults the row id to one write-once row per fork', async () => {
      const publisher = await runFork({ namespace: 'demo' })

      expect(body(controlMessages(publisher)[0])[CDC_FIELDS.id]).toBe('demo:control:fork:1')
    })

    it('lets the route own the row id', async () => {
      const publisher = await runFork({
        control: { fork: ({ epoch }) => ({ id: `rewind-${epoch}`, data: { epoch } }) },
      })

      expect(body(controlMessages(publisher)[0])[CDC_FIELDS.id]).toBe('rewind-1')
    })

    it("carries the route's own filter attributes beside the marker", async () => {
      const publisher = await runFork({
        control: {
          fork: ({ epoch }) => ({ data: { epoch }, attributes: { chain: 'ethereum', table: 'blocks' } }),
        },
      })

      // Without these a subscriber filtering on `chain` would never see the announcement — and
      // a fork is exactly when it must. The target owns `_type` and nothing else.
      expect(controlMessages(publisher)[0].attributes).toMatchObject({
        chain: 'ethereum',
        table: 'blocks',
        [TYPE_ATTRIBUTE]: 'control',
      })
    })

    it('publishes to a separate topic when the route names one', async () => {
      const publisher = await runFork({ control: { ...announceFork, topic: 'blocks-control' } })

      expect(controlMessages(publisher).map((m) => m.topic)).toEqual(['blocks-control'])
      expect(publisher.setupCalls[0]).toEqual(expect.arrayContaining(['blocks', 'blocks-control']))
    })

    it('leaves the producer sequence gapless on the topic it shares', async () => {
      const publisher = await runFork()

      // The announcement takes a number from the same producer-wide counter as the rows around
      // it. On a single-route producer that counter IS the topic's sequence, so a consumer can
      // use it as a completeness barrier — which a number burned on another topic would break.
      const seqs = publisher.published
        .filter((m) => m.topic === 'blocks')
        .map((m) => Number.parseInt(body(m)[CDC_FIELDS.changeSequenceNumber], 16))
        .sort((a, b) => a - b)

      expect(seqs).toEqual(Array.from({ length: seqs.length }, (_, i) => i + 1))
    })
  })

  describe('the finality watermark', () => {
    it("publishes the source's finalized head on the channel", async () => {
      const publisher = await runFinality()
      const watermarks = controlMessages(publisher).map(body)

      expect(watermarks.map((m) => m.finalBlock)).toEqual([1, 3])
      expect(watermarks[0]).toMatchObject({ [CDC_FIELDS.changeType]: 'UPSERT', type: 'finality' })
    })

    it('defaults the row id to one write-once row per watermark', async () => {
      const publisher = await runFinality({ namespace: 'demo' })

      expect(controlMessages(publisher).map((m) => body(m)[CDC_FIELDS.id])).toEqual([
        'demo:control:finality:1',
        'demo:control:finality:3',
      ])
    })

    it('keeps advancing while the table produces no rows', async () => {
      const publisher = await runFinality({ quiet: true })

      // A watermark stamped on rows would stop here, leaving a sparse table's consumer as stale
      // as its last row. This one comes off the commit, so it does not.
      expect(dataMessages(publisher)).toEqual([])
      expect(controlMessages(publisher).map((m) => body(m).finalBlock)).toEqual([1, 3])
    })

    it('attributes the record to the block being committed, not to the finalized one', async () => {
      const publisher = await runFinality()

      // A fresh pipe's go-live block sits at the head; a watermark stamped with the far lower
      // finalized height would read as history the producer never published.
      expect(controlMessages(publisher).map((m) => body(m).observedAt)).toEqual([2, 4])
    })

    it('throttles on the advance of the finalized head', async () => {
      const publisher = await runFinality({
        control: { finality: { everyBlocks: 100, map: ({ finalized }) => ({ data: { at: finalized.number } }) } },
      })

      // The first watermark always goes out — a consumer starting cold needs one — and the
      // second is 2 blocks later, well inside the throttle.
      expect(controlMessages(publisher).map((m) => body(m).at)).toEqual([1])
    })

    it('shares the sequence with the rows it rides beside', async () => {
      const publisher = await runFinality()

      const seqs = publisher.published
        .filter((m) => m.topic === 'blocks')
        .map((m) => Number.parseInt(body(m)[CDC_FIELDS.changeSequenceNumber], 16))
        .sort((a, b) => a - b)

      expect(seqs).toEqual(Array.from({ length: seqs.length }, (_, i) => i + 1))
    })
  })

  describe('fork, end to end', () => {
    it('announces the rewind point once', async () => {
      const publisher = await runFork()

      expect(
        controlMessages(publisher)
          .map(body)
          .map((m) => ({ epoch: m.epoch, rollbackTo: m.rollbackTo })),
      ).toEqual([{ epoch: 1, rollbackTo: 1 }])
    })

    it('leads the drain it belongs to, ahead of every compensation', async () => {
      const publisher = await runFork()

      const announcementSeq = Number.parseInt(body(controlMessages(publisher)[0])[CDC_FIELDS.changeSequenceNumber], 16)
      const compensations = publisher
        .operations('blocks')
        .filter((operation) => operation.op === 'delete')
        .map((operation) => operation.seq)

      expect(compensations.length).toBeGreaterThan(0)
      expect(Math.min(...compensations)).toBeGreaterThan(announcementSeq)
    })

    it('stamps data published after the fork with the raised epoch', async () => {
      const publisher = await runFork()

      const data = dataMessages(publisher).map(body)
      const before = data.filter((row) => row[CDC_FIELDS.changeType] === 'UPSERT' && row.epoch === 0)
      const after = data.filter((row) => row[CDC_FIELDS.changeType] === 'UPSERT' && row.epoch === 1)

      // The re-streamed blocks carry epoch 1, so a consumer folding state can drop the epoch-0
      // copies it already received for the same block numbers.
      expect(before.map((row) => row.at)).toEqual([1, 2])
      expect(after.map((row) => row.at)).toEqual([2, 3])
    })

    it('keeps the announcement out of the compensations histogram', async () => {
      const metrics = mockMetricsServer()
      const publisher = await runFork({ metrics })

      // It repairs nothing, so counting it would report phantom fork blast radius.
      expect(controlMessages(publisher)).toHaveLength(1)
      const observed = metrics.histogram('sqd_pubsub_compensations_per_fork').observations

      expect(observed).toEqual([publisher.operations('blocks').filter((o) => o.op === 'delete').length])
    })

    it('resolves a fork with no control route at all', async () => {
      const publisher = await runFork({ control: null })

      expect(controlMessages(publisher)).toEqual([])
      expect(publisher.operations('blocks').some((operation) => operation.op === 'delete')).toBe(true)
    })
  })

  it('enqueues the announcement inside the transaction that raises the epoch', async () => {
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
        kind: 'control',
        route: 'fork',
        topic: 'blocks-control',
        orderingKey: '',
        op: 'upsert',
        id: `test:fork:${epoch}`,
        attributes: {},
        payload: new TextEncoder().encode(JSON.stringify({ epoch, rollbackTo, deadEnd })),
        blockNumber: rollbackTo?.number ?? 0,
      },
    ])

    expect(safe).toBeNull()
    expect(await state.getMeta('fork_epoch')).toBe('1')
    expect(new TextDecoder().decode((await state.pending())[0].payload)).toContain('"epoch":1')
    await state.close()
  })

  describe('route validation', () => {
    it('refuses a target with no routes at all', () => {
      expect(() =>
        pubsubTarget<Output>({
          pubsub: {} as never,
          publisher: new FakePublisher(),
          state: { path: tempStatePath() },
        }),
      ).toThrowError(expect.objectContaining({ code: PUBSUB_ERROR_CODES.NO_ROUTES }))
    })

    it('refuses a control route with no data route to announce forks about', () => {
      expect(() =>
        pubsubTarget<Output>({
          pubsub: {} as never,
          publisher: new FakePublisher(),
          state: { path: tempStatePath() },
          control: announceFork,
        }),
      ).toThrowError(expect.objectContaining({ code: PUBSUB_ERROR_CODES.NO_ROUTES }))
    })

    it('refuses an ordering key while message ordering is disabled', async () => {
      await expect(
        runFork({ control: { fork: ({ epoch }) => ({ data: { epoch }, orderingKey: 'shard-1' }) } }),
      ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.ORDERING_KEY_NOT_SUPPORTED })
    })
  })
})
