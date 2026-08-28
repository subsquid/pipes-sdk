import { afterEach, describe, expect, it } from 'vitest'

import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { MockPortal, MockResponse, mockPortal } from '~/testing/index.js'

import { MessageDraft, TopicRoute, pubsubTarget } from './pubsub-target.js'
import { FakePublisher, cleanupTempState, keyedBlockDecoder, portalBlocks, tempStatePath } from './test-support.js'

type Blocks = { blocks: { number: number; hash: string; timestamp: number }[] }

const body = (message: FakePublisher['published'][number]) => JSON.parse(message.payload)

let portal: MockPortal | undefined

afterEach(async () => {
  await portal?.close()
  portal = undefined
  cleanupTempState()
})

async function run({
  responses,
  route,
  to,
  publisher = new FakePublisher(),
}: {
  responses: MockResponse[]
  route: TopicRoute<Blocks['blocks']>
  to: number
  publisher?: FakePublisher
}) {
  portal = await mockPortal(responses)

  await evmPortalStream({
    id: 'test-pipe',
    portal: portal.url,
    outputs: keyedBlockDecoder({ from: 0, to }),
  }).pipeTo(
    pubsubTarget<Blocks>({
      pubsub: {} as never,
      publisher,
      state: { path: tempStatePath() },
      publishFrom: 0,
      allowColdStart: true,
      finality: false,
      topics: { blocks: route },
    }),
  )

  return publisher
}

const head = { finalized: { number: 1, hash: '0x1' }, latest: { number: 4 } }

/**
 * Blocks 1–2 on branch `a` (1 finalized), then a 409 whose canonical view keeps only block 1.
 * `resolveForkCursor` walks the persisted ledger against `previousBlocks` and lands there.
 */
const shallowFork: MockResponse[] = [
  { statusCode: 200, data: portalBlocks([1, 2]), head },
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
  { statusCode: 200, data: portalBlocks([2, 3], 'b'), head },
]

/** Same fork point, but with TWO unfinalized blocks (2 and 3) above it. */
const deepFork: MockResponse[] = [
  { statusCode: 200, data: portalBlocks([1, 2]), head },
  { statusCode: 200, data: portalBlocks([3]), head },
  {
    statusCode: 409,
    data: {
      previousBlocks: [
        { number: 1, hash: '0x1' },
        { number: 2, hash: '0x2b' },
        { number: 3, hash: '0x3b' },
        { number: 4, hash: '0x4b' },
      ],
    },
  },
  { statusCode: 200, data: portalBlocks([2, 3, 4], 'b'), head },
]

const eventRoute: TopicRoute<Blocks['blocks']> = {
  topic: 'blocks',
  map: ({ data }) =>
    data.map((header) => ({
      data: { number: header.number },
      block: header,
      attributes: { chain: 'mock' },
    })),
}

/** One long-lived row, revised on every block — the shape aggregated windows publish. */
function materializedRoute(options: { rollbackWhenMissing?: TopicRoute<any>['rollbackWhenMissing'] } = {}) {
  const route: TopicRoute<Blocks['blocks']> = {
    topic: 'blocks',
    mode: 'materialized',
    map: ({ data }) =>
      data.map(
        (header): MessageDraft => ({
          data: { branch: header.hash },
          block: header,
          id: 'one-row',
          attributes: { chain: 'mock' },
        }),
      ),
    rollbackWhenMissing: options.rollbackWhenMissing,
  }

  return route
}

describe('pubsubTarget — fork compensation', () => {
  it('deletes an orphaned write-once event, with the attributes of the upsert it repairs', async () => {
    const publisher = await run({ responses: shallowFork, route: eventRoute, to: 3 })

    const deletes = publisher.published.filter((message) => body(message)._CHANGE_TYPE === 'DELETE')
    expect(deletes).toHaveLength(1)
    expect(body(deletes[0])._id).toBe('test-pipe:blocks:2:0x2:0')
    // The compensation passes the same subscription filters as the operation it repairs.
    expect(deletes[0].attributes['chain']).toBe('mock')
    // BigQuery matches DELETE rows by every declared primary-key column, so the source row is retained.
    expect(body(deletes[0]).number).toBe(2)
  })

  it('publishes compensations before the re-streamed canonical blocks', async () => {
    const publisher = await run({ responses: shallowFork, route: eventRoute, to: 3 })

    expect(
      publisher.published.map((message) => [
        body(message)._CHANGE_TYPE.toLowerCase(),
        body(message)._CHANGE_SEQUENCE_NUMBER,
        body(message).number,
      ]),
    ).toEqual([
      ['upsert', '1', 1],
      ['upsert', '2', 2],
      ['delete', '3', 2],
      ['upsert', '4', 2],
      ['upsert', '5', 3],
    ])
  })

  it('leaves the finalized prefix alone — it was never rollbackable', async () => {
    const publisher = await run({ responses: shallowFork, route: eventRoute, to: 3 })

    const blockOne = publisher.published.filter((message) => body(message)._id.includes(':1:'))
    expect(blockOne.map((message) => body(message)._CHANGE_TYPE)).toEqual(['UPSERT'])
  })

  it('folds several orphaned revisions of one id into a single compensation', async () => {
    const publisher = await run({ responses: deepFork, route: materializedRoute(), to: 4 })

    // Revisions at blocks 2 and 3 are both orphaned; the row is repaired once, not once per
    // revision, and the repair sits between the orphaned suffix and the canonical replay.
    expect(publisher.published.map((message) => body(message).branch)).toEqual([
      '0x1',
      '0x2',
      '0x3',
      '0x1',
      '0x2b',
      '0x3b',
      '0x4b',
    ])
  })

  it('restores a materialized row to the revision that survives at the safe cursor', async () => {
    const publisher = await run({ responses: deepFork, route: materializedRoute(), to: 4 })

    const compensation = publisher.published[3]
    expect(body(compensation)._CHANGE_TYPE).toBe('UPSERT')
    expect(body(compensation)._id).toBe('one-row')
    // Block 1's revision was finalized, so it lives on as the baseline and comes back verbatim.
    expect(body(compensation).branch).toBe('0x1')
  })

  it('keeps a route with rollbackWhenMissing delete-free through the fork', async () => {
    const publisher = await run({
      responses: shallowFork,
      to: 3,
      route: {
        topic: 'blocks',
        mode: 'materialized',
        map: ({ data }) =>
          data.map((header) => ({ data: { branch: header.hash }, block: header, id: `row-${header.number}` })),
        rollbackWhenMissing: () => ({ op: 'upsert', data: { branch: null } }),
      },
    })

    expect(publisher.published.some((message) => body(message)._CHANGE_TYPE === 'DELETE')).toBe(false)

    const compensation = publisher.published[2]
    expect(body(compensation)._id).toBe('row-2')
    expect(body(compensation).branch).toBeNull()
  })

  it('never rewinds the change sequence number', async () => {
    const publisher = await run({ responses: deepFork, route: eventRoute, to: 4 })

    const sequence = publisher.operations().map((operation) => operation.seq)
    expect(sequence).toEqual([...sequence].sort((a, b) => a - b))
    expect(new Set(sequence).size).toBe(sequence.length)
  })

  it('rewinds the block order the next batch is checked against', async () => {
    // The orphaned branch reached block 3; the canonical one restarts at 2. Without the rewind
    // that forward step reads as a mapper going backwards and the batch is refused.
    const publisher = await run({ responses: deepFork, route: eventRoute, to: 4 })

    expect(publisher.operations().map((operation) => [operation.op, JSON.parse(operation.payload).number])).toEqual([
      ['upsert', 1],
      ['upsert', 2],
      ['upsert', 3],
      ['delete', 2],
      ['delete', 3],
      ['upsert', 2],
      ['upsert', 3],
      ['upsert', 4],
    ])
  })

  it('resumes the read loop from the safe cursor', async () => {
    const publisher = new FakePublisher()

    portal = await mockPortal([
      shallowFork[0],
      shallowFork[1],
      {
        ...shallowFork[2],
        validateRequest: (request) => {
          expect(request.fromBlock).toBe(2)
          expect(request.parentBlockHash).toBe('0x1')
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
        publisher,
        state: { path: tempStatePath() },
        publishFrom: 0,
        allowColdStart: true,
        finality: false,
        topics: { blocks: eventRoute },
      }),
    )

    expect(publisher.published).toHaveLength(5)
  })
})
