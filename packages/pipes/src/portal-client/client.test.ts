import { afterEach, describe, expect, it } from 'vitest'

import { HttpError } from '~/http-client/index.js'
import { PortalClient } from '~/portal-client/client.js'
import { isForkException } from '~/portal-client/fork-exception.js'
import { MockPortal, MockResponse, finalizedMockPortal, mockPortal } from '~/testing/index.js'

let portal: MockPortal | undefined

afterEach(async () => {
  await portal?.close()
  portal = undefined
})

const query = {
  type: 'evm',
  fromBlock: 0,
  toBlock: 3,
  fields: { block: { number: true, hash: true } },
} as any

function block(number: number) {
  return { header: { number, hash: `0x${number}` } }
}

/** Block numbers per delivered batch — the shape targets actually observe. */
async function batchShapes(client: PortalClient, perBlockUnfinalized: boolean, options?: { maxIdleTimeMs?: number }) {
  const shapes: number[][] = []
  for await (const batch of client.getStream(query, { ...options, perBlockUnfinalized })) {
    shapes.push(batch.blocks.map((b: any) => b.header.number))
  }

  return shapes
}

/** Requests attributed across every delivered batch, keyed by status. */
async function countRequests(
  client: PortalClient,
  perBlockUnfinalized = false,
  options?: { request?: { retryAttempts?: number; retrySchedule?: number[] } },
) {
  const counted: Record<number, number> = {}
  for await (const batch of client.getStream(query, { ...options, perBlockUnfinalized })) {
    for (const [status, count] of Object.entries(batch.meta.requests ?? {})) {
      counted[Number(status)] = (counted[Number(status)] ?? 0) + count
    }
  }

  return counted
}

describe('PortalClient batching', () => {
  // What every target except Postgres gets.
  it('delivers one batch per response by default', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 2, hash: '0x2' } },
      },
    ])

    const client = new PortalClient({ url: portal.url })

    expect(await batchShapes(client, false)).toEqual([[1, 2, 3]])
  })

  // Postgres keys undo snapshots by the batch's last block.
  it('splits every unfinalized block into its own batch when asked', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 1, hash: '0x1' } },
      },
    ])

    const client = new PortalClient({ url: portal.url })

    expect(await batchShapes(client, true)).toEqual([[1], [2], [3]])
  })

  // Read as "no finality", every hot block would be filed as finalized instead.
  it('treats a finalized head of block 0 as finality, not as its absence', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 0, hash: '0x0' } },
      },
    ])

    const client = new PortalClient({ url: portal.url })

    expect(await batchShapes(client, true)).toEqual([[1], [2], [3]])
  })

  // maxIdleTimeMs is pinned high so only cut() can separate the batches — on the default 300ms an
  // idle flush would hide a regression.
  it('never merges pending finalized blocks into an unfinalized block batch', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block(1)],
        head: { finalized: { number: 1, hash: '0x1' } },
      },
      {
        statusCode: 200,
        data: [block(2), block(3)],
        head: { finalized: { number: 1, hash: '0x1' } },
      },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })

    const shapes = await batchShapes(client, true, { maxIdleTimeMs: 60_000 })

    expect(shapes.some((s) => s.includes(1) && s.length > 1)).toBe(false)
    expect(shapes).toEqual([[1], [2], [3]])
  })
})

describe('PortalClient fork detection', () => {
  /** Drains the stream and returns whatever it threw. */
  async function streamError(client: PortalClient): Promise<unknown> {
    try {
      for await (const _ of client.getStream(query, { request: { retryAttempts: 0 } })) {
        // a fork faults the stream before any batch
      }

      return undefined
    } catch (err: unknown) {
      return err
    }
  }

  // The pre-ADR-011 shape, and still the only part of the body that is read.
  it('reads a fork from a 409 that carries previous blocks alone', async () => {
    portal = await mockPortal([
      { statusCode: 409, data: { previousBlocks: [{ number: 1, hash: '0x1' }] } },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })
    const err = await streamError(client)

    expect(isForkException(err)).toBe(true)
    expect((err as any).canonicalBlocks).toEqual([{ number: 1, hash: '0x1' }])
  })

  // ADR-011 put `error` beside `previousBlocks`, not in place of it.
  it('reads a fork from a 409 that also carries the error envelope', async () => {
    portal = await mockPortal([
      {
        statusCode: 409,
        data: {
          previousBlocks: [{ number: 1, hash: '0x1' }],
          error: {
            type: 'invalid_request_error',
            code: 'base_block_mismatch',
            message: 'Base block mismatch',
          },
        },
      },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })
    const err = await streamError(client)

    expect(isForkException(err)).toBe(true)
    expect((err as any).canonicalBlocks).toEqual([{ number: 1, hash: '0x1' }])
  })

  // Read as a fork, this walks an undefined list and throws a TypeError over the status.
  it('surfaces a 409 that carries no previous blocks instead of misreading it as a fork', async () => {
    portal = await mockPortal([
      {
        statusCode: 409,
        data: {
          error: {
            type: 'invalid_request_error',
            code: 'base_block_mismatch',
            message: 'Base block mismatch',
          },
        },
      },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })
    const err = await streamError(client)

    expect(isForkException(err)).toBe(false)
    expect(err).toBeInstanceOf(HttpError)
    expect((err as HttpError).response.status).toBe(409)
  })

  // No ancestor to walk back to either, and `last()` throws on it.
  it('surfaces a 409 whose previous blocks are an empty list', async () => {
    portal = await mockPortal([{ statusCode: 409, data: { previousBlocks: [] } }] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })
    const err = await streamError(client)

    expect(isForkException(err)).toBe(false)
    expect(err).toBeInstanceOf(HttpError)
    expect((err as HttpError).response.status).toBe(409)
  })
})

describe('PortalClient request accounting', () => {
  // Every hot block gets its own batch, so passing the counters to each would bill one response
  // three times over.
  it('counts one response as one request however many batches it becomes', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 0, hash: '0x0' } },
      },
    ])

    const client = new PortalClient({ url: portal.url })

    expect(await countRequests(client, true)).toEqual({ 200: 1 })
  })

  // A 200 with an empty body hands its counters to no batch. They have to survive into the next
  // response rather than being reset with the loop.
  it('keeps the counters of a response that carries no blocks', async () => {
    portal = await mockPortal([
      { statusCode: 200, data: [], head: { finalized: { number: 3, hash: '0x3' } } },
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 3, hash: '0x3' } },
      },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })

    expect(await countRequests(client)).toEqual({ 200: 2 })
  })

  // The 204 branch delivers its own batch, so its counters must be cleared there — otherwise
  // carrying them forward bills the poll a second time.
  it('counts a head poll once', async () => {
    portal = await mockPortal([
      { statusCode: 204 },
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 3, hash: '0x3' } },
      },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })

    expect(await countRequests(client)).toEqual({ 200: 1, 204: 1 })
  })

  // Retries are counted per attempt, and a chunkless response must not take them down with it.
  it('keeps retry counters across a response that carries no blocks', async () => {
    portal = await mockPortal([
      { statusCode: 503 },
      { statusCode: 200, data: [], head: { finalized: { number: 3, hash: '0x3' } } },
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 3, hash: '0x3' } },
      },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })

    expect(await countRequests(client, false, { request: { retryAttempts: 2, retrySchedule: [1] } })).toEqual({
      200: 2,
      503: 1,
    })
  })
})

describe('PortalClient bounded range above the finalized head', () => {
  // A finalized stream's blocks are final by definition, so delivering `toBlock` is the exit
  // condition — not the current finalized head. The hot stream has no counterpart contract: there
  // finality is a response header, and the range ends once it has been delivered.
  it('waits at the finalized head, polling, until toBlock is delivered', async () => {
    const polled: number[] = []
    portal = await finalizedMockPortal([
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 3, hash: '0x3' } },
      },
      { statusCode: 204, validateRequest: (req) => polled.push(req.fromBlock) },
      { statusCode: 204, validateRequest: (req) => polled.push(req.fromBlock) },
      {
        statusCode: 200,
        data: [block(4), block(5)],
        head: { finalized: { number: 5, hash: '0x5' } },
        validateRequest: (req) => polled.push(req.fromBlock),
      },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url, finalized: true, headPollIntervalMs: 5 })

    const delivered: number[] = []
    for await (const batch of client.getStream({ ...query, fromBlock: 1, toBlock: 5 })) {
      delivered.push(...batch.blocks.map((b: any) => b.header.number))
    }

    // The stream neither ended short at the finalized head (3) nor crashed on the wait.
    expect(delivered).toEqual([1, 2, 3, 4, 5])
    // The tail was genuinely waited for: the endpoint kept being polled at the finalized head.
    expect(polled).toEqual([4, 4, 4])
  })
})
