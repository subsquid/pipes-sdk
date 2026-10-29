import { afterEach, describe, expect, it, vi } from 'vitest'

import { MalformedPortalResponseError, MissingForkAncestorError } from '~/core/errors.js'
import { defaultLogger } from '~/core/logger.js'
import { HttpClient, HttpError } from '~/http-client/index.js'
import { PortalClient } from '~/portal-client/client.js'
import { isForkException } from '~/portal-client/fork-exception.js'
import { streamBodyTimeout } from '~/portal-client/stream-timeout.js'
import { MockPortal, MockResponse, mockPortal } from '~/testing/index.js'
import { withPortal } from '~/testing/portal-fixture.js'

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

/** Block numbers per delivered batch. */
async function batchShapes(client: PortalClient) {
  const shapes: number[][] = []
  for await (const batch of client.getStream(query)) {
    shapes.push(batch.blocks.map((b: any) => b.header.number))
  }

  return shapes
}

/** Requests attributed across every delivered batch, keyed by status. */
async function countRequests(
  client: PortalClient,
  options?: { request?: { retryAttempts?: number; retrySchedule?: number[] } },
) {
  const counted: Record<number, number> = {}
  for await (const batch of client.getStream(query, options)) {
    for (const [status, count] of Object.entries(batch.meta.requests ?? {})) {
      counted[Number(status)] = (counted[Number(status)] ?? 0) + count
    }
  }

  return counted
}

describe('PortalClient batching', () => {
  // The source splits a batch by finality, so the client never does.
  it('delivers one batch per response', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 1, hash: '0x1' } },
      },
    ])

    const client = new PortalClient({ url: portal.url })

    expect(await batchShapes(client)).toEqual([[1, 2, 3]])
  })

  // maxIdleTimeMs is pinned high so that only a flush can separate the batches.
  withPortal('flushes a response with unfinalized blocks at once', async ({ portal: serve }) => {
    const portal = await serve([
      {
        statusCode: 200,
        data: [block(1), block(2)],
        head: { finalized: { number: 0, hash: '0x0' } },
      },
      {
        statusCode: 200,
        data: [block(3)],
        head: { finalized: { number: 3, hash: '0x3' } },
      },
    ])

    // A head of block 0 is finality, not its absence, so blocks 1 and 2 are unfinalized.
    const client = new PortalClient({ url: portal.url, maxIdleTimeMs: 60_000 })

    expect(await batchShapes(client)).toEqual([[1, 2], [3]])
  })

  // The portal can drop the finalized header for a while. Without the head of the earlier response,
  // the source would read the merged blocks as final.
  withPortal('keeps the finalized head when it merges a response without one', async ({ portal: serve }) => {
    const portal = await serve([
      {
        statusCode: 200,
        data: [block(1)],
        head: { finalized: { number: 1, hash: '0x1' } },
      },
      {
        statusCode: 200,
        data: [block(2), block(3)],
      },
    ])

    const client = new PortalClient({ url: portal.url, maxIdleTimeMs: 60_000 })

    const batches = []
    for await (const batch of client.getStream(query)) {
      batches.push({ blocks: batch.blocks.map((b: any) => b.header.number), finalized: batch.head.finalized })
    }

    expect(batches).toEqual([{ blocks: [1, 2, 3], finalized: { number: 1, hash: '0x1' } }])
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
  withPortal('reads a fork from a 409 that carries previous blocks alone', async ({ portal: serve }) => {
    const portal = await serve([{ statusCode: 409, data: { previousBlocks: [{ number: 1, hash: '0x1' }] } }])

    const client = new PortalClient({ url: portal.url })
    const err = await streamError(client)

    expect(isForkException(err)).toBe(true)
    expect((err as any).canonicalBlocks).toEqual([{ number: 1, hash: '0x1' }])
  })

  // ADR-011 put `error` beside `previousBlocks`, not in place of it.
  withPortal('reads a fork from a 409 that also carries the error envelope', async ({ portal: serve }) => {
    const portal = await serve([
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
    ])

    const client = new PortalClient({ url: portal.url })
    const err = await streamError(client)

    expect(isForkException(err)).toBe(true)
    expect((err as any).canonicalBlocks).toEqual([{ number: 1, hash: '0x1' }])
  })

  // Read as a fork, this walks an undefined list and throws a TypeError over the status.
  withPortal('refuses a 409 that carries no previous blocks with a coded error', async ({ portal: serve }) => {
    const portal = await serve([
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
    ])

    const client = new PortalClient({ url: portal.url })
    const err = await streamError(client)

    expect(isForkException(err)).toBe(false)
    expect(err).toBeInstanceOf(MissingForkAncestorError)
    expect((err as Error).message).toContain('The portal said: "Base block mismatch"')
  })

  // No ancestor to walk back to either, and `last()` throws on it.
  withPortal('refuses a 409 whose previous blocks are an empty list', async ({ portal: serve }) => {
    const portal = await serve([{ statusCode: 409, data: { previousBlocks: [] } }])

    const client = new PortalClient({ url: portal.url })
    const err = await streamError(client)

    expect(isForkException(err)).toBe(false)
    expect(err).toBeInstanceOf(MissingForkAncestorError)
  })

  // A 409 is the fork signal only; any other refusal stays a transport error.
  withPortal('keeps an error status other than 409 an HTTP error', async ({ portal: serve }) => {
    const portal = await serve([{ statusCode: 500 }])

    const err = await streamError(new PortalClient({ url: portal.url }))

    expect(err).toBeInstanceOf(HttpError)
  })
})

describe('PortalClient malformed response', () => {
  const range = { ...query, fromBlock: 1, toBlock: 5, includeAllBlocks: true }

  /** Block numbers the stream delivered, and the error it ended with. */
  async function readUntilError(client: PortalClient) {
    const delivered: number[] = []
    try {
      for await (const batch of client.getStream(range, { request: { retryAttempts: 0 } })) {
        delivered.push(...batch.blocks.map((b: any) => b.header.number))
      }
    } catch (error) {
      return { delivered, error: error as Error }
    }

    return { delivered, error: undefined }
  }

  // The blocks before the cut are delivered once, so the next request starts at the cut block.
  withPortal('requests a response cut inside a block again from that block', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    portal.fail({ cutAfter: 2 })

    const logger = defaultLogger({ level: 'silent' })
    const warn = vi.spyOn(logger, 'warn')

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url, logger }))

    expect(error).toBeUndefined()
    expect(delivered).toEqual([1, 2, 3, 4, 5])
    expect(portal.requests.map((r) => [r.query.fromBlock, r.answer?.kind])).toEqual([
      [1, 'cut'],
      [3, 'blocks'],
    ])
    expect(warn).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ message: 'portal response ended inside a block, requesting again from block 3' }),
    )
  })

  withPortal('refuses a response cut inside the same block twice in a row', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    portal.fail({ cutAfter: 2 })
    portal.fail({ cutAfter: 0 })

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url }))

    expect(error).toBeInstanceOf(MalformedPortalResponseError)
    expect(error?.message).toContain('ended its response inside the next block, at or above block 3, twice in a row')
    expect(delivered).toEqual([1, 2].slice(0, delivered.length))
  })

  withPortal('refuses a line that is not valid JSON, naming where it came', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    portal.fail({ lineAfter: 2, line: '{"header": nope' })

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url }))

    expect(error).toBeInstanceOf(MalformedPortalResponseError)
    expect(error?.message).toContain('not valid JSON in place of the next block, at or above block 3')
    expect(error?.message).toContain('Line: {"header": nope')
    expect(delivered).toEqual([1, 2].slice(0, delivered.length))
    expect(portal.requests).toHaveLength(1)
  })

  withPortal('refuses a line without a block number', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    portal.fail({ lineAfter: 0, line: '{"header":{"hash":"0x1"}}' })

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url }))

    expect(error).toBeInstanceOf(MalformedPortalResponseError)
    expect(error?.message).toContain('a line without a block number in place of the next block, at or above block 1')
    expect(delivered).toEqual([])
  })
})

describe('PortalClient block order', () => {
  /** Block numbers the stream delivered, and the error it ended with. */
  async function readUntilError(client: PortalClient, range: { fromBlock: number; toBlock: number }) {
    const delivered: number[] = []
    try {
      for await (const batch of client.getStream({ ...query, ...range }, { request: { retryAttempts: 0 } })) {
        delivered.push(...batch.blocks.map((b: any) => b.header.number))
      }
    } catch (error) {
      return { delivered, error: error as Error }
    }

    return { delivered, error: undefined }
  }

  function blocks(numbers: number[]): MockResponse {
    return { statusCode: 200, data: numbers.map(block), head: { finalized: { number: 0, hash: '0x0' } } }
  }

  // Response chunking decides how much arrives before the bad block, so only a prefix is fixed.
  withPortal('refuses a duplicate block within one response', async ({ portal: serve }) => {
    const portal = await serve([blocks([1, 2, 2, 3])])

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url }), {
      fromBlock: 0,
      toBlock: 3,
    })

    expect(error).toMatchObject({ code: 'E1006', name: 'PortalContract' })
    expect(error?.message).toContain('block 2 after block 2: a duplicate or out-of-order block')
    expect(delivered).toEqual([1, 2].slice(0, delivered.length))
  })

  // Re-requesting from one past the lower block would deliver blocks 3 and above a second time.
  withPortal('refuses a later response that steps back below a delivered block', async ({ portal: serve }) => {
    const portal = await serve([blocks([1, 2, 3]), blocks([2])])

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url }), {
      fromBlock: 0,
      toBlock: 10,
    })

    expect(error).toMatchObject({ code: 'E1006', name: 'PortalContract' })
    expect(error?.message).toContain('block 2 after block 3')
    expect(delivered).toEqual([1, 2, 3].slice(0, delivered.length))
  })

  withPortal('refuses a block below the requested start', async ({ portal: serve }) => {
    const portal = await serve([blocks([3, 5])])

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url }), {
      fromBlock: 5,
      toBlock: 10,
    })

    expect(error).toMatchObject({ code: 'E1006', name: 'PortalContract' })
    expect(error?.message).toContain('block 3, below the requested start 5')
    expect(delivered).toEqual([])
  })

  withPortal('refuses a block above the requested end', async ({ portal: serve }) => {
    const portal = await serve([blocks([1, 2, 3, 4])])

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url }), {
      fromBlock: 0,
      toBlock: 3,
    })

    expect(error).toMatchObject({ code: 'E1006', name: 'PortalContract' })
    expect(error?.message).toContain('block 4, above the requested end 3')
    expect(delivered).toEqual([1, 2, 3].slice(0, delivered.length))
  })

  // Blocks with no matching data are skipped, so a gap between numbers is not a violation.
  withPortal('delivers ascending blocks with gaps between them', async ({ portal: serve }) => {
    const portal = await serve([blocks([1, 5]), blocks([9])])

    const { delivered, error } = await readUntilError(new PortalClient({ url: portal.url }), {
      fromBlock: 0,
      toBlock: 9,
    })

    expect(error).toBeUndefined()
    expect(delivered).toEqual([1, 5, 9])
  })
})

describe('PortalClient request accounting', () => {
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

    expect(await countRequests(client, { request: { retryAttempts: 2, retrySchedule: [1] } })).toEqual({
      200: 2,
      503: 1,
    })
  })

  // 529 is what the portal answers under load instead of 503; surfacing it would fail a stream
  // the portal expects the client to simply retry.
  it('retries an overloaded portal', async () => {
    portal = await mockPortal([
      { statusCode: 529 },
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 3, hash: '0x3' } },
      },
    ] satisfies MockResponse[])

    const client = new PortalClient({ url: portal.url })

    expect(await countRequests(client, { request: { retryAttempts: 2, retrySchedule: [1] } })).toEqual({
      200: 1,
      529: 1,
    })
  })
})

describe('PortalClient stalled response', () => {
  const range = { ...query, fromBlock: 1, toBlock: 5, includeAllBlocks: true }

  async function readBlocks(stream: AsyncIterable<{ blocks: any[] }>) {
    const blocks: { number: number; hash: string }[] = []
    for await (const batch of stream) {
      blocks.push(...batch.blocks.map((b) => b.header))
    }

    return blocks
  }

  // The blocks before the stall are delivered once, so the next request starts after them.
  withPortal('requests a stalled response again from the next block', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    portal.fail({ stallAfter: 2 })

    const logger = defaultLogger({ level: 'silent' })
    const warn = vi.spyOn(logger, 'warn')
    const client = new PortalClient({ url: portal.url, logger })

    const blocks = await readBlocks(client.getStream(range, { request: { bodyTimeout: 100 } }))
    const resumed = portal.requests[1].query

    expect(blocks.map((b) => b.number)).toEqual([1, 2, 3, 4, 5])
    expect(portal.requests.map((r) => r.answer?.kind)).toEqual(['stalled', 'blocks'])
    expect(resumed).toMatchObject({ fromBlock: 3, parentBlockHash: blocks[1].hash })
    expect(warn).toHaveBeenCalledExactlyOnceWith({
      message: expect.stringMatching(/^portal response stalled: no block for \d+s, requesting again from block 3$/),
      url: client.getUrl(),
      fromBlock: 3,
      stalledMs: expect.any(Number),
    })
  })

  // A pipe configures the portal through its HTTP options alone, so they must still set the timeout.
  withPortal('applies the body timeout of the HTTP options to a stream', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    portal.fail({ stallAfter: 0 })

    const client = new PortalClient({ url: portal.url, http: { bodyTimeout: 100 } })

    const blocks = await readBlocks(client.getStream(range))

    expect(blocks.map((b) => b.number)).toEqual([1, 2, 3, 4, 5])
    expect(portal.requests.map((r) => [r.query.fromBlock, r.answer?.kind])).toEqual([
      [1, 'stalled'],
      [1, 'blocks'],
    ])
  })

  // The default takes 30 s to fire, so this checks the timeout the request carries instead.
  withPortal('picks the body timeout from the response when none is configured', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    const http = new HttpClient()
    const request = vi.spyOn(http, 'request')

    await readBlocks(new PortalClient({ url: portal.url, http }).getStream(range))

    expect(request).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ bodyTimeout: streamBodyTimeout }),
    )
  })
})

describe('PortalClient bounded range above the finalized head', () => {
  // A finalized stream's blocks are final by definition, so delivering `toBlock` is the exit
  // condition — not the current finalized head. The hot stream has no counterpart contract: there
  // finality is a response header, and the range ends once it has been delivered.
  it('waits at the finalized head, polling, until toBlock is delivered', async () => {
    const polled: number[] = []
    portal = await mockPortal(
      [
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
      ] satisfies MockResponse[],
      { finalized: true },
    )

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
