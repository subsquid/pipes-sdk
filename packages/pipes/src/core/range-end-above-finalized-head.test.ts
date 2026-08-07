import { afterEach, describe, expect, it } from 'vitest'

import { createTarget } from '~/core/target.js'
import { BlockCursor } from '~/core/types.js'
import { evmPortalStream } from '~/evm/index.js'
import { MockPortal, MockResponse, blockDecoder, finalizedMockPortal, mockPortal, readAll } from '~/testing/index.js'

/**
 * A bounded range (`toBlock` set) whose end sits above the portal's current finalized head.
 *
 * The contract under test, per stream endpoint:
 *
 * - `/finalized-stream`: the stream waits AT the latest finalized block, polling the endpoint
 *   itself, and ends once block `toBlock` has been delivered — a finalized stream's blocks are
 *   final by definition, so delivering `toBlock` is the exit condition. It must not end short
 *   at the current finalized head, and it must not crash on the wait.
 *
 * - `/stream`: the stream delivers the available hot blocks up to `toBlock` and does NOT end
 *   when the last of them arrives — it keeps polling the endpoint until the finalized head
 *   derived from the response headers reaches `toBlock`. Only then may it end, so a
 *   finalized-only consumer is guaranteed to see its whole range become final before the
 *   stream is over. Three cases: no fork at all, a fork resolved entirely below `toBlock`,
 *   and a fork that arrives during the finality wait — after every block below `toBlock` was
 *   already delivered — replacing part of the delivered range.
 *
 * These tests pin the desired behavior; they are expected to fail until it is implemented.
 */
describe('bounded range whose end is above the finalized head', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  const hash = (number: number, chain = 'a') => `0x${chain}${number}`
  const block = (number: number, chain = 'a') => ({
    header: { number, hash: hash(number, chain), timestamp: number * 1000 },
  })
  const blocks = (from: number, to: number, chain = 'a') =>
    Array.from({ length: to - from + 1 }, (_, i) => block(from + i, chain))

  it('finalized stream: waits at the finalized head, polling the endpoint, until toBlock is delivered', async () => {
    // The portal is final up to 3 when a range ending at 5 is requested. Two polls later the
    // remaining blocks have become final and the portal serves them.
    const polled: number[] = []
    portal = await finalizedMockPortal([
      {
        statusCode: 200,
        data: blocks(1, 3),
        head: { finalized: { number: 3, hash: hash(3) }, latest: { number: 3 } },
      },
      { statusCode: 204, validateRequest: (req) => polled.push(req.fromBlock) },
      { statusCode: 204, validateRequest: (req) => polled.push(req.fromBlock) },
      {
        statusCode: 200,
        data: blocks(4, 5),
        head: { finalized: { number: 5, hash: hash(5) }, latest: { number: 5 } },
        validateRequest: (req) => polled.push(req.fromBlock),
      },
    ] satisfies MockResponse[])

    const stream = evmPortalStream({
      id: 'test',
      portal: {
        url: portal.url,
        finalized: true,
        headPollIntervalMs: 5,
        http: { retryAttempts: 3, retrySchedule: [10] },
      },
      outputs: blockDecoder({ from: 1, to: 5 }),
    })

    const res = await readAll(stream)

    // The whole range arrived — the stream neither ended short at the finalized head (3) nor crashed.
    expect(res.map((b) => b.number)).toEqual([1, 2, 3, 4, 5])
    // The tail was genuinely waited for: the endpoint kept being polled at the finalized head.
    expect(polled).toEqual([4, 4, 4])
  })

  describe('hot stream (/stream)', () => {
    /** A target recording every written block and the finalized head it was shown with each batch. */
    function recordingTarget() {
      const written: BlockCursor[] = []
      const finalizedSeen: (number | undefined)[] = []

      const target = createTarget({
        write: async ({ read }) => {
          for await (const { data, ctx } of read()) {
            for (const b of data as { number: number; hash: string }[]) {
              written.push({ number: b.number, hash: b.hash })
            }
            finalizedSeen.push(ctx.stream.head.finalized?.number)
          }
        },
        resolveFork: async (canonicalBlocks) => {
          // Roll back to the highest canonical block we have written, dropping everything above it.
          for (const candidate of [...canonicalBlocks].reverse()) {
            const idx = written.findIndex((w) => w.number === candidate.number && w.hash === candidate.hash)
            if (idx < 0) continue

            written.splice(idx + 1)

            return written[idx]
          }

          return null
        },
      })

      return { target, written, finalizedSeen }
    }

    function hotStream(url: string) {
      return evmPortalStream({
        id: 'test',
        portal: {
          url,
          headPollIntervalMs: 5,
          http: { retryAttempts: 3, retrySchedule: [10] },
        },
        outputs: blockDecoder({ from: 1, to: 5 }),
      })
    }

    it('no fork: keeps polling after the last hot block until the finalized head reaches toBlock', async () => {
      // Every block below toBlock arrives in one response, with finality at 3. The finalized
      // head then advances over the following (blockless) poll responses.
      portal = await mockPortal([
        {
          statusCode: 200,
          data: blocks(1, 5),
          head: { finalized: { number: 3, hash: hash(3) }, latest: { number: 5 } },
        },
        { statusCode: 200, data: [], head: { finalized: { number: 4, hash: hash(4) }, latest: { number: 5 } } },
        { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5) }, latest: { number: 5 } } },
        // Spares, in case the implementation takes extra round-trips.
        { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5) }, latest: { number: 5 } } },
        { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5) }, latest: { number: 5 } } },
      ] satisfies MockResponse[])

      const { target, written, finalizedSeen } = recordingTarget()
      await hotStream(portal.url).pipeTo(target as any)

      expect(written).toEqual([1, 2, 3, 4, 5].map((number) => ({ number, hash: hash(number) })))
      // The stream ended only once the response headers reported the whole range final.
      expect(finalizedSeen.at(-1)).toBe(5)
    })

    it('fork below toBlock: delivers the canonical chain and keeps polling until the finalized head reaches toBlock', async () => {
      // Chain "a" serves 1..3 hot with finality at 1, then reorgs at block 2 into chain "b",
      // which reaches toBlock (5) while finality still sits at 3. The portal's finalized head
      // then advances to 5 over the following (blockless) poll responses.
      portal = await mockPortal([
        {
          statusCode: 200,
          data: blocks(1, 3, 'a'),
          head: { finalized: { number: 1, hash: hash(1, 'a') }, latest: { number: 3 } },
        },
        {
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 1, hash: hash(1, 'a') },
              { number: 2, hash: hash(2, 'b') },
            ],
          },
          validateRequest: (req) => expect(req).toMatchObject({ fromBlock: 4, parentBlockHash: hash(3, 'a') }),
        },
        {
          statusCode: 200,
          data: blocks(2, 5, 'b'),
          head: { finalized: { number: 3, hash: hash(3, 'b') }, latest: { number: 5 } },
          validateRequest: (req) => expect(req).toMatchObject({ fromBlock: 2, parentBlockHash: hash(1, 'a') }),
        },
        { statusCode: 200, data: [], head: { finalized: { number: 4, hash: hash(4, 'b') }, latest: { number: 5 } } },
        { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5, 'b') }, latest: { number: 5 } } },
        // Spares, in case the implementation takes extra round-trips.
        { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5, 'b') }, latest: { number: 5 } } },
        { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5, 'b') }, latest: { number: 5 } } },
      ] satisfies MockResponse[])

      const { target, written, finalizedSeen } = recordingTarget()
      await hotStream(portal.url).pipeTo(target as any)

      // The canonical chain below toBlock, with the reorged blocks rolled back and replaced.
      expect(written).toEqual([
        { number: 1, hash: hash(1, 'a') },
        { number: 2, hash: hash(2, 'b') },
        { number: 3, hash: hash(3, 'b') },
        { number: 4, hash: hash(4, 'b') },
        { number: 5, hash: hash(5, 'b') },
      ])
      expect(finalizedSeen.at(-1)).toBe(5)
    })

    it('fork crossing toBlock: a reorg arriving during the finality wait is rolled back and re-delivered', async () => {
      // Chain "a" serves the whole range 1..5 hot with finality at 3 — so every block below
      // toBlock is already delivered when the chain reorgs at block 4 into chain "b", which
      // continues past toBlock. The stream is still waiting for finality at that point, so it
      // must observe the fork, roll the target back, re-deliver the canonical 4..5 (and nothing
      // above toBlock), and end once the finalized head reaches toBlock.
      portal = await mockPortal([
        {
          statusCode: 200,
          data: blocks(1, 5, 'a'),
          head: { finalized: { number: 3, hash: hash(3, 'a') }, latest: { number: 5 } },
        },
        {
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 3, hash: hash(3, 'a') },
              { number: 4, hash: hash(4, 'b') },
              { number: 5, hash: hash(5, 'b') },
            ],
          },
        },
        {
          statusCode: 200,
          data: blocks(4, 5, 'b'),
          head: { finalized: { number: 5, hash: hash(5, 'b') }, latest: { number: 6 } },
          validateRequest: (req) => expect(req).toMatchObject({ fromBlock: 4, parentBlockHash: hash(3, 'a') }),
        },
        // Spares, in case the implementation takes extra round-trips.
        { statusCode: 200, data: [], head: { finalized: { number: 6, hash: hash(6, 'b') }, latest: { number: 6 } } },
        { statusCode: 200, data: [], head: { finalized: { number: 6, hash: hash(6, 'b') }, latest: { number: 6 } } },
      ] satisfies MockResponse[])

      const { target, written, finalizedSeen } = recordingTarget()
      await hotStream(portal.url).pipeTo(target as any)

      // The reorged-out tail (4a, 5a) must not survive in the target.
      expect(written).toEqual([
        { number: 1, hash: hash(1, 'a') },
        { number: 2, hash: hash(2, 'a') },
        { number: 3, hash: hash(3, 'a') },
        { number: 4, hash: hash(4, 'b') },
        { number: 5, hash: hash(5, 'b') },
      ])
      expect(finalizedSeen.at(-1)).toBeGreaterThanOrEqual(5)
    })
  })
})
