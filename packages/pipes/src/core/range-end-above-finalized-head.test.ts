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
 * - `/stream`: the stream delivers the available hot blocks up to `toBlock` (surviving reorgs
 *   along the way) and does NOT end when the last of them arrives — it keeps polling the
 *   endpoint until the finalized head derived from the response headers reaches `toBlock`.
 *   Only then may it end, so a finalized-only consumer is guaranteed to see its whole range
 *   become final before the stream is over.
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

  it('finalized stream: waits at the finalized head, polling the endpoint, until toBlock is delivered', async () => {
    // The portal is final up to 3 when a range ending at 5 is requested. Two polls later the
    // remaining blocks have become final and the portal serves them.
    const polled: number[] = []
    portal = await finalizedMockPortal([
      {
        statusCode: 200,
        data: [block(1), block(2), block(3)],
        head: { finalized: { number: 3, hash: hash(3) }, latest: { number: 3 } },
      },
      { statusCode: 204, validateRequest: (req) => polled.push(req.fromBlock) },
      { statusCode: 204, validateRequest: (req) => polled.push(req.fromBlock) },
      {
        statusCode: 200,
        data: [block(4), block(5)],
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

  it('hot stream: delivers hot blocks through a reorg and polls until the finalized head reaches toBlock', async () => {
    // Chain "a" serves 1..3 hot with finality at 1, then reorgs at block 2 into chain "b",
    // which reaches toBlock (5) while finality still sits at 3. The portal's finalized head
    // then advances to 5 over the following (blockless) poll responses.
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block(1, 'a'), block(2, 'a'), block(3, 'a')],
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
        data: [block(2, 'b'), block(3, 'b'), block(4, 'b'), block(5, 'b')],
        head: { finalized: { number: 3, hash: hash(3, 'b') }, latest: { number: 5 } },
        validateRequest: (req) => expect(req).toMatchObject({ fromBlock: 2, parentBlockHash: hash(1, 'a') }),
      },
      // Every block below toBlock is delivered now, but finality is still short of it: the
      // stream must keep polling. Extra copies cover an implementation taking more round-trips.
      { statusCode: 200, data: [], head: { finalized: { number: 4, hash: hash(4, 'b') }, latest: { number: 5 } } },
      { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5, 'b') }, latest: { number: 5 } } },
      { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5, 'b') }, latest: { number: 5 } } },
      { statusCode: 200, data: [], head: { finalized: { number: 5, hash: hash(5, 'b') }, latest: { number: 5 } } },
    ] satisfies MockResponse[])

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

    const stream = evmPortalStream({
      id: 'test',
      portal: {
        url: portal.url,
        headPollIntervalMs: 5,
        http: { retryAttempts: 3, retrySchedule: [10] },
      },
      outputs: blockDecoder({ from: 1, to: 5 }),
    })

    await stream.pipeTo(target as any)

    // The canonical chain below toBlock, with the reorged blocks rolled back and replaced.
    expect(written).toEqual([
      { number: 1, hash: hash(1, 'a') },
      { number: 2, hash: hash(2, 'b') },
      { number: 3, hash: hash(3, 'b') },
      { number: 4, hash: hash(4, 'b') },
      { number: 5, hash: hash(5, 'b') },
    ])
    // The stream ended only once the response headers reported the whole range final.
    expect(finalizedSeen.at(-1)).toBe(5)
  })
})
