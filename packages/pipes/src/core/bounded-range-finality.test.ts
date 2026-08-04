import { afterEach, describe, expect, it } from 'vitest'

import { createTarget } from '~/core/target.js'
import { evmPortalStream } from '~/evm/index.js'
import { MockPortal, blockDecoder, mockPortal } from '~/testing/index.js'

/**
 * A bounded stream (`toBlock` set) whose upper end sits above the portal's current finalized head.
 *
 * Delivering every block is not enough to end such a stream: a finalized-only target (parquet)
 * holds rows back until some batch reports them final, and the batch that delivered the tail
 * reported a finalized head BELOW `toBlock`. If the stream ends there, the target discards the
 * tail it was never allowed to commit — the run "succeeds" with the data ending short of
 * `toBlock`, which is how the Avalanche backfill shipped a range its completion marker claimed
 * but its files did not cover.
 *
 * The contract under test: `pipeTo` resolves only after the consumer has been shown a finalized
 * head at (or above) `toBlock` — waiting for finality to catch up, not crashing, not ending short.
 */
describe('bounded range above the finalized head', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  const target = (finalizedSeen: (number | undefined)[]) =>
    createTarget({
      write: async ({ read }) => {
        for await (const { ctx } of read()) {
          finalizedSeen.push(ctx.stream.head.finalized?.number)
        }
      },
    })

  it('does not resolve pipeTo before the finalized head reaches toBlock', async () => {
    // One response delivers every requested block (1..3) but reports finality only up to 1 —
    // the shape a backfill sees when its END_BLOCK is above the current finalized head. The
    // portal's finalized head then advances one block per poll.
    let finalizedNow = 1
    const polled: number[] = []
    portal = await mockPortal(
      [
        {
          statusCode: 200,
          data: [1, 2, 3].map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
          head: { finalized: { number: 1, hash: '0x1' }, latest: { number: 3 } },
        },
      ],
      {
        finalizedHead: () => {
          finalizedNow = Math.min(finalizedNow + 1, 3)
          polled.push(finalizedNow)
          return { number: finalizedNow, hash: `0x${finalizedNow}` }
        },
      },
    )

    const stream = evmPortalStream({
      id: 'test',
      portal: { url: portal.url, headPollIntervalMs: 5 },
      outputs: blockDecoder({ from: 1, to: 3 }),
    })

    const finalizedSeen: (number | undefined)[] = []
    await stream.pipeTo(target(finalizedSeen) as any)

    // Without the finality wait this resolved right after the [1..3] batch: the tail (2..3) was
    // delivered but never reported final, so finalizedSeen ended at 1.
    expect(finalizedSeen.at(-1)).toBe(3)
    // It took more than one poll: finality was genuinely waited for, not assumed.
    expect(polled).toEqual([2, 3])
  })

  it('ends immediately when the delivered blocks are already final', async () => {
    let polls = 0
    portal = await mockPortal(
      [
        {
          statusCode: 200,
          data: [1, 2, 3].map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
          head: { finalized: { number: 5, hash: '0x5' }, latest: { number: 5 } },
        },
      ],
      {
        finalizedHead: () => {
          polls++
          return { number: 5, hash: '0x5' }
        },
      },
    )

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockDecoder({ from: 1, to: 3 }),
    })

    const finalizedSeen: (number | undefined)[] = []
    await stream.pipeTo(target(finalizedSeen) as any)

    expect(finalizedSeen).toEqual([5])
    expect(polls).toBe(0)
  })

  it('ends immediately on a no-finality dataset — there is nothing to wait for', async () => {
    let polls = 0
    portal = await mockPortal(
      [
        {
          statusCode: 200,
          data: [1, 2, 3].map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
          // no head at all → the dataset reports no finality
        },
      ],
      {
        finalizedHead: () => {
          polls++
          return undefined
        },
      },
    )

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockDecoder({ from: 1, to: 3 }),
    })

    const finalizedSeen: (number | undefined)[] = []
    await stream.pipeTo(target(finalizedSeen) as any)

    expect(finalizedSeen).toEqual([undefined])
    expect(polls).toBe(0)
  })

  it('retries a bounded range the portal answers with no data instead of ending it short', async () => {
    // First response: 200 with an empty body (no blocks at fromBlock yet). Second: the data.
    portal = await mockPortal([
      { statusCode: 200, data: [], head: { finalized: { number: 3, hash: '0x3' }, latest: { number: 3 } } },
      {
        statusCode: 200,
        data: [1, 2, 3].map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
        head: { finalized: { number: 3, hash: '0x3' }, latest: { number: 3 } },
      },
    ])

    const stream = evmPortalStream({
      id: 'test',
      portal: { url: portal.url, headPollIntervalMs: 5 },
      outputs: blockDecoder({ from: 1, to: 3 }),
    })

    const finalizedSeen: (number | undefined)[] = []
    await stream.pipeTo(target(finalizedSeen) as any)

    expect(finalizedSeen.at(-1)).toBe(3)
  })
})
