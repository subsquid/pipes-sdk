import { afterEach, describe, expect, it } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { PUBSUB_ERROR_CODES } from './errors.js'
import { pubsubTarget } from './pubsub-target.js'
import { FakePublisher, cleanupTempState, makeBatchContext, tempStatePath } from './test-support.js'
import { WindowRow, windowTopic } from './window-topic.js'

type Candles = { candles: WindowRow<{ volume: number }>[] }

const body = (message: FakePublisher['published'][number]) => JSON.parse(message.payload)

afterEach(() => {
  cleanupTempState()
})

function candle(overrides: Partial<WindowRow<{ volume: number }>> = {}): WindowRow<{ volume: number }> {
  return {
    group: '0xpool',
    windowStart: 1_700_000_000,
    windowEnd: 1_700_000_300,
    timeframe: '5m',
    complete: true,
    empty: false,
    values: { volume: 12 },
    block: { number: 10, hash: '0x10', timestamp: 1_700_000_120 },
    ...overrides,
  }
}

async function publish({
  rows,
  route,
  publisher = new FakePublisher(),
}: {
  rows: WindowRow<{ volume: number }>[][]
  route: ReturnType<typeof windowTopic<{ volume: number }>>
  publisher?: FakePublisher
}) {
  const target = pubsubTarget<Candles>({
    pubsub: {} as never,
    publisher,
    state: { path: tempStatePath() },
    publishFrom: 0,
    topics: { candles: route },
  })

  async function* read() {
    for (const [index, batch] of rows.entries()) {
      const number = 10 + index
      yield {
        data: { candles: batch },
        ctx: makeBatchContext({
          current: { number, hash: `0x${number}` },
          finalized: { number: 0, hash: '0x0' },
        }),
      }
    }
  }

  await target.write({ read: read as never, logger: testLogger(), id: 'uniswap' })

  return publisher
}

describe('windowTopic', () => {
  it('keys every revision of a window by group and window start', async () => {
    const publisher = await publish({
      rows: [[candle()], [candle({ values: { volume: 20 } })]],
      route: windowTopic<{ volume: number }>({
        topic: 'candles-5m',
        attributes: (row) => ({ pool: row.group }),
      }),
    })

    expect(publisher.published.map((message) => body(message)._id)).toEqual([
      'uniswap:candles:0xpool:1700000000',
      'uniswap:candles:0xpool:1700000000',
    ])
    expect(publisher.published.map((message) => body(message)._CHANGE_SEQUENCE_NUMBER)).toEqual(['1', '2'])
    expect(publisher.published.map((message) => body(message)._CHANGE_TYPE)).toEqual(['UPSERT', 'UPSERT'])
  })

  it('carries the timeframe as a filter attribute alongside the route’s own', async () => {
    const publisher = await publish({
      rows: [[candle()]],
      route: windowTopic<{ volume: number }>({
        topic: 'candles-5m',
        attributes: (row) => ({ pool: row.group }),
      }),
    })

    expect(publisher.published[0].attributes).toMatchObject({ pool: '0xpool', timeframe: '5m' })
  })

  it('deletes an emptied window by default', async () => {
    const publisher = await publish({
      rows: [[candle()], [candle({ empty: true, complete: false })]],
      route: windowTopic<{ volume: number }>({ topic: 'candles-5m' }),
    })

    expect(publisher.published.map((message) => body(message)._CHANGE_TYPE)).toEqual(['UPSERT', 'DELETE'])
    expect(body(publisher.published[1])).toMatchObject({
      group: '0xpool',
      timeframe: '5m',
      windowStart: 1_700_000_000,
      values: { volume: 12 },
    })
  })

  it('publishes the neutral value instead when the route is declared delete-free', async () => {
    const publisher = await publish({
      rows: [[candle()], [candle({ empty: true, complete: false })]],
      route: windowTopic<{ volume: number }>({
        topic: 'candles-5m',
        emptyWindows: 'upsert',
        emptyValues: () => ({ volume: 0 }),
      }),
    })

    expect(publisher.published.map((message) => body(message)._CHANGE_TYPE)).toEqual(['UPSERT', 'UPSERT'])
    expect(JSON.parse(publisher.published[1].payload)).toMatchObject({ empty: true, values: { volume: 0 } })
  })

  it('refuses `emptyWindows: "upsert"` without the value it would publish', () => {
    expect(() => windowTopic({ topic: 'candles-5m', emptyWindows: 'upsert' })).toThrow(
      PUBSUB_ERROR_CODES.MISSING_EMPTY_VALUES,
    )
  })
})
