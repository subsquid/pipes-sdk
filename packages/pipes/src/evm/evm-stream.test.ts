import { pino } from 'pino'
import { afterEach, describe, expect, it } from 'vitest'

import { FallbackStrategy, Logger } from '~/core/index.js'

import { MockPortal, mockPortal } from '../testing/index.js'
import { evmQuery } from './evm-query-builder.js'
import { evmStream } from './evm-stream.js'

describe('evmStream', () => {
  let portal: MockPortal | undefined
  let portal2: MockPortal | undefined

  afterEach(async () => {
    await portal?.close()
    await portal2?.close()
    portal = undefined
    portal2 = undefined
  })

  it('should add default fields', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0x123', timestamp: 1000 } },
          { header: { number: 2, hash: '0x456', timestamp: 2000 } },
        ],
      },
    ])

    const fields = {
      log: { address: true, data: true, topics: true },
      block: { number: true, hash: true, timestamp: true },
      transaction: { from: true, to: true, hash: true },
      stateDiff: { address: true, key: true },
      trace: { error: true },
    }

    const stream = evmStream({
      id: 'test',
      source: portal.url,
      outputs: evmQuery().addFields(fields).addRange({ from: 0, to: 2 }),
    })

    for await (const { data } of stream) {
      const [block] = data

      expect(block.logs).toBeInstanceOf(Array)
      expect(block.traces).toBeInstanceOf(Array)
      expect(block.transactions).toBeInstanceOf(Array)
      expect(block.stateDiffs).toBeInstanceOf(Array)
    }
  })

  it('streams through a fallback source list — same pipeline, multiplexed source', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0x1', timestamp: 1000 } },
          { header: { number: 2, hash: '0x2', timestamp: 2000 } },
        ],
      },
    ])

    const stream = evmStream({
      id: 'test',
      source: [portal.url],
      outputs: evmQuery()
        .addFields({ block: { number: true, hash: true } })
        .addRange({ from: 0, to: 2 }),
    })

    const numbers: number[] = []
    for await (const { data } of stream) {
      numbers.push(...data.map((b) => b.header.number))
    }

    expect(numbers).toEqual([1, 2])
  })

  it('fails over mid-range to the standby portal and delivers the full range', async () => {
    // The primary serves block 1 and then errors on every subsequent request.
    portal = await mockPortal([
      { statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] },
      { statusCode: 500 },
      { statusCode: 500 },
      { statusCode: 500 },
      { statusCode: 500 },
    ])
    // The standby is asked to resume just past block 1 and completes the range.
    portal2 = await mockPortal([
      {
        statusCode: 200,
        data: [
          { header: { number: 2, hash: '0x2', timestamp: 2000 } },
          { header: { number: 3, hash: '0x3', timestamp: 3000 } },
        ],
        validateRequest: (body: any) => {
          expect(body.fromBlock).toBe(2)
          expect(body.parentBlockHash).toBe('0x1')
        },
      },
    ])

    const stream = evmStream({
      id: 'test',
      source: [
        { url: portal.url, name: 'primary' },
        { url: portal2.url, name: 'standby' },
      ],
      // Probing is on by default; the probe slice would consume the standby's scripted response,
      // so turn it off for the deterministic mock script.
      fallback: { detection: { capabilityProbe: false, maxLagBlocks: null, maxStalenessMs: null } },
      outputs: evmQuery()
        .addFields({ block: { number: true, hash: true } })
        .addRange({ from: 1, to: 3 }),
    })

    const numbers: number[] = []
    for await (const { data } of stream) {
      numbers.push(...data.map((b) => b.header.number))
    }

    expect(numbers).toEqual([1, 2, 3])
  })

  it('routes selection through a custom fallback strategy (code as config)', async () => {
    portal = await mockPortal([]) // the pinned-away primary must never be queried
    portal2 = await mockPortal([{ statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] }])

    const pinToStandby: FallbackStrategy = (ctx) => {
      if (ctx.event.type === 'select') return { action: 'use', index: 1 }

      return undefined
    }

    const stream = evmStream({
      id: 'test',
      source: [portal.url, portal2.url],
      fallback: {
        strategy: pinToStandby,
        detection: { capabilityProbe: false, maxLagBlocks: null, maxStalenessMs: null },
      },
      outputs: evmQuery()
        .addFields({ block: { number: true, hash: true } })
        .addRange({ from: 1, to: 1 }),
    })

    const numbers: number[] = []
    for await (const { data } of stream) {
      numbers.push(...data.map((b) => b.header.number))
    }

    expect(numbers).toEqual([1])
  })

  it("routes the pipe's logger into the fallback so it cannot log outside the caller's control", async () => {
    // pino's default message key is `msg`; the SDK's own logger renames it to `message`, so read
    // whichever the caller's logger produced.
    const lines: { msg?: string; message?: string }[] = []
    const captured = pino(
      { level: 'warn' },
      { write: (line: string) => lines.push(JSON.parse(line)) },
    ) as unknown as Logger

    portal = await mockPortal([{ statusCode: 500 }, { statusCode: 500 }, { statusCode: 500 }])
    portal2 = await mockPortal([{ statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] }])

    const stream = evmStream({
      id: 'logged-pipe',
      logger: captured,
      source: [portal.url, portal2.url],
      fallback: { detection: { capabilityProbe: false, maxLagBlocks: null, maxStalenessMs: null } },
      outputs: evmQuery()
        .addFields({ block: { number: true, hash: true } })
        .addRange({ from: 1, to: 1 }),
    })

    for await (const b of stream) void b

    // Before this, the fallback logged through a logger of its own, so a pipe set to `silent` still
    // printed its switches and a caller's logger never saw them.
    expect(lines.some((l) => (l.message ?? l.msg)?.includes('marked unhealthy'))).toBe(true)
  })

  it('still accepts the deprecated `portal` spelling of `source`', async () => {
    portal = await mockPortal([{ statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] }])

    const stream = evmStream({
      id: 'test',
      portal: portal.url,
      outputs: evmQuery()
        .addFields({ block: { number: true, hash: true } })
        .addRange({ from: 0, to: 1 }),
    })

    const numbers: number[] = []
    for await (const { data } of stream) {
      for (const block of data) numbers.push(block.header.number)
    }

    expect(numbers).toEqual([1])
  })

  it('rejects a config with neither `source` nor `portal`', () => {
    expect(() =>
      evmStream({
        id: 'test',
        outputs: evmQuery()
          .addFields({ block: { number: true, hash: true } })
          .addRange({ from: 0, to: 1 }),
      } as any),
    ).toThrow(/`source` is required/)
  })

  it('rejects `fallback` with a single (non-array) source', () => {
    expect(() =>
      evmStream({
        id: 'test',
        source: 'http://localhost:1',
        fallback: {},
        outputs: evmQuery()
          .addFields({ block: { number: true, hash: true } })
          .addRange({ from: 0, to: 1 }),
      }),
    ).toThrow(/array of sources/)
  })

  it('rejects the portal cache over a fallback source list at construction', async () => {
    portal = await mockPortal([{ statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] }])
    const url = portal.url

    expect(() =>
      evmStream({
        id: 'test',
        source: [url],
        cache: { getStream: () => ({}) as any },
        outputs: evmQuery()
          .addFields({ block: { number: true, hash: true } })
          .addRange({ from: 0, to: 1 }),
      }),
    ).toThrow(/cache requires a single Portal source/)
  })
})
