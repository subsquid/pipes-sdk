import { pino } from 'pino'
import { describe, expect } from 'vitest'

import { Logger } from '~/core/index.js'

import { withPortal as it } from '../testing/portal-fixture.js'
import { StubPortalClient } from '../testing/stub-portal.js'
import { evmQuery } from './evm-query-builder.js'
import { evmStream } from './evm-stream.js'

describe('evmStream', () => {
  it('should add default fields', async ({ portal: serve }) => {
    const portal = await serve([
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

  it("reads a lone {type: 'custom'} source directly, without a fallback list", async () => {
    const client = new StubPortalClient(async function* () {
      yield {
        blocks: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
        head: {},
        meta: { bytes: 0, requestedFromBlock: 0, lastBlockReceivedAt: new Date(), requests: {} },
      }
    })

    const stream = evmStream({
      id: 'test',
      source: { type: 'custom', client },
      outputs: evmQuery()
        .addFields({ block: { number: true, hash: true } })
        .addRange({ from: 0, to: 1 }),
    })

    const numbers: number[] = []
    for await (const { data } of stream) {
      numbers.push(...data.map((b) => b.header.number))
    }

    expect(numbers).toEqual([1])
  })

  it('streams through a fallback source list — same pipeline, multiplexed source', async ({ portal: serve }) => {
    const portal = await serve([
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

  it('fails over mid-range to the standby portal and delivers the full range', async ({ portal: serve }) => {
    // The primary serves block 1 and then errors on every subsequent request.
    const portal = await serve([
      { statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] },
      { statusCode: 500 },
      { statusCode: 500 },
      { statusCode: 500 },
      { statusCode: 500 },
    ])
    // The standby is asked to resume just past block 1 and completes the range.
    const portal2 = await serve([
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

  it("routes the pipe's logger into the fallback so it cannot log outside the caller's control", async ({
    portal: serve,
  }) => {
    // pino's default message key is `msg`; the SDK's own logger renames it to `message`, so read
    // whichever the caller's logger produced.
    const lines: { msg?: string; message?: string }[] = []
    const captured = pino(
      { level: 'warn' },
      { write: (line: string) => lines.push(JSON.parse(line)) },
    ) as unknown as Logger

    const portal = await serve([{ statusCode: 500 }, { statusCode: 500 }, { statusCode: 500 }])
    const portal2 = await serve([{ statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] }])

    const stream = evmStream({
      id: 'logged-pipe',
      logger: captured,
      source: [portal.url, portal2.url],
      fallback: { detection: { capabilityProbe: false, maxLagBlocks: null, maxStalenessMs: null } },
      outputs: evmQuery()
        .addFields({ block: { number: true, hash: true } })
        .addRange({ from: 1, to: 1 }),
    })

    for await (const b of stream) {
      void b
    }

    // Before this, the fallback logged through a logger of its own, so a pipe set to `silent` still
    // printed its switches and a caller's logger never saw them.
    expect(lines.some((l) => (l.message ?? l.msg)?.includes('marked unhealthy'))).toBe(true)
  })

  it('rejects a config without `source`', () => {
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

  it('rejects the portal cache over a fallback source list at construction', async ({ portal: serve }) => {
    const portal = await serve([{ statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] }])
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
