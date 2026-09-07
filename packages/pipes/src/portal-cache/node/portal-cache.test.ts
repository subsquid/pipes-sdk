import fs from 'node:fs/promises'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { PortalBatch } from '~/core/index.js'
import { evmStream } from '~/evm/index.js'
import { portalSqliteCache } from '~/portal-cache/node/node-sqlite-cache-adapter.js'
import { createMemoryTarget } from '~/targets/memory/memory-target.js'
import { MockPortal, blockDecoder, finalizedMockPortal, mockPortal } from '~/testing/index.js'

// Transform batch to only include data and meta without any functions or complex objects
const transformBatch = ({
  data,
  ctx: {
    stream: {
      head,
      query: {
        // exclude another dynamic, i.e. URL
        url,
        ...query
      },
      progress,
      // exclude progress as it contains timers and dynamic data
      ...streamRest
    },
    batch: {
      bytesSize,
      // exclude other dynamic meta fields
      ...batchRest
    },
  },
}: PortalBatch) => ({
  data,
  meta: {
    head,
    query,
    meta: {
      bytesSize,
    },
    state: streamRest.state,
  },
})

export async function readAllChunks<T>(stream: AsyncIterable<T>) {
  const res: T[] = []

  for await (const batch of stream) {
    res.push(batch)
  }

  return res
}

const DB_PATH = './test.db'

describe('Portal cache', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  beforeEach(async () => {
    await fs.rm(DB_PATH).catch(() => {})
  })

  describe('Sqlite adapter', () => {
    it('should store requests and get the same result on second pass', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5', timestamp: 5000 } },
          ],
          head: { finalized: { number: 5, hash: '0x5' } },
          validateRequest: (res) => {
            expect(res.fromBlock).toBe(0)
          },
        },
      ])

      const cache = portalSqliteCache({ path: DB_PATH })

      const stream = evmStream({
        id: 'test',
        source: portal.url,
        outputs: blockDecoder({ from: 0, to: 5 }),
        cache,
      })

      const res1 = (await readAllChunks(stream)).map(transformBatch)
      const res2 = (await readAllChunks(stream)).map(transformBatch)

      expect(res1).toEqual(res2)
      expect(res2).toMatchInlineSnapshot(`
        [
          {
            "data": [
              {
                "hash": "0x1",
                "number": 1,
                "timestamp": 1000,
              },
              {
                "hash": "0x2",
                "number": 2,
                "timestamp": 2000,
              },
              {
                "hash": "0x3",
                "number": 3,
                "timestamp": 3000,
              },
              {
                "hash": "0x4",
                "number": 4,
                "timestamp": 4000,
              },
              {
                "hash": "0x5",
                "number": 5,
                "timestamp": 5000,
              },
            ],
            "meta": {
              "head": {
                "finalized": {
                  "hash": "0x5",
                  "number": 5,
                },
                "latest": undefined,
              },
              "meta": {
                "bytesSize": 265,
              },
              "query": {
                "hash": "1137b8a50718df4ec48060ae5cef9c8e929b619a23846c34a2535b3548b589c5",
                "raw": {
                  "fields": {
                    "block": {
                      "hash": true,
                      "number": true,
                      "timestamp": true,
                    },
                  },
                  "fromBlock": 0,
                  "logs": undefined,
                  "parentBlockHash": undefined,
                  "stateDiffs": undefined,
                  "toBlock": 5,
                  "traces": undefined,
                  "transactions": undefined,
                  "type": "evm",
                },
              },
              "state": {
                "current": {
                  "hash": "0x5",
                  "number": 5,
                  "timestamp": 5000,
                },
                "initial": 0,
                "last": 5,
                "ranges": [
                  {
                    "from": 0,
                    "to": 5,
                  },
                ],
                "rollbackChain": [],
              },
            },
          },
        ]
      `)

      // check stored rows by query hash
      const rows = await readAllChunks(
        cache.stream({
          queryHash: '1137b8a50718df4ec48060ae5cef9c8e929b619a23846c34a2535b3548b589c5',
          fromBlock: 0,
        }),
      )

      expect(rows).toHaveLength(1)
    })

    it('should hand back lastBlockReceivedAt as a Date on the cached pass', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
          ],
          head: { finalized: { number: 2, hash: '0x2' } },
        },
      ])

      const stream = evmStream({
        id: 'test',
        source: portal.url,
        outputs: blockDecoder({ from: 0, to: 2 }),
        cache: portalSqliteCache({ path: DB_PATH }),
      })

      const live = await readAllChunks(stream)
      const cached = await readAllChunks(stream)

      // JSON has no date type, so the cache round-trip returns an ISO string unless it is
      // revived. Consumers (the pubsub target, the rpc-latency watcher) call .getTime() on it.
      expect(live[0].ctx.batch.lastBlockReceivedAt).toBeInstanceOf(Date)
      expect(cached[0].ctx.batch.lastBlockReceivedAt).toBeInstanceOf(Date)
      expect(() => cached[0].ctx.batch.lastBlockReceivedAt.getTime()).not.toThrow()
    })

    it('should not reuse data that has from greater than request', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [
            { header: { number: 6, hash: '0x6', timestamp: 6000 } },
            { header: { number: 7, hash: '0x7', timestamp: 7000 } },
            { header: { number: 8, hash: '0x8', timestamp: 8000 } },
            { header: { number: 9, hash: '0x9', timestamp: 9000 } },
            { header: { number: 10, hash: '0x10', timestamp: 10000 } },
          ],
          head: { finalized: { number: 20, hash: '0x20' } },
          validateRequest: (req) => {
            expect(req.fromBlock).toBe(6)
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5', timestamp: 5000 } },
          ],
          head: { finalized: { number: 20, hash: '0x20' } },
          validateRequest: (req) => {
            expect(req.fromBlock).toBe(0)
          },
        },
      ])

      const cache = portalSqliteCache({ path: DB_PATH })

      const stream = evmStream({
        id: 'test',
        source: portal.url,
        outputs: blockDecoder({ from: 6, to: 10 }),
        cache,
      })

      await readAllChunks(stream) // first pass to store data

      // now request from 0 to 5, should not reuse data from 6 to 10
      const stream2 = evmStream({
        id: 'test',
        source: portal.url,
        outputs: blockDecoder({ from: 0, to: 5 }),
        cache,
      })

      const res2 = (await readAllChunks(stream2)).map(transformBatch)

      expect(res2).toMatchInlineSnapshot(`
        [
          {
            "data": [
              {
                "hash": "0x1",
                "number": 1,
                "timestamp": 1000,
              },
              {
                "hash": "0x2",
                "number": 2,
                "timestamp": 2000,
              },
              {
                "hash": "0x3",
                "number": 3,
                "timestamp": 3000,
              },
              {
                "hash": "0x4",
                "number": 4,
                "timestamp": 4000,
              },
              {
                "hash": "0x5",
                "number": 5,
                "timestamp": 5000,
              },
            ],
            "meta": {
              "head": {
                "finalized": {
                  "hash": "0x20",
                  "number": 20,
                },
                "latest": undefined,
              },
              "meta": {
                "bytesSize": 265,
              },
              "query": {
                "hash": "1137b8a50718df4ec48060ae5cef9c8e929b619a23846c34a2535b3548b589c5",
                "raw": {
                  "fields": {
                    "block": {
                      "hash": true,
                      "number": true,
                      "timestamp": true,
                    },
                  },
                  "fromBlock": 0,
                  "parentBlockHash": undefined,
                  "toBlock": 5,
                  "type": "evm",
                },
              },
              "state": {
                "current": {
                  "hash": "0x5",
                  "number": 5,
                  "timestamp": 5000,
                },
                "initial": 0,
                "last": 5,
                "ranges": [
                  {
                    "from": 0,
                    "to": 5,
                  },
                ],
                "rollbackChain": [],
              },
            },
          },
        ]
      `)

      // check stored rows by query hash
      const rows = await readAllChunks(
        cache.stream({
          queryHash: '1137b8a50718df4ec48060ae5cef9c8e929b619a23846c34a2535b3548b589c5',
          fromBlock: 0,
        }),
      )

      expect(rows).toHaveLength(2)
    })

    it('should support block gaps and exclude latest blocks', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5', timestamp: 5000 } },
          ],
          head: { finalized: { number: 2, hash: '0x2' } },
          validateRequest: (req) => {
            expect(req.fromBlock).toBe(0)
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 3, hash: '0x3', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5', timestamp: 5000 } },
          ],
          head: { finalized: { number: 5, hash: '0x5' } },
          validateRequest: (req) => {
            expect(req.fromBlock).toBe(3)
          },
        },
      ])

      const cache = portalSqliteCache({ path: DB_PATH })

      const stream = evmStream({
        id: 'test',
        source: portal.url,
        outputs: blockDecoder({ from: 0, to: 5 }),
        cache,
      })

      const res1 = (await readAllChunks(stream)).map(transformBatch)
      const res2 = (await readAllChunks(stream)).map(transformBatch)

      expect(res1).toMatchInlineSnapshot(`
        [
          {
            "data": [
              {
                "hash": "0x1",
                "number": 1,
                "timestamp": 1000,
              },
              {
                "hash": "0x2",
                "number": 2,
                "timestamp": 2000,
              },
              {
                "hash": "0x3",
                "number": 3,
                "timestamp": 3000,
              },
              {
                "hash": "0x4",
                "number": 4,
                "timestamp": 4000,
              },
              {
                "hash": "0x5",
                "number": 5,
                "timestamp": 5000,
              },
            ],
            "meta": {
              "head": {
                "finalized": {
                  "hash": "0x2",
                  "number": 2,
                },
                "latest": undefined,
              },
              "meta": {
                "bytesSize": 265,
              },
              "query": {
                "hash": "1137b8a50718df4ec48060ae5cef9c8e929b619a23846c34a2535b3548b589c5",
                "raw": {
                  "fields": {
                    "block": {
                      "hash": true,
                      "number": true,
                      "timestamp": true,
                    },
                  },
                  "fromBlock": 0,
                  "parentBlockHash": undefined,
                  "toBlock": 5,
                  "type": "evm",
                },
              },
              "state": {
                "current": {
                  "hash": "0x5",
                  "number": 5,
                  "timestamp": 5000,
                },
                "initial": 0,
                "last": 5,
                "ranges": [
                  {
                    "from": 0,
                    "to": 5,
                  },
                ],
                "rollbackChain": [
                  {
                    "hash": "0x3",
                    "number": 3,
                    "timestamp": 3000,
                  },
                  {
                    "hash": "0x4",
                    "number": 4,
                    "timestamp": 4000,
                  },
                  {
                    "hash": "0x5",
                    "number": 5,
                    "timestamp": 5000,
                  },
                ],
              },
            },
          },
        ]
      `)
      expect(res2).toMatchInlineSnapshot(`
        [
          {
            "data": [
              {
                "hash": "0x1",
                "number": 1,
                "timestamp": 1000,
              },
              {
                "hash": "0x2",
                "number": 2,
                "timestamp": 2000,
              },
            ],
            "meta": {
              "head": {
                "finalized": {
                  "hash": "0x2",
                  "number": 2,
                },
                "latest": undefined,
              },
              "meta": {
                "bytesSize": 265,
              },
              "query": {
                "hash": "1137b8a50718df4ec48060ae5cef9c8e929b619a23846c34a2535b3548b589c5",
                "raw": {
                  "fields": {
                    "block": {
                      "hash": true,
                      "number": true,
                      "timestamp": true,
                    },
                  },
                  "fromBlock": 0,
                  "logs": undefined,
                  "parentBlockHash": undefined,
                  "stateDiffs": undefined,
                  "toBlock": 5,
                  "traces": undefined,
                  "transactions": undefined,
                  "type": "evm",
                },
              },
              "state": {
                "current": {
                  "hash": "0x2",
                  "number": 2,
                  "timestamp": 2000,
                },
                "initial": 0,
                "last": 5,
                "ranges": [
                  {
                    "from": 0,
                    "to": 5,
                  },
                ],
                "rollbackChain": [],
              },
            },
          },
          {
            "data": [
              {
                "hash": "0x3",
                "number": 3,
                "timestamp": 3000,
              },
              {
                "hash": "0x4",
                "number": 4,
                "timestamp": 4000,
              },
              {
                "hash": "0x5",
                "number": 5,
                "timestamp": 5000,
              },
            ],
            "meta": {
              "head": {
                "finalized": {
                  "hash": "0x5",
                  "number": 5,
                },
                "latest": undefined,
              },
              "meta": {
                "bytesSize": 159,
              },
              "query": {
                "hash": "1137b8a50718df4ec48060ae5cef9c8e929b619a23846c34a2535b3548b589c5",
                "raw": {
                  "fields": {
                    "block": {
                      "hash": true,
                      "number": true,
                      "timestamp": true,
                    },
                  },
                  "fromBlock": 0,
                  "logs": undefined,
                  "parentBlockHash": undefined,
                  "stateDiffs": undefined,
                  "toBlock": 5,
                  "traces": undefined,
                  "transactions": undefined,
                  "type": "evm",
                },
              },
              "state": {
                "current": {
                  "hash": "0x5",
                  "number": 5,
                  "timestamp": 5000,
                },
                "initial": 0,
                "last": 5,
                "ranges": [
                  {
                    "from": 0,
                    "to": 5,
                  },
                ],
                "rollbackChain": [],
              },
            },
          },
        ]
      `)

      // check stored rows by query hash
      const rows = await readAllChunks(
        cache.stream({
          queryHash: '1137b8a50718df4ec48060ae5cef9c8e929b619a23846c34a2535b3548b589c5',
          fromBlock: 0,
        }),
      )

      expect(rows).toHaveLength(2)
    })

    it('forwards the finalized option to the portal when the target requires it', async () => {
      // The mock serves /finalized-stream ONLY, so the cache's own portal call — the one leg the
      // client-side proxy does not cover — has to carry the flag or this 404s. The second pass then
      // replays the same blocks from the cache without touching the portal at all.
      portal = await finalizedMockPortal([
        {
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
          ],
          head: { finalized: { number: 2, hash: '0x2' } },
        },
      ])

      const cache = portalSqliteCache({ path: DB_PATH })
      const seen: number[][] = []

      const run = () =>
        evmStream({
          id: 'test',
          source: { url: portal.url, finalized: false },
          logger: 'silent',
          outputs: blockDecoder({ from: 0, to: 2 }).pipe((d) => d.map((b) => ({ blockNumber: b.number }))),
          cache,
        }).pipeTo(
          createMemoryTarget({
            onData: (data) => {
              seen.push(data.map((b) => b.blockNumber))
            },
          }),
        )

      await run()
      // The single mocked response is spent; a second portal request would 500.
      await run()

      expect(seen).toEqual([
        [1, 2],
        [1, 2],
      ])
    })
  })
})
