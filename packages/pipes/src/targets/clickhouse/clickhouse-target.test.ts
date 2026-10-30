import { randomBytes } from 'node:crypto'

import { type ClickHouseClient, createClient } from '@clickhouse/client'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { BlockCursor } from '~/core/index.js'
import { evmPortalStream } from '~/evm/index.js'
import { MockChainPortal, MockPortal, MockResponse, blockDecoder, mockPortal, testLogger } from '~/testing/index.js'
import { withPortal } from '~/testing/portal-fixture.js'

import { ClickhouseStore, type RollbackCursor } from './clickhouse-store.js'
import { clickhouseTarget } from './clickouse-target.js'

const client = createClient({
  url: process.env['TEST_CLICKHOUSE_URL'] || 'http://localhost:10123',
  username: process.env['TEST_CLICKHOUSE_USERNAME'] || 'default',
  password: process.env['TEST_CLICKHOUSE_PASSWORD'] || 'default',
  clickhouse_settings: {
    date_time_output_format: 'iso',
  },
})

async function getAllFromSyncTable() {
  const res = await client.query({
    query: 'SELECT * EXCEPT timestamp FROM sync FINAL ORDER BY timestamp ASC',
    format: 'JSONEachRow',
  })
  return await res.json()
}

describe('Clickhouse state', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
    await client.close()
  })

  beforeEach(async () => {
    // Ensure DB is empty before each test
    await client.query({ query: 'DROP DATABASE IF EXISTS default SYNC' })
    await client.query({ query: 'CREATE DATABASE default' })
  })

  describe('progress table', () => {
    it('should store unfinalized blocks to the lastest offset', async () => {
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
        },
      ])

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 5 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          settings: {
            table: 'sync',
          },
          onData: ({ data }) => {
            // console.log(data)
          },
        }),
      )

      const data = await getAllFromSyncTable()
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "current": "{"kind":"start","number":-1,"batchEnd":5}",
            "finalized": "",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
          {
            "current": "{"number":5,"hash":"0x5","timestamp":5000}",
            "finalized": "{"hash":"0x2","number":2}",
            "id": "test",
            "rollback_chain": "[{"number":3,"hash":"0x3","timestamp":3000},{"number":4,"hash":"0x4","timestamp":4000},{"number":5,"hash":"0x5","timestamp":5000}]",
            "sign": 1,
          },
        ]
      `)
    })

    it('should keep 10,000 rows in status table by default', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
          head: { finalized: { number: 1000, hash: '0x1000' } },
        },
        {
          statusCode: 200,
          data: [{ header: { number: 2, hash: '0x2', timestamp: 2000 } }],
          head: { finalized: { number: 1000, hash: '0x1000' } },
        },
        {
          statusCode: 200,
          data: [{ header: { number: 3, hash: '0x3', timestamp: 3000 } }],
          head: { finalized: { number: 1000, hash: '0x1000' } },
        },
      ])

      await evmPortalStream({
        id: 'test',
        portal: {
          url: portal.url,
          // we need to save each response separately
          // to create multiple rows in the status table,
          // so, we set maxBytes to 1 to avoid batching
          maxBytes: 1,
        },
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          settings: {
            table: 'sync',
          },
          onData: () => {},
        }),
      )

      const data = await getAllFromSyncTable()
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "current": "{"kind":"start","number":-1,"batchEnd":1}",
            "finalized": "",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
          {
            "current": "{"number":1,"hash":"0x1","timestamp":1000}",
            "finalized": "{"hash":"0x1000","number":1000}",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
          {
            "current": "{"number":2,"hash":"0x2","timestamp":2000}",
            "finalized": "{"hash":"0x1000","number":1000}",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
          {
            "current": "{"number":3,"hash":"0x3","timestamp":3000}",
            "finalized": "{"hash":"0x1000","number":1000}",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
        ]
      `)
    })

    it('should keep only 1 row in status table', async () => {
      portal = await mockPortal([
        { statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] },
        { statusCode: 200, data: [{ header: { number: 2, hash: '0x2', timestamp: 2000 } }] },
        { statusCode: 200, data: [{ header: { number: 3, hash: '0x3', timestamp: 3000 } }] },
      ])

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          settings: {
            table: 'sync',
            maxRows: 1,
          },
          onData: () => {},
        }),
      )

      const data = await getAllFromSyncTable()
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "current": "{"number":3,"hash":"0x3","timestamp":3000}",
            "finalized": "",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
        ]
      `)
    })

    it('should debounce cleanup with default maxRows', async () => {
      // With the default maxRows (10_000) cleanup runs every 25 saves.
      // For 26 saves we expect cleanup to run exactly twice: on save #1 and save #25.
      //
      // The Vitest pool runs tests without isolation (singleFork + isolate: false),
      // so a prototype spy can catch calls from unrelated stores in other tests.
      // Filter by `instance.client === client` to count only calls on the store
      // that wraps our test client.
      const removeSpy = vi.spyOn(ClickhouseStore.prototype, 'removeAllRowsByQuery')

      const responses = Array.from(
        { length: 26 },
        (_, i): MockResponse => ({
          statusCode: 200,
          data: [{ header: { number: i + 1, hash: `0x${i + 1}`, timestamp: (i + 1) * 1000 } }],
        }),
      )
      portal = await mockPortal(responses)

      try {
        await evmPortalStream({
          id: 'test',
          portal: {
            url: portal.url,
            // force one batch per response so each triggers a saveCursor call
            maxBytes: 1,
          },
          outputs: blockDecoder({ from: 0, to: 26 }),
        }).pipeTo(
          clickhouseTarget({
            client,
            settings: { table: 'sync' },
            onData: () => {},
          }),
        )

        const callsForThisClient = removeSpy.mock.instances.reduce<number>(
          (count, instance) =>
            count + ((instance as unknown as ClickhouseStore | undefined)?.client === client ? 1 : 0),
          0,
        )
        expect(callsForThisClient).toBe(2)
      } finally {
        removeSpy.mockRestore()
      }
    })

    it('should not store chain continuity if finalized head doesnt exist', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
        },
      ])

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 1 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          settings: { table: 'sync' },
          onData: () => {},
        }),
      )

      const data = await getAllFromSyncTable()
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "current": "{"kind":"start","number":-1,"batchEnd":1}",
            "finalized": "",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
          {
            "current": "{"number":1,"hash":"0x1","timestamp":1000}",
            "finalized": "",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
        ]
      `)
    })

    it('should continue from the last block after stop', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
        },
        {
          statusCode: 200,
          data: [{ header: { number: 2, hash: '0x2', timestamp: 1000 } }],
          validateRequest: (req) => {
            expect(req).toMatchObject({
              type: 'evm',
              fromBlock: 2,
              fields: {},
              parentBlockHash: '0x1',
            })
          },
        },
      ])

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 1 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          onData: () => {},
        }),
      )

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 1, to: 2 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          onData: () => {},
        }),
      )
    })

    it('migrates a legacy "stream" cursor to the pipe id and resumes from it', async () => {
      portal = await mockPortal([
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
        },
        {
          statusCode: 200,
          data: [{ header: { number: 2, hash: '0x2', timestamp: 2000 } }],
          validateRequest: (req) => {
            // Resumes where the legacy cursor left off, not from the stream beginning.
            expect(req).toMatchObject({ fromBlock: 2, parentBlockHash: '0x1' })
          },
        },
      ])

      // An older SDK keyed every cursor by the static "stream" id — reproduce that state by
      // pinning the legacy key explicitly for the first run.
      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 1 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          settings: { id: 'stream' },
          onData: () => {},
        }),
      )

      // Restart without an explicit id: the cursor key becomes the pipe id, the legacy rows are
      // migrated to it automatically, and indexing continues from the migrated cursor.
      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 2 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          onData: () => {},
        }),
      )

      // Every surviving sync row is keyed by the pipe id — the legacy rows were re-keyed,
      // and their originals cancelled, so nothing is left under "stream".
      const rows = (await getAllFromSyncTable()) as { id: string; current: string }[]
      expect(rows.length).toBeGreaterThan(0)
      expect(rows.map((r) => r.id)).toEqual(rows.map(() => 'test'))
      expect(JSON.parse(rows.at(-1)!.current)).toMatchObject({ number: 2, hash: '0x2' })
    })

    it('should write to a non-default database when settings.database is set', async () => {
      // The ClickHouse client is connected to the default database, but we tell the
      // target to use a different one via settings.database. Previously the INSERT
      // and cleanup paths used the unqualified table name and would have written to
      // the client's default DB instead of the requested one.
      const customDb = 'pipes_custom_db'
      await client.query({ query: `DROP DATABASE IF EXISTS ${customDb} SYNC` })
      await client.query({ query: `CREATE DATABASE ${customDb}` })

      try {
        portal = await mockPortal([
          {
            statusCode: 200,
            data: [
              { header: { number: 1, hash: '0x1', timestamp: 1000 } },
              { header: { number: 2, hash: '0x2', timestamp: 2000 } },
            ],
          },
        ])

        await evmPortalStream({
          id: 'test',
          portal: portal.url,
          outputs: blockDecoder({ from: 0, to: 2 }),
        }).pipeTo(
          clickhouseTarget({
            client,
            settings: {
              database: customDb,
              table: 'sync',
            },
            onData: () => {},
          }),
        )

        // Rows must land in the custom DB...
        const customRes = await client.query({
          query: `SELECT "current" FROM "${customDb}"."sync" FINAL ORDER BY "timestamp" ASC`,
          format: 'JSONEachRow',
        })
        const customRows = await customRes.json<{ current: string }>()
        expect(customRows.length).toBeGreaterThan(0)
        expect(JSON.parse(customRows.at(-1)!.current)).toMatchObject({ number: 2, hash: '0x2' })

        // ...and the default DB must not have received a stray sync table.
        await expect(
          client.query({
            query: `SELECT count() FROM "default"."sync"`,
            format: 'JSONEachRow',
          }),
        ).rejects.toThrow(/UNKNOWN_TABLE|doesn't exist|Unknown table/i)
      } finally {
        await client.query({ query: `DROP DATABASE IF EXISTS ${customDb} SYNC` })
      }
    })
  })

  describe('forks', () => {
    beforeEach(async () => {
      await client.query({ query: 'DROP TABLE IF EXISTS test' })
      await client.query({
        query: `
            CREATE TABLE IF NOT EXISTS test
            (
                block_number     Int32,
                block_hash       String,
                sign             Int8
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY (block_number, block_hash)
        `,
      })
    })

    it('should handle simple fork', async () => {
      portal = await mockPortal([
        {
          // 1. The First response is okay, it gets 5 blocks
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5', timestamp: 5000 } },
          ],
          head: {
            finalized: {
              number: 1,
              hash: '0x1',
            },
          },
        },

        {
          // 2. A reorg for 2 blocks happens
          statusCode: 409,
          data: {
            previousBlocks: [
              // Unforked blocks
              { number: 2, hash: '0x2' },
              { number: 3, hash: '0x3' },
              // Forked blocks
              { number: 4, hash: '0x4-1' },
              { number: 5, hash: '0x5-1' },
            ],
          },
          validateRequest: (req) => {
            // Request should include block 6 and hash for the previous block
            expect(req).toMatchInlineSnapshot(`
              {
                "fields": {
                  "block": {
                    "hash": true,
                    "number": true,
                    "timestamp": true,
                  },
                },
                "fromBlock": 6,
                "parentBlockHash": "0x5",
                "toBlock": 7,
                "type": "evm",
              }
            `)
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
            { header: { number: 6, hash: '0x6a', timestamp: 6000 } },
            { header: { number: 7, hash: '0x7a', timestamp: 7000 } },
          ],
          validateRequest: (req) => {
            /**
             * Request should include block 4 and hash for the last unforked block
             * which is 3 in that test
             */
            expect(req).toMatchInlineSnapshot(`
              {
                "fields": {
                  "block": {
                    "hash": true,
                    "number": true,
                    "timestamp": true,
                  },
                },
                "fromBlock": 4,
                "parentBlockHash": "0x3",
                "toBlock": 7,
                "type": "evm",
              }
            `)
          },
        },
      ])

      let rollbackCalls = 0
      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 7 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          onData: async ({ store, data }) => {
            await store.insert({
              table: 'test',
              values: data.map((b) => ({
                block_number: b.number,
                block_hash: b.hash,
                sign: 1,
              })),
              format: 'JSONEachRow',
            })
          },
          onRollback: async ({ store, safeCursor }) => {
            rollbackCalls++
            expect(safeCursor).toMatchObject({ number: 3, hash: '0x3' })
            await store.removeAllRows({
              tables: 'test',
              where: `block_number > {latest:UInt32}`,
              params: { latest: safeCursor.number },
            })
          },
        }),
      )

      expect(rollbackCalls).toEqual(1)

      const res = await client.query({
        query: 'SELECT * FROM test FINAL ORDER BY block_number ASC',
        format: 'JSONEachRow',
      })
      const data = await res.json()

      expect(data).toMatchInlineSnapshot(`
          [
            {
              "block_hash": "0x1",
              "block_number": 1,
              "sign": 1,
            },
            {
              "block_hash": "0x2",
              "block_number": 2,
              "sign": 1,
            },
            {
              "block_hash": "0x3",
              "block_number": 3,
              "sign": 1,
            },
            {
              "block_hash": "0x4a",
              "block_number": 4,
              "sign": 1,
            },
            {
              "block_hash": "0x5a",
              "block_number": 5,
              "sign": 1,
            },
            {
              "block_hash": "0x6a",
              "block_number": 6,
              "sign": 1,
            },
            {
              "block_hash": "0x7a",
              "block_number": 7,
              "sign": 1,
            },
          ]
        `)
    })

    it('should handle fork with missing finalized block in stream', async () => {
      portal = await mockPortal([
        {
          // 1. The First response is okay, it gets 5 blocks
          statusCode: 200,
          data: [
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5', timestamp: 5000 } },
          ],
          head: {
            finalized: {
              number: 1,
              hash: '0x1',
            },
          },
        },

        {
          // 2. A reorg for 2 blocks happens
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 4, hash: '0x4a' },
              { number: 5, hash: '0x5a' },
            ],
          },
          validateRequest: (req) => {
            // Request should include block 6 and hash for the previous block
            expect(req).toMatchInlineSnapshot(`
              {
                "fields": {
                  "block": {
                    "hash": true,
                    "number": true,
                    "timestamp": true,
                  },
                },
                "fromBlock": 6,
                "parentBlockHash": "0x5",
                "toBlock": 7,
                "type": "evm",
              }
            `)
          },
        },
        {
          // 2. A reorg for 2 blocks happens
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 1, hash: '0x1' },
              { number: 2, hash: '0x2a' },
              { number: 3, hash: '0x3a' },
            ],
          },
          validateRequest: (req) => {
            // Request should include block 6 and hash for the previous block
            expect(req).toMatchInlineSnapshot(`
              {
                "fields": {
                  "block": {
                    "hash": true,
                    "number": true,
                    "timestamp": true,
                  },
                },
                "fromBlock": 4,
                "parentBlockHash": "0x3",
                "toBlock": 7,
                "type": "evm",
              }
            `)
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 2, hash: '0x2a', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3a', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
            { header: { number: 6, hash: '0x6a', timestamp: 6000 } },
            { header: { number: 7, hash: '0x7a', timestamp: 7000 } },
          ],
          head: { finalized: { number: 4, hash: '0x4a' } },
          validateRequest: (req) => {
            /**
             * Request should include block 4 and hash for the last unforked block
             * which is 3 in that test
             */
            expect(req).toMatchInlineSnapshot(`
              {
                "fields": {
                  "block": {
                    "hash": true,
                    "number": true,
                    "timestamp": true,
                  },
                },
                "fromBlock": 2,
                "parentBlockHash": "0x1",
                "toBlock": 7,
                "type": "evm",
              }
            `)
          },
        },
      ])

      let rollbackCalls = 0

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 7 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          onData: async ({ store, data }) => {
            await store.insert({
              table: 'test',
              values: data.map((b) => ({
                block_number: b.number,
                block_hash: b.hash,
                sign: 1,
              })),
              format: 'JSONEachRow',
            })
          },
          onRollback: async ({ reason, store, safeCursor }) => {
            if (rollbackCalls === 0) {
              expect(safeCursor).toMatchObject({ number: 3, hash: '0x3' })
            } else {
              expect(safeCursor).toMatchObject({ number: 1, hash: '0x1' })
            }

            rollbackCalls++
            await store.removeAllRows({
              tables: 'test',
              where: `block_number > {latest:UInt32}`,
              params: { latest: safeCursor.number },
            })
          },
        }),
      )

      const res = await client.query({
        query: 'SELECT * FROM test FINAL ORDER BY block_number ASC',
        format: 'JSONEachRow',
      })
      const data = await res.json()

      expect(data).toMatchInlineSnapshot(`
        [
          {
            "block_hash": "0x2a",
            "block_number": 2,
            "sign": 1,
          },
          {
            "block_hash": "0x3a",
            "block_number": 3,
            "sign": 1,
          },
          {
            "block_hash": "0x4a",
            "block_number": 4,
            "sign": 1,
          },
          {
            "block_hash": "0x5a",
            "block_number": 5,
            "sign": 1,
          },
          {
            "block_hash": "0x6a",
            "block_number": 6,
            "sign": 1,
          },
          {
            "block_hash": "0x7a",
            "block_number": 7,
            "sign": 1,
          },
        ]
      `)
    })

    it('should handle fork up to last finalized block', async () => {
      portal = await mockPortal([
        {
          // 1. The First response is okay, it gets 5 blocks
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5', timestamp: 5000 } },
          ],
          head: { finalized: { number: 1, hash: '0x1' } },
        },
        {
          // 2. A deep reorg happens
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 1, hash: '0x1' },
              // Forked blocks
              { number: 2, hash: '0x2a' },
              { number: 3, hash: '0x3a' },
              { number: 4, hash: '0x4a' },
              { number: 5, hash: '0x5a' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 2, hash: '0x2a', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3a', timestamp: 3000 } },
          ],
          head: { finalized: { number: 2, hash: '0x2a' } },
        },
        // we mock 2 responses here as the first will fail
        ...new Array(2).fill({
          statusCode: 200,
          data: [
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
            { header: { number: 6, hash: '0x6a', timestamp: 6000 } },
            { header: { number: 7, hash: '0x7a', timestamp: 7000 } },
          ],
          head: { finalized: { number: 4, hash: '0x4a' } },
          validateRequest: (req: any) => {
            expect(req).toMatchObject({
              type: 'evm',
              fromBlock: 4,
              parentBlockHash: '0x3a',
            })
          },
        }),
      ])

      let finished = false
      let crashes = 0

      while (!finished) {
        try {
          await evmPortalStream({
            id: 'test',
            portal: portal.url,
            outputs: blockDecoder({ from: 0, to: 7 }),
          }).pipeTo(
            clickhouseTarget({
              client,
              onData: async ({ data }) => {
                if (data[0].hash === '0x4-1' && crashes === 0) {
                  throw new Error('process failed')
                }
                finished = true
              },
              onRollback: async ({ store, safeCursor }) => {
                await store.removeAllRows({
                  tables: 'test',
                  where: `block_number > {latest:UInt32}`,
                  params: { latest: safeCursor.number },
                })
              },
            }),
          )
        } catch (error) {
          if (error instanceof Error && error.message === 'process failed') {
            crashes++
          } else {
            throw error
          }
        }
      }

      const data = await getAllFromSyncTable()
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "current": "{"kind":"start","number":-1,"batchEnd":5}",
            "finalized": "",
            "id": "test",
            "rollback_chain": "[]",
            "sign": 1,
          },
          {
            "current": "{"number":5,"hash":"0x5","timestamp":5000}",
            "finalized": "{"hash":"0x1","number":1}",
            "id": "test",
            "rollback_chain": "[{"number":2,"hash":"0x2","timestamp":2000},{"number":3,"hash":"0x3","timestamp":3000},{"number":4,"hash":"0x4","timestamp":4000},{"number":5,"hash":"0x5","timestamp":5000}]",
            "sign": 1,
          },
          {
            "current": "{"number":3,"hash":"0x3a","timestamp":3000}",
            "finalized": "{"hash":"0x2a","number":2}",
            "id": "test",
            "rollback_chain": "[{"number":3,"hash":"0x3a","timestamp":3000}]",
            "sign": 1,
          },
          {
            "current": "{"number":7,"hash":"0x7a","timestamp":7000}",
            "finalized": "{"hash":"0x4a","number":4}",
            "id": "test",
            "rollback_chain": "[{"number":5,"hash":"0x5a","timestamp":5000},{"number":6,"hash":"0x6a","timestamp":6000},{"number":7,"hash":"0x7a","timestamp":7000}]",
            "sign": 1,
          },
        ]
      `)
    })

    it('should handle deep fork', async () => {
      portal = await mockPortal([
        {
          // 1. The First response is okay, it gets 5 blocks
          statusCode: 200,
          data: [
            { header: { number: 1, hash: '0x1', timestamp: 1000 } },
            { header: { number: 2, hash: '0x2', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5', timestamp: 5000 } },
          ],
          head: {
            finalized: {
              number: 1,
              hash: '0x1',
            },
          },
        },

        {
          // 2. A reorg for 2 blocks happens
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 4, hash: '0x4a' },
              { number: 5, hash: '0x5a' },
            ],
          },
          validateRequest: (req) => {
            // Request should include block 6 and hash for the previous block
            expect(req).toMatchInlineSnapshot(`
              {
                "fields": {
                  "block": {
                    "hash": true,
                    "number": true,
                    "timestamp": true,
                  },
                },
                "fromBlock": 6,
                "parentBlockHash": "0x5",
                "toBlock": 7,
                "type": "evm",
              }
            `)
          },
        },
        {
          // 2. A reorg for 2 blocks happens
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 1, hash: '0x1' },
              { number: 2, hash: '0x2a' },
              { number: 3, hash: '0x3a' },
            ],
          },
          validateRequest: (req) => {
            // Request should include block 6 and hash for the previous block
            expect(req).toMatchInlineSnapshot(`
              {
                "fields": {
                  "block": {
                    "hash": true,
                    "number": true,
                    "timestamp": true,
                  },
                },
                "fromBlock": 4,
                "parentBlockHash": "0x3",
                "toBlock": 7,
                "type": "evm",
              }
            `)
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 2, hash: '0x2a', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3a', timestamp: 3000 } },
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
            { header: { number: 6, hash: '0x6a', timestamp: 6000 } },
            { header: { number: 7, hash: '0x7a', timestamp: 7000 } },
          ],
          validateRequest: (req) => {
            /**
             * Request should include block 4 and hash for the last unforked block
             * which is 3 in that test
             */
            expect(req).toMatchInlineSnapshot(`
              {
                "fields": {
                  "block": {
                    "hash": true,
                    "number": true,
                    "timestamp": true,
                  },
                },
                "fromBlock": 2,
                "parentBlockHash": "0x1",
                "toBlock": 7,
                "type": "evm",
              }
            `)
          },
        },
      ])

      let rollbackCalls = 0

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 7 }),
      }).pipeTo(
        clickhouseTarget({
          client,
          onData: async ({ store, data }) => {
            await store.insert({
              table: 'test',
              values: data.map((b) => ({
                block_number: b.number,
                block_hash: b.hash,
                sign: 1,
              })),
              format: 'JSONEachRow',
            })
          },
          onRollback: async ({ reason, store, safeCursor }) => {
            if (rollbackCalls === 0) {
              expect(safeCursor).toMatchObject({ number: 3, hash: '0x3' })
            } else {
              expect(safeCursor).toMatchObject({ number: 1, hash: '0x1' })
            }

            rollbackCalls++
            await store.removeAllRows({
              tables: 'test',
              where: `block_number > {latest:UInt32}`,
              params: { latest: safeCursor.number },
            })
          },
        }),
      )

      const res = await client.query({
        query: 'SELECT * FROM test FINAL ORDER BY block_number ASC',
        format: 'JSONEachRow',
      })
      const data = await res.json()

      expect(data).toMatchInlineSnapshot(`
        [
          {
            "block_hash": "0x1",
            "block_number": 1,
            "sign": 1,
          },
          {
            "block_hash": "0x2a",
            "block_number": 2,
            "sign": 1,
          },
          {
            "block_hash": "0x3a",
            "block_number": 3,
            "sign": 1,
          },
          {
            "block_hash": "0x4a",
            "block_number": 4,
            "sign": 1,
          },
          {
            "block_hash": "0x5a",
            "block_number": 5,
            "sign": 1,
          },
          {
            "block_hash": "0x6a",
            "block_number": 6,
            "sign": 1,
          },
          {
            "block_hash": "0x7a",
            "block_number": 7,
            "sign": 1,
          },
        ]
      `)
    })
  })
})

describe('clickhouseTarget missing-rollback diagnostic', () => {
  // Enough of a client for write() to reach the end: the first cursor read reports a missing
  // table, which getCursor answers by creating it and returning no cursor.
  function stubClient() {
    return {
      connectionParams: { database: 'default' },
      query: vi.fn().mockRejectedValue(Object.assign(new Error('no table'), { type: 'UNKNOWN_TABLE' })),
      command: vi.fn().mockResolvedValue(undefined),
      insert: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    } as any
  }

  async function run({ finalized, onRollback }: { finalized: boolean; onRollback?: () => void }) {
    const logger = testLogger()
    const warn = vi.spyOn(logger, 'warn')

    const target = clickhouseTarget<unknown>({ client: stubClient(), onData: async () => {}, onRollback })

    await target.write({
      id: 'test-pipe',
      finalized,
      logger,
      read: async function* () {},
    })

    return warn.mock.calls.flat().join(' ')
  }

  it('warns and flags the fork risk on the hot stream when no rollback handler is configured', async () => {
    const warning = await run({ finalized: false })
    expect(warning).toContain('No onRollback handler is configured')
    expect(warning).toContain('chain fork')
  })

  it('warns about the recovery window on a finalized stream too, but omits the fork note', async () => {
    // The recovery crash window is stream-independent — an unclean restart re-delivers rows on a
    // finalized stream as well. Only the fork half is hot-stream-specific.
    const warning = await run({ finalized: true })
    expect(warning).toContain('No onRollback handler is configured')
    expect(warning).not.toContain('chain fork')
  })

  it('stays silent when a rollback handler is configured', async () => {
    expect(await run({ finalized: false, onRollback: () => {} })).not.toContain('No onRollback handler is configured')
  })

  it('fails a fork with a coded error when no rollback handler is configured', async () => {
    const target = clickhouseTarget<unknown>({ client: stubClient(), onData: async () => {} })

    await expect(target.resolveFork?.([{ number: 10, hash: '0xa' }])).rejects.toMatchObject({
      code: 'E2007',
    })
  })

  it('passes the fork ancestor with no upper bound when the run has saved no cursor', async () => {
    // Rollback records that resolve the fork to block 5, read without write() ever running
    const row = {
      rollback_chain: JSON.stringify([5, 6].map((n) => ({ number: n, hash: `0x${n}` }))),
      finalized: '{"number":4,"hash":"0x4"}',
    }
    const recordsClient = {
      connectionParams: { database: 'default' },
      query: vi.fn().mockResolvedValue({
        stream: async function* () {
          yield [{ json: () => row }]
        },
      }),
      close: vi.fn().mockResolvedValue(undefined),
    } as any
    const onRollback = vi.fn()

    const target = clickhouseTarget<unknown>({ client: recordsClient, onData: async () => {}, onRollback })
    await target.resolveFork?.([
      { number: 5, hash: '0x5' },
      { number: 6, hash: '0x6a' },
    ])

    expect(onRollback).toHaveBeenCalledWith(expect.objectContaining({ safeCursor: { number: 5, hash: '0x5' } }))
  })

  it('does not preempt a fork with the coded error when a handler is present', async () => {
    const onRollback = vi.fn()
    // A handler is present, so resolveFork proceeds to resolve the fork cursor. With no stored
    // rollback records it resolves to a dead-end (null) — not the E2007 refusal, and the handler
    // is never reached. The point is only that E2007 is not thrown here.
    const emptyRecordsClient = {
      connectionParams: { database: 'default' },
      query: vi.fn().mockResolvedValue({ stream: async function* () {} }),
      command: vi.fn().mockResolvedValue(undefined),
      insert: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    } as any

    const target = clickhouseTarget<unknown>({
      client: emptyRecordsClient,
      onData: async () => {},
      onRollback,
    })

    await expect(target.resolveFork?.([{ number: 10, hash: '0xa' }])).resolves.toBeNull()
    expect(onRollback).not.toHaveBeenCalled()
  })
})

describe('clickhouseTarget onRollback bounds', () => {
  const server = {
    url: process.env['TEST_CLICKHOUSE_URL'] || 'http://localhost:10123',
    username: process.env['TEST_CLICKHOUSE_USERNAME'] || 'default',
    password: process.env['TEST_CLICKHOUSE_PASSWORD'] || 'default',
  }

  type Database = { name: string; admin: ClickHouseClient }
  type Block = { number: number }
  type OnData = (ctx: { store: ClickhouseStore; data: Block[] }) => Promise<void>
  type OnRollback = (ctx: {
    reason: 'recovery' | 'fork'
    store: ClickhouseStore
    safeCursor: RollbackCursor
  }) => Promise<void>

  /**
   * A database of the test's own, with the one table the pipe writes: a row per block. Merges are
   * stopped on it, so a block stored twice reads as twice.
   */
  const withDatabase = withPortal.extend<{ database: Database }>({
    database: async ({}, use) => {
      const admin = createClient(server)
      const name = `first_batch_${randomBytes(6).toString('hex')}`
      await admin.command({ query: `CREATE DATABASE ${name}` })
      await admin.command({
        query: `CREATE TABLE ${name}.blocks (block_number UInt64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY block_number`,
      })
      await admin.command({ query: `SYSTEM STOP MERGES ${name}.blocks` })

      await use({ name, admin })

      await admin.command({ query: `DROP DATABASE ${name} SYNC` })
      await admin.close()
    },
  })

  const insertBlocks: OnData = async ({ store, data }) => {
    const values = data.map((block) => ({ block_number: block.number, sign: 1 }))

    await store.insert({ table: 'blocks', values, format: 'JSONEachRow' })
  }

  const removeAbove: OnRollback = async ({ store, safeCursor }) => {
    await store.removeRowsAfter(safeCursor, { tables: ['blocks'] })
  }

  const removeAboveByPredicate: OnRollback = async ({ store, safeCursor }) => {
    await store.removeAllRows({
      tables: ['blocks'],
      where: 'block_number > {latest:UInt32}',
      params: { latest: safeCursor.number },
    })
  }

  /**
   * Streams blocks 0 to `to`, a portal response of up to four blocks per batch. A response carries
   * the first and last block it covers, and the pipe stores a row for each: 0 and 3, 4 and 7, 8 and 9.
   */
  async function runPipe({
    portal,
    database,
    to,
    onData = insertBlocks,
    onRollback,
  }: {
    portal: MockChainPortal
    database: Database
    to: number
    onData?: OnData
    onRollback?: OnRollback
  }) {
    const client = createClient({ ...server, database: database.name })

    try {
      await evmPortalStream({
        id: 'first-batch',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to }),
      }).pipeTo(clickhouseTarget({ client, onData, onRollback }))
    } finally {
      await client.close()
    }
  }

  /** Stores the first batch's rows, then stops before its cursor. */
  async function stopInFirstBatch(portal: MockChainPortal, database: Database, onRollback?: OnRollback) {
    const stopped = new Error('stopped after the first batch stored its rows')
    const storeThenStop: OnData = async (ctx) => {
      await insertBlocks(ctx)
      throw stopped
    }

    await expect(runPipe({ portal, database, to: 7, onData: storeThenStop, onRollback })).rejects.toBe(stopped)
  }

  /** The start cursors the sync table holds net of their cancel rows. */
  async function startCursors({ name, admin }: Database): Promise<string[]> {
    const result = await admin.query({
      query: `SELECT current FROM ${name}.sync WHERE id = 'first-batch'
        GROUP BY current, finalized, rollback_chain, timestamp HAVING sum(sign) > 0 ORDER BY timestamp`,
      format: 'JSONEachRow',
    })
    const rows = await result.json<{ current: string }>()

    return rows.map((row) => row.current)
  }

  /** The stored blocks, each as many times as it is stored net of its cancel rows. */
  async function storedBlocks({ name, admin }: Database): Promise<number[]> {
    const result = await admin.query({
      query: `SELECT block_number, sum(sign) AS net FROM ${name}.blocks GROUP BY block_number HAVING net != 0 ORDER BY block_number`,
      format: 'JSONEachRow',
    })
    const rows = await result.json<{ block_number: string; net: string }>()

    return rows.flatMap((row) => Array<number>(Number(row.net)).fill(Number(row.block_number)))
  }

  withDatabase(
    'calls the hook with the start cursor, and removeRowsAfter removes the batch blocks only',
    async ({ portal, database }) => {
      const chain = await portal({ latest: 9, blocksPerResponse: 4 })
      // Another pipe's row past the first batch, which a removal without a pipe filter must not reach
      await database.admin.insert({
        table: `${database.name}.blocks`,
        values: [{ block_number: 100, sign: 1 }],
        format: 'JSONEachRow',
      })

      await stopInFirstBatch(chain, database)
      const onRollback = vi.fn(removeAbove)
      await runPipe({ portal: chain, database, to: 7, onRollback })
      const safeCursors = onRollback.mock.calls.map(([ctx]) => ctx.safeCursor)

      expect(safeCursors).toEqual([{ number: -1, upTo: 3 }])
      expect(await storedBlocks(database)).toEqual([0, 3, 4, 7, 100])
    },
  )

  withDatabase(
    'replaces the start cursor when the replayed first batch stops again, ending later',
    async ({ portal, database }) => {
      const short = await portal({ latest: 9, blocksPerResponse: 4 })
      const long = await portal({ latest: 9, blocksPerResponse: 8 })

      // The first run stores 0 and 3; the second removes them, then stores 0 and 7
      await stopInFirstBatch(short, database)
      await stopInFirstBatch(long, database, removeAbove)
      const left = await startCursors(database)
      const onRollback = vi.fn(removeAbove)
      await runPipe({ portal: short, database, to: 7, onRollback })
      const safeCursors = onRollback.mock.calls.map(([ctx]) => ctx.safeCursor)

      expect(left).toEqual(['{"kind":"start","number":-1,"batchEnd":7}'])
      expect(safeCursors).toEqual([{ number: -1, upTo: 7 }])
      expect(await storedBlocks(database)).toEqual([0, 3, 4, 7])
    },
  )

  withDatabase(
    'fails the restart at a range start of 0 when the hook binds the cursor as unsigned, removing nothing',
    async ({ portal, database }) => {
      const chain = await portal({ latest: 9, blocksPerResponse: 4 })

      await stopInFirstBatch(chain, database)
      const restart = runPipe({ portal: chain, database, to: 7, onRollback: removeAboveByPredicate })

      await expect(restart).rejects.toThrow("Value -1 cannot be parsed as UInt32 for query parameter 'latest'")
      expect(await storedBlocks(database)).toEqual([0, 3])
    },
  )

  withDatabase(
    'on a fork, removeRowsAfter stops at the cursor the fork rolls back from',
    async ({ portal, database }) => {
      let forked = false
      const chain: MockChainPortal = await portal({
        latest: 12,
        finalized: 2,
        blocksPerResponse: 4,
        // Once the pipe asks past 7, every block above 5 is replaced
        delay: ({ query }) => {
          if (query.fromBlock === 8 && !forked) {
            forked = true
            chain.fork(5)
          }
        },
      })
      // Another pipe's row past every block this pipe stores, which a removal without a pipe filter must not reach
      await database.admin.insert({
        table: `${database.name}.blocks`,
        values: [{ block_number: 100, sign: 1 }],
        format: 'JSONEachRow',
      })
      const onRollback = vi.fn(removeAbove)

      // Stores 0 and 3, 4 and 7 before the fork; 4 is the last block of the old branch in its
      // rollback chain. The stream then resumes from 4 and covers 5 to 8 and 9 to 12.
      await runPipe({ portal: chain, database, to: 12, onRollback })
      const safeCursors = onRollback.mock.calls.map(([ctx]) => ({ reason: ctx.reason, ...ctx.safeCursor }))

      expect(safeCursors).toEqual([expect.objectContaining({ reason: 'fork', number: 4, upTo: 7 })])
      expect(await storedBlocks(database)).toEqual([0, 3, 4, 5, 8, 9, 12, 100])
    },
  )

  withDatabase(
    'resumes over the cursor of a first batch that completed, calling the hook at it',
    async ({ portal, database }) => {
      const chain = await portal({ latest: 9, blocksPerResponse: 4 })
      const onRollback = vi.fn(removeAbove)

      await runPipe({ portal: chain, database, to: 5, onRollback })
      await runPipe({ portal: chain, database, to: 9, onRollback })
      const safeCursors = onRollback.mock.calls.map(([ctx]) => ctx.safeCursor.number)

      // The first run's responses cover 0 to 3 and 4 to 5, the second's 6 to 9
      expect(safeCursors).toEqual([5])
      expect(await storedBlocks(database)).toEqual([0, 3, 4, 5, 6, 9])
    },
  )
})

describe('clickhouseTarget cursor order', () => {
  const server = {
    url: process.env['TEST_CLICKHOUSE_URL'] || 'http://localhost:10123',
    username: process.env['TEST_CLICKHOUSE_USERNAME'] || 'default',
    password: process.env['TEST_CLICKHOUSE_PASSWORD'] || 'default',
  }

  /** A client on a database of the test's own, and a clock the test can set back. */
  const withClock = withPortal.extend<{ client: ClickHouseClient; setClockBack: (ms: number) => void }>({
    client: async ({}, use) => {
      const admin = createClient(server)
      const database = `cursor_order_${randomBytes(6).toString('hex')}`
      await admin.command({ query: `CREATE DATABASE ${database}` })
      const client = createClient({ ...server, database })

      await use(client)

      await client.close()
      await admin.command({ query: `DROP DATABASE ${database} SYNC` })
      await admin.close()
    },
    setClockBack: async ({}, use) => {
      const now = Date.now
      const clock = vi.spyOn(Date, 'now')

      await use((ms) => {
        clock.mockImplementation(() => now() - ms)
      })

      clock.mockRestore()
    },
  })

  async function runPipe(portal: MockChainPortal, client: ClickHouseClient, to: number) {
    await evmPortalStream({
      id: 'cursor-order',
      portal: portal.url,
      outputs: blockDecoder({ from: 0, to }),
    }).pipeTo(clickhouseTarget({ client, onData: () => {} }))
  }

  withClock(
    'resumes from the last commit when the clock went back across a restart',
    async ({ portal, client, setClockBack }) => {
      const chain = await portal({ latest: 3 })

      await runPipe(chain, client, 1)
      setClockBack(60 * 60 * 1000)
      await runPipe(chain, client, 2)
      const thirdRun = chain.requests.length
      await runPipe(chain, client, 3)

      expect(chain.requests[thirdRun].query.fromBlock).toBe(3)
    },
  )
})
