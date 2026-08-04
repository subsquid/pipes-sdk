import { createClient } from '@clickhouse/client'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { evmPortalStream } from '~/evm/index.js'
import { MockPortal, MockResponse, blockDecoder, mockPortal, testLogger } from '~/testing/index.js'

import { ClickhouseStore } from './clickhouse-store.js'
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
      // Two offsets: the batch that delivered 3..5 as unfinalized, then the finality catch-up a
      // bounded stream waits for before ending (the mock portal finalizes everything it served).
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "current": "{"number":5,"hash":"0x5","timestamp":5000}",
            "finalized": "{"hash":"0x2","number":2}",
            "id": "test",
            "rollback_chain": "[{"number":3,"hash":"0x3","timestamp":3000},{"number":4,"hash":"0x4","timestamp":4000},{"number":5,"hash":"0x5","timestamp":5000}]",
            "sign": 1,
          },
          {
            "current": "{"number":5,"hash":"0x5","timestamp":5000}",
            "finalized": "{"number":5,"hash":"0x5"}",
            "id": "test",
            "rollback_chain": "[]",
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
          {
            "current": "{"number":7,"hash":"0x7a","timestamp":7000}",
            "finalized": "{"number":7,"hash":"0x7a"}",
            "id": "test",
            "rollback_chain": "[]",
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
