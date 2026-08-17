import { eq, sql } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/node-postgres'
import { integer, jsonb, numeric, pgTable, timestamp, varchar } from 'drizzle-orm/pg-core'
import { Pool, QueryResultRow } from 'pg'
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'

import { evmPortalStream } from '~/evm/index.js'
import { MockPortal, MockResponse, blockDecoder, mockPortal } from '~/testing/index.js'

import { drizzleTarget } from './index.js'

const dsnUrl = process.env['TEST_POSTGRES_DSN'] || `postgresql://postgres:postgres@localhost:5432/postgres`
const pool = new Pool({
  connectionString: dsnUrl,
})

async function execute<T extends QueryResultRow>(query: string, params: any[] = []) {
  const client = await pool.connect()
  const res = await client.query<T>(query, params)
  client.release()

  return res
}

async function getAllFromSyncTable(schema = 'public') {
  const res = await execute(`SELECT * FROM "${schema}"."sync" ORDER BY current_number ASC`)

  return res.rows
}

/** Blocks 1 and 2 written at the finalized head, then block 2 forks and 2a/3a replace it. */
function forkAtBlockTwo(): MockResponse[] {
  return [
    ...[1, 2].map(
      (block): MockResponse => ({
        statusCode: 200,
        data: [{ header: { number: block, hash: `0x${block}`, timestamp: block * 1000 } }],
        head: { finalized: { number: 1, hash: '0x1' } },
      }),
    ),
    {
      // Forks block 2; the safe ancestor is the finalized block 1.
      statusCode: 409,
      data: {
        previousBlocks: [
          { number: 1, hash: '0x1' },
          { number: 2, hash: '0x2a' },
        ],
      },
    },
    {
      statusCode: 200,
      data: [
        { header: { number: 2, hash: '0x2a', timestamp: 2000 } },
        { header: { number: 3, hash: '0x3a', timestamp: 3000 } },
      ],
      head: { finalized: { number: 1, hash: '0x1' } },
    },
  ]
}

describe('Drizzle target', () => {
  let portal: MockPortal
  const db = drizzle(pool)

  afterEach(async () => {
    await portal?.close()
  })
  afterAll(async () => {
    await pool.end()
  })

  describe('common', () => {
    const testTable = pgTable('test', {
      id: integer().primaryKey(),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;
        CREATE SCHEMA IF NOT EXISTS "public";
      `)
    })

    it('should throw an error if table is not tracked', async () => {
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

      await expect(async () => {
        await evmPortalStream({
          id: 'test',
          portal: portal.url,
          outputs: blockDecoder({ from: 0, to: 5 }),
        }).pipeTo(
          drizzleTarget({
            db,
            tables: [],
            onData: async ({ tx }) => {
              await tx.insert(testTable).values({ id: 1 })
            },
          }),
        )
      }).rejects.toThrow('Table "test" is not tracked for rollbacks')
    })
  })

  describe('state manager', () => {
    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "test" CASCADE; 
        CREATE SCHEMA IF NOT EXISTS "test";
      `)
    })

    it('should save state to custom schema', async () => {
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
        drizzleTarget({
          db,
          tables: [],
          settings: { state: { schema: 'test' } },
          onData: async () => {},
        }),
      )

      const rows = await getAllFromSyncTable('test')
      expect(rows).toMatchInlineSnapshot(`
        [
          {
            "current_hash": "0x2",
            "current_number": "2",
            "current_timestamp": 1970-01-01T00:33:20.000Z,
            "finalized": {
              "hash": "0x2",
              "number": 2,
            },
            "id": "test",
            "rollback_chain": [],
          },
          {
            "current_hash": "0x3",
            "current_number": "3",
            "current_timestamp": 1970-01-01T00:50:00.000Z,
            "finalized": {
              "hash": "0x2",
              "number": 2,
            },
            "id": "test",
            "rollback_chain": [
              {
                "hash": "0x3",
                "number": 3,
                "timestamp": 3000,
              },
            ],
          },
          {
            "current_hash": "0x4",
            "current_number": "4",
            "current_timestamp": 1970-01-01T01:06:40.000Z,
            "finalized": {
              "hash": "0x2",
              "number": 2,
            },
            "id": "test",
            "rollback_chain": [
              {
                "hash": "0x4",
                "number": 4,
                "timestamp": 4000,
              },
            ],
          },
          {
            "current_hash": "0x5",
            "current_number": "5",
            "current_timestamp": 1970-01-01T01:23:20.000Z,
            "finalized": {
              "hash": "0x2",
              "number": 2,
            },
            "id": "test",
            "rollback_chain": [
              {
                "hash": "0x5",
                "number": 5,
                "timestamp": 5000,
              },
            ],
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
        drizzleTarget({
          db,
          tables: [],
          settings: {
            state: { schema: 'test' },
          },
          onData: async () => {},
        }),
      )

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 1, to: 2 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [],
          settings: { state: { schema: 'test' } },
          onData: async () => {},
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

      // An older SDK keyed every sync row by the static "stream" id — reproduce that state by
      // pinning the legacy key explicitly for the first run.
      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 1 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [],
          settings: { state: { schema: 'test', id: 'stream' } },
          onData: async () => {},
        }),
      )

      // Restart without an explicit id: the cursor key becomes the pipe id, the legacy rows are
      // re-keyed to it in a single atomic UPDATE, and indexing continues from the migrated cursor.
      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 2 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [],
          settings: { state: { schema: 'test' } },
          onData: async () => {},
        }),
      )

      // Every surviving sync row is keyed by the pipe id — nothing is left under "stream".
      const rows = await getAllFromSyncTable('test')
      expect(rows.length).toBeGreaterThan(0)
      expect(rows.map((r) => r['id'])).toEqual(rows.map(() => 'test'))
      expect(rows.at(-1)?.['current_number']).toBe('2')
    })
  })

  describe('forks', () => {
    const testTable = pgTable('test', {
      block_number: integer().primaryKey(),
      block_hash: varchar().notNull(),
      data: varchar().default(''),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;       
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE IF NOT EXISTS test
          (
             block_number numeric,
             block_hash   text,
             data         text DEFAULT '',
             CONSTRAINT "test_pk" PRIMARY KEY("block_number")
          );
       `)
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

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 7 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [testTable],
          onBeforeRollback: () => {
            // throw new Error('STOP')
          },
          onData: async ({ tx, data }) => {
            await tx.insert(testTable).values(
              data.map((b) => ({
                block_number: b.number,
                block_hash: b.hash,
              })),
            )
          },
        }),
      )

      const res = await db.select().from(testTable).orderBy(testTable.block_number)
      expect(res).toMatchInlineSnapshot(`
        [
          {
            "block_hash": "0x1",
            "block_number": 1,
            "data": "",
          },
          {
            "block_hash": "0x2",
            "block_number": 2,
            "data": "",
          },
          {
            "block_hash": "0x3",
            "block_number": 3,
            "data": "",
          },
          {
            "block_hash": "0x4a",
            "block_number": 4,
            "data": "",
          },
          {
            "block_hash": "0x5a",
            "block_number": 5,
            "data": "",
          },
          {
            "block_hash": "0x6a",
            "block_number": 6,
            "data": "",
          },
          {
            "block_hash": "0x7a",
            "block_number": 7,
            "data": "",
          },
        ]
      `)

      // The fork drops the rolled-back sync rows above the safe cursor (block 3): the checkpoints
      // for the dead blocks 4/5 are removed, while the unforked checkpoints (finalized block 1, then
      // 2 and 3) and the reprocessed head (7) remain — so getCursor resumes from the last write (7)
      // rather than a stale higher block. Block 1 is finalized, so the portal delivers it in its own
      // batch and it keeps its own checkpoint.
      const sync = await getAllFromSyncTable()
      expect(sync.map((r) => Number(r['current_number']))).toEqual([1, 2, 3, 7])
    })

    it('should rollback updates', async () => {
      portal = await mockPortal([
        ...new Array(4).fill(null).map((_, i): MockResponse => {
          const block = i + 1
          return {
            // 1. The First response is okay, it gets 5 blocks
            statusCode: 200,
            data: [{ header: { number: block, hash: `0x${block}`, timestamp: block * 1000 } }],
            head: {
              finalized: {
                number: 1,
                hash: '0x1',
              },
            },
          }
        }),

        {
          // 2. A reorg for 2 blocks happens
          statusCode: 409,
          data: {
            previousBlocks: [
              // Unforked blocks
              { number: 2, hash: '0x2' },
              { number: 3, hash: '0x3' },
              // Forked blocks
              { number: 4, hash: '0x4a' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
          ],
        },
      ])

      let callCount = 0

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 5 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [testTable],
          onData: async ({ tx, data }) => {
            for (const item of data) {
              await tx
                .insert(testTable)
                .values({
                  block_number: -1,
                  block_hash: 'test',
                })
                .onConflictDoUpdate({
                  target: testTable.block_number,
                  set: {
                    data: JSON.stringify(item),
                  },
                })
            }
          },
          onBeforeRollback: () => {
            // throw new Error('STOP')
          },
          onAfterRollback: async ({ tx, cursor }) => {
            callCount++
            const [row] = await tx.select().from(testTable).where(eq(testTable.block_number, -1))

            expect(cursor.number).toEqual(3)
            expect(cursor.hash).toEqual('0x3')
            expect(JSON.parse(row?.data || '')).toMatchObject(cursor)
          },
        }),
      )

      expect(callCount).toEqual(1)

      const res = await db.select().from(testTable).orderBy(testTable.block_number)
      expect(res).toMatchInlineSnapshot(`
        [
          {
            "block_hash": "test",
            "block_number": -1,
            "data": "{"number":5,"hash":"0x5a","timestamp":5000}",
          },
        ]
      `)
    })

    it('should rollback deletes', async () => {
      portal = await mockPortal([
        ...new Array(4).fill(null).map((_, i): MockResponse => {
          const block = i + 1
          return {
            // 1. The First response is okay, it gets 5 blocks
            statusCode: 200,
            data: [{ header: { number: block, hash: `0x${block}`, timestamp: block * 1000 } }],
            head: {
              finalized: {
                number: 1,
                hash: '0x1',
              },
            },
          }
        }),

        {
          // 2. A reorg for 2 blocks happens
          statusCode: 409,
          data: {
            previousBlocks: [
              // Unforked blocks
              { number: 2, hash: '0x2' },
              { number: 3, hash: '0x3' },
              // Forked blocks
              { number: 4, hash: '0x4a' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
          ],
        },
      ])

      let callCount = 0
      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 5 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [testTable],
          onData: async ({ tx, data }) => {
            for (const item of data) {
              // Rollback happens to block 3
              if (item.number === 3) {
                await tx.delete(testTable).where(eq(testTable.block_number, -1))
              } else {
                await tx
                  .insert(testTable)
                  .values({
                    block_number: -1,
                    block_hash: 'test',
                  })
                  .onConflictDoUpdate({
                    target: testTable.block_number,
                    set: {
                      data: JSON.stringify(item),
                    },
                  })
              }
            }
          },
          onBeforeRollback: async () => {
            // throw new Error(`STOP`)
          },
          onAfterRollback: async ({ tx, cursor }) => {
            callCount++
            const rows = await tx.select().from(testTable).where(eq(testTable.block_number, -1))

            expect(cursor.number).toEqual(3)
            expect(cursor.hash).toEqual('0x3')
            expect(rows).toHaveLength(0)
          },
        }),
      )

      expect(callCount).toEqual(1)

      const res = await db.select().from(testTable).orderBy(testTable.block_number)
      expect(res).toMatchInlineSnapshot(`
        [
          {
            "block_hash": "test",
            "block_number": -1,
            "data": "{"number":5,"hash":"0x5a","timestamp":5000}",
          },
        ]
      `)
    })
  })

  // A fork that rewinds below the first change to a row must restore the row to its value at the
  // fork boundary. That value lives only in the *before-image* of the earliest rolled-back change —
  // an after-image undo log has no record of it and restores the wrong (post-change) value instead.
  describe('class-T undo restores the pre-fork value', () => {
    const balances = pgTable('balances', {
      account: integer().primaryKey(),
      balance: integer().notNull(),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE balances (
          account integer PRIMARY KEY,
          balance integer NOT NULL
        );
      `)
    })

    it('restores the value at the fork boundary, not the earliest rolled-back after-image', async () => {
      // Seed the balance early, leave it untouched through block 3, then move it in blocks 4 and 5.
      // The reorg rewinds to block 3 — below both changes and above the seed — so the balance must
      // come back to its seeded value, which no snapshot above the fork carries as an after-image.
      portal = await mockPortal([
        ...[1, 2, 3, 4, 5].map(
          (block): MockResponse => ({
            statusCode: 200,
            data: [{ header: { number: block, hash: `0x${block}`, timestamp: block * 1000 } }],
            head: { finalized: { number: 1, hash: '0x1' } },
          }),
        ),
        {
          // Reorg forks block 4 onward; the safe ancestor is block 3, below both balance changes.
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 3, hash: '0x3' },
              { number: 4, hash: '0x4a' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
            { header: { number: 6, hash: '0x6a', timestamp: 6000 } },
          ],
          head: { finalized: { number: 1, hash: '0x1' } },
        },
      ])

      let rollbackCount = 0
      let rolledBackBalance: number | undefined

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 6 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [balances],
          onData: async ({ tx, data }) => {
            for (const b of data) {
              if (b.number === 1) {
                await tx.insert(balances).values({ account: 1, balance: 10 })
              } else if (b.number === 4) {
                await tx.update(balances).set({ balance: 20 }).where(eq(balances.account, 1))
              } else if (b.number === 5) {
                await tx.update(balances).set({ balance: 30 }).where(eq(balances.account, 1))
              }
            }
          },
          onAfterRollback: async ({ tx, cursor }) => {
            rollbackCount++

            // Captured at the rollback point, before any reprocessing overwrites it again.
            const [row] = await tx.select().from(balances).where(eq(balances.account, 1))
            rolledBackBalance = row?.balance

            expect(cursor.number).toEqual(3)
          },
        }),
      )

      expect(rollbackCount).toEqual(1)

      // The fork rewinds to block 3, where the balance was still its seeded value. Correct undo
      // restores 10 (block 4's before-image); the after-image bug restores block 4's new value, 20.
      expect(rolledBackBalance).toEqual(10)
    })
  })

  // A finalized block can share a portal batch with the first unfinalized block above it. Its
  // writes must still be attributed to its own (finalized) block in the rollback log, so a fork to
  // the finalized head leaves them untouched — a finalized block must never be rolled back, and it
  // is not re-fetched on resume.
  describe('class-T fork keeps finalized data', () => {
    const balances = pgTable('balances', {
      account: integer().primaryKey(),
      balance: integer().notNull(),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE balances (
          account integer PRIMARY KEY,
          balance integer NOT NULL
        );
      `)
    })

    it('does not roll back a finalized block that shared a batch with the forked block', async () => {
      portal = await mockPortal([
        // Block 1 is finalized, block 2 is not; the two land in the same portal batch.
        {
          statusCode: 200,
          data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
          head: { finalized: { number: 1, hash: '0x1' } },
        },
        {
          statusCode: 200,
          data: [{ header: { number: 2, hash: '0x2', timestamp: 2000 } }],
          head: { finalized: { number: 1, hash: '0x1' } },
        },
        {
          // Reorg forks block 2; the safe ancestor is the finalized block 1.
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 1, hash: '0x1' },
              { number: 2, hash: '0x2a' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 2, hash: '0x2a', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3a', timestamp: 3000 } },
          ],
          head: { finalized: { number: 1, hash: '0x1' } },
        },
      ])

      let forkCursor: number | undefined

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [balances],
          onData: async ({ tx, data }) => {
            for (const b of data) {
              await tx.insert(balances).values({ account: b.number, balance: b.number }).onConflictDoNothing()
            }
          },
          onAfterRollback: async ({ cursor }) => {
            forkCursor = cursor.number
          },
        }),
      )

      // The reorg rewinds to the finalized block 1, whose write must survive the rollback.
      expect(forkCursor).toEqual(1)

      const [finalizedRow] = await db.select().from(balances).where(eq(balances.account, 1))
      expect(finalizedRow?.balance).toEqual(1)
    })
  })

  // Read by truthiness a head of 0 looks like no finality, so the undo log is never written.
  describe('class-T undo with a finalized head of block 0', () => {
    const balances = pgTable('balances', {
      account: integer().primaryKey(),
      balance: integer().notNull(),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE balances (
          account integer PRIMARY KEY,
          balance integer NOT NULL
        );
      `)
    })

    it('rolls back a fork when only the genesis block is finalized', async () => {
      portal = await mockPortal([
        ...[1, 2].map(
          (block): MockResponse => ({
            statusCode: 200,
            data: [{ header: { number: block, hash: `0x${block}`, timestamp: block * 1000 } }],
            head: { finalized: { number: 0, hash: '0x0' } },
          }),
        ),
        {
          // Forks block 2; the safe ancestor is block 1, itself unfinalized.
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 1, hash: '0x1' },
              { number: 2, hash: '0x2a' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 2, hash: '0x2a', timestamp: 2000 } },
            { header: { number: 3, hash: '0x3a', timestamp: 3000 } },
          ],
          head: { finalized: { number: 0, hash: '0x0' } },
        },
      ])

      let rolledBack: number[] | undefined

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [balances],
          onData: async ({ tx, data }) => {
            for (const b of data) {
              await tx.insert(balances).values({ account: b.number, balance: b.number }).onConflictDoNothing()
            }
          },
          onAfterRollback: async ({ tx, cursor }) => {
            expect(cursor.number).toEqual(1)

            const rows = await tx.select().from(balances)
            rolledBack = rows.map((r) => r.account).sort()
          },
        }),
      )

      // Block 2 was undone; block 1 predates the fork and stays.
      expect(rolledBack).toEqual([1])
    })
  })

  describe('forks on tables with foreign keys', () => {
    const parentTable = pgTable('parent', {
      id: integer().primaryKey(),
      text: varchar().notNull(),
    })
    const childTable = pgTable('child', {
      id: integer().primaryKey(),
      text: varchar().notNull(),
      parent_id: integer().references(() => parentTable.id),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;       
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE parent (
          id SERIAL PRIMARY KEY,
          text TEXT NOT NULL
        );

        -- Child table
        CREATE TABLE child (
            id SERIAL PRIMARY KEY,
            parent_id INT NOT NULL REFERENCES parent(id),
            text TEXT NOT NULL
        );
     `)
    })

    it('should remove child entity first', async () => {
      portal = await mockPortal([
        ...new Array(4).fill(null).map((_, i): MockResponse => {
          const block = i + 1
          return {
            // 1. The First response is okay, it gets 5 blocks
            statusCode: 200,
            data: [{ header: { number: block, hash: `0x${block}`, timestamp: block * 1000 } }],
            head: {
              finalized: {
                number: 1,
                hash: '0x1',
              },
            },
          }
        }),

        {
          // 2. A reorg for 2 blocks happens
          statusCode: 409,
          data: {
            previousBlocks: [
              // Unforked blocks
              { number: 2, hash: '0x2' },
              { number: 3, hash: '0x3' },
              // Forked blocks
              { number: 4, hash: '0x4a' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
          ],
        },
      ])

      let callCount = 0

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 5 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [
            /*
             * We deliberately put child after parent to test FK constraints
             */
            parentTable,
            childTable,
          ],
          onData: async ({ tx, data }) => {
            for (const item of data) {
              const [parent] = await tx
                .insert(parentTable)
                .values({
                  id: item.number,
                  text: item.hash,
                })
                .returning()

              await tx.insert(childTable).values({
                id: item.number,
                text: item.hash,
                parent_id: parent.id,
              })
            }
          },
          onAfterRollback: async () => {
            callCount++
          },
        }),
      )

      expect(callCount).toEqual(1)

      const childs = await db.select().from(childTable).orderBy(childTable.id)
      expect(childs).toMatchInlineSnapshot(`
        [
          {
            "id": 1,
            "parent_id": 1,
            "text": "0x1",
          },
          {
            "id": 2,
            "parent_id": 2,
            "text": "0x2",
          },
          {
            "id": 3,
            "parent_id": 3,
            "text": "0x3",
          },
          {
            "id": 4,
            "parent_id": 4,
            "text": "0x4a",
          },
          {
            "id": 5,
            "parent_id": 5,
            "text": "0x5a",
          },
        ]
      `)

      const parents = await db.select().from(parentTable).orderBy(parentTable.id)
      expect(parents).toMatchInlineSnapshot(`
        [
          {
            "id": 1,
            "text": "0x1",
          },
          {
            "id": 2,
            "text": "0x2",
          },
          {
            "id": 3,
            "text": "0x3",
          },
          {
            "id": 4,
            "text": "0x4a",
          },
          {
            "id": 5,
            "text": "0x5a",
          },
        ]
      `)
    })
  })

  // The snapshot trigger runs against the base table's real columns (NEW./OLD.), not the Drizzle
  // property keys. A schema that maps a camelCase key to a snake_case column must still snapshot
  // and roll back correctly instead of failing with `column "..." does not exist`.
  describe('snapshot trigger with a camelCase-to-snake_case column mapping', () => {
    const items = pgTable('items', {
      itemId: integer('item_id').primaryKey(),
      itemValue: integer('item_value').notNull(),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE items (
          item_id integer PRIMARY KEY,
          item_value integer NOT NULL
        );
      `)
    })

    it('rolls back to the pre-fork value without a missing-column error', async () => {
      portal = await mockPortal(forkAtBlockTwo())

      let rollbackCount = 0
      let rolledBackValue: number | undefined

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [items],
          onData: async ({ tx, data }) => {
            for (const b of data) {
              if (b.number === 1) {
                await tx.insert(items).values({ itemId: 1, itemValue: 10 })
              } else if (b.number === 2) {
                await tx.update(items).set({ itemValue: 20 }).where(eq(items.itemId, 1))
              }
            }
          },
          onAfterRollback: async ({ tx }) => {
            rollbackCount++

            // Captured at the rollback point, before block 2's replay overwrites it again.
            const [row] = await tx.select().from(items).where(eq(items.itemId, 1))
            rolledBackValue = row?.itemValue
          },
        }),
      )

      expect(rollbackCount).toEqual(1)
      expect(rolledBackValue).toEqual(10)
    })
  })

  // Columns declared without a name keep the JS property key in `col.name`; only the dialect's
  // casing cache knows the real column, so the trigger has to ask it rather than guess.
  describe('snapshot trigger under dialect-level casing', () => {
    const snakeDb = drizzle(pool, { casing: 'snake_case' })

    const items = pgTable('items', {
      itemId: integer().primaryKey(),
      itemValue: integer().notNull(),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE items (
          item_id integer PRIMARY KEY,
          item_value integer NOT NULL
        );
      `)
    })

    it('rolls back to the pre-fork value without a missing-column error', async () => {
      portal = await mockPortal(forkAtBlockTwo())

      let rolledBackValue: number | undefined

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        drizzleTarget({
          db: snakeDb,
          tables: [items],
          onData: async ({ tx, data }) => {
            for (const b of data) {
              if (b.number === 1) {
                await tx.insert(items).values({ itemId: 1, itemValue: 10 })
              } else if (b.number === 2) {
                await tx.update(items).set({ itemValue: 20 }).where(eq(items.itemId, 1))
              }
            }
          },
          onAfterRollback: async ({ tx }) => {
            const [row] = await tx.select().from(items).where(eq(items.itemId, 1))
            rolledBackValue = row?.itemValue
          },
        }),
      )

      expect(rolledBackValue).toEqual(10)
    })
  })

  // The undo copy runs inside Postgres, so values keep their driver representation. Reading them
  // into JS and writing them back through the ORM would re-encode what the driver already encoded.
  describe('rollback of non-scalar column types', () => {
    const records = pgTable('records', {
      recordId: integer('record_id').primaryKey(),
      payload: jsonb('payload').notNull(),
      amount: numeric('amount').notNull(),
      seenAt: timestamp('seen_at', { withTimezone: true }).notNull(),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE records (
          record_id integer PRIMARY KEY,
          payload jsonb NOT NULL,
          amount numeric NOT NULL,
          seen_at timestamptz NOT NULL
        );
      `)
    })

    it('restores jsonb, numeric and timestamp values unchanged', async () => {
      portal = await mockPortal(forkAtBlockTwo())

      const payload = { tag: 'v1', nested: { n: 1 } }
      const seenAt = new Date('2024-01-01T00:00:00.000Z')
      let restored: typeof records.$inferSelect | undefined

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [records],
          onData: async ({ tx, data }) => {
            for (const b of data) {
              if (b.number === 1) {
                await tx.insert(records).values({ recordId: 1, payload, amount: '123.456', seenAt })
              } else if (b.number === 2) {
                await tx
                  .update(records)
                  .set({ payload: { tag: 'v2' }, amount: '999.999', seenAt: new Date('2025-06-06T12:00:00.000Z') })
                  .where(eq(records.recordId, 1))
              }
            }
          },
          onAfterRollback: async ({ tx }) => {
            const [row] = await tx.select().from(records).where(eq(records.recordId, 1))
            restored = row
          },
        }),
      )

      expect(restored?.payload).toEqual(payload)
      expect(restored?.amount).toEqual('123.456')
      expect(restored?.seenAt?.toISOString()).toEqual(seenAt.toISOString())
    })
  })

  // Postgres owns the value of these columns and rejects an explicit write, so undo has to leave a
  // stored generated column alone and claim an identity key through OVERRIDING SYSTEM VALUE.
  describe('rollback of columns Postgres writes itself', () => {
    const generated = pgTable('gen', {
      id: integer('id').primaryKey(),
      amount: integer('amount').notNull(),
      doubled: integer('doubled').generatedAlwaysAs(sql`"amount" * 2`),
    })

    const identity = pgTable('ident', {
      id: integer('id').generatedAlwaysAsIdentity().primaryKey(),
      amount: integer('amount').notNull(),
    })

    beforeEach(async () => {
      await execute(`
        DROP SCHEMA IF EXISTS "public" CASCADE;
        CREATE SCHEMA IF NOT EXISTS "public";
        CREATE TABLE gen (
          id integer PRIMARY KEY,
          amount integer NOT NULL,
          doubled integer GENERATED ALWAYS AS (amount * 2) STORED
        );
        CREATE TABLE ident (
          id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          amount integer NOT NULL
        );
      `)
    })

    it('rolls back a stored generated column by letting Postgres recompute it', async () => {
      portal = await mockPortal(forkAtBlockTwo())

      let restored: typeof generated.$inferSelect | undefined

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [generated],
          onData: async ({ tx, data }) => {
            for (const b of data) {
              if (b.number === 1) {
                await tx.insert(generated).values({ id: 1, amount: 10 })
              } else if (b.number === 2) {
                await tx.update(generated).set({ amount: 20 }).where(eq(generated.id, 1))
              }
            }
          },
          onAfterRollback: async ({ tx }) => {
            const [row] = await tx.select().from(generated).where(eq(generated.id, 1))
            restored = row
          },
        }),
      )

      expect(restored?.amount).toEqual(10)
      expect(restored?.doubled).toEqual(20)
    })

    it('rolls back a GENERATED ALWAYS AS IDENTITY key', async () => {
      portal = await mockPortal(forkAtBlockTwo())

      let restored: typeof identity.$inferSelect | undefined

      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        drizzleTarget({
          db,
          tables: [identity],
          onData: async ({ tx, data }) => {
            for (const b of data) {
              if (b.number === 1) {
                await tx.insert(identity).values({ amount: 10 })
              } else if (b.number === 2) {
                await tx.update(identity).set({ amount: 20 }).where(eq(identity.id, 1))
              }
            }
          },
          onAfterRollback: async ({ tx }) => {
            const [row] = await tx.select().from(identity).where(eq(identity.id, 1))
            restored = row
          },
        }),
      )

      expect(restored?.id).toEqual(1)
      expect(restored?.amount).toEqual(10)
    })
  })
})
