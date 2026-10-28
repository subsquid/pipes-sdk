import { randomBytes } from 'node:crypto'

import { createClient } from '@clickhouse/client'
import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  ClickhouseStore,
  ROLLBACK_INDEX_NAME,
  SUPPORTED_ROLLBACK_ENGINES,
} from '~/targets/clickhouse/clickhouse-store.js'

const TEST_DB = 'sqd_store_test'

const connection = {
  url: process.env['TEST_CLICKHOUSE_URL'] || 'http://localhost:10123',
  username: process.env['TEST_CLICKHOUSE_USERNAME'] || 'default',
  password: process.env['TEST_CLICKHOUSE_PASSWORD'] || 'default',
}

// The admin client is not bound to the test database so it can drop and recreate it;
// everything else runs through `client`, whose unqualified names resolve to TEST_DB
const admin = createClient(connection)
const client = createClient({ ...connection, database: TEST_DB })

async function selectFinal(store: ClickhouseStore, table: string) {
  const res = await store.query({ query: `SELECT * FROM ${table} FINAL` })

  return (await res.json()).data
}

async function rawBalance(store: ClickhouseStore, table: string) {
  const res = await store.query({
    query: `SELECT toInt64(count()) AS rows, toInt64(sum(sign)) AS balance FROM ${table}`,
  })
  const [row] = (await res.json()).data as { rows: number; balance: number }[]

  return row
}

describe('Clickhouse store', () => {
  afterAll(async () => {
    await client.close()
    await admin.close()
  })

  beforeEach(async () => {
    await admin.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB} SYNC` })
    await admin.command({ query: `CREATE DATABASE ${TEST_DB}` })
  })

  it('should work with uint128/uint256 ', async () => {
    const store = new ClickhouseStore(client)

    await store.query({
      query: `
        CREATE TABLE big_numbers
        (
            id        UInt64,
            date      Date,
            value128  UInt128,
            value256  UInt256,
            sign      Int8   -- 1 = insert, -1 = delete/cancel
        )
        ENGINE = CollapsingMergeTree(sign)
        ORDER BY (id);
    `,
    })

    await store.insert({
      table: 'big_numbers',
      values: [
        {
          id: 1,
          date: '2024-01-01',
          value128: '340282366920938463463374607431768211455', // max UInt128
          value256: '115792089237316195423570985008687907853269984665640564039457584007913129639935', // max UInt256
          sign: 1,
        },
      ],
      format: 'JSONEachRow',
    })

    const res = await store.removeAllRows({
      tables: 'big_numbers',
      where: 'id = {id:UInt64}',
      params: { id: 1 },
    })

    expect(res).toMatchInlineSnapshot(`
      [
        {
          "count": 1,
          "table": "big_numbers",
        },
      ]
    `)

    const rows = await selectFinal(store, 'big_numbers')
    expect(rows).toHaveLength(0)
  })

  describe('rollback netting', () => {
    const createEventsTable = async (store: ClickhouseStore, table = 'events') => {
      await store.query({
        query: `
          CREATE TABLE ${table}
          (
              block_number UInt32,
              account      String,
              value        Int64,
              sign         Int8 DEFAULT 1
          )
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (account, block_number)
        `,
      })
    }

    const insertEvents = async (store: ClickhouseStore, values: Record<string, unknown>[], table = 'events') => {
      await store.insert({ table, values, format: 'JSONEachRow' })
    }

    it('cancels insert-retry duplicates exactly', async () => {
      const store = new ClickhouseStore(client)
      await createEventsTable(store)

      // The same row written twice — e.g. a retried insert after a network error
      const row = { block_number: 10, account: 'alice', value: 100, sign: 1 }
      await insertEvents(store, [row])
      await insertEvents(store, [row])

      const res = await store.removeAllRows({
        tables: 'events',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'events', count: 2 }])

      expect(await rawBalance(store, 'events')).toEqual({ rows: 4, balance: 0 })
      expect(await selectFinal(store, 'events')).toHaveLength(0)
    })

    it('does not double-cancel unmerged cancel rows from a previous rollback', async () => {
      const store = new ClickhouseStore(client)
      await createEventsTable(store)

      // Two inserts plus one cancel row left behind by a partially completed rollback —
      // separate inserts, as they would arrive in reality
      await insertEvents(store, [{ block_number: 10, account: 'alice', value: 100, sign: 1 }])
      await insertEvents(store, [{ block_number: 10, account: 'alice', value: 100, sign: 1 }])
      await insertEvents(store, [{ block_number: 10, account: 'alice', value: 100, sign: -1 }])

      const res = await store.removeAllRows({
        tables: 'events',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'events', count: 1 }])

      expect(await rawBalance(store, 'events')).toEqual({ rows: 4, balance: 0 })
      expect(await selectFinal(store, 'events')).toHaveLength(0)
    })

    it('is idempotent — a second rollback is a no-op', async () => {
      const store = new ClickhouseStore(client)
      await createEventsTable(store)

      await insertEvents(store, [
        { block_number: 10, account: 'alice', value: 100, sign: 1 },
        { block_number: 11, account: 'bob', value: 200, sign: 1 },
      ])

      const first = await store.removeAllRows({
        tables: 'events',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(first).toEqual([{ table: 'events', count: 2 }])

      const second = await store.removeAllRows({
        tables: 'events',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(second).toEqual([{ table: 'events', count: 0 }])

      expect(await selectFinal(store, 'events')).toHaveLength(0)
    })

    it('skips net-negative groups and warns instead of dropping them silently', async () => {
      const store = new ClickhouseStore(client)
      const warn = vi.fn()
      store.bindLogger({ warn } as any)

      await createEventsTable(store)

      // An unmatched cancel row — pre-existing corruption
      await insertEvents(store, [{ block_number: 10, account: 'alice', value: 100, sign: -1 }])

      const res = await store.removeAllRows({
        tables: 'events',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'events', count: 0 }])

      expect(warn).toHaveBeenCalledOnce()
      expect(await rawBalance(store, 'events')).toEqual({ rows: 1, balance: -1 })
    })

    it('propagates the rollback through a sum(x * sign) materialized view', async () => {
      const store = new ClickhouseStore(client)
      await createEventsTable(store)

      await store.query({
        query: `
          CREATE TABLE totals (account String, total Int64)
          ENGINE = SummingMergeTree
          ORDER BY (account)
        `,
      })
      await store.query({
        query: `
          CREATE MATERIALIZED VIEW totals_mv TO totals AS
          SELECT account, sum(value * sign) AS total
          FROM events
          GROUP BY account
        `,
      })

      await insertEvents(store, [
        { block_number: 1, account: 'alice', value: 100, sign: 1 },
        { block_number: 10, account: 'alice', value: 20, sign: 1 },
        { block_number: 11, account: 'alice', value: 30, sign: 1 },
      ])

      await store.removeAllRows({
        tables: 'events',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })

      const res = await store.query({
        query: `SELECT toInt64(sum(total)) AS total FROM totals GROUP BY account`,
      })
      expect((await res.json()).data).toEqual([{ total: 100 }])
    })

    it('works on a table with columns named like the netting alias', async () => {
      const store = new ClickhouseStore(client)

      // `_cnt` broke the original alias, `_sqd_net` forces the collision loop to rename
      await store.query({
        query: `
          CREATE TABLE aliased
          (
              block_number UInt32,
              _cnt         UInt32,
              _sqd_net     UInt32,
              sign         Int8 DEFAULT 1
          )
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (block_number)
        `,
      })
      await store.insert({
        table: 'aliased',
        values: [{ block_number: 10, _cnt: 7, _sqd_net: 8, sign: 1 }],
        format: 'JSONEachRow',
      })

      const res = await store.removeAllRows({
        tables: 'aliased',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'aliased', count: 1 }])

      expect(await rawBalance(store, 'aliased')).toEqual({ rows: 2, balance: 0 })
      expect(await selectFinal(store, 'aliased')).toHaveLength(0)
    })

    it('works on a table with ALIAS, MATERIALIZED and EPHEMERAL columns', async () => {
      const store = new ClickhouseStore(client)

      // None of the three can round-trip through the netting query: ALIAS and MATERIALIZED
      // columns cannot be inserted back, EPHEMERAL columns cannot even be selected
      await store.query({
        query: `
          CREATE TABLE special_columns
          (
              block_number UInt32,
              raw          String EPHEMERAL,
              value        String DEFAULT raw,
              value_upper  String MATERIALIZED upper(value),
              value_alias  String ALIAS value,
              sign         Int8 DEFAULT 1
          )
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (block_number)
        `,
      })
      await store.insert({
        table: 'special_columns',
        values: [{ block_number: 10, value: 'hello', sign: 1 }],
        format: 'JSONEachRow',
      })

      const res = await store.removeAllRows({
        tables: 'special_columns',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'special_columns', count: 1 }])

      expect(await rawBalance(store, 'special_columns')).toEqual({ rows: 2, balance: 0 })
      expect(await selectFinal(store, 'special_columns')).toHaveLength(0)
    })

    it('works on VersionedCollapsingMergeTree', async () => {
      const store = new ClickhouseStore(client)

      await store.query({
        query: `
          CREATE TABLE versioned
          (
              block_number UInt32,
              account      String,
              value        Int64,
              sign         Int8,
              version      UInt32
          )
          ENGINE = VersionedCollapsingMergeTree(sign, version)
          ORDER BY (account, block_number)
        `,
      })

      await store.insert({
        table: 'versioned',
        values: [{ block_number: 10, account: 'alice', value: 100, sign: 1, version: 3 }],
        format: 'JSONEachRow',
      })

      const res = await store.removeAllRows({
        tables: 'versioned',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'versioned', count: 1 }])

      expect(await selectFinal(store, 'versioned')).toHaveLength(0)
    })

    it('works with database-qualified table names', async () => {
      const store = new ClickhouseStore(client)

      await store.query({ query: 'DROP DATABASE IF EXISTS rollback_test SYNC' })
      await store.query({ query: 'CREATE DATABASE rollback_test' })
      await store.query({
        query: `
          CREATE TABLE rollback_test.events
          (
              block_number UInt32,
              value        Int64,
              sign         Int8 DEFAULT 1
          )
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (block_number)
        `,
      })
      await store.insert({
        table: 'rollback_test.events',
        values: [{ block_number: 10, value: 100, sign: 1 }],
        format: 'JSONEachRow',
      })

      const res = await store.removeAllRows({
        tables: 'rollback_test.events',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'rollback_test.events', count: 1 }])

      expect(await selectFinal(store, 'rollback_test.events')).toHaveLength(0)

      await store.query({ query: 'DROP DATABASE rollback_test SYNC' })
    })

    it('works with a quoted table name containing a dot', async () => {
      const store = new ClickhouseStore(client)

      await store.query({
        query: `
          CREATE TABLE \`my.events\`
          (
              block_number UInt32,
              value        Int64,
              sign         Int8 DEFAULT 1
          )
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (block_number)
        `,
      })
      await store.insert({
        table: '`my.events`',
        values: [{ block_number: 10, value: 100, sign: 1 }],
        format: 'JSONEachRow',
      })

      const res = await store.removeAllRows({
        tables: '`my.events`',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: '`my.events`', count: 1 }])

      expect(await selectFinal(store, '`my.events`')).toHaveLength(0)
    })

    it('round-trips Decimal columns exactly in cancel rows', async () => {
      const store = new ClickhouseStore(client)

      await store.query({
        query: `
          CREATE TABLE decimals
          (
              block_number UInt32,
              amount       Decimal(38, 18),
              sign         Int8 DEFAULT 1
          )
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (block_number)
        `,
      })

      // Unrepresentable in a Float64: silently truncated if the read-back does not quote decimals
      const amount = '123456789.123456789012345678'
      await store.insert({
        table: 'decimals',
        values: [{ block_number: 10, amount, sign: 1 }],
        format: 'JSONEachRow',
      })

      const res = await store.removeAllRows({
        tables: 'decimals',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'decimals', count: 1 }])

      const cancel = await store.query({
        query: 'SELECT toString(amount) AS amount FROM decimals WHERE sign = -1',
      })
      expect((await cancel.json()).data).toEqual([{ amount }])
      expect(await selectFinal(store, 'decimals')).toHaveLength(0)
    })

    it('falls back to the FINAL cancel-row rollback when table metadata is unreadable', async () => {
      const setupStore = new ClickhouseStore(client)
      await createEventsTable(setupStore, 'meta_denied')
      await insertEvents(setupStore, [{ block_number: 10, account: 'alice', value: 100, sign: 1 }], 'meta_denied')

      // Simulates a locked-down server where the client has no access to system.tables
      const restrictedClient = {
        query: (params: any) => {
          if (params.query.includes('system.')) {
            throw new Error('Not enough privileges')
          }

          return client.query(params)
        },
        insert: (params: any) => client.insert(params),
        command: (params: any) => client.command(params),
      } as any

      const store = new ClickhouseStore(restrictedClient)
      const warn = vi.fn()
      store.bindLogger({ warn } as any)

      const res = await store.removeAllRows({
        tables: 'meta_denied',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'meta_denied', count: 1 }])

      expect(warn).toHaveBeenCalledOnce()
      expect(await selectFinal(setupStore, 'meta_denied')).toHaveLength(0)
    })
  })

  describe('engine check', () => {
    it('accepts double-quoted qualified names, as used by ClickhouseState', async () => {
      const store = new ClickhouseStore(client)

      await store.query({
        query: `
          CREATE TABLE quoted (id UInt64, sign Int8 DEFAULT 1)
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (id)
        `,
      })

      const count = await store.removeAllRowsByQuery({
        table: `"${TEST_DB}"."quoted"`,
        query: `SELECT * FROM "${TEST_DB}"."quoted" FINAL`,
      })

      expect(count).toBe(0)
    })

    it('falls back to a lightweight DELETE on a non-collapsing engine and warns', async () => {
      const store = new ClickhouseStore(client)
      const warn = vi.fn()
      store.bindLogger({ warn } as any)

      await store.query({
        query: `
          CREATE TABLE replacing (block_number UInt32, value Int64)
          ENGINE = ReplacingMergeTree
          ORDER BY (block_number)
        `,
      })
      await store.insert({
        table: 'replacing',
        values: [
          { block_number: 4, value: 1 },
          { block_number: 10, value: 2 },
          { block_number: 11, value: 3 },
        ],
        format: 'JSONEachRow',
      })

      const res = await store.removeAllRows({
        tables: 'replacing',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(res).toEqual([{ table: 'replacing', count: 2 }])
      expect(warn).toHaveBeenCalledOnce()

      const rows = await store.query({ query: 'SELECT block_number FROM replacing' })
      expect((await rows.json()).data).toEqual([{ block_number: 4 }])

      // Idempotent: nothing left to delete
      const second = await store.removeAllRows({
        tables: 'replacing',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })
      expect(second).toEqual([{ table: 'replacing', count: 0 }])
    })

    it('rejects a collapsing table whose collapse column is not "sign"', async () => {
      const store = new ClickhouseStore(client)

      await store.query({
        query: `
          CREATE TABLE no_sign (id UInt64, flag Int8)
          ENGINE = CollapsingMergeTree(flag)
          ORDER BY (id)
        `,
      })

      await expect(store.removeAllRows({ tables: 'no_sign', where: 'id > 0' })).rejects.toThrow(
        /collapses on "flag", not "sign"/,
      )
    })

    it('rejects a table with a stray "sign" column that the engine does not collapse on', async () => {
      const store = new ClickhouseStore(client)

      await store.query({
        query: `
          CREATE TABLE stray_sign (id UInt64, flag Int8, sign Int8 DEFAULT 1)
          ENGINE = CollapsingMergeTree(flag)
          ORDER BY (id)
        `,
      })

      await expect(store.removeAllRows({ tables: 'stray_sign', where: 'id > 0' })).rejects.toThrow(
        /collapses on "flag", not "sign"/,
      )
    })

    it('removeAllRowsByQuery performs no engine check — the caller owns the semantics', async () => {
      const store = new ClickhouseStore(client)

      await store.query({
        query: `
          CREATE TABLE replacing_by_query (id UInt64, sign Int8 DEFAULT 1)
          ENGINE = ReplacingMergeTree
          ORDER BY (id)
        `,
      })

      const count = await store.removeAllRowsByQuery({
        table: 'replacing_by_query',
        query: 'SELECT * FROM replacing_by_query FINAL WHERE id > 0',
      })

      expect(count).toBe(0)
    })

    it('accepts ClickHouse Cloud Shared* engine names', () => {
      expect(SUPPORTED_ROLLBACK_ENGINES.test('SharedCollapsingMergeTree')).toBe(true)
      expect(SUPPORTED_ROLLBACK_ENGINES.test('SharedVersionedCollapsingMergeTree')).toBe(true)
      expect(SUPPORTED_ROLLBACK_ENGINES.test('ReplicatedVersionedCollapsingMergeTree')).toBe(true)
      expect(SUPPORTED_ROLLBACK_ENGINES.test('SharedMergeTree')).toBe(false)
      expect(SUPPORTED_ROLLBACK_ENGINES.test('SharedReplacingMergeTree')).toBe(false)
    })

    it('rejects a Distributed table, naming the underlying local table', async () => {
      const store = new ClickhouseStore(client)

      await store.query({
        query: `
          CREATE TABLE dist_local (id UInt64, sign Int8 DEFAULT 1)
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (id)
        `,
      })
      await store.query({
        query: `CREATE TABLE dist AS dist_local ENGINE = Distributed('default', '${TEST_DB}', 'dist_local')`,
      })

      await expect(store.removeAllRows({ tables: 'dist', where: 'id > 0' })).rejects.toThrow(
        new RegExp(`Distributed table.*${TEST_DB}\\.dist_local`, 's'),
      )
    })

    it('rejects a table that does not exist', async () => {
      const store = new ClickhouseStore(client)

      await expect(store.removeAllRows({ tables: 'missing', where: 'id > 0' })).rejects.toThrow(
        /"missing" does not exist/,
      )
    })
  })

  describe('rollback index', () => {
    const indexCount = async (store: ClickhouseStore, table: string) => {
      const res = await store.query({
        query: `
          SELECT count() AS count
          FROM system.data_skipping_indices
          WHERE database = currentDatabase() AND table = {table:String} AND name = {index:String}
        `,
        query_params: { table, index: ROLLBACK_INDEX_NAME },
      })
      const [row] = (await res.json()).data as { count: string }[]

      return Number(row.count)
    }

    const createUnorderedTable = async (store: ClickhouseStore, table: string) => {
      // ORDER BY does not start with block_number — primary-key pruning does not apply
      await store.query({
        query: `
          CREATE TABLE ${table}
          (
              block_number UInt32,
              account      String,
              sign         Int8 DEFAULT 1
          )
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (account)
        `,
      })
    }

    it('ensureRollbackIndex creates the index and is idempotent', async () => {
      const store = new ClickhouseStore(client)
      await createUnorderedTable(store, 'idx_manual')

      await store.insert({
        table: 'idx_manual',
        values: [{ block_number: 1, account: 'alice', sign: 1 }],
        format: 'JSONEachRow',
      })

      await store.ensureRollbackIndex({ table: 'idx_manual' })
      expect(await indexCount(store, 'idx_manual')).toBe(1)

      await store.ensureRollbackIndex({ table: 'idx_manual' })
      expect(await indexCount(store, 'idx_manual')).toBe(1)
    })

    it('removeAllRows auto-creates the index on tables with a block_number column', async () => {
      const store = new ClickhouseStore(client)
      await createUnorderedTable(store, 'idx_auto')

      await store.insert({
        table: 'idx_auto',
        values: [{ block_number: 10, account: 'alice', sign: 1 }],
        format: 'JSONEachRow',
      })

      await store.removeAllRows({
        tables: 'idx_auto',
        where: 'block_number > {latest:UInt32}',
        params: { latest: 5 },
      })

      expect(await indexCount(store, 'idx_auto')).toBe(1)
      expect(await selectFinal(store, 'idx_auto')).toHaveLength(0)
    })
  })
})

describe('ClickhouseStore.removeRowsAfter', () => {
  type Events = { store: ClickhouseStore; blocks: (pipe: string) => Promise<number[]> }

  /** A database of the test's own, with an `events` table two pipes write: a row per block each. */
  const withEvents = it.extend<{ events: Events }>({
    events: async ({}, use) => {
      const name = `store_rows_after_${randomBytes(6).toString('hex')}`
      await admin.command({ query: `CREATE DATABASE ${name}` })
      await admin.command({
        query: `CREATE TABLE ${name}.events (block_number UInt32, pipe String, sign Int8)
          ENGINE = CollapsingMergeTree(sign) ORDER BY (pipe, block_number)`,
      })
      const scoped = createClient({ ...connection, database: name })
      const store = new ClickhouseStore(scoped)
      const rows = ['a', 'b'].flatMap((pipe) => range(0, 9).map((block) => ({ block_number: block, pipe, sign: 1 })))
      await store.insert({ table: 'events', values: rows, format: 'JSONEachRow' })

      const blocks = async (pipe: string) => {
        const result = await store.query({
          query: `SELECT block_number FROM events WHERE pipe = {pipe:String}
            GROUP BY block_number HAVING sum(sign) > 0 ORDER BY block_number`,
          query_params: { pipe },
          format: 'JSONEachRow',
        })
        const stored = await result.json<{ block_number: number }>()

        return stored.map((row) => row.block_number)
      }

      await use({ store, blocks })

      await scoped.close()
      await admin.command({ query: `DROP DATABASE ${name} SYNC` })
    },
  })

  const range = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, i) => from + i)

  withEvents('removes the rows above the cursor that `where` picks', async ({ events }) => {
    await events.store.removeRowsAfter({ number: 2 }, { tables: 'events', where: "pipe = 'a'" })

    expect(await events.blocks('a')).toEqual([0, 1, 2])
    expect(await events.blocks('b')).toEqual(range(0, 9))
  })

  withEvents('takes a cursor of -1 as one before every block', async ({ events }) => {
    await events.store.removeRowsAfter({ number: -1 }, { tables: 'events', where: "pipe = 'a'" })

    expect(await events.blocks('a')).toEqual([])
  })

  withEvents('refuses a block column that is not a plain identifier (E2006)', async ({ events }) => {
    const removal = events.store.removeRowsAfter({ number: 2 }, { tables: 'events', column: 'block_number OR 1' })

    await expect(removal).rejects.toMatchObject({ code: 'E2006' })
    expect(await events.blocks('a')).toEqual(range(0, 9))
  })

  withEvents('given `upTo`, removes the blocks up to it only, with no `where` too', async ({ events }) => {
    await events.store.removeRowsAfter({ number: 1, upTo: 4 }, { tables: 'events' })

    expect(await events.blocks('a')).toEqual([0, 1, 5, 6, 7, 8, 9])
    expect(await events.blocks('b')).toEqual([0, 1, 5, 6, 7, 8, 9])
  })

  withEvents('given `upTo` over a cursor at -1, removes from block 0', async ({ events }) => {
    await events.store.removeRowsAfter({ number: -1, upTo: 3 }, { tables: 'events', where: "pipe = 'a'" })

    expect(await events.blocks('a')).toEqual([4, 5, 6, 7, 8, 9])
  })
})
