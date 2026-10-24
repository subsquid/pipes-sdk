import { describe, expect } from 'vitest'

import { type Database, it } from './harness.js'
import { readClickhouseSnapshot, rowsOfClickhouse } from './probe.js'

const PIPE_ID = 'probe'
const BEYOND_DOUBLE = '9007199254740993'

const cursor = (number: number, branch = '') => ({ number, hash: `0x${branch}${number}` })

type SyncRow = {
  id?: string
  /** The `current` column as stored: cursor JSON, or anything else to test refusal. */
  current: string
  finalized?: string
  chain?: string
  /** Milliseconds; rows are written in this order unless a test says otherwise. */
  written: number
  sign?: 1 | -1
}

const json = (value: unknown) => JSON.stringify(value)

/** One insert would fold its rows that share a sorting key, as a merge does; the probe must see each. */
const UNFOLDED = { optimize_on_insert: 0 } as const

/** IB-20's sync table, as the binding makes it, with `rows` in it. */
async function writeSync({ admin, name }: Database, rows: SyncRow[]) {
  await admin.command({
    query: `CREATE TABLE IF NOT EXISTS "${name}"."sync" (id String, current String, finalized String,
      rollback_chain String, timestamp DateTime(3), sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY (timestamp, id)`,
  })
  await admin.insert({
    table: `"${name}"."sync"`,
    values: rows.map((row) => ({
      id: row.id ?? PIPE_ID,
      current: row.current,
      finalized: row.finalized ?? '',
      rollback_chain: row.chain ?? '[]',
      timestamp: row.written,
      sign: row.sign ?? 1,
    })),
    format: 'JSONEachRow',
    clickhouse_settings: UNFOLDED,
  })
}

async function writeRows({ admin, name }: Database, table: string, rows: [number | string, string, 1 | -1][]) {
  await admin.insert({
    table: `"${name}"."${table}"`,
    values: rows.map(([blockNumber, value, sign]) => ({ blockNumber, value, sign })),
    format: 'JSONEachRow',
    clickhouse_settings: UNFOLDED,
  })
}

function read({ admin, name }: Database) {
  return readClickhouseSnapshot({ client: admin, database: name, id: PIPE_ID, tables: ['transfers', 'approvals'] })
}

const rowAt = (table: string, block: number, value: string) => ({
  table,
  block,
  value: { blockNumber: BigInt(block), value },
})

describe('readClickhouseSnapshot', () => {
  it('reads a store the pipe never wrote as no state, no sync row and empty tables', async ({ database }) => {
    const snapshot = await read(database)

    expect(snapshot).toStrictEqual({
      sync: [],
      state: undefined,
      tables: { transfers: { rows: [] }, approvals: { rows: [] } },
    })
  })

  it('orders the sync rows by their timestamp, and takes C and F from the newest', async ({ database }) => {
    await writeSync(database, [
      { current: json(cursor(59)), finalized: json(cursor(50)), written: 3_000 },
      { current: json(cursor(6)), written: 1_000 },
      { current: json({ ...cursor(13), timestamp: 13 }), finalized: json(cursor(4)), written: 2_000 },
    ])

    const snapshot = await read(database)

    expect(snapshot.sync).toStrictEqual([
      { current: cursor(6), finalized: undefined, rollbackChain: [] },
      { current: { ...cursor(13), timestamp: 13 }, finalized: cursor(4), rollbackChain: [] },
      { current: cursor(59), finalized: cursor(50), rollbackChain: [] },
    ])
    expect(snapshot.state).toStrictEqual({ current: cursor(59), finalized: cursor(50), rollbackChain: [] })
  })

  it('reads only the rows of its own pipe id, and none a cancel row nets out', async ({ database }) => {
    await writeSync(database, [
      { current: json(cursor(6)), written: 1_000 },
      { current: json(cursor(9)), written: 2_000 },
      { current: json(cursor(9)), written: 2_000, sign: -1 },
      { id: 'other', current: json(cursor(70)), written: 3_000 },
    ])

    const snapshot = await read(database)

    expect(snapshot.sync.map((row) => row.current.number)).toEqual([6])
  })

  it('builds RC from the newest fragment of each block, above the newest floor and up to the cursor', async ({
    database,
  }) => {
    // A fork from 3 writes no row: the old branch's fragments stay below the new one's.
    await writeSync(database, [
      { current: json(cursor(2)), finalized: json(cursor(0)), chain: json([cursor(1), cursor(2)]), written: 1_000 },
      {
        current: json(cursor(5)),
        finalized: json(cursor(1)),
        chain: json([3, 4, 5].map((number) => cursor(number))),
        written: 2_000,
      },
      {
        current: json(cursor(4, 'b')),
        finalized: json(cursor(1)),
        chain: json([cursor(3), cursor(4, 'b')]),
        written: 3_000,
      },
    ])

    const snapshot = await read(database)

    expect(snapshot.state).toStrictEqual({
      current: cursor(4, 'b'),
      finalized: cursor(1),
      rollbackChain: [cursor(2), cursor(3), cursor(4, 'b')],
    })
  })

  it('reads each table net of its cancel rows: a row inserted twice is there twice', async ({ database }) => {
    await writeRows(database, 'transfers', [
      [1, 'a', 1],
      [5, 'b', 1],
      [5, 'b', 1],
      [9, 'c', 1],
      [9, 'c', -1],
      [9, 'd', 1],
    ])
    await writeRows(database, 'approvals', [[1, 'x', 1]])

    const snapshot = await read(database)

    expect(rowsOfClickhouse(snapshot)).toEqual([
      rowAt('transfers', 1, 'a'),
      rowAt('transfers', 5, 'b'),
      rowAt('transfers', 5, 'b'),
      rowAt('transfers', 9, 'd'),
      rowAt('approvals', 1, 'x'),
    ])
  })

  it('refuses a row cancelled more often than inserted', async ({ database }) => {
    await writeRows(database, 'transfers', [
      [1, 'a', 1],
      [1, 'a', -1],
      [1, 'a', -1],
    ])

    await expect(read(database)).rejects.toThrow(`'transfers' holds {"blockNumber":"1","value":"a"} -1 times`)
  })

  it('refuses a block past 2^53, which no number holds exactly', async ({ database }) => {
    await writeRows(database, 'transfers', [[BEYOND_DOUBLE, 'a', 1]])

    await expect(read(database)).rejects.toThrow(`whose block column is not a block: ${BEYOND_DOUBLE}`)
  })

  it('refuses two sync rows in one millisecond: the binding cannot tell which is newer (GAP-10)', async ({
    database,
  }) => {
    await writeSync(database, [
      { current: json(cursor(6)), written: 1_000 },
      { current: json(cursor(13)), written: 1_000 },
    ])

    await expect(read(database)).rejects.toThrow('sync rows 0 and 1 share the timestamp 1000: no commit order (GAP-10)')
  })

  it.for([
    ['current that is not JSON', { current: 'block 6' }, 'current "block 6" is not JSON'],
    ['current with no hash', { current: json({ number: 6 }) }, 'current {"number":6} has no number and hash'],
    ['finalized null', { current: json(cursor(6)), finalized: 'null' }, 'finalized null has no number and hash'],
    ['a chain that is not a list', { current: json(cursor(6)), chain: '{}' }, 'rollback_chain {} is not a list'],
    [
      'a chain entry with no hash',
      { current: json(cursor(6)), chain: json([{ number: 5 }]) },
      'rollback_chain holds {"number":5}, which has no number and hash',
    ],
  ] as const)('refuses a sync row with %s', async ([, row, reason], { database }) => {
    await writeSync(database, [{ ...row, written: 1_000 }])

    await expect(read(database)).rejects.toThrow(`sync row 0 is not an IB-20 sync row: ${reason}`)
  })

  it('refuses a sync row that is in the table twice', async ({ database }) => {
    await writeSync(database, [
      { current: json(cursor(6)), written: 1_000 },
      { current: json(cursor(6)), written: 1_000 },
    ])

    await expect(read(database)).rejects.toThrow('sync row 0 is not an IB-20 sync row: it is in the table 2 times')
  })
})
