import { describe, expect } from 'vitest'

import { type Database, it } from './harness.js'
import { readPostgresSnapshot, rowsOfPostgres } from './probe.js'

const PIPE_ID = 'probe'
const BEYOND_DOUBLE = 9007199254740993n

const cursor = (number: number) => ({ number, hash: `0x${number}` })

type SyncRow = {
  id?: string
  number: number | string
  timestamp?: number
  /** Raw SQL for the jsonb column; IB-21 writes `'{}'` for no floor. */
  finalized?: string
  /** Raw SQL for the jsonb column. */
  chain?: string
}

const json = (value: unknown) => `'${JSON.stringify(value)}'::jsonb`

/** IB-21's sync table, as the binding creates it. */
async function writeSync({ admin, schema }: Database, rows: SyncRow[]) {
  await admin.query(`
    CREATE TABLE IF NOT EXISTS "${schema}"."sync" (
      id text NOT NULL, current_number numeric NOT NULL, current_hash text NOT NULL,
      "current_timestamp" timestamptz, finalized jsonb, rollback_chain jsonb,
      PRIMARY KEY (id, current_number)
    )`)

  for (const row of rows) {
    const timestamp = row.timestamp === undefined ? 'NULL' : `to_timestamp(${row.timestamp})`

    await admin.query(
      `INSERT INTO "${schema}"."sync" VALUES ($1, ${row.number}, $2, ${timestamp}, ${row.finalized ?? `'{}'`}, ${row.chain ?? `'[]'`})`,
      [row.id ?? PIPE_ID, `0x${row.number}`],
    )
  }
}

function read({ admin, schema }: Database) {
  return readPostgresSnapshot({ pool: admin, schema, id: PIPE_ID, tables: ['transfers', 'approvals'] })
}

describe('readPostgresSnapshot', () => {
  it('reads a store the pipe never wrote as no state, no sync row and empty tables', async ({ database }) => {
    const snapshot = await read(database)

    expect(snapshot).toStrictEqual({
      sync: [],
      state: undefined,
      records: [],
      tables: { transfers: { rows: [], undo: [] }, approvals: { rows: [], undo: [] } },
    })
  })

  it('orders the sync rows by block number, not as text, and takes the cursor from the newest', async ({
    database,
  }) => {
    await writeSync(database, [{ number: 59 }, { number: 6 }, { number: 13 }])

    const snapshot = await read(database)
    const numbers = snapshot.sync.map((row) => row.current.number)

    expect(numbers).toEqual([6, 13, 59])
    expect(snapshot.state?.current).toStrictEqual(cursor(59))
  })

  it('reads `{}` as no floor, and the timestamp in seconds', async ({ database }) => {
    await writeSync(database, [{ number: 4, timestamp: 4000, finalized: json(cursor(2)) }, { number: 9 }])

    const snapshot = await read(database)

    expect(snapshot.sync).toStrictEqual([
      { current: { ...cursor(4), timestamp: 4000 }, finalized: cursor(2), rollbackChain: [] },
      { current: cursor(9), finalized: undefined, rollbackChain: [] },
    ])
  })

  it('assembles each chain from every fragment up to its row, above its floor (DEF-7)', async ({ database }) => {
    await writeSync(database, [
      { number: 4, finalized: json(cursor(2)), chain: json([cursor(3), cursor(4)]) },
      { number: 6, finalized: json(cursor(3)), chain: json([cursor(5), cursor(6)]) },
      { number: 9, finalized: json(cursor(5)), chain: json([cursor(9)]) },
    ])

    const snapshot = await read(database)

    expect(snapshot.records).toStrictEqual([
      { rollbackChain: [cursor(3), cursor(4)], finalized: cursor(2) },
      { rollbackChain: [cursor(4), cursor(5), cursor(6)], finalized: cursor(3) },
      { rollbackChain: [cursor(6), cursor(9)], finalized: cursor(5) },
    ])
    expect(snapshot.state).toStrictEqual({
      current: cursor(9),
      finalized: cursor(5),
      rollbackChain: [cursor(6), cursor(9)],
    })
  })

  it('reads only the sync rows of its own pipe', async ({ database }) => {
    await writeSync(database, [{ number: 4 }, { id: 'other', number: 9 }])

    const snapshot = await read(database)

    expect(snapshot.state?.current).toStrictEqual(cursor(4))
  })

  it('reads int8 at full width, and the block tag of every undo-log record', async ({ database }) => {
    const { admin, schema } = database
    await admin.query(`
      INSERT INTO "${schema}"."transfers" VALUES (5, 'a'), (1, 'b');
      CREATE TABLE "${schema}"."transfers__snapshots" ("blockNumber" int8, "___sqd__block_number" int8);
      INSERT INTO "${schema}"."transfers__snapshots" VALUES (5, 5), (1, 3);
      CREATE TABLE "${schema}"."wide" ("blockNumber" int8, "amount" int8);
      INSERT INTO "${schema}"."wide" VALUES (2, ${BEYOND_DOUBLE});
    `)

    const snapshot = await read(database)
    const wide = await readPostgresSnapshot({ pool: admin, schema, id: PIPE_ID, tables: ['wide'] })

    expect(rowsOfPostgres(snapshot)).toEqual([
      { table: 'transfers', block: 1, value: { blockNumber: 1n, value: 'b' } },
      { table: 'transfers', block: 5, value: { blockNumber: 5n, value: 'a' } },
    ])
    expect(snapshot.tables.transfers.undo).toEqual([3, 5])
    expect(rowsOfPostgres(wide)).toEqual([
      { table: 'wide', block: 2, value: { blockNumber: 2n, amount: BEYOND_DOUBLE } },
    ])
  })

  it('fails the read on a sync row whose hash is not text', async ({ database }) => {
    const { admin, schema } = database
    await admin.query(`
      CREATE TABLE "${schema}"."sync" (
        id text, current_number numeric, current_hash bytea, "current_timestamp" timestamptz,
        finalized jsonb, rollback_chain jsonb
      );
      INSERT INTO "${schema}"."sync" VALUES ('${PIPE_ID}', 4, '\\x04', NULL, '{}', '[]');
    `)

    await expect(read(database)).rejects.toThrow('sync row 0 is not an IB-21 sync row: current_hash is not text')
  })

  it('fails the read on a block column that is not a block', async ({ database }) => {
    const { admin, schema } = database
    await admin.query(`INSERT INTO "${schema}"."transfers" VALUES (${BEYOND_DOUBLE}, 'a')`)

    await expect(read(database)).rejects.toThrow(
      `'transfers' holds a row whose block column is not a block: ${BEYOND_DOUBLE}`,
    )
  })

  it.for([
    { label: 'a fractional block', row: { number: '1.5' }, reason: 'current_number 1.5 is not a block' },
    { label: 'an SQL NULL floor', row: { number: 4, finalized: 'NULL' }, reason: 'finalized null is not an object' },
    {
      label: 'a JSON null floor',
      row: { number: 4, finalized: `'null'::jsonb` },
      reason: 'finalized null is not an object',
    },
    {
      label: 'a floor with no hash',
      row: { number: 4, finalized: json({ number: 2 }) },
      reason: 'finalized {"number":2} has no number and hash',
    },
    {
      label: 'a chain that is not a list',
      row: { number: 4, chain: json({ number: 3 }) },
      reason: 'rollback_chain {"number":3} is not a list',
    },
    {
      label: 'a chain entry with no hash',
      row: { number: 4, chain: json([cursor(3), { number: 4 }]) },
      reason: 'rollback_chain holds {"number":4}, which has no number and hash',
    },
  ] satisfies { label: string; row: SyncRow; reason: string }[])('fails the read on a sync row with $label', async ({
    row,
    reason,
  }, { database }) => {
    await writeSync(database, [row])

    await expect(read(database)).rejects.toThrow(`sync row 0 is not an IB-21 sync row: ${reason}`)
  })
})
