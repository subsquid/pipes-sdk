import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { ParquetSchema, ParquetWriter } from '@dsnp/parquetjs'
import { it as base, describe, expect } from 'vitest'

import { readParquetSnapshot, rowsOf } from './probe.js'

const PIPE_ID = 'probe'
const BEYOND_DOUBLE = 9007199254740993n
const STATE = { id: PIPE_ID, cursor: { number: 23, hash: '0x23' }, coverage: { transfers: 20 } }

const it = base.extend<{ dir: string }>({
  dir: async ({}, use) => {
    const dir = await mkdtemp(path.join(tmpdir(), 'sqd-probe-'))
    await use(dir)
    await rm(dir, { recursive: true, force: true })
  },
})

async function writeUnit(
  dir: string,
  name: string,
  rows: { blockNumber: bigint; amount: bigint }[],
  table = 'transfers',
) {
  const schema = new ParquetSchema({
    blockNumber: { type: 'INT64' },
    amount: { type: 'INT64' },
  })

  await mkdir(path.join(dir, table), { recursive: true })
  const writer = await ParquetWriter.openFile(schema, path.join(dir, table, name))
  for (const row of rows) {
    await writer.appendRow(row)
  }
  await writer.close()
}

describe('readParquetSnapshot', () => {
  const read = (dir: string) => readParquetSnapshot({ dir, id: PIPE_ID, tables: ['transfers'] })

  it('reads INT64 values at full width', async ({ dir }) => {
    await writeUnit(dir, '000000000000-000000000009.parquet', [{ blockNumber: 5n, amount: BEYOND_DOUBLE }])

    const rows = rowsOf(await read(dir))

    expect(rows).toEqual([{ table: 'transfers', block: 5, value: { blockNumber: 5n, amount: BEYOND_DOUBLE } }])
  })

  it('reads the units, the state and the coverage the sidecar records, in one snapshot (IB-22)', async ({ dir }) => {
    await writeUnit(dir, '000000000000-000000000009.parquet', [{ blockNumber: 5n, amount: 1n }])
    await writeUnit(dir, '000000000010-000000000019.parquet', [])
    await writeFile(
      path.join(dir, `_sqd_parquet_state.${PIPE_ID}.json`),
      JSON.stringify({
        id: PIPE_ID,
        cursor: { number: 23, hash: '0x23' },
        finalized: { number: 23, hash: '0x23' },
        coverage: { transfers: 20 },
      }),
    )

    const snapshot = await read(dir)
    const { units, next } = snapshot.tables.transfers
    const windows = units.map(({ name, from, to }) => ({ name, from, to }))

    expect(windows).toEqual([
      { name: '000000000000-000000000009.parquet', from: 0, to: 9 },
      { name: '000000000010-000000000019.parquet', from: 10, to: 19 },
    ])
    expect(snapshot.state).toStrictEqual({
      current: { number: 23, hash: '0x23' },
      finalized: { number: 23, hash: '0x23' },
      rollbackChain: [],
    })
    expect(next).toBe(20)
    expect(snapshot.residue).toEqual([])
  })

  it('reads every declared table, each with its own coverage, rows in declaration order', async ({ dir }) => {
    await writeUnit(dir, '000000000000-000000000009.parquet', [{ blockNumber: 7n, amount: 1n }], 'transfers')
    await writeUnit(dir, '000000000000-000000000004.parquet', [{ blockNumber: 2n, amount: 2n }], 'approvals')
    await writeFile(
      path.join(dir, `_sqd_parquet_state.${PIPE_ID}.json`),
      JSON.stringify({ ...STATE, coverage: { transfers: 10, approvals: 5 } }),
    )

    const snapshot = await readParquetSnapshot({ dir, id: PIPE_ID, tables: ['transfers', 'approvals'] })
    const nextStarts = Object.entries(snapshot.tables).map(([table, { next }]) => [table, next])
    const rows = rowsOf(snapshot).map(({ table, block }) => [table, block])

    expect(nextStarts).toEqual([
      ['transfers', 10],
      ['approvals', 5],
    ])
    expect(rows).toEqual([
      ['transfers', 7],
      ['approvals', 2],
    ])
    expect(snapshot.residue).toEqual([])
  })

  it('reads nothing from a sink that never committed', async ({ dir }) => {
    const snapshot = await read(dir)

    expect(snapshot).toStrictEqual({
      state: undefined,
      tables: { transfers: { units: [], next: undefined } },
      residue: [],
    })
  })

  it('refuses a block column that is no block, or that it cannot read exactly', async ({ dir }) => {
    for (const blockNumber of [-1n, BEYOND_DOUBLE]) {
      await writeUnit(dir, '000000000000-000000000009.parquet', [{ blockNumber, amount: 1n }])

      await expect(read(dir)).rejects.toThrow(`block column is not a block: ${blockNumber}`)
    }
  })

  it('reports as residue every file that is neither a unit nor its state, unpadded names included', async ({ dir }) => {
    await writeUnit(dir, '000000000000-000000000009.parquet', [{ blockNumber: 5n, amount: 1n }])
    await writeUnit(dir, '10-19.parquet', [{ blockNumber: 12n, amount: 1n }])
    await writeFile(path.join(dir, 'transfers', '.tmp-open.parquet'), 'in flight')
    await writeFile(path.join(dir, '.tmp-state-x.json'), '{}')
    await mkdir(path.join(dir, 'another-table'))
    await writeFile(path.join(dir, `_sqd_parquet_state.${PIPE_ID}.json`), JSON.stringify(STATE))

    const snapshot = await read(dir)
    const unitNames = snapshot.tables.transfers.units.map((unit) => unit.name)

    expect(unitNames).toEqual(['000000000000-000000000009.parquet'])
    expect(snapshot.residue).toEqual([
      '.tmp-state-x.json',
      'another-table',
      'transfers/.tmp-open.parquet',
      'transfers/10-19.parquet',
    ])
  })

  it('fails on a state record that is not IB-22, instead of reading it as no state', async ({ dir }) => {
    const statePath = path.join(dir, `_sqd_parquet_state.${PIPE_ID}.json`)
    const stateFiles = {
      noCursor: { id: PIPE_ID, coverage: {} },
      otherPipe: { ...STATE, id: 'another' },
      textCoverage: { ...STATE, coverage: { transfers: '20' } },
      negativeCoverage: { ...STATE, coverage: { transfers: -1 } },
      listCoverage: { ...STATE, coverage: [20] },
      unknownField: { ...STATE, rollbackChain: [] },
      noFinalizedHash: { ...STATE, finalized: { number: 20 } },
      nullFinalized: { ...STATE, finalized: null },
      notAnObject: null,
    }

    for (const [name, state] of Object.entries(stateFiles)) {
      await writeFile(statePath, JSON.stringify(state))

      await expect(read(dir), name).rejects.toThrow(/is not an IB-22 state record/)
    }
  })

  it('fails on a store it cannot read, instead of reading it as empty', async ({ dir }) => {
    const statePath = path.join(dir, `_sqd_parquet_state.${PIPE_ID}.json`)

    await mkdir(statePath)
    await expect(read(dir)).rejects.toThrow(/EISDIR/)

    await rm(statePath, { recursive: true })
    await writeFile(path.join(dir, 'transfers'), 'a file where the table directory belongs')
    await expect(read(dir)).rejects.toThrow(/ENOTDIR/)
  })
})
