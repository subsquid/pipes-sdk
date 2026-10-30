import { copyFileSync, readFileSync } from 'node:fs'

import Database from 'better-sqlite3'
import { describe, expect } from 'vitest'

import type { BusMessage } from './bus.js'
import { it } from './harness.js'
import { materialize, readPubsubSnapshot } from './probe.js'

const PIPE_ID = 'probe'

type StateFile = {
  meta?: Record<string, string>
  cursors?: { id?: string; latest: unknown; finalized?: unknown }[]
  ledger?: { number: number; hash: string | null; timestamp: number | null }[]
  outbox?: { seq: number; op: string; id: string; attributes: string; payload: string }[]
}

/** The IB-28 tables the probe reads, as the binding creates them, holding `file`. */
function writeState(path: string, file: StateFile) {
  const db = new Database(path)
  db.exec(`
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE cursor (id TEXT PRIMARY KEY, latest TEXT, finalized TEXT, updated_at INTEGER);
    CREATE TABLE outbox (row_id INTEGER PRIMARY KEY AUTOINCREMENT, route TEXT, topic TEXT, op TEXT, id TEXT,
      ordering_key TEXT, seq INTEGER, attributes TEXT, payload BLOB, block_number INTEGER);
    CREATE TABLE ledger_blocks (number INTEGER PRIMARY KEY, hash TEXT, timestamp INTEGER);
  `)

  for (const [key, value] of Object.entries(file.meta ?? {})) {
    db.prepare('INSERT INTO meta VALUES (?, ?)').run(key, value)
  }

  for (const { id = PIPE_ID, latest, finalized } of file.cursors ?? []) {
    const json = (value: unknown) => (value === undefined ? null : JSON.stringify(value))
    db.prepare('INSERT INTO cursor VALUES (?, ?, ?, 0)').run(id, json(latest), json(finalized))
  }

  for (const { number, hash, timestamp } of file.ledger ?? []) {
    db.prepare('INSERT INTO ledger_blocks VALUES (?, ?, ?)').run(number, hash, timestamp)
  }

  for (const { seq, op, id, attributes, payload } of file.outbox ?? []) {
    db.prepare(
      `INSERT INTO outbox (route, topic, op, id, ordering_key, seq, attributes, payload, block_number)
       VALUES ('rows', 'ct-erc20', ?, ?, '', ?, ?, ?, 0)`,
    ).run(op, id, seq, attributes, Buffer.from(payload))
  }

  db.close()
}

function busMessage(body: object, attributes: Record<string, string> = {}): BusMessage {
  return { topic: 'ct-erc20', data: Buffer.from(JSON.stringify(body)), attributes, orderingKey: '', call: 1 }
}

const cursor = (number: number) => ({ number, hash: `0x${number}` })

describe('readPubsubSnapshot', () => {
  it('reads a state file that is not there as no state, number 0 and an empty outbox', ({ pubsub }) => {
    const snapshot = readPubsubSnapshot({ path: pubsub.statePath, id: PIPE_ID, messages: [] })

    expect(snapshot).toStrictEqual({ state: undefined, sequence: 0, outbox: [], wire: [], rows: [] })
  })

  it('reads the cursor row of its id, and the ledger above the folded mark only (IB-28)', ({ pubsub }) => {
    writeState(pubsub.statePath, {
      meta: { sequence: '7', folded_through: '4' },
      cursors: [
        { latest: { ...cursor(9), timestamp: 90 }, finalized: cursor(4) },
        { id: 'other', latest: cursor(99) },
      ],
      ledger: [
        { number: 4, hash: '0x4', timestamp: 40 },
        { number: 6, hash: '0x6', timestamp: null },
        { number: 5, hash: '0x5', timestamp: 50 },
      ],
    })

    const snapshot = readPubsubSnapshot({ path: pubsub.statePath, id: PIPE_ID, messages: [] })

    expect(snapshot.sequence).toBe(7)
    expect(snapshot.state).toStrictEqual({
      current: { ...cursor(9), timestamp: 90 },
      finalized: cursor(4),
      rollbackChain: [{ ...cursor(5), timestamp: 50 }, cursor(6)],
    })
  })

  it('reads what a killed producer committed, and leaves its file and log as the kill left them', ({ pubsub }) => {
    writeState(pubsub.statePath, { meta: { sequence: '1' }, cursors: [{ latest: cursor(9) }] })
    const producer = new Database(pubsub.statePath)
    producer.exec('PRAGMA journal_mode = WAL;')
    producer.exec("UPDATE meta SET value = '2' WHERE key = 'sequence'")

    // Copied while the producer is open: closing it would checkpoint the log away.
    const killed = `${pubsub.statePath}.killed`
    copyFileSync(pubsub.statePath, killed)
    copyFileSync(`${pubsub.statePath}-wal`, `${killed}-wal`)
    producer.close()
    const files = () => [readFileSync(killed), readFileSync(`${killed}-wal`)]
    const before = files()

    const snapshot = readPubsubSnapshot({ path: killed, id: PIPE_ID, messages: [] })

    expect(snapshot.sequence).toBe(2)
    expect(files()).toEqual(before)
  })

  it('reads the outbox in enqueue order, each row as the operation it records', ({ pubsub }) => {
    writeState(pubsub.statePath, {
      meta: { sequence: '3' },
      cursors: [{ latest: cursor(9) }],
      outbox: [
        { seq: 3, op: 'delete', id: 'transfers:5', attributes: '{"table":"transfers"}', payload: '{"blockNumber":5}' },
        { seq: 2, op: 'upsert', id: 'transfers:4', attributes: '{}', payload: '{"blockNumber":4,"value":"x"}' },
      ],
    })

    const snapshot = readPubsubSnapshot({ path: pubsub.statePath, id: PIPE_ID, messages: [] })

    expect(snapshot.outbox).toStrictEqual([
      {
        topic: 'ct-erc20',
        seq: 3,
        op: 'delete',
        id: 'transfers:5',
        attributes: { table: 'transfers' },
        row: { blockNumber: 5 },
      },
      {
        topic: 'ct-erc20',
        seq: 2,
        op: 'upsert',
        id: 'transfers:4',
        attributes: {},
        row: { blockNumber: 4, value: 'x' },
      },
    ])
  })

  it('reads every message on the bus, the finality reference apart from the filter attributes (RP-45)', ({
    pubsub,
  }) => {
    const body = { _CHANGE_SEQUENCE_NUMBER: '1A', _CHANGE_TYPE: 'UPSERT', _id: 'approvals:3', blockNumber: 3 }
    const message = busMessage(body, { table: 'approvals', _finalized: '2' })

    const snapshot = readPubsubSnapshot({ path: pubsub.statePath, id: PIPE_ID, messages: [message, message] })

    expect(snapshot.wire).toHaveLength(2)
    expect(snapshot.wire[0]).toStrictEqual({
      seq: 26,
      op: 'upsert',
      id: 'approvals:3',
      row: { blockNumber: 3 },
      topic: 'ct-erc20',
      attributes: { table: 'approvals' },
      orderingKey: '',
      finalized: 2,
      body: JSON.stringify(body),
    })
  })

  it('fails on a body that is not a CDC row, and on a cursor that is not a block', ({ pubsub }) => {
    const lowercase = busMessage({ _CHANGE_SEQUENCE_NUMBER: '1a', _CHANGE_TYPE: 'UPSERT', _id: 'x' })
    writeState(pubsub.statePath, { cursors: [{ latest: { hash: '0x1' } }], meta: {} })

    expect(() => readPubsubSnapshot({ path: pubsub.statePath, id: 'none', messages: [lowercase] })).toThrow(
      'is not a CDC row',
    )
    expect(() => readPubsubSnapshot({ path: pubsub.statePath, id: PIPE_ID, messages: [] })).toThrow(
      "the cursor column 'latest' holds",
    )
  })
  it('fails on an operation code that IB-28 does not name, on the bus and in the outbox', ({ pubsub }) => {
    const merge = busMessage({ _CHANGE_SEQUENCE_NUMBER: '1', _CHANGE_TYPE: 'MERGE', _id: 'x' })
    writeState(pubsub.statePath, {
      cursors: [{ latest: cursor(1) }],
      outbox: [{ seq: 1, op: 'replace', id: 'x', attributes: '{}', payload: '{}' }],
    })

    expect(() => readPubsubSnapshot({ path: pubsub.statePath, id: PIPE_ID, messages: [merge] })).toThrow(
      '"MERGE" is not a changelog operation',
    )
    expect(() => readPubsubSnapshot({ path: pubsub.statePath, id: PIPE_ID, messages: [] })).toThrow(
      '"replace" is not a changelog operation',
    )
  })
})

describe('materialize', () => {
  const upsert = (seq: number, id: string, value: string) => ({
    seq,
    op: 'upsert' as const,
    id,
    row: { table: 'transfers', blockNumber: Number(id.split(':')[1]), value },
  })

  it('keeps, per id, the operation with the highest number, whatever order they come in (RP-24)', () => {
    const rows = materialize([
      upsert(5, 'transfers:1', 'new'),
      upsert(2, 'transfers:1', 'old'),
      upsert(3, 'transfers:2', 'b'),
    ])

    expect(rows).toStrictEqual([
      { table: 'transfers', block: 1, value: { value: 'new', blockNumber: 1n } },
      { table: 'transfers', block: 2, value: { value: 'b', blockNumber: 2n } },
    ])
  })

  it('drops an id whose newest operation is a delete', () => {
    const deleted = { ...upsert(4, 'transfers:1', ''), op: 'delete' as const }

    expect(materialize([upsert(1, 'transfers:1', 'a'), deleted])).toEqual([])
  })
})
