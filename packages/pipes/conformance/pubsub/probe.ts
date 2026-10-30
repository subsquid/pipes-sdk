import { copyFileSync, existsSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import Database from 'better-sqlite3'

import type { Cursor, OracleRow, OracleState } from '../oracle/reference-model.js'
import { type ChangelogOperation, type PubsubSnapshot, type WireOperation, isHeight } from '../oracle/validators.js'
import type { BusMessage } from './bus.js'

/** The finality reference of RP-45: each message carries it anew, so it is no part of the operation. */
const FINALIZED_ATTRIBUTE = '_finalized'

type CursorRow = { latest: string | null; finalized: string | null }
type LedgerRow = { number: number; hash: string | null; timestamp: number | null }
type OutboxRow = { seq: number; topic: string; op: string; id: string; attributes: string; payload: Buffer }

/** An operation code as IB-28 stores it in the outbox and as its CDC change type says it on the wire. */
function opOf(value: unknown): ChangelogOperation['op'] {
  if (value === 'upsert' || value === 'UPSERT') {
    return 'upsert'
  }

  if (value === 'delete' || value === 'DELETE') {
    return 'delete'
  }

  throw new Error(`${JSON.stringify(value)} is not a changelog operation (IB-28)`)
}

/** A message body: the BigQuery CDC row of IB-28, its sequence number in uppercase hexadecimal. */
function decodeBody(body: string): Pick<ChangelogOperation, 'seq' | 'op' | 'id' | 'row'> {
  const { _CHANGE_SEQUENCE_NUMBER: seq, _CHANGE_TYPE: type, _id: id, ...row } = JSON.parse(body)

  const isRow = typeof seq === 'string' && /^[0-9A-F]+$/.test(seq) && typeof id === 'string'
  if (!isRow) {
    throw new Error(`the message body ${body} is not a CDC row (IB-28)`)
  }

  return { seq: Number.parseInt(seq, 16), op: opOf(type), id, row }
}

/** Every message the bus holds, as the operation it carries, in the order the bus took them. */
export function wireOperations(messages: readonly BusMessage[]): WireOperation[] {
  return messages.map((message) => {
    const { [FINALIZED_ATTRIBUTE]: finalized, ...attributes } = message.attributes
    const body = message.data.toString('utf8')

    return {
      ...decodeBody(body),
      topic: message.topic,
      attributes,
      orderingKey: message.orderingKey,
      finalized: finalized === undefined ? undefined : Number(finalized),
      body,
    }
  })
}

/** A stored cursor, as IB-28 writes it: JSON of the block, or nothing. */
function parseCursor(text: string | null, column: string): Cursor | undefined {
  if (text === null) {
    return undefined
  }

  const value = JSON.parse(text) as Cursor
  if (!isHeight(value?.number)) {
    throw new Error(`the cursor column '${column}' holds ${text}, not a block`)
  }

  return value
}

/** A ledger block as a chain entry: a column the binding left empty is absent, not null. */
function ledgerEntry({ number, hash, timestamp }: LedgerRow): Cursor {
  const entry: Cursor = { number, hash: hash as string }
  if (timestamp !== null) {
    entry.timestamp = timestamp
  }

  return entry
}

/** An outbox row as the operation it records: its payload is the author's columns, as canonical JSON. */
function outboxOperation(row: OutboxRow): ChangelogOperation {
  return {
    topic: row.topic,
    seq: row.seq,
    op: opOf(row.op),
    id: row.id,
    attributes: JSON.parse(row.attributes),
    row: JSON.parse(row.payload.toString('utf8')),
  }
}

/**
 * A copy of a state file and its write-ahead log. The first connection to a file a killed producer
 * left in WAL mode recovers it, and the restart must find the file as the kill left it.
 */
function copyState(path: string): { path: string; remove: () => void } {
  const directory = mkdtempSync(join(tmpdir(), 'sqd-ct-probe-'))
  const copy = join(directory, 'state.sqlite')

  copyFileSync(path, copy)
  if (existsSync(`${path}-wal`)) {
    copyFileSync(`${path}-wal`, `${copy}-wal`)
  }

  return { path: copy, remove: () => rmSync(directory, { recursive: true, force: true }) }
}

/**
 * The rows a consumer holds once it has applied `operations`: per id, the operation with the
 * highest sequence number decides (RP-24); an upsert keeps the row, a delete drops it. Each row is
 * attributed to the block in `blockColumn`, under the table in `tableColumn`; its other columns,
 * with the block as an INT64, are its value.
 */
export function materialize(
  operations: readonly Pick<ChangelogOperation, 'seq' | 'op' | 'id' | 'row'>[],
  { tableColumn = 'table', blockColumn = 'blockNumber' }: { tableColumn?: string; blockColumn?: string } = {},
): OracleRow[] {
  const newest = new Map<string, Pick<ChangelogOperation, 'seq' | 'op' | 'row'>>()
  for (const operation of operations) {
    const held = newest.get(operation.id)
    if (held === undefined || operation.seq > held.seq) {
      newest.set(operation.id, operation)
    }
  }

  const rows: OracleRow[] = []
  for (const { op, row } of newest.values()) {
    if (op === 'delete') {
      continue
    }

    const { [tableColumn]: table, [blockColumn]: block, ...columns } = row
    const value = { ...columns, [blockColumn]: BigInt(block as number) }
    rows.push({ table: String(table), block: block as number, value })
  }

  return rows
}

/**
 * The class-C store probe: one read of the producer's IB-28 state file, beside every message on the
 * bus. The state is ⟨C, F, RC⟩: the cursor row of `id`, and the ledger blocks above the
 * `folded_through` mark, at or below which IB-28 says to ignore them. A file that does not exist,
 * or holds no cursor row, reads as no state; a cursor or a body it cannot parse fails the read.
 *
 * Read only a file no producer holds: a live one keeps it under an exclusive lock.
 */
export function readPubsubSnapshot({
  path,
  id,
  messages,
}: {
  path: string
  /** The cursor key of the producer's cursor row. */
  id: string
  messages: readonly BusMessage[]
}): PubsubSnapshot {
  const wire = wireOperations(messages)
  const snapshot = (state: OracleState | undefined, sequence: number, outbox: ChangelogOperation[]) => ({
    state,
    sequence,
    outbox,
    wire,
    rows: materialize([...wire, ...outbox]),
  })

  if (!existsSync(path)) {
    return snapshot(undefined, 0, [])
  }

  const copy = copyState(path)
  const db = new Database(copy.path, { fileMustExist: true })
  try {
    const stamped = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'").get()
    if (stamped === undefined) {
      return snapshot(undefined, 0, [])
    }

    const meta = (key: string) =>
      db.prepare<[string], { value: string }>('SELECT value FROM meta WHERE key = ?').get(key)?.value
    const folded = Number(meta('folded_through') ?? '-1')
    const sequence = Number(meta('sequence') ?? '0')

    const cursor = db.prepare<[string], CursorRow>('SELECT latest, finalized FROM cursor WHERE id = ?').get(id)
    const current = parseCursor(cursor?.latest ?? null, 'latest')
    const ledger = db
      .prepare<[number], LedgerRow>(
        'SELECT number, hash, timestamp FROM ledger_blocks WHERE number > ? ORDER BY number',
      )
      .all(folded)

    const state: OracleState | undefined =
      current === undefined
        ? undefined
        : {
            current,
            finalized: parseCursor(cursor?.finalized ?? null, 'finalized'),
            rollbackChain: ledger.map(ledgerEntry),
          }

    const outbox = db
      .prepare<[], OutboxRow>('SELECT seq, topic, op, id, attributes, payload FROM outbox ORDER BY row_id')
      .all()
      .map(outboxOperation)

    return snapshot(state, sequence, outbox)
  } finally {
    db.close()
    copy.remove()
  }
}
