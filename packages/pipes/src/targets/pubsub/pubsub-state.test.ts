import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { BlockCursor } from '~/core/index.js'
import { testLogger } from '~/testing/index.js'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import { CommitInput, DeliveryProfile, PendingOperation, SqlitePubsubState } from './pubsub-state.js'

const encoder = new TextEncoder()
const text = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

let dirs: string[] = []
const opened: SqlitePubsubState[] = []

afterEach(async () => {
  for (const state of opened.splice(0)) {
    await state.close()
  }
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true })
  }
})

function statePath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'pubsub-state-'))
  dirs.push(dir)

  return join(dir, 'state.sqlite')
}

async function openState(path: string, delivery: DeliveryProfile = 'lww') {
  const state = new SqlitePubsubState({ path, delivery })
  const { coldStart } = await state.open({ cursorKey: 'test-pipe', logger: testLogger() })
  opened.push(state)

  return { state, coldStart }
}

/** Every case here exercises a fork-capable pipe; `forkCapable` is the interesting default. */
async function commit(state: SqlitePubsubState, input: Omit<CommitInput, 'forkCapable'>) {
  await state.commit({ ...input, forkCapable: true })
}

function block(number: number, suffix = 'a'): BlockCursor {
  return { number, hash: `0x${number}${suffix}`, timestamp: 1_700_000_000 + number }
}

function operation(overrides: Partial<PendingOperation> & { id?: string } = {}): PendingOperation {
  return {
    topic: 'transfers',
    orderingKey: '',
    mode: 'event',
    op: 'upsert',
    id: 'row-1',
    attributes: { token: '0x42' },
    payload: encoder.encode('{"a":1}'),
    blockNumber: 100,
    rollbackable: true,
    ...overrides,
  }
}

describe('SqlitePubsubState', () => {
  it('reports a cold start once and resumes warm afterwards', async () => {
    const path = statePath()

    const first = await openState(path)
    expect(first.coldStart).toBe(true)
    await first.state.close()

    const second = await openState(path)
    expect(second.coldStart).toBe(false)
  })

  it('round-trips the cursor with an explicit finalized floor', async () => {
    const { state } = await openState(statePath())

    expect(await state.getCursor()).toBeUndefined()

    await commit(state, { operations: [], ledger: [], cursor: block(100), finalized: block(90) })

    expect(await state.getCursor()).toEqual({ latest: block(100), finalized: block(90) })
  })

  it('states the absence of a finalized head as null, never as a missing key', async () => {
    const { state } = await openState(statePath())

    await commit(state, { operations: [], ledger: [], cursor: block(100), finalized: null })

    const cursor = await state.getCursor()
    expect(cursor).toHaveProperty('finalized', null)
  })

  it('assigns one producer-wide sequence under lww', async () => {
    const { state } = await openState(statePath(), 'lww')

    await commit(state, {
      operations: [
        operation({ id: 'a', topic: 'one' }),
        operation({ id: 'b', topic: 'two' }),
        operation({ id: 'c', topic: 'one' }),
      ],
      ledger: [],
      cursor: block(100),
      finalized: null,
    })

    expect((await state.pending()).map((row) => [row.topic, row.seq])).toEqual([
      ['one', 1],
      ['two', 2],
      ['one', 3],
    ])
  })

  it('assigns a dense per-partition sequence under ordered', async () => {
    const { state } = await openState(statePath(), 'ordered')

    await commit(state, {
      operations: [
        operation({ id: 'a', topic: 'one', orderingKey: 'one' }),
        operation({ id: 'b', topic: 'two', orderingKey: 'two' }),
        operation({ id: 'c', topic: 'one', orderingKey: 'one' }),
      ],
      ledger: [],
      cursor: block(100),
      finalized: null,
    })

    expect((await state.pending()).map((row) => [row.topic, row.seq])).toEqual([
      ['one', 1],
      ['two', 1],
      ['one', 2],
    ])
  })

  it('continues the sequence after a producer restart on the same state', async () => {
    const path = statePath()

    const first = await openState(path)
    await commit(first.state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null })
    await first.state.confirm((await first.state.pending()).map((row) => row.rowId))
    await first.state.close()

    const second = await openState(path)
    await commit(second.state, {
      operations: [operation({ id: 'row-2' })],
      ledger: [],
      cursor: block(101),
      finalized: null,
    })

    expect((await second.state.pending()).map((row) => row.seq)).toEqual([2])
  })

  it('keeps unconfirmed outbox rows for the restart drain', async () => {
    const path = statePath()

    const first = await openState(path)
    await commit(first.state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null })
    await first.state.close()

    const second = await openState(path)
    const pending = await second.state.pending()

    expect(pending).toHaveLength(1)
    expect(pending[0]).toMatchObject({ topic: 'transfers', op: 'upsert', id: 'row-1', seq: 1 })
    expect(text(pending[0].payload)).toBe('{"a":1}')

    await second.state.confirm([pending[0].rowId])
    expect(await second.state.pending()).toHaveLength(0)
  })

  it('refuses a second producer on the same state file', async () => {
    const path = statePath()
    await openState(path)

    await expect(openState(path)).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.STATE_LOCKED })
  })

  it('refuses a state file written by another schema version', async () => {
    const path = statePath()
    const first = await openState(path)
    await first.state.setMeta('schema_version', '99')
    await first.state.close()

    await expect(openState(path)).rejects.toBeInstanceOf(PubsubTargetError)
  })

  describe('fork compensation', () => {
    async function seed(delivery: DeliveryProfile = 'lww') {
      const { state } = await openState(statePath(), delivery)

      return state
    }

    it('deletes an orphaned write-once event, inheriting its attributes', async () => {
      const state = await seed()

      await commit(state, {
        operations: [operation({ id: 'evt-1', blockNumber: 101 })],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      const safe = await state.fork([block(100), block(101, 'b')])
      expect(safe).toEqual(block(100))

      const pending = await state.pending()
      expect(pending).toHaveLength(1)
      expect(pending[0]).toMatchObject({ op: 'delete', id: 'evt-1', attributes: { token: '0x42' }, seq: 2 })
    })

    it('leaves an event alone when it also exists below the fork point', async () => {
      const state = await seed()

      await commit(state, {
        operations: [operation({ id: 'evt-1', blockNumber: 100 })],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(90),
      })
      await commit(state, {
        operations: [operation({ id: 'evt-1', blockNumber: 101 })],
        ledger: [block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      const safe = await state.fork([block(100), block(101, 'b')])

      expect(safe).toEqual(block(100))
      expect(await state.pending()).toHaveLength(0)
    })

    it('folds several orphaned revisions of a materialized id into ONE restore', async () => {
      const state = await seed()

      const window = (revision: string, blockNumber: number) =>
        operation({
          id: 'candle-1',
          mode: 'materialized',
          blockNumber,
          payload: encoder.encode(revision),
        })

      await commit(state, {
        operations: [window('A', 100)],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(90),
      })
      await commit(state, {
        operations: [window('B', 101), window('C', 101)],
        ledger: [block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await state.fork([block(100), block(101, 'b')])

      const pending = await state.pending()
      expect(pending).toHaveLength(1)
      expect(pending[0]).toMatchObject({ op: 'upsert', id: 'candle-1' })
      expect(text(pending[0].payload)).toBe('A')
    })

    it('restores the finalized baseline once the surviving revisions are pruned', async () => {
      const state = await seed()

      await commit(state, {
        operations: [
          operation({ id: 'candle-1', mode: 'materialized', blockNumber: 100, payload: encoder.encode('A') }),
        ],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(100),
      })
      await commit(state, {
        operations: [
          operation({ id: 'candle-1', mode: 'materialized', blockNumber: 101, payload: encoder.encode('B') }),
        ],
        ledger: [block(101)],
        cursor: block(101),
        finalized: block(100),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      expect((await state.stats()).manifest).toBe(1)

      await state.fork([block(100), block(101, 'b')])

      const pending = await state.pending()
      expect(pending).toHaveLength(1)
      expect(text(pending[0].payload)).toBe('A')
    })

    it('rolls back an orphaned delete by restoring the revision it removed', async () => {
      const state = await seed()

      await commit(state, {
        operations: [
          operation({ id: 'candle-1', mode: 'materialized', blockNumber: 100, payload: encoder.encode('A') }),
        ],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(90),
      })
      await commit(state, {
        operations: [
          operation({
            id: 'candle-1',
            mode: 'materialized',
            op: 'delete',
            blockNumber: 101,
            payload: new Uint8Array(),
          }),
        ],
        ledger: [block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await state.fork([block(100), block(101, 'b')])

      const pending = await state.pending()
      expect(pending).toHaveLength(1)
      expect(pending[0].op).toBe('upsert')
      expect(text(pending[0].payload)).toBe('A')
    })

    it('emits the route’s stored inverse when every revision is orphaned', async () => {
      const state = await seed()

      await commit(state, {
        operations: [
          operation({
            id: 'candle-1',
            mode: 'materialized',
            blockNumber: 101,
            payload: encoder.encode('B'),
            inverse: { op: 'upsert', payload: encoder.encode('EMPTY') },
          }),
        ],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await state.fork([block(100), block(101, 'b')])

      const pending = await state.pending()
      expect(pending).toHaveLength(1)
      expect(pending[0]).toMatchObject({ op: 'upsert', id: 'candle-1' })
      expect(text(pending[0].payload)).toBe('EMPTY')
    })

    it('keeps the inverse across a restart between first publish and fork', async () => {
      const path = statePath()

      const first = await openState(path)
      await commit(first.state, {
        operations: [
          operation({
            id: 'candle-1',
            mode: 'materialized',
            blockNumber: 101,
            inverse: { op: 'upsert', payload: encoder.encode('EMPTY') },
          }),
        ],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await first.state.confirm((await first.state.pending()).map((row) => row.rowId))
      await first.state.close()

      const second = await openState(path)
      await second.state.fork([block(100), block(101, 'b')])

      expect(text((await second.state.pending())[0].payload)).toBe('EMPTY')
    })

    it('never rewinds the sequence across a fork', async () => {
      const state = await seed('ordered')

      await commit(state, {
        operations: [operation({ id: 'evt-1', orderingKey: 'k', blockNumber: 101 })],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await state.fork([block(100), block(101, 'b')])
      expect((await state.pending())[0].seq).toBe(2)
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await commit(state, {
        operations: [operation({ id: 'evt-2', orderingKey: 'k', blockNumber: 101 })],
        ledger: [block(101, 'b')],
        cursor: block(101, 'b'),
        finalized: block(90),
      })

      expect((await state.pending())[0].seq).toBe(3)
    })

    it('lands a compensation on the same partition as the operation it repairs', async () => {
      const state = await seed('ordered')

      await commit(state, {
        operations: [operation({ id: 'evt-1', orderingKey: 'pool-a', blockNumber: 101 })],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await state.fork([block(100), block(101, 'b')])

      expect((await state.pending())[0].orderingKey).toBe('pool-a')
    })

    it('prunes the orphaned manifest and rewinds the cursor', async () => {
      const state = await seed()

      await commit(state, {
        operations: [operation({ id: 'evt-1', blockNumber: 101 })],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })

      await state.fork([block(100), block(101, 'b')])

      expect((await state.stats()).manifest).toBe(0)
      expect((await state.getCursor())?.latest).toEqual(block(100))
    })

    it('compensates the whole manifest and returns null on a dead-end fork', async () => {
      const state = await seed()

      await commit(state, {
        operations: [operation({ id: 'evt-1', blockNumber: 101 }), operation({ id: 'evt-2', blockNumber: 102 })],
        ledger: [block(101), block(102)],
        cursor: block(102),
        finalized: block(100),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      const safe = await state.fork([{ number: 50, hash: '0xdead' }])

      expect(safe).toBeNull()
      expect((await state.pending()).map((row) => [row.id, row.op])).toEqual([
        ['evt-1', 'delete'],
        ['evt-2', 'delete'],
      ])
      expect((await state.stats()).manifest).toBe(0)
    })
  })

  describe('finalization advance', () => {
    it('baselines a materialized row that arrives already finalized', async () => {
      const state = await openState(statePath()).then((s) => s.state)

      // Never rollbackable, so it never enters the manifest — but a fork rewinding to it still
      // has to restore its value, which is exactly what the baseline is for.
      await commit(state, {
        operations: [
          operation({
            id: 'candle-1',
            mode: 'materialized',
            blockNumber: 100,
            payload: encoder.encode('A'),
            rollbackable: false,
          }),
        ],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(100),
      })
      await commit(state, {
        operations: [
          operation({ id: 'candle-1', mode: 'materialized', blockNumber: 101, payload: encoder.encode('B') }),
        ],
        ledger: [block(101)],
        cursor: block(101),
        finalized: block(100),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await state.fork([block(100), block(101, 'b')])

      const pending = await state.pending()
      expect(pending).toHaveLength(1)
      expect(pending[0].op).toBe('upsert')
      expect(text(pending[0].payload)).toBe('A')
    })

    it('prunes finalized manifest, ledger and inverse rows', async () => {
      const { state } = await openState(statePath())

      await commit(state, {
        operations: [
          operation({ id: 'evt-1', blockNumber: 100, inverse: { op: 'delete', payload: new Uint8Array() } }),
        ],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(90),
      })
      expect((await state.stats()).manifest).toBe(1)

      await commit(state, { operations: [], ledger: [], cursor: block(101), finalized: block(101) })

      expect((await state.stats()).manifest).toBe(0)
    })

    it('drops the baseline of a materialized id whose last finalized revision was a delete', async () => {
      const { state } = await openState(statePath())

      await commit(state, {
        operations: [
          operation({ id: 'candle-1', mode: 'materialized', blockNumber: 100, payload: encoder.encode('A') }),
        ],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(100),
      })
      await commit(state, {
        operations: [
          operation({
            id: 'candle-1',
            mode: 'materialized',
            op: 'delete',
            blockNumber: 101,
            payload: new Uint8Array(),
          }),
        ],
        ledger: [block(101)],
        cursor: block(101),
        finalized: block(101),
      })
      await commit(state, {
        operations: [
          operation({ id: 'candle-1', mode: 'materialized', blockNumber: 102, payload: encoder.encode('C') }),
        ],
        ledger: [block(102)],
        cursor: block(102),
        finalized: block(101),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await state.fork([block(101), block(102, 'b')])

      const pending = await state.pending()
      expect(pending).toHaveLength(1)
      expect(pending[0].op).toBe('delete')
    })
  })
})
