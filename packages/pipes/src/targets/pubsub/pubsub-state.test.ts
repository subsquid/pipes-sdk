import { dirname, join } from 'node:path'

import { describe, expect } from 'vitest'

import { BlockCursor } from '~/core/index.js'
import { SqliteOptions, SqliteSync, loadSqlite } from '~/drivers/sqlite/sqlite.js'
import { testLogger } from '~/testing/index.js'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import {
  COMPACTION_CHUNK_ROWS,
  CommitInput,
  INTERNAL_DRIVER,
  PendingOperation,
  SqlitePubsubState,
} from './pubsub-state.js'
import { downgradeToSchemaV3, inspectStateFile, withStatePath } from './test-support.js'

const encoder = new TextEncoder()
const text = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

type SqlitePubsubStateDriver = (options: SqliteOptions) => Promise<SqliteSync>

type Fault = { error: Error; selfRollback: boolean }

/**
 * A real driver with one statement rigged to fail. `selfRollback` models the SQLITE_FULL /
 * SQLITE_IOERR class, where SQLite unwinds the transaction before handing the error back.
 */
class FaultyDriver {
  #inner?: SqliteSync
  faultOn?: (sql: string) => Fault | undefined

  readonly load: SqlitePubsubStateDriver = async (options) => {
    const inner = await loadSqlite(options)
    this.#inner = inner

    return {
      get: (sql, params) => inner.get(sql, params),
      all: (sql, params) => inner.all(sql, params),
      stream: (sql, params) => inner.stream(sql, params),
      close: () => inner.close(),
      exec: (sql, params) => {
        const fault = this.faultOn?.(sql)
        if (!fault) {
          return inner.exec(sql, params)
        }

        if (fault.selfRollback) {
          inner.exec('ROLLBACK')
        }

        throw fault.error
      },
    }
  }

  /** Reads bypass the fault, so a test can assert what the failed transaction left behind. */
  count(table: string): number {
    return this.#inner!.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`)!.n
  }
}

type OpenOptions = {
  driver?: SqlitePubsubStateDriver
  /** Default `test-pipe`. */
  cursorKey?: string
  allowColdStart?: boolean
  /** Another state file in the same test directory. Default: the test's one state file. */
  file?: string
}

type OpenState = (options?: OpenOptions) => Promise<{ state: SqlitePubsubState; coldStart: boolean }>

/**
 * A test with an `openState` fixture. Every call opens the test's state file (`statePath`) again,
 * or `file` next to it, and every state it opens is closed when the test ends. `withStatePath`
 * then removes the directory.
 */
const withStateFile = withStatePath.extend<{ openState: OpenState }>({
  openState: async ({ statePath }, use) => {
    const opened: SqlitePubsubState[] = []

    await use(async ({ driver, cursorKey = 'test-pipe', allowColdStart, file } = {}) => {
      const path = file ? join(dirname(statePath), `${file}.sqlite`) : statePath
      const state = new SqlitePubsubState({ path, [INTERNAL_DRIVER]: driver })
      const { coldStart } = await state.open({ cursorKey, logger: testLogger(), allowColdStart })
      opened.push(state)

      return { state, coldStart }
    })

    for (const state of opened) {
      await state.close()
    }
  },
})

/** Lets every task queued with setImmediate so far run, including one compaction chunk. */
const nextTurn = () => new Promise((resolve) => setImmediate(resolve))

function sqliteError(code: string, message: string): Error {
  return Object.assign(new Error(message), { code })
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
    route: 'transfers',
    topic: 'transfers',
    orderingKey: '',
    mode: 'event',
    op: 'upsert',
    id: 'row-1',
    idSource: 'draft',
    attributes: { token: '0x42' },
    payload: encoder.encode('{"a":1}'),
    blockNumber: 100,
    rollbackable: true,
    ...overrides,
  }
}

describe('SqlitePubsubState', () => {
  withStateFile('reports a cold start once and resumes warm afterwards', async ({ openState }) => {
    const first = await openState()
    expect(first.coldStart).toBe(true)
    await first.state.close()

    const second = await openState()
    expect(second.coldStart).toBe(false)
  })

  // Stamping the schema version is what makes the next open read as warm, so a caller that
  // refuses a cold start must not have it written behind its back.
  withStateFile('writes no bootstrap markers when the caller forbids a cold start', async ({ openState }) => {
    const { state, coldStart } = await openState({ allowColdStart: false })
    expect(coldStart).toBe(true)
    expect(await state.getMeta('schema_version')).toBeUndefined()
    await state.close()

    expect((await openState()).coldStart).toBe(true)
  })

  withStateFile('round-trips the cursor with an explicit finalized floor', async ({ openState }) => {
    const { state } = await openState()

    expect(await state.getCursor()).toBeUndefined()

    await commit(state, { operations: [], ledger: [], cursor: block(100), finalized: block(90) })

    expect(await state.getCursor()).toEqual({ latest: block(100), finalized: block(90) })
  })

  withStateFile('states the absence of a finalized head as null, never as a missing key', async ({ openState }) => {
    const { state } = await openState()

    await commit(state, { operations: [], ledger: [], cursor: block(100), finalized: null })

    const cursor = await state.getCursor()
    expect(cursor).toHaveProperty('finalized', null)
  })

  withStateFile('assigns one producer-wide sequence across topics and ordering keys', async ({ openState }) => {
    const { state } = await openState()

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
      ['two', 2],
      ['one', 3],
    ])
  })

  withStateFile('continues the sequence after a producer restart on the same state', async ({ openState }) => {
    const first = await openState()
    await commit(first.state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null })
    await first.state.confirm((await first.state.pending()).map((row) => row.rowId))
    await first.state.close()

    const second = await openState()
    await commit(second.state, {
      operations: [operation({ id: 'row-2' })],
      ledger: [],
      cursor: block(101),
      finalized: null,
    })

    expect((await second.state.pending()).map((row) => row.seq)).toEqual([2])
  })

  withStateFile('refuses to repeat the sequence after Number.MAX_SAFE_INTEGER', async ({ openState }) => {
    const { state } = await openState()
    await state.setMeta('sequence', String(Number.MAX_SAFE_INTEGER))

    await expect(
      commit(state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null }),
    ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.SEQUENCE_EXHAUSTED })

    expect(await state.pending()).toHaveLength(0)
    expect(await state.getMeta('sequence')).toBe(String(Number.MAX_SAFE_INTEGER))
  })

  withStateFile('keeps one id source for every materialized route', async ({ openState }) => {
    const { state } = await openState()

    await commit(state, {
      operations: [operation({ id: 'row-1', idSource: 'row', mode: 'materialized' })],
      ledger: [],
      cursor: block(100),
      finalized: null,
    })

    await expect(
      commit(state, {
        operations: [operation({ id: 'row-1', idSource: 'draft', mode: 'materialized', blockNumber: 101 })],
        ledger: [],
        cursor: block(101),
        finalized: null,
      }),
    ).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED })
  })

  withStateFile('keeps unconfirmed outbox rows for the restart drain', async ({ openState }) => {
    const first = await openState()
    await commit(first.state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null })
    await first.state.close()

    const second = await openState()
    const pending = await second.state.pending()

    expect(pending).toHaveLength(1)
    expect(pending[0]).toMatchObject({ topic: 'transfers', op: 'upsert', id: 'row-1', seq: 1 })
    expect(text(pending[0].payload)).toBe('{"a":1}')

    await second.state.confirm([pending[0].rowId])
    expect(await second.state.pending()).toHaveLength(0)
  })

  withStateFile('refuses a second producer on the same state file', async ({ openState }) => {
    await openState()

    await expect(openState()).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.STATE_LOCKED })
  })

  withStateFile('refuses a state file that belongs to another producer', async ({ openState }) => {
    const first = await openState({ cursorKey: 'pipe-a' })
    await commit(first.state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null })
    await first.state.close()

    // The outbox, the manifest and the counters are producer-wide — only the cursor row is
    // keyed — so adopting this file would publish pipe-a's pending operations as pipe-b.
    await expect(openState({ cursorKey: 'pipe-b' })).rejects.toMatchObject({
      code: PUBSUB_ERROR_CODES.STATE_IDENTITY_MISMATCH,
    })
  })

  withStateFile('releases the lock when open refuses, so a corrected retry can proceed', async ({ openState }) => {
    const first = await openState({ cursorKey: 'pipe-a' })
    await first.state.close()

    await expect(openState({ cursorKey: 'pipe-b' })).rejects.toMatchObject({
      code: PUBSUB_ERROR_CODES.STATE_IDENTITY_MISMATCH,
    })

    // A refusal must not leave the file locked — the retry is a first producer, not a second.
    const retry = await openState({ cursorKey: 'pipe-a' })
    expect(retry.coldStart).toBe(false)
  })

  // Only schema 3 migrates in place; anything older is refused rather than guessed at.
  withStateFile('refuses a state file written by a schema it cannot migrate', async ({ openState }) => {
    const first = await openState()
    await first.state.setMeta('schema_version', '2')
    await first.state.close()

    await expect(openState()).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.STATE_SCHEMA_VERSION })
  })

  describe('schema 3 migration', () => {
    withStateFile('keeps the change sequence and the unconfirmed outbox', async ({ statePath, openState }) => {
      const first = await openState()
      await commit(first.state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null })
      await first.state.confirm((await first.state.pending()).map((row) => row.rowId))
      await commit(first.state, {
        operations: [operation({ id: 'row-2' })],
        ledger: [],
        cursor: block(101),
        finalized: null,
      })
      await first.state.close()
      await downgradeToSchemaV3(statePath)

      const migrated = await openState()
      expect(migrated.coldStart).toBe(false)
      expect(await migrated.state.getCursor()).toMatchObject({ latest: { number: 101 } })

      await commit(migrated.state, {
        operations: [operation({ id: 'row-3' })],
        ledger: [],
        cursor: block(102),
        finalized: null,
      })
      const sequenced = (await migrated.state.pending()).map((row) => [row.id, row.seq])
      expect(sequenced).toEqual([
        ['row-2', 2],
        ['row-3', 3],
      ])
    })

    withStateFile('leaves the file in the shape schema 4 creates', async ({ statePath, openState }) => {
      const first = await openState()
      await first.state.close()
      await downgradeToSchemaV3(statePath)

      const migrated = await openState()
      await migrated.state.close()

      const file = await inspectStateFile(statePath)
      expect(file).toMatchObject({ schemaVersion: '4', forkEpoch: undefined })
      expect(file.outboxColumns).not.toContain('kind')
    })

    withStateFile('rewrites the wire config the way schema 4 stores it', async ({ statePath, openState }) => {
      const wireConfig = JSON.stringify(['test-pipe', true, false, '{}', [['transfers', 'canonical']]])

      const first = await openState()
      await first.state.setMeta('wire_config', wireConfig)
      await first.state.close()
      await downgradeToSchemaV3(statePath, { signalRoutes: ['events'] })

      const { state } = await openState()

      expect(await state.getMeta('wire_config')).toBe(wireConfig)
    })

    // Schema 4 has no signal routes, so nothing could ever publish those rows.
    withStateFile(
      'refuses unpublished signal operations and leaves the file as it was',
      async ({ statePath, openState }) => {
        const first = await openState()
        await commit(first.state, { operations: [operation()], ledger: [], cursor: block(100), finalized: null })
        const [signal] = await first.state.pending()
        await first.state.close()
        await downgradeToSchemaV3(statePath, { signalRowIds: [signal.rowId] })

        await expect(openState()).rejects.toMatchObject({ code: PUBSUB_ERROR_CODES.STATE_SCHEMA_VERSION })

        const file = await inspectStateFile(statePath)
        expect(file).toMatchObject({ schemaVersion: '3', forkEpoch: '0' })
        expect(file.outboxColumns).toContain('kind')
      },
    )
  })

  describe('fork compensation', () => {
    withStateFile('deletes an orphaned write-once event, inheriting its attributes', async ({ openState }) => {
      const { state } = await openState()

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
      expect(text(pending[0].payload)).toBe('{"a":1}')
    })

    withStateFile('leaves an event alone when it also exists below the fork point', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile('folds several orphaned revisions of a materialized id into ONE restore', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile('restores the finalized baseline once the surviving revisions are pruned', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile('rolls back an orphaned delete by restoring the revision it removed', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile('emits the route’s stored inverse when every revision is orphaned', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile('keeps the inverse across a restart between first publish and fork', async ({ openState }) => {
      const first = await openState()
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

      const second = await openState()
      await second.state.fork([block(100), block(101, 'b')])

      expect(text((await second.state.pending())[0].payload)).toBe('EMPTY')
    })

    withStateFile('keeps ids whose separator-joined identity would collide apart', async ({ openState }) => {
      const { state } = await openState()

      // ("pool a", "b") and ("pool", "a b") join to the same string under a space separator;
      // grouped together, the fork would publish one compensation where two are owed.
      await commit(state, {
        operations: [
          operation({ id: 'b', orderingKey: 'pool a', blockNumber: 101 }),
          operation({ id: 'a b', orderingKey: 'pool', blockNumber: 101 }),
        ],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await state.confirm((await state.pending()).map((row) => row.rowId))

      await state.fork([block(100), block(101, 'b')])

      // Order follows the manifest scan, not the insertion order; what matters is that both
      // rows are repaired rather than folded into one.
      const pending = await state.pending()
      expect(pending.map((row) => `${row.orderingKey}/${row.id}/${row.op}`).sort()).toEqual([
        'pool a/b/delete',
        'pool/a b/delete',
      ])
    })

    withStateFile('never rewinds the sequence across a fork', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile('lands a compensation on the same partition as the operation it repairs', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile('prunes the orphaned manifest and rewinds the cursor', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile('compensates the whole manifest and returns null on a dead-end fork', async ({ openState }) => {
      const { state } = await openState()

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

  describe('materialized identity', () => {
    async function revise(state: SqlitePubsubState, overrides: Partial<PendingOperation>) {
      await commit(state, {
        operations: [operation({ id: 'candle-1', mode: 'materialized', blockNumber: 101, ...overrides })],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })
    }

    withStateFile('refuses a revision that changes the row’s filter attributes', async ({ openState }) => {
      const { state } = await openState()
      await revise(state, { attributes: { token: 'A' } })

      // A fork restores the row under the attributes it was FIRST published with, so a
      // subscription filtered on B would keep the orphaned revision forever.
      await expect(revise(state, { attributes: { token: 'B' } })).rejects.toMatchObject({
        code: PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED,
      })
    })

    withStateFile(
      'refuses filter attribute changes across a restart when the source cannot fork',
      async ({ openState }) => {
        let { state } = await openState()

        async function commitFinalizedRevision(blockNumber: number, token: string) {
          await state.commit({
            operations: [
              operation({
                id: 'candle-1',
                mode: 'materialized',
                blockNumber,
                attributes: { token },
                rollbackable: false,
              }),
            ],
            ledger: [],
            cursor: block(blockNumber),
            finalized: block(blockNumber),
            forkCapable: false,
          })
        }

        await commitFinalizedRevision(101, 'A')
        await state.close()
        state = (await openState()).state

        // No fork is required for corruption: a subscription filtered on A never receives the
        // revision carrying B, so it keeps the old materialized value indefinitely.
        await expect(commitFinalizedRevision(102, 'B')).rejects.toMatchObject({
          code: PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED,
        })
      },
    )

    withStateFile('refuses a revision that moves the row to another partition or topic', async ({ openState }) => {
      const first = await openState()
      await revise(first.state, { orderingKey: 'pool-a' })
      await expect(revise(first.state, { orderingKey: 'pool-b' })).rejects.toMatchObject({
        code: PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED,
      })

      const second = await openState({ file: 'second' })
      await revise(second.state, { topic: 'one' })
      await expect(revise(second.state, { topic: 'two' })).rejects.toMatchObject({
        code: PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED,
      })
    })

    withStateFile('accepts the same attributes written in a different order', async ({ openState }) => {
      const { state } = await openState()

      await revise(state, { attributes: { token: 'A', pool: 'P' } })
      await expect(revise(state, { attributes: { pool: 'P', token: 'A' } })).resolves.toBeUndefined()
    })

    withStateFile(
      'holds the invariant against a row that only exists as a finalized baseline',
      async ({ openState }) => {
        const { state } = await openState()

        await commit(state, {
          operations: [
            operation({
              id: 'candle-1',
              mode: 'materialized',
              blockNumber: 100,
              attributes: { token: 'A' },
              rollbackable: false,
            }),
          ],
          ledger: [block(100)],
          cursor: block(100),
          finalized: block(100),
        })

        await expect(revise(state, { attributes: { token: 'B' } })).rejects.toMatchObject({
          code: PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED,
        })
      },
    )

    withStateFile('leaves event routes alone — a write-once id is never revised', async ({ openState }) => {
      const { state } = await openState()

      await commit(state, {
        operations: [operation({ id: 'evt', attributes: { token: 'A' }, blockNumber: 101 })],
        ledger: [block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await expect(
        commit(state, {
          operations: [operation({ id: 'evt', attributes: { token: 'B' }, blockNumber: 102 })],
          ledger: [block(102)],
          cursor: block(102),
          finalized: block(90),
        }),
      ).resolves.toBeUndefined()
    })
  })

  describe('finalization advance', () => {
    withStateFile('baselines a materialized row that arrives already finalized', async ({ openState }) => {
      const { state } = await openState()

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

    withStateFile(
      'drops the baseline of a materialized id whose last finalized revision was a delete',
      async ({ openState }) => {
        const { state } = await openState()

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
      },
    )
  })

  describe('compaction', () => {
    withStateFile('hides finalized rows at once and deletes them later', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })

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
      expect(driver.count('manifest')).toBe(1)

      await state.compact()

      expect(driver.count('manifest')).toBe(0)
      expect(driver.count('ledger_blocks')).toBe(0)
      expect(driver.count('rollback_inverse')).toBe(0)
    })

    withStateFile('deletes one chunk per event loop turn, so commits run in between', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })
      const rows = COMPACTION_CHUNK_ROWS * 2 + 1

      await commit(state, {
        operations: Array.from({ length: rows }, (_, i) => operation({ id: `evt-${i}`, blockNumber: 100 })),
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(90),
      })
      await commit(state, { operations: [], ledger: [], cursor: block(101), finalized: block(101) })

      await nextTurn()
      expect(driver.count('manifest')).toBe(rows - COMPACTION_CHUNK_ROWS)

      await commit(state, {
        operations: [operation({ id: 'evt-live', blockNumber: 102 })],
        ledger: [block(102)],
        cursor: block(102),
        finalized: block(101),
      })

      await nextTurn()
      await nextTurn()
      expect(driver.count('manifest')).toBe(1)
      expect((await state.stats()).manifest).toBe(1)
    })

    withStateFile('finishes on the next open what the previous run left', async ({ openState }) => {
      const { state: first } = await openState()

      await commit(first, {
        operations: [operation({ id: 'evt-1', blockNumber: 100 })],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(90),
      })
      await commit(first, { operations: [], ledger: [], cursor: block(101), finalized: block(101) })
      await first.close()

      const driver = new FaultyDriver()
      await openState({ driver: driver.load })
      expect(driver.count('manifest')).toBe(1)

      await nextTurn()
      expect(driver.count('manifest')).toBe(0)
    })

    withStateFile('keeps the rows after a failed chunk and deletes them on a retry', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })

      await commit(state, {
        operations: [operation({ id: 'evt-1', blockNumber: 100 })],
        ledger: [block(100)],
        cursor: block(100),
        finalized: block(90),
      })

      driver.faultOn = (sql) =>
        sql.startsWith('DELETE FROM manifest WHERE rowid')
          ? { error: sqliteError('SQLITE_FULL', 'database or disk is full'), selfRollback: true }
          : undefined
      await commit(state, { operations: [], ledger: [], cursor: block(101), finalized: block(101) })
      await nextTurn()

      expect(driver.count('manifest')).toBe(1)
      expect((await state.stats()).manifest).toBe(0)

      driver.faultOn = undefined
      await commit(state, { operations: [], ledger: [], cursor: block(102), finalized: block(102) })
      await nextTurn()

      expect(driver.count('manifest')).toBe(0)
    })

    withStateFile('lets a materialized id change identity after its finalized delete', async ({ openState }) => {
      const { state } = await openState()

      await commit(state, {
        operations: [operation({ id: 'candle-1', mode: 'materialized', blockNumber: 100, attributes: { token: 'A' } })],
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
            attributes: { token: 'A' },
            payload: new Uint8Array(),
          }),
        ],
        ledger: [block(101)],
        cursor: block(101),
        finalized: block(101),
      })

      // The finalized rows are not compacted yet, but the deleted id is free all the same.
      await expect(
        commit(state, {
          operations: [
            operation({ id: 'candle-1', mode: 'materialized', blockNumber: 102, attributes: { token: 'B' } }),
          ],
          ledger: [block(102)],
          cursor: block(102),
          finalized: block(101),
        }),
      ).resolves.toBeUndefined()
    })

    withStateFile('resolves a fork without the finalized ledger blocks', async ({ openState }) => {
      const { state } = await openState()

      await commit(state, {
        operations: [operation({ id: 'evt-1', blockNumber: 101 })],
        ledger: [block(100), block(101)],
        cursor: block(101),
        finalized: block(90),
      })
      await commit(state, {
        operations: [operation({ id: 'evt-2', blockNumber: 102 })],
        ledger: [block(102)],
        cursor: block(102),
        finalized: block(101),
      })

      // Block 101 is the finalized floor and is not compacted yet. The search must not reach it
      // and take it as the common ancestor.
      expect(await state.fork([block(102, 'b')])).toBeNull()
    })

    withStateFile('replaces the inverse of an id whose rows are all finalized', async ({ openState }) => {
      const { state } = await openState()

      await commit(state, {
        operations: [
          operation({
            id: 'candle-1',
            mode: 'materialized',
            op: 'delete',
            blockNumber: 100,
            payload: new Uint8Array(),
            inverse: { op: 'upsert', payload: encoder.encode('old') },
          }),
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
            blockNumber: 101,
            payload: encoder.encode('C'),
            inverse: { op: 'upsert', payload: encoder.encode('new') },
          }),
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
      expect(text(pending[0].payload)).toBe('new')
    })
  })

  describe('storage faults', () => {
    withStateFile('reports the disk failure, not the rollback that SQLite already performed', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })

      driver.faultOn = (sql) =>
        sql.startsWith('INSERT INTO manifest')
          ? { error: sqliteError('SQLITE_FULL', 'database or disk is full'), selfRollback: true }
          : undefined

      const failure = await commit(state, {
        operations: [operation()],
        ledger: [block(100)],
        cursor: block(100),
        finalized: null,
      }).catch((e) => e)

      expect(failure).toBeInstanceOf(PubsubTargetError)
      expect(failure.code).toBe(PUBSUB_ERROR_CODES.STATE_WRITE_FAILED)
      expect(failure.message).toContain('database or disk is full')
      expect(failure.message).toContain('nothing was published')
      expect(failure.message).not.toContain('no transaction is active')
      expect(failure.cause).toMatchObject({ code: 'SQLITE_FULL' })
    })

    withStateFile('leaves nothing of the failed batch behind', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })

      driver.faultOn = (sql) =>
        sql.startsWith('INSERT INTO ledger_blocks')
          ? { error: sqliteError('SQLITE_FULL', 'database or disk is full'), selfRollback: true }
          : undefined

      await expect(
        commit(state, { operations: [operation()], ledger: [block(100)], cursor: block(100), finalized: null }),
      ).rejects.toThrow(PubsubTargetError)

      driver.faultOn = undefined
      expect(driver.count('outbox')).toBe(0)
      expect(driver.count('manifest')).toBe(0)
      expect(await state.getCursor()).toBeUndefined()
    })

    withStateFile('still rolls back a failure that leaves the transaction open', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })

      driver.faultOn = (sql) =>
        sql.startsWith('INSERT INTO manifest')
          ? { error: sqliteError('SQLITE_CONSTRAINT_PRIMARYKEY', 'UNIQUE constraint failed'), selfRollback: false }
          : undefined

      const failure = await commit(state, {
        operations: [operation()],
        ledger: [block(100)],
        cursor: block(100),
        finalized: null,
      }).catch((e) => e)

      // Not a storage failure: the driver error reaches the caller untranslated.
      expect(failure).not.toBeInstanceOf(PubsubTargetError)
      expect(failure.code).toBe('SQLITE_CONSTRAINT_PRIMARYKEY')

      driver.faultOn = undefined
      expect(driver.count('outbox')).toBe(0)
    })

    withStateFile('reports a disk failure while draining the outbox', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })

      await commit(state, { operations: [operation()], ledger: [block(100)], cursor: block(100), finalized: null })
      const [row] = await state.pending()

      driver.faultOn = (sql) =>
        sql.startsWith('DELETE FROM outbox')
          ? { error: sqliteError('SQLITE_IOERR_WRITE', 'disk I/O error'), selfRollback: true }
          : undefined

      const failure = await state.confirm([row.rowId]).catch((e) => e)

      expect(failure.code).toBe(PUBSUB_ERROR_CODES.STATE_WRITE_FAILED)
      expect(failure.message).toContain('disk I/O error')

      // The batch is already on the wire here. Saying it was not would send an operator
      // rewinding the cursor and replaying messages consumers have seen.
      expect(failure.message).toContain('already published')
      expect(failure.message).not.toContain('nothing was published')

      // The row stays queued: an unconfirmed publish is retried, never dropped.
      driver.faultOn = undefined
      expect(await state.pending()).toHaveLength(1)
    })

    withStateFile('reports a volume remounted read-only under the producer', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })

      driver.faultOn = (sql) =>
        sql.startsWith('INSERT INTO outbox')
          ? { error: sqliteError('SQLITE_READONLY', 'attempt to write a readonly database'), selfRollback: false }
          : undefined

      const failure = await commit(state, {
        operations: [operation()],
        ledger: [block(100)],
        cursor: block(100),
        finalized: null,
      }).catch((e) => e)

      expect(failure.code).toBe(PUBSUB_ERROR_CODES.STATE_WRITE_FAILED)
      expect(failure.message).toContain('attempt to write a readonly database')
    })

    withStateFile('clears a transaction left open by a rollback that could not run', async ({ openState }) => {
      const driver = new FaultyDriver()
      const { state } = await openState({ driver: driver.load })

      // Neither the write nor the rollback after it gets through, so the transaction that the
      // failed batch opened is still there when the next one starts.
      driver.faultOn = (sql) => {
        if (sql.startsWith('INSERT INTO manifest')) {
          return { error: sqliteError('SQLITE_CONSTRAINT_PRIMARYKEY', 'UNIQUE constraint failed'), selfRollback: false }
        }
        if (sql === 'ROLLBACK') {
          return { error: sqliteError('SQLITE_BUSY', 'database is locked'), selfRollback: false }
        }

        return undefined
      }

      await expect(
        commit(state, { operations: [operation()], ledger: [block(100)], cursor: block(100), finalized: null }),
      ).rejects.toThrow('UNIQUE constraint failed')

      driver.faultOn = undefined
      await commit(state, {
        operations: [operation({ id: 'row-2' })],
        ledger: [block(101)],
        cursor: block(101),
        finalized: null,
      })

      expect((await state.getCursor())?.latest).toEqual(block(101))
    })
  })
})
