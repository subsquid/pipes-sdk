import type { BigQuery } from '@google-cloud/bigquery'
import { managedwriter } from '@google-cloud/bigquery-storage'
import * as protobuf from 'protobufjs'
import { type Mock, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { BatchContext, BlockCursor, Logger } from '~/core/index.js'
import { mockMetricsServer, testLogger } from '~/testing/index.js'

import { BigQuerySyncState } from './bigquery-state.js'
import { BigQueryWriter, type ProtoWriterFactory } from './bigquery-store.js'
import { bigqueryTarget } from './bigquery-target.js'
import { BIGQUERY_ERROR_CODES } from './errors.js'
import type { TrackedTable } from './tables.js'

/** Decode pre-encoded proto rows back to JSON for assertion-side inspection. */
function decodeProtoRows(
  serializedRows: Uint8Array[],
  descriptor: Parameters<ProtoWriterFactory>[0]['protoDescriptor'],
) {
  const Type = protobuf.Type.fromDescriptor(descriptor as Parameters<typeof protobuf.Type.fromDescriptor>[0])
  return serializedRows.map((b) => Type.decode(b).toJSON() as Record<string, unknown>)
}

/**
 * A fake proto Writer factory for tests. Avoids the real `managedwriter.Writer`, which
 * needs a live gRPC StreamConnection. The store never inspects the writer's return values,
 * so a no-op resolve is enough — what tests assert is what flows to `WriterClient` (mocked
 * via `makeWriter`) and to `BigQuery` (mocked via `makeBigQuery`).
 */
const fakeProtoWriterFactory: ProtoWriterFactory = () => ({
  appendRows: () => ({ getResult: async () => ({ acked: true }) }),
  close: () => {},
})

// -----------------------------------------------------------------------------
// Mock factories for the @google-cloud clients
// -----------------------------------------------------------------------------

type StreamConnectionStub = {
  finalize: ReturnType<typeof vi.fn>
  close: ReturnType<typeof vi.fn>
  on: ReturnType<typeof vi.fn>
}

function makeWriter() {
  const calls = {
    createWriteStream: [] as string[],
    createStreamConnection: [] as string[],
    finalize: [] as string[],
    batchCommitWriteStream: [] as { parent: string; writeStreams: string[] }[],
    close: 0,
  }
  let nextStreamId = 0

  const writer = {
    createWriteStream: vi.fn(async ({ destinationTable }: { destinationTable: string }) => {
      calls.createWriteStream.push(destinationTable)
      return `${destinationTable}/streams/${++nextStreamId}`
    }),
    createStreamConnection: vi.fn(async ({ streamId }: { streamId: string }) => {
      calls.createStreamConnection.push(streamId)
      const conn: StreamConnectionStub = {
        finalize: vi.fn(async () => {
          calls.finalize.push(streamId)
          return {}
        }),
        close: vi.fn(),
        on: vi.fn(),
      }
      return conn
    }),
    batchCommitWriteStream: vi.fn(async (req: { parent: string; writeStreams: string[] }) => {
      calls.batchCommitWriteStream.push(req)
      return {}
    }),
    close: vi.fn(() => {
      calls.close++
    }),
  }

  return { writer: writer as unknown as managedwriter.WriterClient, calls }
}

function makeBigQuery(
  opts: { metadata?: unknown; metadataError?: unknown; queryRows?: unknown[]; numDmlAffectedRows?: string } = {},
) {
  const dmlCalls: { sql: string; params: Record<string, unknown> }[] = []
  const queryCalls: { sql: string; params: Record<string, unknown> }[] = []

  // Default metadata satisfies the schema diff for all fields TABLES declares.
  const defaultMetadata = {
    rangePartitioning: { field: 'block_number' },
    schema: {
      fields: [
        { name: 'block_number', type: 'INT64', mode: 'REQUIRED' },
        { name: 'tx_hash', type: 'STRING' },
        { name: 'value', type: 'STRING' },
      ],
    },
  }

  const bq = {
    projectId: 'p',
    query: vi.fn(async ({ query, params }: { query: string; params?: Record<string, unknown> }) => {
      queryCalls.push({ sql: query, params: params ?? {} })
      return [opts.queryRows ?? []]
    }),
    createQueryJob: vi.fn(async ({ query, params }: { query: string; params?: Record<string, unknown> }) => {
      dmlCalls.push({ sql: query, params: params ?? {} })
      const job = {
        getQueryResults: vi.fn(async () => [[]]),
        metadata: { statistics: { query: { numDmlAffectedRows: opts.numDmlAffectedRows ?? '0' } } },
      }
      return [job]
    }),
    dataset: vi.fn(() => ({
      table: vi.fn(() => ({
        getMetadata: vi.fn(async () => {
          if (opts.metadataError) throw opts.metadataError
          return [opts.metadata ?? defaultMetadata, undefined]
        }),
      })),
    })),
  }

  return { bq: bq as unknown as BigQuery, dmlCalls, queryCalls }
}

const TABLES: TrackedTable[] = [
  {
    table: 'events',
    blockNumberColumn: 'block_number',
    schema: [
      { name: 'block_number', type: 'INT64', mode: 'REQUIRED' },
      { name: 'tx_hash', type: 'STRING' },
    ],
  },
  {
    table: 'transfers',
    blockNumberColumn: 'block_number',
    schema: [
      { name: 'block_number', type: 'INT64', mode: 'REQUIRED' },
      { name: 'value', type: 'STRING' },
    ],
  },
]

const cursor = (number: number, hash = `0x${number}`): BlockCursor => ({ number, hash })

function makeBatchContext(
  current: BlockCursor,
  rollbackChain: BlockCursor[] = [],
  initial = 0,
  metrics?: ReturnType<typeof mockMetricsServer>,
): BatchContext {
  const profilerStub: Record<string, unknown> = {
    start: () => profilerStub,
    measure: async (_: unknown, fn: () => unknown) => fn(),
    end: () => {},
  }
  return {
    id: 'test-pipe',
    logger: testLogger(),
    profiler: profilerStub,
    metrics: (metrics ?? mockMetricsServer()).server.metrics,
    stream: {
      state: { current, rollbackChain, initial },
      head: { finalized: undefined, latest: current },
    },
  } as unknown as BatchContext
}

// -----------------------------------------------------------------------------
// BigQueryWriter — Pending pipeline (covers the SDK-mocked path)
// -----------------------------------------------------------------------------

describe('BigQueryWriter — Committed stream pipeline', () => {
  afterEach(() => vi.restoreAllMocks())

  it('reuses one long-lived proto Writer per table across batches (no per-batch churn)', async () => {
    // Committed streams are opened once per table and reused — every batch uses the same
    // Writer so we don't hit GFE with `CreateWriteStream` storms and don't leak per-batch
    // schema-update listeners on the underlying connection.
    let writersConstructed = 0
    let writersClosed = 0
    const { writer } = makeWriter()
    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: () => {
        writersConstructed++
        return {
          appendRows: () => ({ getResult: async () => ({}) }),
          close: () => {
            writersClosed++
          },
        }
      },
    })
    store.insert('events', [{ block_number: 1 }])
    await store.commitBatch()
    store.insert('events', [{ block_number: 2 }])
    await store.commitBatch()

    // Same table written twice → exactly one writer constructed, none closed yet.
    expect(writersConstructed).toBe(1)
    expect(writersClosed).toBe(0)

    // Closing the store tears the cached writer down.
    store.close()
    expect(writersClosed).toBe(1)
  })

  it('passes a cumulative per-stream offset to Writer.appendRows across batches', async () => {
    // Committed streams use the cumulative offset for exactly-once semantics: a retry that
    // resends the same offset is server-deduped, so a lost ack never produces a duplicate
    // row. The offset MUST keep climbing across batches against the same long-lived stream
    // — resetting to 0 each batch would let server-side dedup mistake new rows for retries.
    const appendCalls: { rowCount: number; offset: number | string | null | undefined }[] = []
    const { writer } = makeWriter()
    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: () => ({
        appendRows: ({ serializedRows }, offset) => {
          appendCalls.push({ rowCount: serializedRows.length, offset })
          return { getResult: async () => ({}) }
        },
        close: () => {},
      }),
    })

    store.insert('events', [{ block_number: 1 }, { block_number: 2 }])
    await store.commitBatch()
    store.insert('events', [{ block_number: 3 }, { block_number: 4 }, { block_number: 5 }])
    await store.commitBatch()

    expect(appendCalls).toHaveLength(2)
    expect(appendCalls[0].offset).toBe(0)
    expect(appendCalls[0].rowCount).toBe(2)
    // Second batch picks up where the first left off — 0 + 2 = 2.
    expect(appendCalls[1].offset).toBe(2)
    expect(appendCalls[1].rowCount).toBe(3)
  })

  it('opens one Committed stream per table on first write, reuses it for subsequent batches', async () => {
    const { writer, calls } = makeWriter()
    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    await store.commitBatch()
    store.insert('events', [{ block_number: 2, tx_hash: '0xb' }])
    await store.commitBatch()

    // One create per table across both batches — no per-batch CreateWriteStream churn.
    expect(calls.createWriteStream).toHaveLength(1)
    expect(calls.createWriteStream[0]).toBe('projects/p/datasets/d/tables/events')
    expect(calls.createStreamConnection).toHaveLength(1)
    // No finalize / batchCommit on Committed streams — rows are visible immediately.
    expect(calls.finalize).toHaveLength(0)
    expect(calls.batchCommitWriteStream).toHaveLength(0)
  })

  it('opens a separate Committed stream per tracked table', async () => {
    const { writer, calls } = makeWriter()
    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })

    store.insert('events', [{ block_number: 1 }])
    store.insert('transfers', [{ block_number: 1 }])
    await store.commitBatch()

    const targets = calls.createWriteStream.sort()
    expect(targets).toEqual(['projects/p/datasets/d/tables/events', 'projects/p/datasets/d/tables/transfers'])
  })

  it('dispatches per-table pipelines in parallel via Promise.all (N3)', async () => {
    const { writer, calls } = makeWriter()
    let inFlightAppends = 0
    let maxInFlight = 0

    // Slow down createWriteStream so we can observe overlap.
    writer.createWriteStream = vi.fn(async ({ destinationTable }) => {
      calls.createWriteStream.push(destinationTable)
      inFlightAppends++
      maxInFlight = Math.max(maxInFlight, inFlightAppends)
      await new Promise((r) => setTimeout(r, 5))
      inFlightAppends--
      return `${destinationTable}/streams/x`
    }) as typeof writer.createWriteStream

    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })

    store.insert('events', [{ block_number: 1 }])
    store.insert('transfers', [{ block_number: 1 }])
    await store.commitBatch()

    expect(maxInFlight).toBe(2)
  })

  it('commitBatch is a no-op when buffer is empty', async () => {
    const { writer, calls } = makeWriter()
    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })
    await store.commitBatch()
    expect(calls.createWriteStream).toHaveLength(0)
  })

  it('commitSyncRow uses the sync table FQN and bypasses the allowlist', async () => {
    const { writer, calls } = makeWriter()
    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })
    await store.commitSyncRow([{ name: 'id', type: 'STRING', mode: 'REQUIRED' }], { id: 'stream' })
    expect(calls.createWriteStream[0]).toBe('projects/p/datasets/d/tables/sync')
  })

  it('executeDml calls createQueryJob and parses numDmlAffectedRows', async () => {
    const { writer } = makeWriter()
    const { bq, dmlCalls } = makeBigQuery({ numDmlAffectedRows: '42' })
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })
    const result = await store.executeDml('DELETE FROM x WHERE id = @id', { id: 1 })
    expect(result.rowCount).toBe(42)
    expect(dmlCalls).toHaveLength(1)
    expect(dmlCalls[0].params).toEqual({ id: 1 })
  })

  it('query passes params and returns rows from the BQ client', async () => {
    const { writer } = makeWriter()
    const { bq, queryCalls } = makeBigQuery({ queryRows: [{ a: 1 }, { a: 2 }] })
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })
    const rows = await store.query('SELECT * FROM x WHERE id = @id', { id: 'stream' })
    expect(rows).toEqual([{ a: 1 }, { a: 2 }])
    expect(queryCalls[0].params).toEqual({ id: 'stream' })
  })

  it('close() invokes WriterClient.close()', () => {
    const { writer, calls } = makeWriter()
    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })
    store.close()
    expect(calls.close).toBe(1)
  })

  it('attaches an error listener on StreamConnection so transient gRPC errors do not crash the process', async () => {
    // Regression for the production crash: SDK StreamConnection re-emits transport errors
    // via emit('error', err) even after the matching getResult() promise has been rejected.
    // For RESOURCE_EXHAUSTED (code 8) the SDK's own suppress branch is bypassed (code 8 not
    // in StreamConnection.isRetryableError), so the second emit ALWAYS runs. Without a
    // listener attached, Node EventEmitter throws synchronously → uncaught exception →
    // process exits before doWithRetry sees the rejected promise. The store must attach a
    // no-op listener so the emit is safely consumed.
    const { EventEmitter } = await import('node:events')
    const realConnections: import('node:events').EventEmitter[] = []

    const writerFake = {
      createWriteStream: vi.fn(async ({ destinationTable }: { destinationTable: string }) => {
        return `${destinationTable}/streams/1`
      }),
      createStreamConnection: vi.fn(async () => {
        const conn = new EventEmitter()
        realConnections.push(conn)
        // The store doesn't call finalize/close on the connection in the happy path covered
        // here, but include them so the type cast still works if call-paths change.
        ;(conn as unknown as { close: () => void }).close = () => {}
        return conn
      }),
      close: vi.fn(),
    }

    const { bq } = makeBigQuery()
    const store = new BigQueryWriter(bq, writerFake as unknown as managedwriter.WriterClient, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })

    // Trigger stream creation by writing a row.
    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    await store.commitBatch()

    expect(realConnections).toHaveLength(1)
    const conn = realConnections[0]

    // Without an attached 'error' listener, Node EventEmitter throws synchronously here
    // (see https://nodejs.org/api/events.html#error-events). The store must have attached
    // a listener inside #getOrCreateStream — assert that this emit is safely consumed.
    expect(() =>
      conn.emit('error', { code: 8, message: 'RESOURCE_EXHAUSTED: Bandwidth exhausted or memory limit exceeded' }),
    ).not.toThrow()

    // The error listener must be present on the connection (proof of contract, not just
    // accidental absence of throw — the listener should be from our store, not the SDK).
    expect(conn.listenerCount('error')).toBeGreaterThan(0)

    // Store remains usable after the transient stream error — next batch goes through
    // the same cached stream (SDK auto-reconnects internally on send()).
    store.insert('events', [{ block_number: 2, tx_hash: '0xb' }])
    await expect(store.commitBatch()).resolves.toMatchObject({ events: { rows: 1 } })
  })
})

// -----------------------------------------------------------------------------
// bigqueryTarget — write/fork lifecycle orchestration
// -----------------------------------------------------------------------------

describe('bigqueryTarget — write lifecycle', () => {
  let writerSetup: ReturnType<typeof makeWriter>
  let bqSetup: ReturnType<typeof makeBigQuery>

  beforeEach(() => {
    writerSetup = makeWriter()
    bqSetup = makeBigQuery()
  })

  afterEach(() => vi.restoreAllMocks())

  function buildTarget(opts?: {
    onData?: (ctx: { store: BigQueryWriter; data: unknown; ctx: unknown }) => void | Promise<void>
    onStart?: (ctx: { store: BigQueryWriter; logger: unknown }) => void | Promise<void>
    onBeforeRollback?: (ctx: { cursor: BlockCursor }) => void | Promise<void>
    onAfterRollback?: (ctx: { cursor: BlockCursor }) => void | Promise<void>
  }) {
    return bigqueryTarget<unknown>({
      client: { bigquery: bqSetup.bq, writer: writerSetup.writer },
      dataset: 'd',
      tables: TABLES,
      settings: { protoWriterFactory: fakeProtoWriterFactory },
      onStart: opts?.onStart,
      onData: opts?.onData ?? (() => {}),
      onBeforeRollback: opts?.onBeforeRollback,
      onAfterRollback: opts?.onAfterRollback,
    })
  }

  it('throws if projectId cannot be inferred', () => {
    expect(() =>
      bigqueryTarget<unknown>({
        client: { bigquery: { query: vi.fn() } as unknown as BigQuery, writer: writerSetup.writer },
        dataset: 'd',
        tables: TABLES,
        onData: () => {},
      }),
    ).toThrow(/cannot determine GCP project id/i)
  })

  it('calls onStart before reading the first batch', async () => {
    const onStart = vi.fn()
    const target = buildTarget({ onStart })
    async function* read() {
      // empty
    }
    await target.write({ read: read as never, logger: testLogger() })
    expect(onStart).toHaveBeenCalledTimes(1)
  })

  it('validates / auto-creates each tracked table during onStart phase', async () => {
    const target = buildTarget()
    async function* read() {}
    await target.write({ read: read as never, logger: testLogger() })
    // dataset().table() should have been called for each tracked table.
    expect(bqSetup.bq.dataset).toHaveBeenCalled()
  })

  it('per batch: writes IN_FLIGHT_COMMIT sync row → onData → commitBatch → COMMITTED sync row', async () => {
    const onData = vi.fn(async ({ store, data }) => {
      store.insert('events', [data as Record<string, unknown>])
    })
    const target = buildTarget({ onData })

    async function* read() {
      yield { data: { block_number: 10, tx_hash: '0xa' }, ctx: makeBatchContext(cursor(10)) }
    }

    await target.write({ read: read as never, logger: testLogger() })

    // Committed streams: one CreateWriteStream per table per process, then long-lived. So a
    // single batch over one tracked table opens TWO streams: sync + events. The two sync
    // rows (IN_FLIGHT_COMMIT + COMMITTED) flow through the same long-lived sync stream.
    const syncCreates = writerSetup.calls.createWriteStream.filter((p) => p.endsWith('/sync'))
    expect(syncCreates).toHaveLength(1)
    const dataCreates = writerSetup.calls.createWriteStream.filter((p) => p.endsWith('/events'))
    expect(dataCreates).toHaveLength(1)

    expect(onData).toHaveBeenCalledTimes(1)
  })

  it('first batch uses ctx.stream.state.initial (not 0) as the IN_FLIGHT_COMMIT range_low', async () => {
    // If we hardcoded `low = 0` when no previousCursor exists, recovery on a first-batch
    // crash for a backfill starting at block 12_345_678 would issue
    // `DELETE WHERE block_number BETWEEN 0 AND 12_345_678` across every tracked table —
    // wiping out any rows from a prior run. The fix uses ctx.stream.state.initial as the
    // lower bound for the very first batch.
    //
    // We assert by decoding the proto rows handed to the writer: the first sync-table
    // append must carry range_low = 12_345_678 (initial), not 0.
    const syncRowsAppended: Record<string, unknown>[] = []
    const target = bigqueryTarget<unknown>({
      client: { bigquery: bqSetup.bq, writer: writerSetup.writer },
      dataset: 'd',
      tables: TABLES,
      settings: {
        protoWriterFactory: ({ protoDescriptor }) => ({
          appendRows: ({ serializedRows }) => {
            const decoded = decodeProtoRows(serializedRows, protoDescriptor)
            // Sync rows carry the WAL `op` field; data rows don't — distinguish by descriptor
            // shape rather than table FQN since the factory call doesn't carry the table.
            for (const row of decoded) if ('op' in row) syncRowsAppended.push(row)
            return { getResult: async () => ({}) }
          },
          close: () => {},
        }),
      },
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
    })

    async function* read() {
      yield {
        data: { block_number: 12_345_678, tx_hash: '0xa' },
        ctx: makeBatchContext(cursor(12_345_678), [], 12_345_678 /* initial */),
      }
    }
    await target.write({ read: read as never, logger: testLogger() })

    const inFlightRow = syncRowsAppended[0] as { range_low: string | number; committed: boolean }
    expect(inFlightRow.committed).toBe(false)
    // INT64 fields decode as strings via protobufjs (Long-safety) — coerce to compare.
    expect(Number(inFlightRow.range_low)).toBe(12_345_678) // NOT 0 — would over-delete on recovery
  })

  it('opens sync stream first (saveCommitPre), then data stream (commitBatch); reuses sync for COMMITTED', async () => {
    // Committed streams: each table's stream is opened on first write and reused. The WAL
    // ordering pre-sync → data → post-sync still holds, but we observe it via stream-create
    // order, not via per-batch commit calls (Committed has none).
    const onData = vi.fn(async ({ store, data }) => {
      store.insert('events', [data as Record<string, unknown>])
    })
    const target = buildTarget({ onData })

    async function* read() {
      yield { data: { block_number: 10 }, ctx: makeBatchContext(cursor(10)) }
    }

    await target.write({ read: read as never, logger: testLogger() })

    // First create: sync (from saveCommitPre). Second: events (from commitBatch). The
    // post-commit sync row writes through the existing sync stream — no third create.
    expect(writerSetup.calls.createWriteStream).toEqual([
      'projects/p/datasets/d/tables/sync',
      'projects/p/datasets/d/tables/events',
    ])
  })

  it('does not close the writer in write() finally (avoids race with concurrent fork())', async () => {
    // fork() runs inside the read() generator that this for-await consumes; closing the
    // writer at end-of-write would race with any in-flight fork that needs to write WAL
    // sync rows. The WriterClient is owned by the user — explicit shutdown is their job.
    const target = buildTarget({
      onData: async () => {
        throw new Error('user code blew up')
      },
    })
    async function* read() {
      yield { data: {}, ctx: makeBatchContext(cursor(10)) }
    }
    await expect(target.write({ read: read as never, logger: testLogger() })).rejects.toThrow(/user code blew up/)
    expect(writerSetup.calls.close).toBe(0)
  })

  it('closes an internally-constructed writer even when onStart throws (startup-failure leak guard)', async () => {
    // The writer-close finally must wrap the ENTIRE write() body, including
    // ensureTrackedTable, onStart, and getCursor. Otherwise a startup throw leaks the
    // gRPC handle for the rest of the process lifetime.
    const desc = Object.getOwnPropertyDescriptor(managedwriter, 'WriterClient')!
    let closed = 0
    Object.defineProperty(managedwriter, 'WriterClient', {
      configurable: true,
      get: () =>
        function (this: unknown) {
          return { close: () => closed++ } as unknown as managedwriter.WriterClient
        },
    })
    try {
      const target = bigqueryTarget<unknown>({
        client: { bigquery: bqSetup.bq },
        dataset: 'd',
        tables: TABLES,
        settings: { protoWriterFactory: fakeProtoWriterFactory },
        onStart: async () => {
          throw new Error('startup blew up')
        },
        onData: () => {},
      })
      async function* read() {}
      await expect(target.write({ read: read as never, logger: testLogger() })).rejects.toThrow(/startup blew up/)
      expect(closed).toBe(1) // writer closed despite startup throw
    } finally {
      Object.defineProperty(managedwriter, 'WriterClient', desc)
    }
  })

  it('closes the writer when WE constructed it internally (no leak on the docs path)', async () => {
    // When the user passes only `client.bigquery` and we allocate the WriterClient
    // internally, write()'s finally must close it — otherwise the gRPC channel leaks
    // for the lifetime of the process. fork() can no longer fire after write() exits, so
    // close-in-finally is safe here.
    //
    // managedwriter.WriterClient is exposed as a getter, so we redefine it temporarily
    // via Object.defineProperty (the descriptor is configurable per the SDK's namespace).
    const desc = Object.getOwnPropertyDescriptor(managedwriter, 'WriterClient')!
    let constructed = 0
    let closed = 0
    Object.defineProperty(managedwriter, 'WriterClient', {
      configurable: true,
      get: () =>
        function (this: unknown) {
          constructed++
          return { close: () => closed++ } as unknown as managedwriter.WriterClient
        },
    })
    try {
      const target = bigqueryTarget<unknown>({
        client: { bigquery: bqSetup.bq }, // no writer — internal construction
        dataset: 'd',
        tables: TABLES,
        settings: { protoWriterFactory: fakeProtoWriterFactory },
        onData: () => {},
      })
      async function* read() {}
      await target.write({ read: read as never, logger: testLogger() })
      expect(constructed).toBe(1)
      expect(closed).toBe(1)
    } finally {
      Object.defineProperty(managedwriter, 'WriterClient', desc)
    }
  })
})

// -----------------------------------------------------------------------------
// bigqueryTarget — Track 2 metrics (commit lag / commit duration / append errors)
// -----------------------------------------------------------------------------

describe('bigqueryTarget — commit metrics', () => {
  let writerSetup: ReturnType<typeof makeWriter>
  let bqSetup: ReturnType<typeof makeBigQuery>

  beforeEach(() => {
    writerSetup = makeWriter()
    bqSetup = makeBigQuery()
  })

  afterEach(() => {
    // Restore real timers BEFORE clearing other mocks — `vi.restoreAllMocks` doesn't undo
    // `vi.useFakeTimers()`. Without this, the lag test below leaves frozen Date.now() in
    // place and any subsequent test that relies on real wallclock (e.g. the duration test
    // asserting `>= 0` only by accident) silently degrades.
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  function buildTarget(opts?: {
    onData?: (ctx: { store: BigQueryWriter; data: unknown; ctx: unknown }) => void | Promise<void>
    protoWriterFactory?: ProtoWriterFactory
  }) {
    return bigqueryTarget<unknown>({
      client: { bigquery: bqSetup.bq, writer: writerSetup.writer },
      dataset: 'd',
      tables: TABLES,
      settings: { protoWriterFactory: opts?.protoWriterFactory ?? fakeProtoWriterFactory },
      onData: opts?.onData ?? (() => {}),
    })
  }

  it('observes block_to_commit_lag using commit-ack wallclock minus block.timestamp (seconds)', async () => {
    // Pin wallclock so commit_end_ms is deterministic; the lag observation is
    // commit_end_seconds - cursor.timestamp_seconds. Setting commit_end at epoch 1_700_000_005 and
    // the block timestamp at 1_700_000_000 should yield exactly 5s of lag.
    vi.useFakeTimers()
    vi.setSystemTime(new Date(1_700_000_005 * 1000))

    const metrics = mockMetricsServer()
    const target = buildTarget({
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
    })

    async function* read() {
      yield {
        data: { block_number: 10, tx_hash: '0xa' },
        ctx: makeBatchContext({ number: 10, hash: '0xa', timestamp: 1_700_000_000 }, [], 0, metrics),
      }
    }
    await target.write({ read: read as never, logger: testLogger() })

    const histogram = metrics.histogram('sqd_bigquery_block_to_commit_lag_seconds')
    expect(histogram.observations).toEqual([5])
  })

  it('skips block_to_commit_lag observation when cursor.timestamp is missing', async () => {
    // Cursor timestamp is optional — emitting `now − 0` would put a 50+ year lag into the
    // histogram and ruin the rate(...) bucket counts. Better to skip the observation.
    const metrics = mockMetricsServer()
    const target = buildTarget({
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
    })

    async function* read() {
      // cursor() helper returns { number, hash } — no timestamp by design.
      yield { data: { block_number: 10, tx_hash: '0xa' }, ctx: makeBatchContext(cursor(10), [], 0, metrics) }
    }
    await target.write({ read: read as never, logger: testLogger() })

    const histogram = metrics.histogram('sqd_bigquery_block_to_commit_lag_seconds')
    expect(histogram.observations).toEqual([])
  })

  it('normalizes millisecond and rejects out-of-range block timestamps', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date(1_700_000_005 * 1000))

    const metrics = mockMetricsServer()
    const target = buildTarget({
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
    })

    async function* read() {
      // tron and substrate declare epoch ms; verbatim subtraction would read ~-1.7e12.
      yield {
        data: { block_number: 10, tx_hash: '0xa' },
        ctx: makeBatchContext({ number: 10, hash: '0xa', timestamp: 1_700_000_000_000 }, [], 0, metrics),
      }
      // Above 2^53 — tron's portal emits these, and one would freeze `_sum` permanently.
      yield {
        data: { block_number: 11, tx_hash: '0xb' },
        ctx: makeBatchContext({ number: 11, hash: '0xb', timestamp: Number('639208360527210660') }, [], 0, metrics),
      }
    }
    await target.write({ read: read as never, logger: testLogger() })

    expect(metrics.histogram('sqd_bigquery_block_to_commit_lag_seconds').observations).toEqual([5])
  })

  it('observes commit_duration once per batch', async () => {
    // Two batches → two observations on the same registered histogram. We don't pin values
    // (that's wallclock-dependent and brittle); we just assert count and non-negativity.
    const metrics = mockMetricsServer()
    const target = buildTarget({
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
    })

    async function* read() {
      yield { data: { block_number: 10, tx_hash: '0xa' }, ctx: makeBatchContext(cursor(10), [], 0, metrics) }
      yield { data: { block_number: 11, tx_hash: '0xb' }, ctx: makeBatchContext(cursor(11), [], 0, metrics) }
    }
    await target.write({ read: read as never, logger: testLogger() })

    const histogram = metrics.histogram('sqd_bigquery_commit_duration_seconds')
    expect(histogram.observations).toHaveLength(2)
    for (const v of histogram.observations) expect(v).toBeGreaterThanOrEqual(0)
  })

  it('classifies AppendRows failures into append_errors_total{kind}', async () => {
    // Inject a fake writer that rejects with a gRPC RESOURCE_EXHAUSTED (code 8). The
    // classifier must label this as "resource_exhausted" and the counter must increment by 1.
    // Using doWithRetry's transient retry: code 8 is retried up to 8 times before bubbling —
    // we want a NON-transient code here so the failure surfaces immediately to the metric path.
    // Use code 3 (INVALID_ARGUMENT) which is non-retried and maps to "invalid_argument".
    const metrics = mockMetricsServer()
    const failingFactory: ProtoWriterFactory = () => ({
      appendRows: () => ({
        getResult: async () => {
          const err = new Error('schema mismatch') as Error & { code: number }
          err.code = 3
          throw err
        },
      }),
      close: () => {},
    })

    const target = buildTarget({
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
      protoWriterFactory: failingFactory,
    })

    async function* read() {
      yield {
        data: { block_number: 10, tx_hash: '0xa' },
        ctx: makeBatchContext({ number: 10, hash: '0xa', timestamp: 1_700_000_000 }, [], 0, metrics),
      }
    }
    await expect(target.write({ read: read as never, logger: testLogger() })).rejects.toThrow(/schema mismatch/)

    const counter = metrics.counter('sqd_bigquery_append_errors_total')
    // Exactly one increment, labeled with the pipe id and the classified kind.
    expect(counter.calls).toHaveLength(1)
    expect(counter.calls[0]).toEqual({ labels: { id: 'test-pipe', kind: 'invalid_argument' }, value: 1 })

    // The success-path observations must NOT fire when the commit threw.
    expect(metrics.histogram('sqd_bigquery_commit_duration_seconds').observations).toEqual([])
    expect(metrics.histogram('sqd_bigquery_block_to_commit_lag_seconds').observations).toEqual([])
  })

  it('reuses the same Histogram/Counter handles across batches (lazy registration cached)', async () => {
    // The metrics-server interface returns the same registered metric on duplicate names; we
    // also cache the handles in a closure variable so we don't re-resolve on every batch. Two
    // batches → exactly one registered histogram with two observations on it.
    const metrics = mockMetricsServer()
    const target = buildTarget({
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
    })

    async function* read() {
      yield {
        data: { block_number: 10, tx_hash: '0xa' },
        ctx: makeBatchContext({ number: 10, hash: '0xa', timestamp: 1_700_000_000 }, [], 0, metrics),
      }
      yield {
        data: { block_number: 11, tx_hash: '0xb' },
        ctx: makeBatchContext({ number: 11, hash: '0xb', timestamp: 1_700_000_001 }, [], 0, metrics),
      }
    }
    await target.write({ read: read as never, logger: testLogger() })

    const lag = metrics.histogram('sqd_bigquery_block_to_commit_lag_seconds')
    expect(lag.observations).toHaveLength(2)
    // Single registered histogram for the metric name (no duplicates from re-registration).
    expect(metrics.keys().filter((k) => k === 'sqd_bigquery_block_to_commit_lag_seconds')).toHaveLength(1)
  })

  it('does not observe duration/lag when WAL post-commit fails (no phantom success)', async () => {
    // Per-batch AppendRows order:
    //   1. saveCommitPre  → IN_FLIGHT_COMMIT row on /sync   (call 1)
    //   2. commitBatch    → data row on /events             (call 2)
    //   3. saveCommitPost → COMMITTED row on /sync          (call 3)
    // Failing call 3 leaves the batch uncommitted from the recovery POV (next getCursor()
    // re-runs the bounded DELETE for the in-flight range). The success metrics MUST NOT
    // observe — otherwise the dashboard shows a happy commit lag for a batch that wasn't.
    const metrics = mockMetricsServer()
    let appendCalls = 0
    // Use a NON-transient code so doWithRetry doesn't retry past the failure — code 3
    // (INVALID_ARGUMENT) is fatal in `isTransientError`, so the first throw on call 3
    // propagates immediately.
    const failOnPostCommit: ProtoWriterFactory = () => ({
      appendRows: () => ({
        getResult: async () => {
          appendCalls++
          if (appendCalls === 3) {
            const err = new Error('post-commit broke') as Error & { code: number }
            err.code = 3
            throw err
          }
          return { acked: true }
        },
      }),
      close: () => {},
    })

    const target = buildTarget({
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
      protoWriterFactory: failOnPostCommit,
    })

    async function* read() {
      yield {
        data: { block_number: 10, tx_hash: '0xa' },
        ctx: makeBatchContext({ number: 10, hash: '0xa', timestamp: 1_700_000_000 }, [], 0, metrics),
      }
    }
    await expect(target.write({ read: read as never, logger: testLogger() })).rejects.toThrow(/post-commit broke/)

    // Error counter increments — trackBqErrors classifies the post-commit gRPC failure.
    const counter = metrics.counter('sqd_bigquery_append_errors_total')
    expect(counter.calls).toEqual([{ labels: { id: 'test-pipe', kind: 'invalid_argument' }, value: 1 }])

    // Success-path histograms stay empty: post-commit never returned, so we never reached
    // the observe() calls. This is the central regression check — moving observe() before
    // saveCommitPost would silently fail this test.
    expect(metrics.histogram('sqd_bigquery_commit_duration_seconds').observations).toEqual([])
    expect(metrics.histogram('sqd_bigquery_block_to_commit_lag_seconds').observations).toEqual([])
  })
})

// -----------------------------------------------------------------------------
// BigQuerySyncState — crash recovery (the "no permanent corruption" keystone)
// -----------------------------------------------------------------------------

describe('BigQuerySyncState — crash recovery on getCursor', () => {
  // The lifecycle paths above all seed `committed: true` rows. The recovery code path runs
  // when `committed: false` is the latest row — i.e. process crashed between IN_FLIGHT and
  // COMMITTED markers. This block exercises the three branches:
  //   - (op=commit, committed=false)   → re-DELETE the in-flight write range
  //   - (op=rollback, committed=false) → re-DELETE the in-flight rollback range
  //   - range_low/high IS NULL on an in-flight row → CORRUPT_INFLIGHT_ROW

  function makeRecoveryFixture(latestRow: {
    op: 'commit' | 'rollback'
    committed: boolean
    range_low: number | null
    range_high: number | null
    cursorBlock?: number
    finalized?: { number: number; hash: string } | null
  }) {
    const writer = makeWriter().writer
    const { bq, dmlCalls } = makeBigQuery({
      queryRows: [
        {
          id: 'stream',
          op: latestRow.op,
          current: latestRow.cursorBlock != null ? JSON.stringify(cursor(latestRow.cursorBlock)) : null,
          finalized: latestRow.finalized != null ? JSON.stringify(latestRow.finalized) : null,
          rollback_chain: '[]',
          range_low: latestRow.range_low,
          range_high: latestRow.range_high,
          committed: latestRow.committed,
          timestamp: '2025-01-01T00:00:00Z',
        },
      ],
    })
    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory: fakeProtoWriterFactory,
    })
    const state = new BigQuerySyncState({
      store,
      bigquery: bq,
      trackedTables: TABLES.map((t) => ({
        table: t.table,
        fqn: `p.d.${t.table}`,
        blockNumberColumn: t.blockNumberColumn,
      })),
      options: { projectId: 'p', dataset: 'd' },
    })

    return { state, dmlCalls }
  }

  it('on (commit, false): DELETEs [range_low, range_high] from every tracked table and returns pre-batch cursor', async () => {
    const { state, dmlCalls } = makeRecoveryFixture({
      op: 'commit',
      committed: false,
      range_low: 100,
      range_high: 199,
      cursorBlock: 99, // pre-batch cursor recorded in the IN_FLIGHT row
    })

    const cur = await state.getCursor({ logger: testLogger() })

    // Recovery DELETE fired for every tracked table over the in-flight range.
    expect(dmlCalls).toHaveLength(TABLES.length)
    for (const call of dmlCalls) {
      expect(call.sql).toMatch(/DELETE FROM/)
      expect(call.params).toMatchObject({ low: 100, high: 199 })
    }
    // Cursor returned is the PRE-batch position (99), not the in-flight high (199).
    expect(cur?.latest?.number).toBe(99)
    // No finalized head was stored on this in-flight row.
    expect(cur?.finalized).toBeNull()
  })

  it('on (commit, false): hands the persisted finalized head back on the recovery path', async () => {
    const { state } = makeRecoveryFixture({
      op: 'commit',
      committed: false,
      range_low: 100,
      range_high: 199,
      cursorBlock: 99,
      finalized: { number: 90, hash: '0x90' },
    })

    const cur = await state.getCursor({ logger: testLogger() })

    // The recovery branch returns the same TargetState shape as the committed branch, including
    // the persisted finalized head so the source can re-seed its watermark after the crash.
    expect(cur?.latest?.number).toBe(99)
    expect(cur?.finalized).toEqual({ number: 90, hash: '0x90' })
  })

  it('on (rollback, false): re-runs the bounded DELETEs idempotently and returns the safe cursor', async () => {
    const { state, dmlCalls } = makeRecoveryFixture({
      op: 'rollback',
      committed: false,
      range_low: 51,
      range_high: 100,
      cursorBlock: 50, // safe cursor from the original fork
    })

    const cur = await state.getCursor({ logger: testLogger() })

    expect(dmlCalls).toHaveLength(TABLES.length)
    for (const call of dmlCalls) {
      expect(call.params).toMatchObject({ low: 51, high: 100 })
    }
    expect(cur?.latest?.number).toBe(50)
  })

  it('throws CORRUPT_INFLIGHT_ROW when the in-flight row has NULL range_low / range_high', async () => {
    const { state } = makeRecoveryFixture({
      op: 'commit',
      committed: false,
      range_low: null,
      range_high: null,
    })

    await expect(state.getCursor({ logger: testLogger() })).rejects.toThrow(
      /sync row in commit IN_FLIGHT state has NULL range_low\/range_high/,
    )
    await expect(state.getCursor({ logger: testLogger() })).rejects.toMatchObject({
      code: BIGQUERY_ERROR_CODES.CORRUPT_INFLIGHT_ROW,
    })
  })
})

// -----------------------------------------------------------------------------
// bigqueryTarget — fork lifecycle orchestration
// -----------------------------------------------------------------------------

describe('bigqueryTarget — fork lifecycle', () => {
  let writerSetup: ReturnType<typeof makeWriter>
  let bqSetup: ReturnType<typeof makeBigQuery>

  beforeEach(() => {
    writerSetup = makeWriter()
    bqSetup = makeBigQuery()
  })

  afterEach(() => vi.restoreAllMocks())

  it('returns null when state.fork has no common ancestor (deep fork beyond rollback chain)', async () => {
    const target = bigqueryTarget<unknown>({
      client: { bigquery: bqSetup.bq, writer: writerSetup.writer },
      dataset: 'd',
      tables: TABLES,
      settings: { protoWriterFactory: fakeProtoWriterFactory },
      onData: () => {},
    })

    // No prior sync rows → fork can't resolve a safe cursor.
    const result = await target.resolveFork!([cursor(5, 'BAD')])
    expect(result).toBeNull()
  })

  it('writes IN_FLIGHT_ROLLBACK BEFORE invoking onBeforeRollback (crash-safety ordering)', async () => {
    // The user's onBeforeRollback hook can perform DML; if the WAL row hadn't been written
    // first, a crash inside the hook would leave that DML permanent with no recovery info.
    // We assert: the IN_FLIGHT_ROLLBACK sync row is committed before onBeforeRollback runs.
    bqSetup = makeBigQuery({
      queryRows: [
        {
          id: 'stream',
          op: 'commit',
          current: JSON.stringify(cursor(10, '0x10')),
          finalized: null,
          rollback_chain: JSON.stringify([cursor(10, '0x10')]),
          range_low: null,
          range_high: null,
          committed: true,
          timestamp: '2025-01-01T00:00:00Z',
        },
      ],
    })
    let syncStreamOpenedAtHookEntry = false
    const target = bigqueryTarget<unknown>({
      client: { bigquery: bqSetup.bq, writer: writerSetup.writer },
      dataset: 'd',
      tables: TABLES,
      settings: { protoWriterFactory: fakeProtoWriterFactory },
      onData: () => {},
      onBeforeRollback: () => {
        // Committed streams open lazily on first write — by the time the hook fires the
        // sync stream must have been opened (the IN_FLIGHT_ROLLBACK row went through it).
        syncStreamOpenedAtHookEntry = writerSetup.calls.createWriteStream.some((p) => p.endsWith('/sync'))
      },
    })
    await target.resolveFork!([cursor(10, '0x10'), cursor(11, 'BAD11')])
    expect(syncStreamOpenedAtHookEntry).toBe(true) // IN_FLIGHT_ROLLBACK already written when hook fires
  })

  it('on successful fork: IN_FLIGHT_ROLLBACK → onBeforeRollback → DELETEs → ROLLED_BACK → onAfterRollback', async () => {
    const events: string[] = []

    // Seed sync table with a committed row that gives us a common ancestor.
    bqSetup = makeBigQuery({
      queryRows: [
        {
          id: 'stream',
          op: 'commit',
          current: JSON.stringify(cursor(10, '0x10')),
          finalized: null,
          rollback_chain: JSON.stringify([cursor(10, '0x10')]),
          range_low: null,
          range_high: null,
          committed: true,
          timestamp: '2025-01-01T00:00:00Z',
        },
      ],
    })

    const target = bigqueryTarget<unknown>({
      client: { bigquery: bqSetup.bq, writer: writerSetup.writer },
      dataset: 'd',
      tables: TABLES,
      settings: { protoWriterFactory: fakeProtoWriterFactory },
      onData: () => {},
      onBeforeRollback: () => {
        events.push('before')
      },
      onAfterRollback: () => {
        events.push('after')
      },
    })

    const result = await target.resolveFork!([cursor(10, '0x10'), cursor(11, 'BAD11')])
    expect(result?.hash).toBe('0x10')

    // The DELETE-then-mark cycle ran:
    //   - IN_FLIGHT_ROLLBACK sync write (creates sync stream on first use)
    //   - per-table DELETEs via createQueryJob
    //   - ROLLED_BACK sync write (reuses the same long-lived sync stream)
    expect(bqSetup.dmlCalls.length).toBe(TABLES.length) // one DELETE per tracked table
    const syncCreates = writerSetup.calls.createWriteStream.filter((p) => p.endsWith('/sync'))
    expect(syncCreates).toHaveLength(1) // sync stream opened once for both rollback rows

    expect(events).toEqual(['before', 'after'])
  })

  it('does not call onAfterRollback if state.fork returns null', async () => {
    const onAfterRollback = vi.fn()
    const target = bigqueryTarget<unknown>({
      client: { bigquery: bqSetup.bq, writer: writerSetup.writer },
      dataset: 'd',
      tables: TABLES,
      settings: { protoWriterFactory: fakeProtoWriterFactory },
      onData: () => {},
      onAfterRollback,
    })

    await target.resolveFork!([cursor(5, 'BAD')])
    expect(onAfterRollback).not.toHaveBeenCalled()
  })

  it('after a fork, the first reprocessed batch anchors its WAL range/cursor to the safe cursor, not the stale pre-fork cursor', async () => {
    // Regression (data integrity): fork() rewinds the stream to a safe cursor S below the pre-fork
    // committed cursor P and DELETEs (S, upper]. write() tracks previousCursor in a closure fork()
    // cannot reach; if it stays at P, the first reprocessed batch writes an IN_FLIGHT_COMMIT row
    // with current=P and range_low=P+1. On a crash mid-batch, getCursor recovery then rewinds to P
    // and — since range_low > range_high — skips the cleanup DELETE, permanently gapping blocks
    // S+1..P and orphaning the partial write. The fix makes write() adopt the fork's safe cursor,
    // so the row anchors to S (current=S, range_low=S+1, range_low <= range_high).

    // Committed row: getCursor resumes at P=10, and its rollback chain lets the fork resolve a
    // common ancestor at S=5.
    const chain = [
      cursor(5, '0x5'),
      cursor(6, '0x6'),
      cursor(7, '0x7'),
      cursor(8, '0x8'),
      cursor(9, '0x9'),
      cursor(10, '0x10'),
    ]
    bqSetup = makeBigQuery({
      queryRows: [
        {
          id: 'stream',
          op: 'commit',
          current: JSON.stringify(cursor(10, '0x10')),
          finalized: null,
          rollback_chain: JSON.stringify(chain),
          range_low: null,
          range_high: null,
          committed: true,
          timestamp: '2025-01-01T00:00:00Z',
        },
      ],
    })

    // Capture every WAL sync row appended (the `op` field is present on sync rows, absent on data
    // rows) so we can inspect the reprocessed batch's IN_FLIGHT_COMMIT row.
    const syncRows: Record<string, unknown>[] = []
    const target = bigqueryTarget<unknown>({
      client: { bigquery: bqSetup.bq, writer: writerSetup.writer },
      dataset: 'd',
      tables: TABLES,
      settings: {
        protoWriterFactory: ({ protoDescriptor }) => ({
          appendRows: ({ serializedRows }) => {
            for (const row of decodeProtoRows(serializedRows, protoDescriptor)) {
              if ('op' in row) syncRows.push(row)
            }
            return { getResult: async () => ({}) }
          },
          close: () => {},
        }),
      },
      onData: async ({ store, data }) => {
        store.insert('events', [data as Record<string, unknown>])
      },
    })

    // The new canonical chain diverges from ours above block 5 (hashes 0xB6..0xB10), so the fork
    // resolves the safe cursor to block 5.
    const canonicalBlocks = [
      cursor(5, '0x5'),
      cursor(6, '0xB6'),
      cursor(7, '0xB7'),
      cursor(8, '0xB8'),
      cursor(9, '0xB9'),
      cursor(10, '0xB10'),
    ]

    async function* read() {
      // The reorg fires while write()'s for-await is suspended, before the first reprocessed batch.
      const safe = await target.resolveFork!(canonicalBlocks)
      expect(safe?.number).toBe(5) // fork resolved to S=5

      // First reprocessed batch on the new chain, ending at block 7 (S=5 < 7 <= P=10).
      yield {
        data: { block_number: 6, tx_hash: '0x6-new' },
        ctx: makeBatchContext(cursor(7, '0x7-new'), [cursor(6, '0x6-new'), cursor(7, '0x7-new')], 0),
      }
    }

    await target.write({ read: read as never, logger: testLogger() })

    // Among the sync rows (fork's rollback pair, then the batch's commit pair), the first
    // op='commit' row is the reprocessed batch's IN_FLIGHT_COMMIT (pre-commit) row.
    const inFlight = syncRows.find((r) => r['op'] === 'commit') as {
      current: string
      range_low: string | number
      range_high: string | number
    }
    expect(inFlight).toBeDefined()

    // Anchored to the safe cursor S=5, NOT the stale pre-fork P=10.
    expect(JSON.parse(inFlight.current).number).toBe(5)
    // Range floor is S+1=6 (INT64 decodes as a string via protobufjs — coerce to compare).
    expect(Number(inFlight.range_low)).toBe(6)
    expect(Number(inFlight.range_high)).toBe(7)
    // And not inverted: low <= high, so recovery's bounded DELETE actually runs (the bug produced
    // range_low=11 > range_high=7, which recovery skips → the gap).
    expect(Number(inFlight.range_low)).toBeLessThanOrEqual(Number(inFlight.range_high))
  })
})

// -----------------------------------------------------------------------------
// BigQueryWriter — visibility of a slow or failing append
// -----------------------------------------------------------------------------

describe('BigQueryWriter — append reporting', () => {
  /** A logger whose children are itself, so one spy sees every call the store makes. */
  function captureLogger() {
    const logger = {
      warn: vi.fn(),
      info: vi.fn(),
      debug: vi.fn(),
      error: vi.fn(),
      child: () => logger,
    }

    return logger as unknown as Logger & { warn: Mock; info: Mock }
  }

  const EVENTS_TABLE = 'projects/p/datasets/d/tables/events'

  function makeStore(protoWriterFactory: ProtoWriterFactory) {
    const { writer } = makeWriter()
    const { bq } = makeBigQuery()

    const store = new BigQueryWriter(bq, writer, {
      projectId: 'p',
      dataset: 'd',
      trackedTables: TABLES,
      syncTable: { dataset: 'd', table: 'sync' },
      protoWriterFactory,
    })

    return Object.assign(store, { writerClient: writer })
  }

  afterEach(() => {
    vi.useRealTimers()
  })

  it('reports an append that neither succeeds nor fails', async () => {
    // The failure mode that hangs a pipeline forever: `getResult()` has no deadline, so a
    // dropped bidi stream leaves it pending and every layer above just waits on the promise.
    vi.useFakeTimers()

    const logger = captureLogger()
    const store = makeStore(() => ({
      appendRows: () => ({ getResult: () => new Promise(() => {}) }),
      close: () => {},
    }))
    store.bindLogger(logger)

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    store.commitBatch().catch(() => {})

    await vi.advanceTimersByTimeAsync(15_000)

    expect(logger.warn).toHaveBeenCalledExactlyOnceWith({
      message: `appendRows(${EVENTS_TABLE}, offset=0) has not returned after 15s`,
      table: EVENTS_TABLE,
      offset: 0,
      elapsedMs: 15_000,
    })
  })

  it('says nothing about an append that returns in time', async () => {
    vi.useFakeTimers()

    const logger = captureLogger()
    const store = makeStore(fakeProtoWriterFactory)
    store.bindLogger(logger)

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    await store.commitBatch()

    await vi.advanceTimersByTimeAsync(60_000)

    expect(logger.warn).not.toHaveBeenCalled()
    expect(logger.info).not.toHaveBeenCalled()
  })

  it('closes out a reported append when it finally returns', async () => {
    vi.useFakeTimers()

    const logger = captureLogger()
    let ack = () => {}
    const store = makeStore(() => ({
      appendRows: () => ({
        getResult: () =>
          new Promise((resolve) => {
            ack = () => resolve({})
          }),
      }),
      close: () => {},
    }))
    store.bindLogger(logger)

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    const commit = store.commitBatch()

    await vi.advanceTimersByTimeAsync(20_000)
    expect(logger.warn).toHaveBeenCalledOnce()

    ack()
    await commit

    expect(logger.info).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        message: `appendRows(${EVENTS_TABLE}, offset=0) returned after 20s`,
        table: EVENTS_TABLE,
        offset: 0,
      }),
    )
  })

  it('does not say an append returned when it failed', async () => {
    // "returned" reads as "succeeded". Logging it on the way out of a failure sends whoever is
    // triaging the hang looking for the next problem instead of at this one.
    vi.useFakeTimers()

    const logger = captureLogger()
    const store = makeStore(() => ({
      appendRows: () => ({
        getResult: () =>
          new Promise((_, reject) => {
            setTimeout(() => reject(Object.assign(new Error('INVALID_ARGUMENT'), { code: 3 })), 20_000)
          }),
      }),
      close: () => {},
    }))
    store.bindLogger(logger)

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    const commit = store.commitBatch().catch((error) => error)

    await vi.advanceTimersByTimeAsync(20_000)

    expect(await commit).toBeInstanceOf(Error)
    expect(logger.warn).toHaveBeenCalledOnce()
    expect(logger.info).not.toHaveBeenCalled()
  })

  it('does not report an append whose attempts each return in time', async () => {
    // The watchdog is armed per attempt: neither the retry budget's own backoff nor the time
    // spent by earlier attempts may add up into a stall report for a call that is progressing.
    vi.useFakeTimers()

    const logger = captureLogger()
    let attempts = 0
    const store = makeStore(() => ({
      appendRows: () => ({
        getResult: () =>
          new Promise((resolve, reject) => {
            attempts++
            const attempt = attempts

            setTimeout(() => {
              if (attempt < 4) {
                reject(Object.assign(new Error('UNAVAILABLE'), { code: 14 }))
              } else {
                resolve({})
              }
            }, 10_000)
          }),
      }),
      close: () => {},
    }))
    store.bindLogger(logger)

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    const commit = store.commitBatch()

    await vi.advanceTimersByTimeAsync(60_000)
    await commit

    expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        message: `appendRows(${EVENTS_TABLE}, offset=0) succeeded after 3 retries`,
      }),
    )
    expect(logger.info).not.toHaveBeenCalled()
  })

  it('reports a transport error the client delivers only as a connection event', async () => {
    // The connection listener exists to keep an EventEmitter throw from killing the process.
    // Swallowing the event as well loses the only signal of a stream that broke without ever
    // rejecting the append.
    const logger = captureLogger()
    const store = makeStore(fakeProtoWriterFactory)
    store.bindLogger(logger)

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    await store.commitBatch()

    const createStreamConnection = store.writerClient.createStreamConnection as unknown as Mock
    const connection = await createStreamConnection.mock.results[0].value
    const onError = connection.on.mock.calls.find(([event]: [string]) => event === 'error')?.[1]
    onError(Object.assign(new Error('UNAVAILABLE'), { code: 14 }))

    expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        message: `write stream for ${EVENTS_TABLE} reported a transport error`,
        table: EVENTS_TABLE,
      }),
    )
  })

  it('survives a transport error delivered after the logger has gone', async () => {
    // The listener's whole job is to keep an EventEmitter throw off the process. A write to a
    // log transport already torn down at shutdown throws, and would put it right back.
    const logger = captureLogger()
    const store = makeStore(fakeProtoWriterFactory)
    store.bindLogger(logger)

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    await store.commitBatch()

    const createStreamConnection = store.writerClient.createStreamConnection as unknown as Mock
    const connection = await createStreamConnection.mock.results[0].value
    const onError = connection.on.mock.calls.find(([event]: [string]) => event === 'error')?.[1]

    logger.warn.mockImplementation(() => {
      throw new Error('log transport is gone')
    })

    expect(() => onError(Object.assign(new Error('UNAVAILABLE'), { code: 14 }))).not.toThrow()
  })

  it('reports a transient append failure once, after the retries have carried it', async () => {
    // One line per call, not per attempt: a commit that spent part of its budget and then
    // succeeded is worth knowing about, eight warnings per hiccup are not.
    let attempts = 0
    const logger = captureLogger()
    const store = makeStore(() => ({
      appendRows: () => ({
        getResult: async () => {
          attempts++
          if (attempts < 3) {
            throw Object.assign(new Error('UNAVAILABLE'), { code: 14 })
          }

          return {}
        },
      }),
      close: () => {},
    }))
    store.bindLogger(logger)

    store.insert('events', [{ block_number: 1, tx_hash: '0xa' }])
    await store.commitBatch()

    expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        message: `appendRows(${EVENTS_TABLE}, offset=0) succeeded after 2 retries`,
        attempts: 3,
      }),
    )
  })
})
