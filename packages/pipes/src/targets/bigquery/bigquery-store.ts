import type { BigQuery, TableField } from '@google-cloud/bigquery'
import { adapt, managedwriter } from '@google-cloud/bigquery-storage'
// JSONEncoder is the SDK's row-to-proto-bytes encoder, used internally by JSONWriter. The
// public managedwriter index does not re-export it (it's marked "internal" in the SDK), so
// we reach in via the dist path. We rely on it for one specific reason: getting the EXACT
// proto-encoded byte size of each row before sending — JSON size is a noisy estimator that
// over- or under-counts depending on field types, leaving us blind to the AppendRows
// per-request size limit. The SDK has no public "encode without sending" entry point.
import { JSONEncoder } from '@google-cloud/bigquery-storage/build/src/managedwriter/encoder.js'

import { type Logger, formatDuration } from '~/core/index.js'
import { doWithRetry } from '~/internal/function.js'
import { stallWatchdog } from '~/internal/stall-watchdog.js'

import { BIGQUERY_ERROR_CODES, BigQueryTargetError } from './errors.js'
import { type TrackedTable, normalizePartitionColumn } from './tables.js'
import { isTransientError } from './utils.js'

/**
 * Target byte size per AppendRows request, measured on encoded proto rows only (not the
 * full request envelope, which adds writer schema, traceId, offset, descriptors).
 *
 * Documented hard limit on the entire AppendRowsRequest is 10 MB — over that, the server
 * returns `INVALID_ARGUMENT`. In practice the server accepts up to ~16 MB of encoded rows
 * before rejecting on size, so we cap there.
 */
const APPEND_ROWS_MAX_BYTES = 16 * 1024 * 1024

/**
 * Retry budget for transient gRPC errors (RESOURCE_EXHAUSTED, UNAVAILABLE, ABORTED, etc).
 * Exponential backoff with jitter — base 250 ms doubled per attempt up to 8 attempts gives
 * ~30 s of total budget, enough for transport-level stalls to clear without compounding the
 * very throughput burst that triggered them. Fatal errors (INVALID_ARGUMENT, NOT_FOUND,
 * schema-mismatch) skip the backoff and surface immediately via `shouldRetry`.
 */
const RETRY_OPTIONS = {
  retries: 8,
  delayMs: 250,
  backoff: 'exp' as const,
  shouldRetry: isTransientError,
}

/**
 * How long a single AppendRows attempt may stay in flight before it is reported.
 *
 * `getResult()` carries no deadline of its own: a bidi stream that is dropped rather than
 * failed leaves the promise unsettled forever, and every layer above — retry, WAL, progress
 * reporter — is waiting on a promise, so none of them says anything. Armed per attempt, so
 * the retry budget's own backoff can never trip it; well under the pipeline stall warning so
 * the more specific line lands first.
 */
const APPEND_STALL_WARNING_MS = 15_000

export type BigQueryStoreOptions = {
  projectId: string
  /** Tracked tables registered up-front; defines the allowlist (B4 fix). */
  trackedTables: TrackedTable[]
  /** Sync table location — its writes bypass the allowlist (it's framework-managed). */
  syncTable: { dataset: string; table: string }
  /** Dataset that hosts every tracked table. */
  dataset: string
  /**
   * Optional factory for the proto Writer used by the Committed-stream pipeline. Exists for
   * testing — `vi.mock` on `@google-cloud/bigquery-storage` is fragile under v8 coverage
   * instrumentation, so unit tests inject a fake Writer via this seam instead. Production
   * callers leave it undefined and get the real `managedwriter.Writer`.
   */
  protoWriterFactory?: ProtoWriterFactory
}

/**
 * Minimal shape of `managedwriter.Writer` we depend on.
 *
 * The Writer accepts pre-encoded proto rows (Uint8Array) directly via `serializedRows`,
 * unlike `JSONWriter` which takes JS objects and encodes internally. We split the encode and
 * send phases so the encoder's output (with exact byte counts) drives chunk boundaries.
 */
export type ProtoWriterLike = {
  appendRows(
    rows: { serializedRows: Uint8Array[] },
    offsetValue?: number | string | null,
  ): { getResult(): Promise<unknown> }
  close(): void
}

/** Per-column instruction for fields absent from the proto descriptor. */
type MissingValueInterpretations = ConstructorParameters<typeof managedwriter.Writer>[0]['missingValueInterpretations']

export type ProtoWriterFactory = (opts: {
  connection: Awaited<ReturnType<managedwriter.WriterClient['createStreamConnection']>>
  protoDescriptor: ReturnType<typeof adapt.convertStorageSchemaToProto2Descriptor>
  missingValueInterpretations?: MissingValueInterpretations
}) => ProtoWriterLike

const defaultProtoWriterFactory: ProtoWriterFactory = ({ connection, protoDescriptor, missingValueInterpretations }) =>
  new managedwriter.Writer({ connection, protoDescriptor, missingValueInterpretations })

type Row = { [key: string]: unknown }

/** Per-table commit summary returned from `commitBatch` for log/observability use. */
export type CommitTableStats = {
  rows: number
  /** Exact proto-encoded byte size sent to AppendRows (sum across all chunks). */
  bytes: number
}

/** Per-table long-lived Committed stream state, cached across batches. */
type StreamState = {
  streamId: string
  connection: Awaited<ReturnType<managedwriter.WriterClient['createStreamConnection']>>
  writer: ProtoWriterLike
  /**
   * Cumulative offset across the lifetime of this stream — Committed streams use the
   * offset for exactly-once semantics: a retry of the same offset is server-deduped.
   */
  nextOffset: number
}

/**
 * Storage Write API wrapper using **Committed streams**.
 *
 * Why Committed (and not Pending or Default):
 *   - Rows are immediately visible after `AppendRows` acks. The 2025 GA closed the historic
 *     streaming-buffer lockout that disallowed DML on recently streamed rows for 30-90
 *     minutes after write — DML now works on freshly streamed rows just like any others, so
 *     fork DELETEs scoped to recent block ranges run in seconds instead of being blocked.
 *   - One long-lived stream per table is opened on first write and reused for every batch.
 *     Pending streams require a fresh `CreateWriteStream` + `BatchCommitWriteStreams` per
 *     batch; under high batch rates GFE rejects new streams with `RESOURCE_EXHAUSTED:
 *     Bandwidth exhausted or memory limit exceeded`. Committed streams collapse that churn
 *     to one create-per-table-per-process.
 *   - Exactly-once semantics via cumulative per-stream `nextOffset`: a retried AppendRows
 *     resends the same offset and the server dedupes. Default streams (at-least-once) would
 *     silently double-write rows on lost acks — disqualifying for our WAL model.
 *
 * Across tables there is no atomic-flush primitive (Committed has no batch-commit), but the
 * WAL state machine in `bigquery-state.ts` provides the same atomicity guarantee at the
 * cursor level: the `IN_FLIGHT_COMMIT` row is written before any AppendRows; a crash mid-
 * write leaves the cursor unmoved and recovery DELETEs the in-flight range from every
 * tracked table.
 *
 * The store buffers rows in memory during `onData` (via `insert(table, rows)`) and flushes
 * them all in parallel when the target calls `commitBatch()`.
 */
export class BigQueryWriter {
  readonly #bigquery: BigQuery
  readonly #writer: managedwriter.WriterClient
  readonly #allowlist: Set<string>
  readonly #schemas: Map<string, TableField[]>
  readonly #projectId: string
  readonly #dataset: string
  readonly #syncTable: string
  readonly #protoWriterFactory: ProtoWriterFactory
  readonly #buffer: Map<string, Row[]> = new Map()
  // Lazily populated proto-encoded buffers, keyed by table name. `getBufferStats` fills this
  // on the first read, so the byte total is accurate; `commitBatch` reuses the cache so we don't
  // re-encode the same rows. Cleared after commit and invalidated on every fresh `insert`.
  readonly #encodedBuffer: Map<string, Uint8Array[]> = new Map()
  // Long-lived Committed-stream connections, keyed by table FQN path. Opened lazily on the
  // first write to each table and reused for every subsequent batch — the per-batch
  // CreateWriteStream churn that Pending streams imposed (and that GFE pushed back on with
  // `RESOURCE_EXHAUSTED: Bandwidth exhausted or memory limit exceeded` once the rate climbed)
  // collapses to a single create-per-table-per-process. Closed by `close()`.
  readonly #streams: Map<string, StreamState> = new Map()
  // Bound by the target once `write()` starts: the store is constructed at target-creation
  // time, before the pipe's logger exists.
  #logger?: Logger

  constructor(bigquery: BigQuery, writer: managedwriter.WriterClient, options: BigQueryStoreOptions) {
    this.#bigquery = bigquery
    this.#writer = writer
    this.#projectId = options.projectId
    this.#dataset = options.dataset
    this.#syncTable = options.syncTable.table
    this.#allowlist = new Set(options.trackedTables.map((t) => t.table))
    // Normalize the partition column on read just like the DDL does (review fix #4).
    // The proto descriptor used for AppendRows MUST agree with the live table's column
    // type/mode — auto-create coerces partition columns to INT64 NOT NULL, so the
    // schema we use for proto generation must do the same. Otherwise a user who declared
    // the partition as STRING gets a mismatch on the very first append.
    this.#schemas = new Map(
      options.trackedTables.map((t) => [t.table, normalizePartitionColumn(t.schema, t.blockNumberColumn)]),
    )
    this.#protoWriterFactory = options.protoWriterFactory ?? defaultProtoWriterFactory
  }

  /** Attaches the pipe's logger, so retries and stalled appends are reported. */
  bindLogger(logger: Logger): void {
    this.#logger = logger.child({ module: 'bigquery' })
  }

  /**
   * User-facing insert. Buffers rows until `commitBatch()` is called.
   *
   * Throws synchronously if `table` is not in the registered tracked-tables allowlist (B4 fix).
   * Without this guard, rows written to an unregistered table would survive every fork rollback
   * — the equivalent of `tracker.fork()` deleting from `events` while leaving stale rows in
   * `unknown_table` to be layered on top of new-chain data after the reorg.
   */
  insert(table: string, rows: Row[]): void {
    if (!this.#allowlist.has(table)) {
      throw new BigQueryTargetError(
        BIGQUERY_ERROR_CODES.UNREGISTERED_TABLE,
        `Table '${table}' is not registered for fork tracking. ` +
          `Registered tables: ${[...this.#allowlist].sort().join(', ') || '(none)'}. ` +
          `Add it to the bigqueryTarget({ tables: [...] }) config so its rows can be cleaned up on reorg.`,
      )
    }
    if (rows.length === 0) return
    const existing = this.#buffer.get(table)
    if (existing) {
      existing.push(...rows)
    } else {
      this.#buffer.set(table, [...rows])
    }
    // New rows landed → any cached encoding for this table is now stale.
    this.#encodedBuffer.delete(table)
  }

  /**
   * Drops any buffered (uncommitted) rows for every tracked table. Used by `target.write`
   * on entry so a previous invocation that threw between `insert` and `commitBatch` doesn't
   * leak its rows into the next run's first commit (would produce duplicates).
   */
  resetBuffer(): void {
    this.#buffer.clear()
    this.#encodedBuffer.clear()
  }

  /**
   * Snapshot of `{ table → { rows, bytes } }` for everything currently buffered. Encodes
   * each table's rows lazily on first call to surface the exact proto byte size; subsequent
   * reads (and the eventual `commitBatch`) reuse the encoded buffers — encoding never runs
   * more than once per buffered batch.
   */
  getBufferStats(): Record<string, CommitTableStats> {
    const stats: Record<string, CommitTableStats> = {}
    for (const [table, rows] of this.#buffer) {
      const encoded = this.#encodeIfNeeded(table, rows)
      const bytes = encoded.reduce((sum, b) => sum + b.byteLength, 0)
      stats[table] = { rows: rows.length, bytes }
    }

    return stats
  }

  /** Encode rows to proto bytes (cached per table) — used by both `getBufferStats` and `commitBatch`. */
  #encodeIfNeeded(table: string, rows: Row[]): Uint8Array[] {
    const cached = this.#encodedBuffer.get(table)
    if (cached) return cached
    const schema = this.#schemas.get(table)
    if (!schema) {
      throw new BigQueryTargetError(
        BIGQUERY_ERROR_CODES.INTERNAL_SCHEMA_MAP,
        `Internal: no schema registered for tracked table '${table}'`,
      )
    }
    const protoDescriptor = buildProtoDescriptor(schema)
    const encoder = new JSONEncoder({ protoDescriptor })
    const encoded = encoder.encodeRows(rows as Parameters<typeof encoder.encodeRows>[0])
    this.#encodedBuffer.set(table, encoded)

    return encoded
  }

  /**
   * Commits all buffered data tables in parallel via `Promise.all`.
   *
   * Per-table operations (createWriteStream, appendRows, finalize, batchCommitWriteStream)
   * are independent and run concurrently — serial dispatch would cost N × RTT on a multi-table
   * batch (~5s/table × 10 tables = 50s).
   *
   * Throws if ANY table commit fails. The WAL pattern in `bigquery-state.ts` ensures that a
   * partial commit is recovered on restart by deleting the in-flight range from every tracked
   * table (idempotent on tables that didn't actually receive the partial write).
   *
   * Returns per-table `{ rows, bytes }` (proto-encoded byte size from the encoder, exact —
   * not a JSON estimate) so callers can log the size of what was just committed.
   */
  async commitBatch(): Promise<Record<string, CommitTableStats>> {
    if (this.#buffer.size === 0) return {}

    const entries = [...this.#buffer.entries()]
    this.#buffer.clear()

    const results = await Promise.all(
      entries.map(async ([table, rows]) => {
        const schema = this.#schemas.get(table)
        if (!schema) {
          // Unreachable — allowlist and schema map are populated together — but throw with
          // context if it ever happens, rather than failing inside the proto encoder.
          throw new BigQueryTargetError(
            BIGQUERY_ERROR_CODES.INTERNAL_SCHEMA_MAP,
            `Internal: no schema registered for tracked table '${table}'`,
          )
        }
        // Pass cached encoded buffers if `getBufferStats` already populated them; otherwise
        // `#commitTable` encodes inline. `#encodeIfNeeded` returns the cached entry directly,
        // no re-encoding cost on the commit path.
        const encoded = this.#encodeIfNeeded(table, rows)
        const stats = await this.#commitTable(this.#tablePath(table), schema, rows, {}, encoded)

        return [table, stats] as const
      }),
    )
    this.#encodedBuffer.clear()

    return Object.fromEntries(results)
  }

  /**
   * Append a single sync row via the same Committed-stream pipeline used for tracked tables.
   * Sync writes bypass the allowlist (the sync table is framework-managed). Cleanup of old
   * sync rows uses DELETE — the GA window for DML on streamed rows is far longer than any
   * cleanup interval.
   */
  async commitSyncRow(schema: TableField[], row: Row): Promise<void> {
    await this.#commitTable(this.#tablePath(this.#syncTable), schema, [row])
  }

  /**
   * Per-table Committed-stream pipeline: get-or-create the long-lived stream, append the
   * batch's chunks against it with cumulative offsets, return.
   *
   * No `finalize`, no `BatchCommitWriteStreams` — Committed streams make rows visible the
   * moment AppendRows acks. Exactly-once is enforced by the cumulative `nextOffset` tracked
   * per stream: a retry that resends the same offset is server-deduped, so duplicate rows
   * are impossible even when an ack is lost mid-flight.
   *
   * Each step is wrapped in `doWithRetry` with `isTransientError`. Fatal errors
   * (INVALID_ARGUMENT, NOT_FOUND, schema mismatch) propagate immediately; transient errors
   * (ABORTED, UNAVAILABLE, RESOURCE_EXHAUSTED) succeed within the retry budget.
   */
  async #commitTable(
    tableFqnPath: string,
    schema: TableField[],
    rows: Row[],
    writerOptions: { missingValueInterpretations?: MissingValueInterpretations } = {},
    preEncoded?: Uint8Array[],
  ): Promise<CommitTableStats> {
    if (rows.length === 0) return { rows: 0, bytes: 0 }

    const encodedRows =
      preEncoded ??
      new JSONEncoder({ protoDescriptor: buildProtoDescriptor(schema) }).encodeRows(
        rows as Parameters<JSONEncoder['encodeRows']>[0],
      )
    const totalBytes = encodedRows.reduce((sum, b) => sum + b.byteLength, 0)

    const stream = await this.#getOrCreateStream(tableFqnPath, schema, writerOptions)

    for (const chunk of chunkBuffersByByteSize(encodedRows)) {
      const chunkOffset = stream.nextOffset // capture for the retry closure
      const response = await doWithRetry(
        () =>
          this.#whileAppending(tableFqnPath, chunkOffset, () =>
            stream.writer.appendRows({ serializedRows: chunk }, chunkOffset).getResult(),
          ),
        {
          ...RETRY_OPTIONS,
          title: `appendRows(${tableFqnPath}, offset=${chunkOffset})`,
          logger: this.#logger,
        },
      )
      assertNoRowErrors(response, tableFqnPath, chunkOffset)
      stream.nextOffset += chunk.length
    }

    return { rows: rows.length, bytes: totalBytes }
  }

  /**
   * Runs a single append attempt and reports it if it neither succeeds nor fails in time.
   *
   * A stalled append is otherwise indistinguishable from an idle source: the pipe stops
   * advancing, the progress line keeps printing the same numbers, and nothing is logged.
   */
  async #whileAppending<T>(tableFqnPath: string, offset: number, run: () => Promise<T>): Promise<T> {
    const watchdog = stallWatchdog({
      thresholdMs: APPEND_STALL_WARNING_MS,
      onStall: ({ elapsedMs }) => {
        this.#logger?.warn({
          message: `appendRows(${tableFqnPath}, offset=${offset}) has not returned after ${formatDuration(elapsedMs)}`,
          table: tableFqnPath,
          offset,
          elapsedMs,
        })
      },
      onRecover: ({ elapsedMs }) => {
        this.#logger?.info({
          message: `appendRows(${tableFqnPath}, offset=${offset}) returned after ${formatDuration(elapsedMs)}`,
          table: tableFqnPath,
          offset,
          elapsedMs,
        })
      },
    })

    watchdog.begin('append')
    try {
      const result = await run()
      watchdog.end()

      return result
    } finally {
      // An attempt that threw did not return: stop the clock, but never say it came back.
      watchdog.abort()
    }
  }

  /**
   * Returns the cached Committed-stream state for `tableFqnPath`, opening one on first
   * call. The writer + underlying gRPC connection are kept alive for the rest of the
   * process lifetime and torn down by `close()`.
   *
   * `writerOptions` must be stable per table: only the FIRST call's value takes effect
   * (subsequent calls return the cached writer). For the sync table this is always
   * `{ timestamp: 'DEFAULT_VALUE' }`; for tracked tables it's always empty.
   */
  async #getOrCreateStream(
    tableFqnPath: string,
    schema: TableField[],
    writerOptions: { missingValueInterpretations?: MissingValueInterpretations },
  ): Promise<StreamState> {
    const cached = this.#streams.get(tableFqnPath)
    if (cached) return cached

    const protoDescriptor = buildProtoDescriptor(schema)
    const streamId = await doWithRetry(
      () =>
        this.#writer.createWriteStream({
          streamType: managedwriter.CommittedStream,
          destinationTable: tableFqnPath,
        }),
      { ...RETRY_OPTIONS, title: `createWriteStream(${tableFqnPath})`, logger: this.#logger },
    )
    const connection = await this.#writer.createStreamConnection({ streamId })
    // The `@google-cloud/bigquery-storage` client delivers a transport error twice: once as a
    // rejected `getResult()` promise (which `doWithRetry` handles), and once as an 'error'
    // event on this connection. The listener has to stay — without it the second delivery
    // becomes an uncaught EventEmitter throw and kills the process — but it must not swallow
    // the error silently: a failure mode that delivers ONLY the event leaves nothing else to
    // say the stream broke. Recovery is still the client's: it reconnects the bidi on the next
    // `appendRows` attempt, same streamId, offsets preserved.
    connection.on('error', (error: unknown) => {
      try {
        this.#logger?.warn({
          message: `write stream for ${tableFqnPath} reported a transport error`,
          error,
          table: tableFqnPath,
          stream: streamId,
        })
      } catch {
        // A throw here (a log transport already torn down at shutdown, say) would become the
        // uncaught EventEmitter exception this listener exists to prevent.
      }
    })

    const writer = this.#protoWriterFactory({
      connection,
      protoDescriptor,
      missingValueInterpretations: writerOptions.missingValueInterpretations,
    })

    const state: StreamState = { streamId, connection, writer, nextOffset: 0 }
    this.#streams.set(tableFqnPath, state)

    return state
  }

  /**
   * Run a single-statement DML (DELETE for fork cleanup or sync maintenance).
   *
   * Single-statement is mandatory: BigQuery disallows multi-statement transactions on
   * recently streamed data, so `BEGIN; DELETE; INSERT; COMMIT` is not an option.
   */
  async executeDml(sql: string, params: Record<string, unknown> = {}): Promise<{ rowCount: number }> {
    const [job] = await doWithRetry(() => this.#bigquery.createQueryJob({ query: sql, params }), {
      ...RETRY_OPTIONS,
      title: 'createQueryJob',
      logger: this.#logger,
    })
    await job.getQueryResults()
    const stats = job.metadata?.statistics?.query
    const numDmlAffectedRows = (stats?.numDmlAffectedRows ?? '0') as string

    return { rowCount: Number.parseInt(numDmlAffectedRows, 10) }
  }

  /**
   * Run a SELECT and return rows. Used by state.getCursor / state.fork.
   * Wraps in retry to absorb transient slot-allocation hiccups.
   */
  async query<T = Record<string, unknown>>(sql: string, params: Record<string, unknown> = {}): Promise<T[]> {
    const [rows] = await doWithRetry(() => this.#bigquery.query({ query: sql, params }), {
      ...RETRY_OPTIONS,
      title: 'query',
      logger: this.#logger,
    })
    return rows as T[]
  }

  close(): void {
    // Tear down every cached Committed-stream writer + its underlying gRPC connection. Each
    // close is best-effort — a Writer.close() / connection.close() throw must NOT propagate
    // because we're often called from a finally block during normal shutdown and a single
    // failed close shouldn't mask the original control flow.
    for (const state of this.#streams.values()) {
      try {
        state.writer.close()
      } catch {
        // best-effort
      }
      try {
        state.connection.close()
      } catch {
        // best-effort
      }
    }
    this.#streams.clear()

    // WriterClient.close() is synchronous and returns void.
    this.#writer.close()
  }

  /** Storage Write API expects path-style FQNs: projects/{project}/datasets/{ds}/tables/{t} */
  #tablePath(table: string): string {
    return `projects/${this.#projectId}/datasets/${this.#dataset}/tables/${table}`
  }

  /** @internal — exposed for testing. */
  get _allowlist(): ReadonlySet<string> {
    return this.#allowlist
  }
}

/**
 * Build a proto2 descriptor from a BigQuery table schema, suitable for the proto encoder
 * and the lower-level `Writer`.
 *
 * The Storage Write API encodes rows as protobuf, not JSON. The SDK provides a two-step
 * conversion: BQ schema → Storage TableSchema → proto2 DescriptorProto.
 */
export function buildProtoDescriptor(fields: TableField[]) {
  const storageSchema = adapt.convertBigQuerySchemaToStorageTableSchema({
    fields: fields as unknown as Parameters<typeof adapt.convertBigQuerySchemaToStorageTableSchema>[0]['fields'],
  })
  return adapt.convertStorageSchemaToProto2Descriptor(storageSchema, 'root')
}

/**
 * Inspects an `AppendRowsResponse` for per-row validation errors and throws if any are
 * present. The Storage Write SDK does NOT propagate row errors as rejected promises —
 * `getResult()` resolves successfully even when BigQuery rejected every row in the request
 * (proto-schema mismatch, NOT NULL violation, type coercion failure, etc.). Without this
 * check the data is silently dropped while our code believes the write succeeded.
 */
function assertNoRowErrors(response: unknown, tableFqnPath: string, offset: number): void {
  // Response shape: AppendRowsResponse { rowErrors?: { index, code, message }[], ... }
  const r = response as { rowErrors?: Array<{ index?: number | string; code?: number | string; message?: string }> }
  const errs = r?.rowErrors
  if (!errs || errs.length === 0) return
  const summary = errs
    .slice(0, 3)
    .map((e) => `row ${e.index}: ${e.message ?? `code ${e.code}`}`)
    .join('; ')
  const more = errs.length > 3 ? ` (and ${errs.length - 3} more)` : ''
  throw new BigQueryTargetError(
    BIGQUERY_ERROR_CODES.APPEND_ROW_REJECTED,
    `BigQuery rejected ${errs.length} row(s) in AppendRows for ${tableFqnPath} at offset ` +
      `${offset}: ${summary}${more}.\n\n` +
      `These rows are NOT written. Common causes: proto-schema mismatch with table schema, ` +
      `NOT NULL violation, value out of column type range. Inspect the live table schema ` +
      `and compare against the descriptor passed to the writer.`,
  )
}

/**
 * Chunks pre-encoded proto-row buffers so each chunk's total `byteLength` stays under the
 * AppendRows per-request limit.
 *
 * The Storage Write API rejects any AppendRowsRequest whose serialized rows exceed 20 MB.
 * Because the input here is already the wire-format byte-length, the cap is exact — unlike
 * a JSON-byte-size estimate, which can over- or under-count for varint INT64s,
 * length-prefixed strings, and per-field tag overhead.
 *
 * A single buffer larger than the cap is emitted as its own chunk rather than dropped — the
 * server will surface the size violation, but losing data silently would be worse.
 */
export function* chunkBuffersByByteSize(buffers: Uint8Array[]): Generator<Uint8Array[]> {
  let chunk: Uint8Array[] = []
  let chunkBytes = 0

  for (const buf of buffers) {
    if (chunkBytes + buf.byteLength > APPEND_ROWS_MAX_BYTES && chunk.length > 0) {
      yield chunk
      chunk = []
      chunkBytes = 0
    }
    chunk.push(buf)
    chunkBytes += buf.byteLength
  }

  if (chunk.length > 0) yield chunk
}
