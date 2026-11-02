import type { BigQuery, TableField } from '@google-cloud/bigquery'
import type { managedwriter } from '@google-cloud/bigquery-storage'
import { beforeAll, describe, expect, it } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { BigQuerySyncState } from './bigquery-state.js'
import { BigQueryWriter } from './bigquery-store.js'
import { bigqueryTarget } from './bigquery-target.js'
import {
  DATASET,
  PREFIX,
  PROJECT,
  RUN,
  makeBatchContext,
  partitioning,
  projectId,
  setupIntegrationClients,
  trackedTable,
} from './integration-helpers.js'
import { type TrackedTable, ensureTrackedTable, syncTableDdl, trackedTableDdl } from './tables.js'

/**
 * Integration tests for the BigQuery target — gated by `BIGQUERY_TEST_PROJECT`.
 *
 * Runs against real BigQuery only. The project must have BigQuery + BigQuery Storage Write
 * APIs enabled and application-default credentials configured (`gcloud auth
 * application-default login`).
 *
 * Run:
 *   BIGQUERY_TEST_PROJECT=my-gcp-project \
 *   BIGQUERY_TEST_DATASET=pipes_target_test \
 *   pnpm vitest run src/targets/bigquery/bigquery-target.integration.test.ts
 *
 * Scope of this file: schema management, Storage Write API visibility, type-mapping
 * round-trip. Fork lifecycle lives in `bigquery-target-fork.integration.test.ts`. Shared
 * scaffolding (env vars, dataset bootstrap, BatchContext stub) lives in `./integration-helpers`.
 */

describe.skipIf(!RUN)('bigquery target — integration', () => {
  let bigquery: BigQuery
  let writer: managedwriter.WriterClient

  beforeAll(async () => {
    ;({ bigquery, writer } = await setupIntegrationClients())
  }, 60_000)

  describe('DDL parse — generated SQL is accepted by the live BigQuery parser', () => {
    it('sync-table DDL: backtick-quoted reserved keywords + DEFAULT ordering', async () => {
      // Without backtick-quoting on `current` / `timestamp` etc the parser would reject with
      // `Syntax error: ... keyword CURRENT`. And `DEFAULT CURRENT_TIMESTAMP()` must come
      // BEFORE `NOT NULL` in BQ's column-definition grammar — the wrong order trips a
      // different syntax error. Both bugs are invisible to the unit suite, which never runs
      // the SQL through a parser.
      const localSync = `${PREFIX}sync_ddl_${Date.now()}`
      const ddl = syncTableDdl({ fqn: `${PROJECT}.${DATASET}.${localSync}`, dataset: DATASET, table: localSync })
      await bigquery.query({ query: ddl })
    }, 30_000)

    it('tracked-table DDL parses', async () => {
      const localEvents = `${PREFIX}events_ddl_${Date.now()}`
      const ddl = trackedTableDdl(
        `${PROJECT}.${DATASET}.${localEvents}`,
        { ...trackedTable, table: localEvents },
        partitioning,
      )
      await bigquery.query({ query: ddl })
    }, 30_000)
  })

  describe('schema management — REST path', () => {
    it('ensureTrackedTable auto-creates a missing tracked table via the NotFound branch', async () => {
      const localEvents = `${PREFIX}events_create_${Date.now()}`
      await ensureTrackedTable({
        bigquery,
        projectId,
        dataset: DATASET,
        trackedTable: { ...trackedTable, table: localEvents },
        partitioning,
      })

      const [exists] = await bigquery.dataset(DATASET).table(localEvents).exists()
      expect(exists).toBe(true)
    }, 30_000)

    it('BigQuerySyncState.getCursor lazy-creates the sync table on Not Found and returns undefined', async () => {
      const localSync = `${PREFIX}sync_lazy_${Date.now()}`
      const store = new BigQueryWriter(bigquery, writer, {
        projectId,
        dataset: DATASET,
        trackedTables: [],
        syncTable: { dataset: DATASET, table: localSync },
      })
      const state = new BigQuerySyncState({
        store,
        bigquery,
        trackedTables: [],
        options: { projectId, dataset: DATASET, table: localSync },
      })

      const cursor = await state.getCursor({ logger: testLogger() })
      expect(cursor).toBeUndefined()

      const [exists] = await bigquery.dataset(DATASET).table(localSync).exists()
      expect(exists).toBe(true)
    }, 30_000)
  })

  describe('Storage Write API — visibility', () => {
    it('Committed-stream sync row written via the store is immediately visible to SQL SELECT', async () => {
      // Isolates the question "are Committed-stream writes immediately readable by SQL?"
      // from the fork-resolution chain. If this passes but the fork test fails, the bug is
      // in resolveForkCursor / our paging — not in BigQuery's read-after-write semantics.
      const localSync = `${PREFIX}sync_visibility_${Date.now()}`
      const store = new BigQueryWriter(bigquery, writer, {
        projectId,
        dataset: DATASET,
        trackedTables: [],
        syncTable: { dataset: DATASET, table: localSync },
      })
      const state = new BigQuerySyncState({
        store,
        bigquery,
        trackedTables: [],
        options: { projectId, dataset: DATASET, table: localSync },
      })

      await state.getCursor({ logger: testLogger() }) // lazy-creates the table

      // Write one COMMITTED sync row through the same Storage Write API path the production
      // WAL uses (saveCommitPost → commitSyncRow → Committed-stream AppendRows).
      await state.saveCommitPost({
        logger: testLogger(),
        cursor: { number: 42, hash: '0x42' },
        finalized: undefined,
        rollbackChain: [{ number: 42, hash: '0x42' }],
      })

      const [rows] = await bigquery.query({
        query: `SELECT JSON_EXTRACT_SCALAR(\`current\`, '$.number') AS n FROM \`${projectId}.${DATASET}.${localSync}\` WHERE \`committed\` = TRUE`,
      })
      expect(rows.length).toBe(1)
      expect(Number(rows[0].n)).toBe(42)
    }, 30_000)
  })

  // ---------------------------------------------------------------------------------------
  // Type-mapping coverage — Storage Write API round-trip.
  //
  // Verifies what JS shapes the BigQueryWriter accepts per BQ column type, and that the value
  // SQL reads back equals what we wrote. Each column owns one bullet of the matrix (no hidden
  // coupling) so a regression on, say, NUMERIC fails its own assertion line and not the rest.
  //
  // Specifically targets the NUMERIC/BIGNUMERIC claim in the docs example: SDK 5.x maps both
  // to proto TYPE_STRING (see @google-cloud/bigquery-storage/.../adapt/proto_mappings.js), and
  // the proto-row encoder turns number/bigint into base-10 strings while passing JS strings
  // through verbatim. If the older "string hangs silently on AppendRows" still bites, this
  // suite is where it surfaces — and the docs comment can be retired.
  // ---------------------------------------------------------------------------------------

  describe('type mappings (Storage Write API round-trip)', () => {
    // Shared values across the auto-DDL and manual-DDL tests so a regression that only affects
    // one path (e.g. encoder vs DDL emission) is easy to spot — same input, different setup.

    // BIGNUMERIC range is ±5.79e38 — i.e. up to 38 digits before the decimal point, plus 38
    // after. 38 before is comfortably outside INT64 (~19 digits), so a regression that
    // quietly truncates to INT64 still surfaces; going past 38 before would overflow the
    // type itself ("Invalid BIGNUMERIC value" from AppendRows).
    const bigNumericString = '12345678901234567890123456789012345678.1234567890'
    // NUMERIC range: ~38 digits, scale 9 — keep well within bounds, with a non-zero fractional
    // part so a "lost the fraction" bug fails the equality check.
    const numericFromString = '12345.6789'
    const dateValue = new Date(Date.UTC(2026, 4, 9)) // 2026-05-09
    const timestampValue = new Date(Date.UTC(2026, 4, 9, 12, 34, 56, 789))

    const SCALAR_FIELDS = [
      { name: 'block_number', type: 'INT64', mode: 'REQUIRED' },
      { name: 'col_int64', type: 'INT64' },
      { name: 'col_int64_str', type: 'INT64' }, // INT64 written as JS string (max-safe-integer fix)
      { name: 'col_float64', type: 'FLOAT64' },
      { name: 'col_bool', type: 'BOOL' },
      { name: 'col_string', type: 'STRING' },
      { name: 'col_bytes', type: 'BYTES' },
      // The whole point of the suite — if these fail, the docs example's STRING workaround is
      // justified. If they pass, we update the example.
      { name: 'col_numeric_from_number', type: 'NUMERIC' },
      { name: 'col_numeric_from_bigint', type: 'NUMERIC' },
      { name: 'col_numeric_from_string', type: 'NUMERIC' },
      { name: 'col_bignumeric_from_string', type: 'BIGNUMERIC' },
      { name: 'col_date', type: 'DATE' }, // expects JS Date per encoder.js:140-145
      { name: 'col_timestamp', type: 'TIMESTAMP' }, // expects JS Date per encoder.js:140-153
      { name: 'col_json', type: 'JSON' }, // BQ JSON type — encoder treats as STRING in proto
    ] as const satisfies readonly TableField[]

    const scalarRow: Record<string, unknown> = {
      block_number: 1,
      col_int64: 42,
      // Numbers above 2^53 must travel as a string-typed field — JS number loses precision
      // around 9.007e15 and the encoder would silently round.
      col_int64_str: '9007199254740993',
      col_float64: 3.14,
      col_bool: true,
      col_string: 'hello, мир',
      col_bytes: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
      col_numeric_from_number: 1234,
      col_numeric_from_bigint: 1234567890123456789n,
      col_numeric_from_string: numericFromString,
      col_bignumeric_from_string: bigNumericString,
      col_date: dateValue,
      col_timestamp: timestampValue,
      col_json: '{"a":1,"b":[2,3]}',
    }

    type ScalarReadRow = {
      col_int64: string
      col_int64_str: string
      col_float64: number
      col_bool: boolean
      col_string: string
      col_bytes_hex: string
      num_number: string
      num_bigint: string
      num_string: string
      bignum_string: string
      d: string
      ts: string
      j: string
    }

    /**
     * Cast every non-trivial type to STRING BQ-side so the JS client doesn't surface
     * Big.js / PreciseDate / Buffer-with-quirks — and so INT64 values past 2^53 round-trip
     * intact. The default `bigquery.query` returns INT64 as a JS Number, which truncates
     * `9007199254740993` to `9007199254740992` on the read path even though the stored
     * value is correct. CAST(... AS STRING) forces lossless transit.
     */
    const SCALAR_SELECT = `
      CAST(col_int64 AS STRING) AS col_int64,
      CAST(col_int64_str AS STRING) AS col_int64_str,
      col_float64,
      col_bool,
      col_string,
      TO_HEX(col_bytes) AS col_bytes_hex,
      CAST(col_numeric_from_number AS STRING) AS num_number,
      CAST(col_numeric_from_bigint AS STRING) AS num_bigint,
      CAST(col_numeric_from_string AS STRING) AS num_string,
      CAST(col_bignumeric_from_string AS STRING) AS bignum_string,
      CAST(col_date AS STRING) AS d,
      FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', col_timestamp, 'UTC') AS ts,
      TO_JSON_STRING(col_json) AS j
    `

    function assertScalarRow(r: ScalarReadRow): void {
      expect(Number(r.col_int64)).toBe(42)
      expect(String(r.col_int64_str)).toBe('9007199254740993')
      expect(Number(r.col_float64)).toBeCloseTo(3.14, 6)
      expect(r.col_bool).toBe(true)
      expect(r.col_string).toBe('hello, мир')
      expect(r.col_bytes_hex).toBe('deadbeef')
      // BQ NUMERIC normalizes scale on read — '12345.6789' may come back as '12345.678900000'.
      // Use Number-equality instead of string-equality so we test the value, not the format.
      expect(Number(r.num_number)).toBe(1234)
      expect(String(r.num_bigint)).toBe('1234567890123456789')
      expect(Number(r.num_string)).toBe(12345.6789)
      // BIGNUMERIC has 76-digit precision; can't compare via JS Number. Strip trailing zeros
      // to compare meaningful digits only.
      expect(r.bignum_string.replace(/0+$/, '')).toBe(bigNumericString.replace(/0+$/, ''))
      expect(r.d).toBe('2026-05-09')
      expect(r.ts).toBe('2026-05-09T12:34:56.789Z')
      // BQ JSON normalizes whitespace; assert structural equality, not byte-equality.
      expect(JSON.parse(r.j)).toEqual({ a: 1, b: [2, 3] })
    }

    it('auto-DDL: round-trips every flat scalar type the SDK auto-creates', async () => {
      // Pure auto-create path. No REPEATED, no RECORD — those are the auto-DDL guard's
      // responsibility (see assertFlatSchema). Anything in here MUST work end-to-end via
      // ensureTrackedTable + Storage Write API + SQL read-back.
      const localEvents = `${PREFIX}types_auto_${Date.now()}`
      const trackedTableScalars: TrackedTable = {
        table: localEvents,
        blockNumberColumn: 'block_number',
        schema: [...SCALAR_FIELDS],
      }

      const target = bigqueryTarget<Record<string, unknown>>({
        client: { bigquery, writer },
        dataset: DATASET,
        tables: [trackedTableScalars],
        settings: { state: { table: `${PREFIX}types_auto_sync_${Date.now()}` } },
        onData: async ({ store, data }) => {
          store.insert(localEvents, [data])
        },
      })

      async function* read() {
        yield { data: scalarRow, ctx: makeBatchContext({ number: 1, hash: '0x1' }) }
      }
      await target.write({ read: read as never, logger: testLogger(), id: 'test-pipe' })

      const [rows] = await bigquery.query({
        query: `SELECT ${SCALAR_SELECT} FROM \`${projectId}.${DATASET}.${localEvents}\` WHERE block_number = 1`,
      })
      expect(rows.length).toBe(1)
      assertScalarRow(rows[0] as ScalarReadRow)
    }, 120_000)

    it('manual DDL: scalar matrix + REPEATED INT64 + nested RECORD round-trip', async () => {
      // Pre-create the table manually so the schema can carry REPEATED / RECORD — those modes
      // are blocked by `assertFlatSchema` in the auto-DDL path. The SDK's documented escape
      // hatch is "create the table yourself and re-run; the target will validate". This test
      // exercises that path. Partition clause matches `partitioningWithDefaults()` so the
      // validator accepts.
      const localEvents = `${PREFIX}types_manual_${Date.now()}`

      if (!partitioning) throw new Error('integration suite assumes default partitioning is enabled')
      await bigquery.query({
        query: `
          CREATE TABLE \`${projectId}.${DATASET}.${localEvents}\` (
            block_number INT64 NOT NULL,
            col_int64 INT64,
            col_int64_str INT64,
            col_float64 FLOAT64,
            col_bool BOOL,
            col_string STRING,
            col_bytes BYTES,
            col_numeric_from_number NUMERIC,
            col_numeric_from_bigint NUMERIC,
            col_numeric_from_string NUMERIC,
            col_bignumeric_from_string BIGNUMERIC,
            col_date DATE,
            col_timestamp TIMESTAMP,
            col_json JSON,
            col_array_int64 ARRAY<INT64>,
            col_record STRUCT<inner_int INT64, inner_string STRING>
          )
          PARTITION BY RANGE_BUCKET(block_number, GENERATE_ARRAY(0, ${partitioning.maxBlocks}, ${partitioning.bucketSize}))
        `,
      })

      const trackedTableNested: TrackedTable = {
        table: localEvents,
        blockNumberColumn: 'block_number',
        schema: [
          ...SCALAR_FIELDS,
          { name: 'col_array_int64', type: 'INT64', mode: 'REPEATED' },
          {
            name: 'col_record',
            type: 'RECORD',
            fields: [
              { name: 'inner_int', type: 'INT64' },
              { name: 'inner_string', type: 'STRING' },
            ],
          },
        ],
      }

      const target = bigqueryTarget<Record<string, unknown>>({
        client: { bigquery, writer },
        dataset: DATASET,
        tables: [trackedTableNested],
        settings: { state: { table: `${PREFIX}types_manual_sync_${Date.now()}` } },
        onData: async ({ store, data }) => {
          store.insert(localEvents, [data])
        },
      })

      const row: Record<string, unknown> = {
        ...scalarRow,
        col_array_int64: [10, 20, 30],
        col_record: { inner_int: 7, inner_string: 'inside' },
      }

      async function* read() {
        yield { data: row, ctx: makeBatchContext({ number: 1, hash: '0x1' }) }
      }
      await target.write({ read: read as never, logger: testLogger(), id: 'test-pipe' })

      const [rows] = await bigquery.query({
        query: `
          SELECT
            ${SCALAR_SELECT},
            col_array_int64,
            col_record.inner_int AS rec_int,
            col_record.inner_string AS rec_string
          FROM \`${projectId}.${DATASET}.${localEvents}\`
          WHERE block_number = 1
        `,
      })
      expect(rows.length).toBe(1)
      type NestedRow = ScalarReadRow & {
        col_array_int64: unknown[]
        rec_int: number | string
        rec_string: string
      }
      const r = rows[0] as NestedRow

      assertScalarRow(r)
      expect(r.col_array_int64.map((v) => Number(v))).toEqual([10, 20, 30])
      expect(Number(r.rec_int)).toBe(7)
      expect(r.rec_string).toBe('inside')
    }, 120_000)

    it('writes NULL for omitted fields and explicit nulls', async () => {
      // The encoder skips keys whose value is null/undefined (encoder.js convertRow). For
      // NULLABLE columns BQ writes NULL — the same behavior whether the key is omitted or
      // present-and-null. This pins both paths so a future encoder change that, say, writes
      // empty-string for missing STRING is caught immediately.
      const localEvents = `${PREFIX}types_null_${Date.now()}`
      const trackedTableNulls: TrackedTable = {
        table: localEvents,
        blockNumberColumn: 'block_number',
        schema: [
          { name: 'block_number', type: 'INT64', mode: 'REQUIRED' },
          { name: 'col_string_nullable', type: 'STRING' },
          { name: 'col_int64_nullable', type: 'INT64' },
          { name: 'col_numeric_nullable', type: 'NUMERIC' },
        ],
      }

      await ensureTrackedTable({
        bigquery,
        projectId,
        dataset: DATASET,
        trackedTable: trackedTableNulls,
        partitioning,
      })

      const target = bigqueryTarget<Record<string, unknown>>({
        client: { bigquery, writer },
        dataset: DATASET,
        tables: [trackedTableNulls],
        settings: { state: { table: `${PREFIX}types_null_sync_${Date.now()}` } },
        onData: async ({ store, data }) => {
          store.insert(localEvents, [data])
        },
      })

      async function* read() {
        // block 1 omits keys; block 2 sets them to null. Both should land as SQL NULL.
        yield { data: { block_number: 1 }, ctx: makeBatchContext({ number: 1, hash: '0x1' }) }
        yield {
          data: {
            block_number: 2,
            col_string_nullable: null,
            col_int64_nullable: null,
            col_numeric_nullable: null,
          },
          ctx: makeBatchContext({ number: 2, hash: '0x2' }),
        }
      }
      await target.write({ read: read as never, logger: testLogger(), id: 'test-pipe' })

      const [rows] = await bigquery.query({
        query: `SELECT block_number, col_string_nullable, col_int64_nullable, CAST(col_numeric_nullable AS STRING) AS num
                FROM \`${projectId}.${DATASET}.${localEvents}\`
                ORDER BY block_number`,
      })
      expect(rows.length).toBe(2)
      type NullRow = { col_string_nullable: string | null; col_int64_nullable: number | null; num: string | null }
      for (const r of rows as NullRow[]) {
        expect(r.col_string_nullable).toBeNull()
        expect(r.col_int64_nullable).toBeNull()
        expect(r.num).toBeNull()
      }
    }, 60_000)
  })
})
