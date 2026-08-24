import { BigQuery } from '@google-cloud/bigquery'
import { managedwriter } from '@google-cloud/bigquery-storage'

import type { BatchContext, BlockCursor } from '~/core/index.js'
import { mockMetricsServer, testLogger } from '~/testing/index.js'

import { type TrackedTable, partitioningWithDefaults } from './tables.js'

/**
 * Shared scaffolding for the BigQuery integration test files. The integration suite is split
 * across multiple files (basic + type-mappings here, fork lifecycle in
 * `bigquery-target-fork.integration.test.ts`); this module owns the env-vars, prefix, and
 * fixture builders so each file stays focused on its own assertions.
 *
 * Gating: every file does `describe.skipIf(!RUN)`. With `BIGQUERY_TEST_PROJECT` unset the
 * suite is skipped wholesale and nothing here ever runs against a real GCP project.
 */

export const PROJECT = process.env['BIGQUERY_TEST_PROJECT']
export const DATASET = process.env['BIGQUERY_TEST_DATASET'] ?? 'pipes_target_test'
export const RUN = !!PROJECT
// Narrowed at module top-level: only ever read inside `describe.skipIf(!RUN)` bodies, so by
// the time a test executes PROJECT was non-empty (the gate is the same `RUN` flag).
export const projectId = PROJECT as string

// Every table the integration suites create uses this prefix. The `setupIntegrationDataset`
// sweep matches it exactly so leftover tables from a failed run get cleaned up on the next
// run, while never touching the user's own data sharing the dataset.
export const PREFIX = 'e2e_test_'

export const partitioning = partitioningWithDefaults()

/** A two-column tracked-table template; copy and override `table` per test. */
export const trackedTable: TrackedTable = {
  table: 'unused', // overridden per-test
  blockNumberColumn: 'block_number',
  schema: [
    { name: 'block_number', type: 'INT64', mode: 'REQUIRED' },
    { name: 'tx_hash', type: 'STRING' },
  ],
}

/**
 * Creates a fresh BigQuery REST client + Storage Write API client for the suite, ensures the
 * dataset exists, and sweeps any leftover `${PREFIX}*` tables from previous runs. Tests
 * deliberately do NOT clean up after themselves — failures leave inspectable state behind,
 * and this sweep makes sure those leftovers don't accumulate beyond one run.
 */
export async function setupIntegrationClients(): Promise<{
  bigquery: BigQuery
  writer: managedwriter.WriterClient
}> {
  const bigquery = new BigQuery({ projectId })
  const writer = new managedwriter.WriterClient({ projectId })

  const [exists] = await bigquery.dataset(DATASET).exists()
  if (!exists) await bigquery.createDataset(DATASET)

  const [allTables] = await bigquery.dataset(DATASET).getTables()
  await Promise.all(
    allTables
      .filter((t) => (t.id ?? '').startsWith(PREFIX))
      .map((t) => t.delete({ ignoreNotFound: true }).catch(() => {})),
  )

  return { bigquery, writer }
}

/**
 * Builds a synthetic `BatchContext` for tests that drive `target.write()` directly (i.e.
 * without `evmStream`). Includes the `id` + `metrics` slots the BigQuery target reads
 * for Track-2 commit-stage metrics — without them, the first batch crashes on
 * `registerBqTargetMetrics(ctx.metrics)`.
 */
export function makeBatchContext(current: BlockCursor): BatchContext {
  const profilerStub: Record<string, unknown> = {
    start: () => profilerStub,
    measure: async (_: unknown, fn: () => unknown) => fn(),
    end: () => {},
  }

  return {
    id: 'integration-test',
    logger: testLogger(),
    profiler: profilerStub,
    metrics: mockMetricsServer().server.metrics,
    stream: {
      state: { current, rollbackChain: [current] },
      head: { finalized: undefined, latest: current },
    },
  } as unknown as BatchContext
}
