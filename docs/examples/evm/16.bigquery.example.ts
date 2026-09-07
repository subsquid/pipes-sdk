/**
 * BigQuery target with multiple tracked tables.
 *
 * Indexes ERC20 Transfer + Approval events from Ethereum mainnet into two BigQuery tables
 * within the same dataset, demonstrating the multi-table commit + fork-rollback story:
 *
 *   - Both tables auto-created on first run with `PARTITION BY RANGE_BUCKET(block_number, …)`.
 *     The partition column is forced to `INT64 NOT NULL` regardless of what we declare —
 *     NULLable / FLOAT64 / STRING partition columns silently corrupt fork DELETE, so the
 *     target rejects them with a clear error.
 *
 *   - Each batch buffers rows via `store.insert(table, rows)` (synchronous; throws if the
 *     table isn't in `tables[]`), then commits all tables in parallel via Pending streams
 *     when `onData` returns.
 *
 *   - On reorg, the target opens an `IN_FLIGHT_ROLLBACK` row in the `sync` table, runs
 *     `DELETE FROM <T> WHERE block_number BETWEEN @safe+1 AND @upper` on every tracked
 *     table in parallel, then closes with `ROLLED_BACK`. If the process dies between the
 *     two markers, the next `getCursor()` re-runs the bounded DELETEs idempotently.
 *
 * Prerequisites:
 *   - GCP project with BigQuery API enabled, application-default credentials configured
 *     (`gcloud auth application-default login`).
 *   - A dataset to write into. The example creates it on demand if missing.
 *
 * To run:
 * ```bash
 * BIGQUERY_PROJECT=my-gcp-project \
 * BIGQUERY_DATASET=eth_transfers \
 * bun run docs/examples/evm/16.bigquery.example.ts
 * ```
 *
 * Inspect the result (after a minute or two of indexing):
 * ```sql
 * SELECT block_number, log_index, `from`, `to`, amount, amount_raw
 * FROM `my-gcp-project.eth_transfers.transfers`
 * ORDER BY block_number DESC LIMIT 10;
 *
 * -- Find rows where the BIGNUMERIC was clamped (the raw decimal didn't fit):
 * SELECT COUNT(*) FROM `my-gcp-project.eth_transfers.approvals`
 * WHERE amount_raw != CAST(amount AS STRING);
 *
 * SELECT current
 * FROM `my-gcp-project.eth_transfers.sync`
 * WHERE id = 'erc20-transfers' AND committed = TRUE
 * ORDER BY timestamp DESC LIMIT 1;
 * ```
 */

import { BigQuery } from '@google-cloud/bigquery'
import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'
import { bigqueryTarget } from '@subsquid/pipes/targets/bigquery'

// Default BIGNUMERIC is precision 76, scale 38 — i.e. up to 38 integer digits before the
// decimal point (max ≈ 5.79e38). Pure integer ERC20 amounts ≥ 10^38 overflow; the
// canonical "infinite approval" sentinel `2^256-1` (~1.16e77) is the obvious offender.
// We clamp to this cap so the BIGNUMERIC column is always populated, and preserve the
// exact decimal in `amount_raw` so downstream consumers can detect the clamp.
const BIGNUMERIC_INT_MAX = 10n ** 38n - 1n

const clampBignumeric = (v: bigint): string => (v > BIGNUMERIC_INT_MAX ? BIGNUMERIC_INT_MAX : v).toString()

const PROJECT = process.env['BIGQUERY_PROJECT']
const DATASET = process.env['BIGQUERY_DATASET'] ?? 'eth_transfers'

if (!PROJECT) {
  console.error('Set BIGQUERY_PROJECT (and optionally BIGQUERY_DATASET) before running.')
  process.exit(1)
}

async function main() {
  const bigquery = new BigQuery({ projectId: PROJECT })

  // Ensure the destination dataset exists. The target creates tables on demand inside it
  // but does not create datasets — that's a one-time setup decision the operator owns.
  const [datasetExists] = await bigquery.dataset(DATASET).exists()
  if (!datasetExists) {
    await bigquery.createDataset(DATASET)
  }

  await evmStream({
    id: 'erc20-transfers',
    source: { url: 'https://portal.sqd.dev/datasets/ethereum-mainnet' },
    outputs: evmEventDecoder({
      // 'latest' starts near the head — useful for exercising reorg handling. Use a fixed
      // block number for deterministic backfills.
      range: { from: '0' },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
        approvals: commonAbis.erc20.events.Approval,
      },
    }),
  }).pipeTo(
    // The decoded data type is inferred from the evmEventDecoder events configuration above.
    bigqueryTarget({
      // Pass only the BigQuery client — the target constructs the Storage Write API client
      // internally from the same projectId / apiEndpoint. Pass `client.writer` explicitly
      // only if you need custom credentials or retry settings on the WriterClient.
      client: { bigquery },
      dataset: DATASET,
      // Both tables are auto-created on first run if missing. The schema you declare here
      // is also enforced if the table already exists — a column type mismatch fails fast at
      // startup with the diff, rather than silently mis-encoding rows.
      tables: [
        {
          table: 'transfers',
          blockNumberColumn: 'block_number',
          schema: [
            { name: 'block_number', type: 'INT64', mode: 'REQUIRED' },
            { name: 'log_index', type: 'INT64', mode: 'REQUIRED' },
            { name: 'tx_index', type: 'INT64', mode: 'REQUIRED' },
            // TIMESTAMP wire format is INT64 microseconds since epoch — the Storage Write API
            // JSONWriter does not parse RFC3339/ISO strings, so the caller has to encode the
            // value before passing it in (see how `block_timestamp` is built below).
            { name: 'block_timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
            { name: 'token', type: 'STRING', mode: 'REQUIRED' },
            { name: 'from', type: 'STRING', mode: 'REQUIRED' },
            { name: 'to', type: 'STRING', mode: 'REQUIRED' },
            // `amount` is BIGNUMERIC for arithmetic (SUM/AVG over transfers), clamped to
            // ±BIGNUMERIC_INT_MAX so out-of-range uint256 values still upload as a valid
            // numeric. `amount_raw` keeps the exact decimal — compare the two to detect
            // clamping (e.g. `WHERE amount_raw != CAST(amount AS STRING)`).
            { name: 'amount', type: 'BIGNUMERIC', mode: 'NULLABLE' },
            { name: 'amount_raw', type: 'STRING', mode: 'REQUIRED' },
          ],
          // Cluster on token + from for typical "all transfers of token X from address Y"
          // queries; reduces bytes scanned by 100×+ on filtered reads.
          clusterBy: ['token', 'from'],
        },
        {
          table: 'approvals',
          blockNumberColumn: 'block_number',
          schema: [
            { name: 'block_number', type: 'INT64', mode: 'REQUIRED' },
            { name: 'log_index', type: 'INT64', mode: 'REQUIRED' },
            { name: 'tx_index', type: 'INT64', mode: 'REQUIRED' },
            { name: 'block_timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
            { name: 'token', type: 'STRING', mode: 'REQUIRED' },
            { name: 'owner', type: 'STRING', mode: 'REQUIRED' },
            { name: 'spender', type: 'STRING', mode: 'REQUIRED' },
            // See note on `transfers.amount` — Approvals hit the clamp far more often
            // than Transfers because of the `2^256-1` "infinite approval" idiom.
            { name: 'amount', type: 'BIGNUMERIC', mode: 'NULLABLE' },
            { name: 'amount_raw', type: 'STRING', mode: 'REQUIRED' },
          ],
          clusterBy: ['token', 'owner'],
        },
      ],
      onData: async ({ store, data, ctx }) => {
        ctx.logger.debug(`batch: ${data.transfers.length} transfers, ${data.approvals.length} approvals`)

        // Buffer rows for both tables. Calls are synchronous and accumulate into an internal
        // per-table buffer; the actual Pending-stream commit runs once `onData` returns.
        //
        // Writing to a table that wasn't declared in `tables[]` would throw HERE
        // (synchronously, before any RPC) — that's the allowlist guard preventing silent
        // post-fork corruption.
        store.insert(
          'transfers',
          data.transfers.map((t) => ({
            block_number: t.block.number,
            log_index: t.rawEvent.logIndex,
            tx_index: t.rawEvent.transactionIndex,
            // BQ TIMESTAMP wire format is INT64 microseconds since epoch — the Storage Write
            // API JSONWriter does NOT parse Date / ISO strings on its own (`Long.fromString:
            // interior hyphen`). Multiply ms × 1000 yourself.
            block_timestamp: t.timestamp ? t.timestamp.getTime() * 1000 : 0,
            token: t.rawEvent.address,
            from: t.event.from,
            to: t.event.to,
            amount: clampBignumeric(t.event.value),
            amount_raw: t.event.value.toString(),
          })),
        )

        store.insert(
          'approvals',
          data.approvals.map((a) => ({
            block_number: a.block.number,
            log_index: a.rawEvent.logIndex,
            tx_index: a.rawEvent.transactionIndex,
            block_timestamp: a.timestamp ? a.timestamp.getTime() * 1000 : 0,
            token: a.rawEvent.address,
            owner: a.event.owner,
            spender: a.event.spender,
            amount: clampBignumeric(a.event.value),
            amount_raw: a.event.value.toString(),
          })),
        )
      },
      onBeforeRollback: async ({ cursor }) => {
        // Called after the safe cursor is resolved, before per-table DELETEs run.
        // Useful for cache invalidation or external notifications.
        console.log(`reorg detected → rolling back to block ${cursor.number} (${cursor.hash})`)
      },
    }),
  )
}

void main()
