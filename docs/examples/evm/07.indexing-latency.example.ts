import { formatBlock } from '@subsquid/pipes'
import { evmPortalStream, evmRpcLatencyWatcher } from '@subsquid/pipes/evm'
import { metricsServer } from '@subsquid/pipes/metrics/node'

/**
 * This example demonstrates how to track and compare block indexing latency
 * between the Subsquid Portal and external RPC providers.
 * It listens for new block heads and measures the time until blocks are
 * observed on the client side through both RPC endpoints and the Portal.

 ******************************************************************************
 * ⚠️ Important:
 * - The measured values INCLUDE client-side network latency.
 * - For RPC, only the *arrival time* of the block is measured — this does NOT
 *   capture the node’s internal processing or response latency if queried directly.
 *****************************************************************************

 * In other words, the results represent end-to-end delays as experienced by the client,
 * not the pure Portal latency or RPC processing performance.
 *
 * Each sample is emitted once both sides have reported the head, so `portalDelayMs` is signed:
 * positive when the portal was later than the RPC, negative when it was first. Rows for a head an
 * endpoint never reported carry `unresolved` instead of a delay and are skipped here.
 */

async function main() {
  // Create a stream of new blocks from the Base mainnet portal
  const stream = evmPortalStream({
    id: 'indexing-latency',
    portal: 'https://portal.sqd.dev/datasets/base-mainnet',
    outputs: evmRpcLatencyWatcher({
      rpcUrl: ['https://base.drpc.org', 'https://base-rpc.publicnode.com'], // RPC endpoints to monitor
    }).pipe((data, { metrics }) => {
      const gauge = metrics.gauge({
        name: 'rpc_latency_ms',
        help: 'Portal delay against an RPC endpoint, in ms (negative: the portal delivered first)',
        labelNames: ['url'],
      })

      for (const sample of data) {
        for (const rpc of sample.rpc) {
          if (rpc.portalDelayMs === undefined) {
            continue // The endpoint never reported this head — see `unresolved`
          }

          gauge.set({ url: rpc.url }, rpc.portalDelayMs)
        }
      }

      return data
    }), // Start from the latest block

    metrics: metricsServer({
      port: 9090,
    }),
  })

  // Iterate over the stream, logging block and RPC latency data
  for await (const { data } of stream) {
    for (const sample of data) {
      // Log block number and timestamp
      console.log(`-------------------------------------`)
      console.log(`BLOCK DATA: ${formatBlock(sample.number)} / ${sample.timestamp.toString()}`)
      // Log RPC latency table for the block
      console.table(sample.rpc)
    }
  }
  /*
  Example output:
  -------------------------------------
  BLOCK DATA: 36,046,611 / Fri Sep 26 2025 14:29:29 GMT+0400 (Georgia Standard Time)
  ┌───┬─────────────────────────────────┬──────────────────────────┬───────────────┐
  │   │ url                             │ receivedAt               │ portalDelayMs │
  ├───┼─────────────────────────────────┼──────────────────────────┼───────────────┤
  │ 0 │ https://base.drpc.org           │ 2025-09-26T10:29:29.134Z │ 646           │
  │ 1 │ https://base-rpc.publicnode.com │ 2025-09-26T10:29:29.130Z │ 642           │
  └───┴─────────────────────────────────┴──────────────────────────┴───────────────┘
  -------------------------------------
  BLOCK DATA: 36,046,617 / Fri Sep 26 2025 14:29:41 GMT+0400 (Georgia Standard Time)
  ┌───┬─────────────────────────────────┬──────────────────────────┬───────────────┐
  │   │ url                             │ receivedAt               │ portalDelayMs │
  ├───┼─────────────────────────────────┼──────────────────────────┼───────────────┤
  │ 0 │ https://base.drpc.org           │ 2025-09-26T10:29:41.217Z │ 826           │
  │ 1 │ https://base-rpc.publicnode.com │ 2025-09-26T10:29:41.218Z │ 827           │
  └───┴─────────────────────────────────┴──────────────────────────┴───────────────┘
  */
}

// Start the main function
void main()
