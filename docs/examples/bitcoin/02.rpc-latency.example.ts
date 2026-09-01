import { formatBlock } from '@subsquid/pipes'
import { bitcoinPortalStream, bitcoinRpcLatencyWatcher } from '@subsquid/pipes/bitcoin'

/**
 * Compares Subsquid Portal indexing latency against a public Bitcoin Core JSON-RPC node.
 *
 * Bitcoin Core does NOT expose WebSocket subscriptions, so the latency watcher
 * polls `getbestblockhash` + `getblockheader` over HTTP. The default poll
 * interval is 4s; tune via `intervalMs`. Each request has its own AbortController
 * timeout (`requestTimeoutMs`, default `max(1000, intervalMs)`) so a stalled RPC
 * can never block the loop.
 *
 * For self-hosted nodes that require auth, encode credentials in the URL:
 *   `http://user:pass@127.0.0.1:8332`
 * and the watcher will emit `Authorization: Basic <...>` automatically (Node's
 * `fetch` does not honor URL credentials by itself).
 *
 * A sample is emitted once both sides have reported the head, so `portalDelayMs` is
 * signed: positive when the portal was later than the node, negative when it was first.
 * If the node never reports the head within `resolveTimeoutMs` (default 60s), the row
 * carries `unresolved` instead of a delay — `rpc-behind` when the node had not reached
 * the head, `rpc-missing` when it is already past it (backfill, reorg, dropped update).
 *
 * ⚠️ The reported latency includes client-side network RTT and does NOT capture
 * the node's internal block-validation time.
 */
async function main() {
  const stream = bitcoinPortalStream({
    id: 'bitcoin-indexing-latency',
    portal: process.env['PORTAL_URL'] || 'https://portal.sqd.dev/datasets/bitcoin-mainnet',

    outputs: bitcoinRpcLatencyWatcher({
      // Public, keyless Bitcoin Core JSON-RPC. PublicNode is the simplest one to
      // smoke-test against; for a production deploy, point this at your own
      // bitcoind or a keyed provider (QuickNode, GetBlock, Ankr, ...).
      rpcUrl: ['https://bitcoin-rpc.publicnode.com'],
      intervalMs: 4_000,
    }).pipe((data) => data),
  })

  for await (const { data } of stream) {
    for (const sample of data) {
      console.log(`-------------------------------------`)
      console.log(`BLOCK DATA: ${formatBlock(sample.number)} / ${sample.timestamp.toString()}`)
      console.table(sample.rpc)
    }
  }

  /*
  Example output:
  -------------------------------------
  BLOCK DATA: 900,123 / Mon Jan 06 2025 12:34:56 GMT+0400
  ┌───┬───────────────────────────────────────┬──────────────────────────┬───────────────┐
  │   │ url                                   │ receivedAt               │ portalDelayMs │
  ├───┼───────────────────────────────────────┼──────────────────────────┼───────────────┤
  │ 0 │ https://bitcoin-rpc.publicnode.com    │ 2025-01-06T08:34:56.812Z │ 1843          │
  │ 1 │ https://another-bitcoin-rpc.example   │ 2025-01-06T08:35:00.021Z │ -1366         │
  └───┴───────────────────────────────────────┴──────────────────────────┴───────────────┘
  */
}

void main()
