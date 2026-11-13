import { TronQueryBuilder, tronPortalStream } from '@subsquid/pipes/tron'

/**
 * Basic TRON example: stream USDT (TRC-20) transfers and decode them from the
 * smart-contract logs they emit.
 *
 * The DataRequest model mirrors the Rust portal query (see
 * https://github.com/subsquid/data/blob/master/crates/query/src/query/tron.rs):
 *   - `addTransactionRequest` — request transactions by `type`, optionally including
 *     their `logs` / `internalTransactions`.
 *   - `addTransferTransactionRequest` / `addTransferAssetTransactionRequest` — native TRX and
 *     TRC-10 transfers, filterable by `owner` / `to` (/ `asset`).
 *   - `addTriggerSmartContractTransactionRequest` — smart-contract calls, filterable by
 *     `owner` / `contract` / `sighash` (the 4-byte method selector).
 *   - `addLogRequest` — event logs, filterable by `address` / `topic0..3`.
 *   - `addInternalTransactionRequest` — internal transfers, by `caller` / `transferTo`.
 *   - `includeAllBlocks` — also include blocks that have no matching data.
 *
 * A few TRON-specific notes:
 *   - Hex values are BARE (no `0x`). On-chain addresses are 21 bytes prefixed
 *     with `41` (`41a614f803…`), while log topics/addresses use the 20-byte
 *     EVM-style form. We rebuild the `41…` form from a topic below.
 *   - Amounts (`feeLimit`, `fee`, the `energy*`/`net*` family) arrive as decimal
 *     strings and are surfaced as `bigint`.
 *   - `timestamp` / `expiration` are Unix milliseconds.
 */

// USDT (TRC-20) contract, `TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t` in base58.
const USDT_CONTRACT = '41a614f803b6fd780986a42c78ec9c7f77e6ded13c'

// `transfer(address,uint256)` selector and the `Transfer(address,address,uint256)`
// event signature hash (same as ERC-20 — TRON's EVM is Ethereum-compatible).
const TRANSFER_SIGHASH = 'a9059cbb'
const TRANSFER_TOPIC0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

/** A 32-byte ABI-padded topic -> TRON hex address (`41` + last 20 bytes). */
function topicToTronAddress(topic: string): string {
  return `41${topic.slice(-40)}`
}

async function main() {
  const stream = tronPortalStream({
    id: 'tron-usdt-transfers',
    source: process.env['PORTAL_URL'] || 'https://portal.sqd.dev/datasets/tron-mainnet',
    outputs: new TronQueryBuilder()
      .addFields({
        block: {
          number: true,
          hash: true,
          timestamp: true,
        },
        transaction: {
          transactionIndex: true,
          hash: true,
          type: true,
          // Decimal-string amounts -> bigint.
          feeLimit: true,
          energyUsageTotal: true,
          result: true,
        },
        log: {
          transactionIndex: true,
          logIndex: true,
          address: true,
          topics: true,
          data: true,
        },
      })
      // Pull USDT `transfer(...)` calls and the logs they emit.
      .addTriggerSmartContractTransactionRequest({
        request: {
          contract: [USDT_CONTRACT],
          sighash: [TRANSFER_SIGHASH],
          logs: true,
        },
        // ≈ mid-2026 mainnet height. Adjust to the range you want.
        range: { from: 84_000_000, to: 84_000_010 },
      }),
  })

  for await (const { data } of stream) {
    for (const block of data) {
      const transfers = block.logs.filter((log) => log.topics?.[0] === TRANSFER_TOPIC0)
      if (transfers.length === 0) continue

      console.log(`-------------------------------------`)
      console.log(`Block ${block.header.number} (${block.header.hash}) — ts=${block.header.timestamp}`)
      console.log(`  ${block.transactions.length} txs / ${transfers.length} USDT transfers`)

      for (const log of transfers.slice(0, 5)) {
        // Transfer(from indexed, to indexed, value) — addresses in topics[1..2],
        // value in `data`. USDT uses 6 decimals.
        const from = topicToTronAddress(log.topics![1])
        const to = topicToTronAddress(log.topics![2])
        const raw = BigInt(`0x${log.data}`)
        const usdt = Number(raw) / 1e6

        const tx = block.transactions.find((t) => t.transactionIndex === log.transactionIndex)
        console.log(
          `    tx[${log.transactionIndex}] ${tx?.hash ?? '—'}  ${from} → ${to}  ${usdt} USDT` +
            (tx?.energyUsageTotal != null ? `  (energy=${tx.energyUsageTotal})` : ''),
        )
      }
    }
  }

  /*
  Example output (illustrative):
  -------------------------------------
  Block 84,000,000 (000000000501bd00…) — ts=1782669669000
    67 txs / 12 USDT transfers
      tx[12] 326752540f29…  41d95174a0903d3cc16a9b41e9668cf60d01af64f8 → 415bdb8b4c4a3d0a93df56b88f8e2158cbe788fb39  320 USDT  (energy=64285)
  */
}

void main()
