import { BitcoinQueryBuilder, bitcoinPortalStream } from '@subsquid/pipes/bitcoin'

/**
 * Basic Bitcoin example: stream a small range of recent blocks and parse
 * the UTXO scripts on each transaction's inputs and outputs.
 *
 * The DataRequest model mirrors the Rust portal query (see
 * https://github.com/subsquid/data/blob/master/crates/query/src/query/bitcoin.rs):
 *   - `addTransactionRequest` — request transactions in a range, optionally including
 *     their inputs / outputs.
 *   - `addInputRequest` / `addOutputRequest` — request inputs / outputs by script type or
 *     address, optionally pulling in the parent transaction.
 *   - `includeAllBlocks` — also include blocks that have no matching data
 *     (otherwise the portal skips them for efficiency).
 *
 * Bitcoin Core gives us pre-disassembled scripts for free: `scriptPubKeyAsm`
 * is the human-readable opcode form, `scriptPubKeyType` is the standard
 * classification (`pubkeyhash`, `scripthash`, `witness_v0_keyhash`, …,
 * `nonstandard`), and `scriptPubKeyAddress` is the encoded address when one
 * exists. We surface all three plus a small `classifyScript()` helper that
 * groups the type tags into the higher-level BIP names users expect (P2PKH,
 * P2SH, P2WPKH, P2WSH, P2TR, OP_RETURN, multisig).
 */

type ScriptKind = 'P2PKH' | 'P2SH' | 'P2WPKH' | 'P2WSH' | 'P2TR' | 'OP_RETURN' | 'multisig' | 'p2pk' | 'unknown'

function classifyScript(type: string | undefined): ScriptKind {
  switch (type) {
    case 'pubkeyhash':
      return 'P2PKH'
    case 'scripthash':
      return 'P2SH'
    case 'witness_v0_keyhash':
      return 'P2WPKH'
    case 'witness_v0_scripthash':
      return 'P2WSH'
    case 'witness_v1_taproot':
      return 'P2TR'
    case 'nulldata':
      return 'OP_RETURN'
    case 'multisig':
      return 'multisig'
    case 'pubkey':
      return 'p2pk'
    default:
      return 'unknown'
  }
}

/** Pulls a UTF-8 payload out of an OP_RETURN if it's printable, otherwise returns the raw asm. */
function decodeOpReturn(asm: string): string {
  // asm format is "OP_RETURN <hex>" — grab the hex word(s) after the opcode.
  const m = asm.match(/^OP_RETURN\s+([0-9a-f]+)/i)
  if (!m) return asm
  const hex = m[1]
  const buf = Buffer.from(hex, 'hex')
  // Treat as text only if every byte is printable ASCII.
  const printable = buf.every((b) => b >= 0x20 && b <= 0x7e)
  return printable ? `"${buf.toString('utf8')}"` : `0x${hex}`
}

async function main() {
  const stream = bitcoinPortalStream({
    id: 'bitcoin-simple',
    source: process.env['PORTAL_URL'] || 'https://portal.sqd.dev/datasets/bitcoin-mainnet',
    outputs: new BitcoinQueryBuilder()
      .addFields({
        block: {
          number: true,
          hash: true,
          timestamp: true,
        },
        transaction: {
          transactionIndex: true,
          txid: true,
          size: true,
        },
        input: {
          transactionIndex: true,
          inputIndex: true,
          // Coinbase inputs have no prev tx; everything else points back at one.
          coinbase: true,
          txid: true,
          vout: true,
          // The parent UTXO that this input is spending.
          prevoutValue: true,
          prevoutScriptPubKeyType: true,
          prevoutScriptPubKeyAddress: true,
          prevoutScriptPubKeyAsm: true,
        },
        output: {
          transactionIndex: true,
          outputIndex: true,
          value: true,
          scriptPubKeyType: true,
          scriptPubKeyAddress: true,
          scriptPubKeyAsm: true,
        },
      })
      .addTransactionRequest({
        request: { inputs: true, outputs: true },
        // 900_000 ≈ early-2025 mainnet height. Adjust to the range you want.
        range: { from: 900_000, to: 900_002 },
      }),
  })

  for await (const { data } of stream) {
    for (const block of data) {
      console.log(`-------------------------------------`)
      console.log(`Block ${block.header.number} (${block.header.hash}) — ts=${block.header.timestamp}`)
      console.log(
        `  ${block.transactions.length} txs / ${block.inputs.length} inputs / ${block.outputs.length} outputs`,
      )

      // Pick a non-coinbase tx to demonstrate script parsing.
      const tx =
        block.transactions.find((t) =>
          block.inputs.some((i) => i.transactionIndex === t.transactionIndex && !i.coinbase),
        ) ?? block.transactions[0]
      if (!tx) continue

      const inputs = block.inputs.filter((i) => i.transactionIndex === tx.transactionIndex)
      const outputs = block.outputs.filter((o) => o.transactionIndex === tx.transactionIndex)

      console.log(`  tx[${tx.transactionIndex}] = ${tx.txid} (${tx.size} bytes)`)

      console.log(`    inputs:`)
      for (const inp of inputs.slice(0, 4)) {
        if (inp.coinbase) {
          console.log(`      [#${inp.inputIndex}] coinbase: ${inp.coinbase}`)
          continue
        }
        const kind = classifyScript(inp.prevoutScriptPubKeyType)
        const sats = inp.prevoutValue ?? 0
        console.log(
          `      [#${inp.inputIndex}] ${kind}  value=${sats} sat  prev=${inp.txid}:${inp.vout}  addr=${inp.prevoutScriptPubKeyAddress ?? '—'}`,
        )
      }

      console.log(`    outputs:`)
      for (const out of outputs.slice(0, 4)) {
        const kind = classifyScript(out.scriptPubKeyType)
        const decoded = kind === 'OP_RETURN' ? decodeOpReturn(out.scriptPubKeyAsm) : (out.scriptPubKeyAddress ?? '—')
        console.log(`      [#${out.outputIndex}] ${kind}  value=${out.value} sat  ${decoded}`)
      }
    }
  }

  /*
  Example output (illustrative):
  -------------------------------------
  Block 900,000 (0000…) — ts=1735689600
    3,521 txs / 8,712 inputs / 9,034 outputs
    tx[42] = 7f3a… (226 bytes)
      inputs:
        [#0] P2WPKH  value=350000 sat  prev=ab12…:1  addr=bc1qxy…
      outputs:
        [#0] P2WPKH  value=200000 sat  bc1q9z…
        [#1] P2WPKH  value=149800 sat  bc1qxy…
        [#2] OP_RETURN  value=0 sat  "Hello, world"
  */
}

void main()
