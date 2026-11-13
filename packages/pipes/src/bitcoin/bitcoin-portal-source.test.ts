import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { afterEach, describe, expect, it } from 'vitest'

import { MockPortal, mockPortal } from '../testing/index.js'
import { bitcoinPortalStream } from './bitcoin-portal-source.js'
import { BitcoinQueryBuilder } from './bitcoin-query-builder.js'

const fixture = (name: string) =>
  JSON.parse(readFileSync(fileURLToPath(new URL(`./__fixtures__/${name}`, import.meta.url)), 'utf8'))

describe('Bitcoin portal stream', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  it('streams blocks with selected fields and array defaults', async () => {
    // Hashes/txids are bare hex (no `0x`) — matching the real Bitcoin portal wire format.
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          {
            header: { number: 1, hash: '00000000abcd1', timestamp: 1700000000 },
            transactions: [{ transactionIndex: 0, txid: 'deadbeef' }],
          },
          {
            header: { number: 2, hash: '00000000abcd2', timestamp: 1700000600 },
          },
        ],
      },
    ])

    const stream = bitcoinPortalStream({
      id: 'test',
      source: portal.url,
      outputs: new BitcoinQueryBuilder()
        .addFields({
          block: { number: true, hash: true, timestamp: true },
          transaction: { transactionIndex: true, txid: true },
        })
        .addRange({ from: 0, to: 2 }),
    })

    for await (const { data } of stream) {
      expect(data).toHaveLength(2)
      expect(data[0].transactions).toEqual([{ transactionIndex: 0, txid: 'deadbeef' }])
      expect(data[0].header).toEqual({ number: 1, hash: '00000000abcd1', timestamp: 1700000000 })
      // Bitcoin block schema fills missing collections with empty arrays
      expect(data[1].transactions).toEqual([])
      expect(data[1].inputs).toEqual([])
      expect(data[1].outputs).toEqual([])
    }
  })

  // Regression: an earlier version of the schema used `BYTES`/`NAT` validators
  // borrowed from EVM, which rejected real Bitcoin data on two counts:
  //   1. Bitcoin hex strings have no `0x` prefix (BYTES requires it).
  //   2. Output `value` / `prevoutValue` are BTC as JSON floats (NAT only
  //      accepts non-negative integers).
  // Both surfaced as validation errors at runtime against the live portal but
  // were invisible in tests because mocks used `0xabcd1`-style fake data. This
  // test pumps an unmodified portal response (Bitcoin block #100000) through
  // the full schema so any future regression on either front fails here.
  it('validates a real portal response (block #100000) end-to-end', async () => {
    const block = fixture('block-100000.json')

    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block],
      },
    ])

    const stream = bitcoinPortalStream({
      id: 'real-data',
      source: portal.url,
      outputs: new BitcoinQueryBuilder()
        .addFields({
          block: {
            number: true,
            hash: true,
            parentHash: true,
            timestamp: true,
            medianTime: true,
            version: true,
            merkleRoot: true,
            nonce: true,
            target: true,
            bits: true,
            difficulty: true,
            chainWork: true,
            strippedSize: true,
            size: true,
            weight: true,
          },
          transaction: { transactionIndex: true, txid: true, hash: true, hex: true },
          input: {
            transactionIndex: true,
            inputIndex: true,
            type: true,
            txid: true,
            vout: true,
            sequence: true,
            coinbase: true,
            txInWitness: true,
            prevoutGenerated: true,
            prevoutHeight: true,
            prevoutValue: true,
            prevoutScriptPubKeyHex: true,
            prevoutScriptPubKeyType: true,
            prevoutScriptPubKeyAddress: true,
          },
          output: {
            transactionIndex: true,
            outputIndex: true,
            value: true,
            scriptPubKeyHex: true,
            scriptPubKeyAsm: true,
            scriptPubKeyType: true,
            scriptPubKeyAddress: true,
          },
        })
        .addRange({ from: 100_000, to: 100_000 }),
    })

    for await (const { data } of stream) {
      expect(data).toHaveLength(1)
      const [b] = data

      // Bare-hex hash survives validation untouched.
      expect(b.header.hash).toBe('000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506')
      expect(b.header.parentHash).toBe('000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250')
      expect(b.header.difficulty).toBeCloseTo(14484.16, 2)

      // Block #100000 has 4 transactions; the coinbase produces a 50 BTC output.
      expect(b.transactions).toHaveLength(4)
      const coinbaseInput = b.inputs.find((i) => i.type === 'coinbase')
      expect(coinbaseInput).toBeDefined()
      expect(coinbaseInput!.txid).toBeUndefined()
      expect(coinbaseInput!.coinbase).toBe('044c86041b020602')

      const coinbaseOutput = b.outputs.find((o) => o.transactionIndex === 0 && o.outputIndex === 0)
      expect(coinbaseOutput!.value).toBe(50)

      // Real outputs include fractional BTC values — must round-trip as floats,
      // not be coerced to integers or rejected.
      const fractional = b.outputs.find((o) => o.value === 0.01)
      expect(fractional).toBeDefined()

      // Inputs that spend prior outputs carry float `prevoutValue` too.
      const spendingInput = b.inputs.find((i) => i.prevoutValue !== undefined)
      expect(spendingInput!.prevoutValue).toBeGreaterThan(0)
    }
  })
})
