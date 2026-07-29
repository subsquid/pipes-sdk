import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { afterEach, describe, expect, it } from 'vitest'

import { MockPortal, mockPortal, readAll } from '../testing/index.js'
import { tronPortalStream } from './tron-portal-source.js'
import { TronQueryBuilder } from './tron-query-builder.js'

const fixture = (name: string) =>
  JSON.parse(readFileSync(fileURLToPath(new URL(`./__fixtures__/${name}`, import.meta.url)), 'utf8'))

describe('Tron portal stream', () => {
  let portal: MockPortal

  afterEach(async () => {
    await portal?.close()
  })

  it('streams blocks with selected fields and array defaults', async () => {
    // Hashes/addresses are bare hex (no `0x`) and amounts are decimal strings —
    // matching the real TRON portal wire format.
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          {
            header: { number: 1, hash: '00000000abcd1', timestamp: 1782669669000 },
            transactions: [{ transactionIndex: 0, hash: 'deadbeef', type: 'TransferContract', feeLimit: '1000' }],
          },
          {
            header: { number: 2, hash: '00000000abcd2', timestamp: 1782669672000 },
          },
        ],
      },
    ])

    const stream = tronPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: new TronQueryBuilder()
        .addFields({
          block: { number: true, hash: true, timestamp: true },
          transaction: { transactionIndex: true, hash: true, type: true, feeLimit: true },
        })
        .addRange({ from: 0, to: 2 }),
    })

    for await (const { data } of stream) {
      expect(data).toHaveLength(2)
      // Decimal-string amount is parsed to a bigint.
      expect(data[0].transactions).toEqual([
        { transactionIndex: 0, hash: 'deadbeef', type: 'TransferContract', feeLimit: 1000n },
      ])
      expect(data[0].header).toEqual({ number: 1, hash: '00000000abcd1', timestamp: 1782669669000 })
      // The TRON block schema fills missing collections with empty arrays.
      expect(data[1].transactions).toEqual([])
      expect(data[1].logs).toEqual([])
      expect(data[1].internalTransactions).toEqual([])
    }
  })

  // Regression guard: real TRON data validated end-to-end through the full schema.
  // The portal serializes big amounts as signed DECIMAL strings (not 0x-hex), so they
  // go through BIG_INT (-> bigint), while ms timestamps stay plain numbers
  // and `parameter`/`ret`/`callValueInfo` pass through as raw JSON. Mocks with
  // fake `0x...` data would hide a wrong-validator regression; this pumps an
  // unmodified portal response (block #84000000) so any such regression fails here.
  it('validates a real portal response (block #84000000) end-to-end', async () => {
    const block = fixture('block-84000000.json')

    portal = await mockPortal([
      {
        statusCode: 200,
        data: [block],
      },
    ])

    const stream = tronPortalStream({
      id: 'real-data',
      portal: portal.url,
      outputs: new TronQueryBuilder()
        .addFields({
          block: {
            number: true,
            hash: true,
            parentHash: true,
            txTrieRoot: true,
            version: true,
            timestamp: true,
            witnessAddress: true,
            witnessSignature: true,
          },
          transaction: {
            transactionIndex: true,
            hash: true,
            ret: true,
            signature: true,
            type: true,
            parameter: true,
            permissionId: true,
            feeLimit: true,
            expiration: true,
            timestamp: true,
            rawDataHex: true,
            fee: true,
            contractResult: true,
            result: true,
            energyUsageTotal: true,
            netFee: true,
            netUsage: true,
          },
          log: { transactionIndex: true, logIndex: true, address: true, data: true, topics: true },
          internalTransaction: {
            transactionIndex: true,
            internalTransactionIndex: true,
            hash: true,
            callerAddress: true,
            transferToAddress: true,
            callValueInfo: true,
            note: true,
            rejected: true,
            extra: true,
          },
        })
        .addRange({ from: 84_000_000, to: 84_000_000 }),
    })

    for await (const { data } of stream) {
      expect(data).toHaveLength(1)
      const [b] = data

      // Bare-hex hash and ms timestamp survive validation untouched.
      expect(b.header.hash).toBe('000000000501bd00573eff7d866a350d2f036683d0d29518dfdc360342427b90')
      expect(b.header.timestamp).toBe(1782669669000)
      expect(b.header.version).toBe(35)

      const trigger = b.transactions.find((t) => t.type === 'TriggerSmartContract')!
      expect(trigger).toBeDefined()
      // Decimal-string amounts become bigints.
      expect(trigger.feeLimit).toBe(26_400_000n)
      expect(trigger.energyUsageTotal).toBe(64_285n)
      expect(trigger.netUsage).toBe(345n)
      // Millisecond timestamps stay plain numbers.
      expect(trigger.expiration).toBe(1782669723000)
      expect(trigger.timestamp).toBe(1782669665096)
      // JSON passthrough fields keep their raw shape.
      expect(trigger.ret).toEqual([{ contractRet: 'SUCCESS' }])
      expect(Array.isArray(trigger.signature)).toBe(true)
      expect(trigger.parameter.type_url).toBe('type.googleapis.com/protocol.TriggerSmartContract')
      // A selected-but-null field is normalized to undefined.
      expect(trigger.permissionId).toBeUndefined()

      const paid = b.transactions.find((t) => t.fee !== undefined)!
      expect(paid.fee).toBe(269_000n)
      expect(paid.netFee).toBe(269_000n)

      // A TRC-20 Transfer log: bare-hex address, rolled-up topics array.
      const log = b.logs[0]
      expect(log.address).toBe('a614f803b6fd780986a42c78ec9c7f77e6ded13c')
      expect(log.topics).toHaveLength(3)
      expect(log.topics![0]).toBe('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')

      // Internal transaction: callValueInfo passes through as raw JSON, null
      // optionals collapse to undefined.
      const internal = b.internalTransactions[0]
      expect(internal.callerAddress).toBe('41c736ce43d4dcfa293846c9b442e6c9d68d9e0e66')
      expect(internal.callValueInfo).toEqual([{ callValue: null, tokenId: null }])
      expect(internal.rejected).toBeUndefined()
      expect(internal.extra).toBeUndefined()
    }
  })

  // Regression: int64 ms timestamps above 2^53 (e.g. 639208360527210660) arrive as JSON numbers;
  // `NAT` rejected the resulting imprecise floats and halted the stream.
  it('tolerates int64 timestamps above the safe-integer range', async () => {
    // string-built so the imprecision reads as intentional, not a typo
    const hugeMs = Number('639208360527210660')
    expect(Number.isSafeInteger(hugeMs)).toBe(false)

    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          {
            header: { number: 84856193, hash: '00000000abcd1', timestamp: hugeMs },
            transactions: [
              {
                transactionIndex: 0,
                hash: 'deadbeef',
                type: 'TransferContract',
                expiration: hugeMs,
                timestamp: hugeMs,
              },
            ],
          },
        ],
      },
    ])

    const stream = tronPortalStream({
      id: 'huge-timestamp',
      portal: portal.url,
      outputs: new TronQueryBuilder()
        .addFields({
          block: { number: true, hash: true, timestamp: true },
          transaction: { transactionIndex: true, hash: true, type: true, expiration: true, timestamp: true },
        })
        .addRange({ from: 84856193, to: 84856193 }),
    })

    const data = await readAll(stream)
    expect(data).toHaveLength(1)

    // survive validation as imprecise floats instead of throwing
    expect(data[0].header.timestamp).toBe(hugeMs)
    const [tx] = data[0].transactions
    expect(tx.expiration).toBe(hugeMs)
    expect(tx.timestamp).toBe(hugeMs)
  })

  // Regression: int64 amounts can be negative (e.g. `feeLimit: "-18395898"`); `BIG_NAT` rejected
  // them. Every amount field must use the signed decoder.
  it('parses negative int64 amount fields instead of halting', async () => {
    portal = await mockPortal([
      {
        statusCode: 200,
        data: [
          {
            header: { number: 51068797, hash: '00000000abcd2', timestamp: 1782669669000 },
            transactions: [
              {
                transactionIndex: 0,
                hash: 'cafe',
                type: 'TransferContract',
                feeLimit: '-18395898',
                fee: '-269000',
                netFee: '-269000',
                energyUsage: '-5',
                withdrawAmount: '-1',
              },
            ],
          },
        ],
      },
    ])

    const stream = tronPortalStream({
      id: 'negative-amounts',
      portal: portal.url,
      outputs: new TronQueryBuilder()
        .addFields({
          block: { number: true, hash: true },
          transaction: {
            transactionIndex: true,
            hash: true,
            type: true,
            feeLimit: true,
            fee: true,
            netFee: true,
            energyUsage: true,
            withdrawAmount: true,
          },
        })
        .addRange({ from: 51068797, to: 51068797 }),
    })

    const data = await readAll(stream)
    expect(data).toHaveLength(1)

    const [tx] = data[0].transactions
    expect(tx.feeLimit).toBe(-18395898n)
    expect(tx.fee).toBe(-269000n)
    expect(tx.netFee).toBe(-269000n)
    expect(tx.energyUsage).toBe(-5n)
    expect(tx.withdrawAmount).toBe(-1n)
  })
})
