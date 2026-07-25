import { cast } from '@subsquid/util-internal-validation'
import { describe, expect, it } from 'vitest'

import { type BlockHeaderFieldSelection, getBlockSchema } from './evm.js'

function castNonce(nonce: unknown) {
  const schema = getBlockSchema({ transaction: { nonce: true } })
  const block = {
    header: {},
    transactions: [{ nonce }],
  }
  return cast(schema, block).transactions[0].nonce
}

describe('TransactionFields nonce validation', () => {
  it('accepts number and casts to bigint', () => {
    expect(castNonce(42)).toBe(42n)
  })

  it('accepts zero', () => {
    expect(castNonce(0)).toBe(0n)
  })

  it('accepts string and casts to bigint', () => {
    expect(castNonce('42')).toBe(42n)
  })

  it('accepts large numbers as string', () => {
    expect(castNonce('9007199254740993')).toBe(BigInt(Number.MAX_SAFE_INTEGER) + 2n)
  })

  it('rejects negative number', () => {
    expect(() => castNonce(-1)).toThrow()
  })

  it('rejects non-numeric string', () => {
    expect(() => castNonce('abc')).toThrow()
  })

  it('rejects null', () => {
    expect(() => castNonce(null)).toThrow()
  })
  it('rejects float', () => {
    expect(() => castNonce(3.14)).toThrow()
  })
})

function castAccessList(accessList: unknown) {
  const schema = getBlockSchema({ transaction: { accessList: true } })
  const block = {
    header: {},
    transactions: [{ accessList }],
  }
  return cast(schema, block).transactions[0].accessList
}

describe('TransactionFields accessList validation', () => {
  const ADDR = `0x${'ab'.repeat(20)}`
  const KEY = `0x${'cd'.repeat(32)}`

  it('parses entries with address and storage keys', () => {
    expect(
      castAccessList([
        { address: ADDR, storageKeys: [KEY, KEY] },
        { address: ADDR, storageKeys: [] },
      ]),
    ).toEqual([
      { address: ADDR, storageKeys: [KEY, KEY] },
      { address: ADDR, storageKeys: [] },
    ])
  })

  it('is optional (undefined when the field is absent)', () => {
    const schema = getBlockSchema({ transaction: { accessList: true } })
    const out = cast(schema, { header: {}, transactions: [{}] })
    expect(out.transactions[0].accessList).toBeUndefined()
  })

  it('rejects a non-hex address', () => {
    expect(() => castAccessList([{ address: 'nothex', storageKeys: [] }])).toThrow()
  })
})

describe('TraceSuicideAction.refundAddress validation', () => {
  function castSuicideTrace(refundAddress: unknown) {
    const schema = getBlockSchema({
      trace: {
        type: true,
        transactionIndex: true,
        traceAddress: true,
        subtraces: true,
        error: true,
        suicideAddress: true,
        suicideRefundAddress: true,
        suicideBalance: true,
      },
    })
    const block = {
      header: {},
      traces: [
        {
          type: 'suicide',
          transactionIndex: 0,
          traceAddress: [],
          subtraces: 0,
          error: null,
          action: {
            address: '0x0000000000000000000000000000000000000001',
            refundAddress,
            balance: '0x0',
          },
        },
      ],
    }
    return cast(schema, block).traces[0]
  }

  it('accepts null refundAddress (real SELFDESTRUCT edge case)', () => {
    const trace = castSuicideTrace(null) as { action: { refundAddress: unknown } }
    expect(trace.action.refundAddress).toBeNull()
  })

  it('accepts a valid hex refundAddress', () => {
    const trace = castSuicideTrace('0x000000000000000000000000000000000000dead') as {
      action: { refundAddress: unknown }
    }
    expect(trace.action.refundAddress).toBe('0x000000000000000000000000000000000000dead')
  })

  it('rejects a non-hex refundAddress', () => {
    expect(() => castSuicideTrace('not-hex')).toThrow()
  })
})

function castLogsBloom(logsBloom: unknown) {
  const schema = getBlockSchema({ transaction: { logsBloom: true } })
  return cast(schema, { header: {}, transactions: [{ logsBloom }] }).transactions[0].logsBloom
}

describe('TransactionFields logsBloom validation', () => {
  const BLOOM = `0x${'00'.repeat(255)}01` // 256-byte receipt bloom

  it('parses a hex bloom', () => {
    expect(castLogsBloom(BLOOM)).toBe(BLOOM)
  })

  it('is optional (undefined when the field is absent)', () => {
    const schema = getBlockSchema({ transaction: { logsBloom: true } })
    const out = cast(schema, { header: {}, transactions: [{}] })
    expect(out.transactions[0].logsBloom).toBeUndefined()
  })

  it('rejects a non-hex value', () => {
    expect(() => castLogsBloom('nothex')).toThrow()
  })
})

function castUncles(uncles: unknown) {
  const schema = getBlockSchema({ block: { uncles: true } })
  return cast(schema, { header: { uncles } }).header.uncles
}

describe('BlockHeaderFields uncles validation', () => {
  const HASH = `0x${'ab'.repeat(32)}`

  it('parses an array of uncle hashes', () => {
    expect(castUncles([HASH, HASH])).toEqual([HASH, HASH])
  })

  it('is optional (undefined when the field is absent)', () => {
    const schema = getBlockSchema({ block: { uncles: true } })
    expect(cast(schema, { header: {} }).header.uncles).toBeUndefined()
  })

  it('rejects a non-hex entry', () => {
    expect(() => castUncles(['nothex'])).toThrow()
  })
})

function castWithdrawalsRoot(withdrawalsRoot: unknown) {
  const schema = getBlockSchema({ block: { withdrawalsRoot: true } })
  return cast(schema, { header: { withdrawalsRoot } }).header.withdrawalsRoot
}

describe('BlockHeaderFields withdrawalsRoot validation', () => {
  const ROOT = `0x${'ef'.repeat(32)}`

  it('parses a hex root', () => {
    expect(castWithdrawalsRoot(ROOT)).toBe(ROOT)
  })

  it('is optional (undefined when the field is absent)', () => {
    const schema = getBlockSchema({ block: { withdrawalsRoot: true } })
    expect(cast(schema, { header: {} }).header.withdrawalsRoot).toBeUndefined()
  })

  it('rejects a non-hex value', () => {
    expect(() => castWithdrawalsRoot('nothex')).toThrow()
  })
})

function castWithdrawals(withdrawals: unknown) {
  const schema = getBlockSchema({ block: { withdrawals: true } })
  return cast(schema, { header: { withdrawals } }).header.withdrawals
}

describe('BlockHeaderFields withdrawals validation', () => {
  const ADDR = `0x${'12'.repeat(20)}`

  it('parses entries and casts QTY fields to bigint', () => {
    expect(castWithdrawals([{ index: '0x1', validatorIndex: '0x2', address: ADDR, amount: '0x3b9aca00' }])).toEqual([
      { index: 1n, validatorIndex: 2n, address: ADDR, amount: 1000000000n },
    ])
  })

  it('is optional (undefined when the field is absent)', () => {
    const schema = getBlockSchema({ block: { withdrawals: true } })
    expect(cast(schema, { header: {} }).header.withdrawals).toBeUndefined()
  })

  it('rejects a non-hex address', () => {
    expect(() => castWithdrawals([{ index: '0x1', validatorIndex: '0x2', address: 'nothex', amount: '0x3' }])).toThrow()
  })
})

describe('BlockHeaderFields optional pre-fork fields', () => {
  function castHeader(fields: BlockHeaderFieldSelection, header: Record<string, unknown>) {
    const schema = getBlockSchema({ block: fields })
    return cast(schema, { header, transactions: [] }).header as Record<string, unknown>
  }

  it('accepts a header without baseFeePerGas (pre-London) when the field is selected', () => {
    expect(castHeader({ baseFeePerGas: true }, {})['baseFeePerGas']).toBeUndefined()
  })

  it('casts baseFeePerGas hex QTY to bigint when present', () => {
    expect(castHeader({ baseFeePerGas: true }, { baseFeePerGas: '0x7' })['baseFeePerGas']).toBe(7n)
  })

  it('accepts a header without blobGasUsed/excessBlobGas (pre-Cancun) when selected', () => {
    const h = castHeader({ blobGasUsed: true, excessBlobGas: true }, {})
    expect(h['blobGasUsed']).toBeUndefined()
    expect(h['excessBlobGas']).toBeUndefined()
  })

  it('casts blobGasUsed/excessBlobGas hex QTY to bigint when present', () => {
    const h = castHeader({ blobGasUsed: true, excessBlobGas: true }, { blobGasUsed: '0x10', excessBlobGas: '0x20' })
    expect(h['blobGasUsed']).toBe(16n)
    expect(h['excessBlobGas']).toBe(32n)
  })
})
