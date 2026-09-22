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

describe('Trace result gasUsed validation (null on failed create/call frames)', () => {
  function castCreateResult(gasUsed: unknown) {
    const schema = getBlockSchema({
      trace: {
        type: true,
        transactionIndex: true,
        traceAddress: true,
        subtraces: true,
        error: true,
        createResultGasUsed: true,
        createResultCode: true,
        createResultAddress: true,
      },
    })
    const block = {
      header: {},
      traces: [
        {
          type: 'create',
          transactionIndex: 0,
          traceAddress: [],
          subtraces: 0,
          error: 'execution reverted',
          result: { gasUsed, code: null, address: null },
        },
      ],
    }
    return cast(schema, block).traces[0] as { result: { gasUsed: unknown } }
  }

  function castCallResult(gasUsed: unknown) {
    const schema = getBlockSchema({
      trace: {
        type: true,
        transactionIndex: true,
        traceAddress: true,
        subtraces: true,
        error: true,
        callResultGasUsed: true,
        callResultOutput: true,
      },
    })
    const block = {
      header: {},
      traces: [
        {
          type: 'call',
          transactionIndex: 0,
          traceAddress: [],
          subtraces: 0,
          error: 'execution reverted',
          result: { gasUsed, output: null },
        },
      ],
    }
    return cast(schema, block).traces[0] as { result: { gasUsed: unknown } }
  }

  it('accepts null gasUsed on a create result', () => {
    expect(castCreateResult(null).result.gasUsed).toBeNull()
  })

  it('accepts null gasUsed on a call result', () => {
    expect(castCallResult(null).result.gasUsed).toBeNull()
  })

  it('still parses a present gasUsed as a bigint', () => {
    expect(castCreateResult('0x5208').result.gasUsed).toBe(21000n)
    expect(castCallResult('0x5208').result.gasUsed).toBe(21000n)
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

function castHeader(fields: BlockHeaderFieldSelection, header: Record<string, unknown>) {
  const schema = getBlockSchema({ block: fields })
  return cast(schema, { header, transactions: [] }).header as Record<string, unknown>
}

describe('BlockHeaderFields optional pre-fork fields', () => {
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

describe('BlockHeaderFields avalanche fields', () => {
  const AVALANCHE_FIELDS = {
    extDataHash: true,
    blockExtraData: true,
    blockGasCost: true,
    extDataGasUsed: true,
    timestampMilliseconds: true,
    minDelayExcess: true,
    targetExponent: true,
    minPriceExponent: true,
    settledHeight: true,
    settledGasUnix: true,
    settledGasNumerator: true,
    settledExcess: true,
  } satisfies BlockHeaderFieldSelection

  // Fuji C-Chain block 58542683, post-Helicon: every avalanche field is set.
  const FUJI_HEADER = {
    extDataHash: '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
    blockExtraData: '0x',
    blockGasCost: '0x0',
    extDataGasUsed: '0x0',
    timestampMilliseconds: '0x1a0c45107ef',
    minDelayExcess: '0x6cd69c',
    targetExponent: '0xf0a451',
    minPriceExponent: '0xd49a784bcd1b8b0',
    settledHeight: '0x37d4a57',
    settledGasUnix: '0x6ab13ba2',
    settledGasNumerator: '0x23669b',
    settledExcess: '0x131b0ff4',
  }

  it('casts heights and unix times to number and quantities to bigint', () => {
    expect(castHeader(AVALANCHE_FIELDS, FUJI_HEADER)).toEqual({
      extDataHash: '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
      blockExtraData: '0x',
      blockGasCost: 0n,
      extDataGasUsed: 0n,
      timestampMilliseconds: 1790000039919,
      minDelayExcess: 7132828n,
      targetExponent: 15770705n,
      minPriceExponent: 957480584338323632n,
      settledHeight: 58542679,
      settledGasUnix: 1790000034,
      settledGasNumerator: 2320027n,
      settledExcess: 320540660n,
    })
  })

  it('maps null and absent fields to undefined', () => {
    const nulls = Object.fromEntries(Object.keys(AVALANCHE_FIELDS).map((field) => [field, null]))

    expect(castHeader(AVALANCHE_FIELDS, nulls)).toEqual({})
    expect(castHeader(AVALANCHE_FIELDS, {})).toEqual({})
  })

  it('rejects a non-hex value', () => {
    expect(() => castHeader({ settledExcess: true }, { settledExcess: 'nothex' })).toThrow()
    expect(() => castHeader({ settledHeight: true }, { settledHeight: 'nothex' })).toThrow()
  })

  it('rejects a height beyond the safe integer range instead of rounding it', () => {
    expect(() => castHeader({ settledHeight: true }, { settledHeight: '0x20000000000000' })).toThrow()
  })
})

function castTransaction(fields: Record<string, boolean>, tx: Record<string, unknown>) {
  const schema = getBlockSchema({ transaction: fields })
  return cast(schema, { header: {}, transactions: [tx] }).transactions[0]
}

describe('TransactionFields blob gas encoding', () => {
  const FIELDS = { blobGasPrice: true, blobGasUsed: true }

  it('accepts the hex encoding', () => {
    expect(castTransaction(FIELDS, { blobGasPrice: '0x1', blobGasUsed: '0x20000' })).toEqual({
      blobGasPrice: 1n,
      blobGasUsed: 131072n,
    })
  })

  it('accepts the decimal encoding some blocks carry', () => {
    expect(castTransaction(FIELDS, { blobGasPrice: '1', blobGasUsed: '131072' })).toEqual({
      blobGasPrice: 1n,
      blobGasUsed: 131072n,
    })
  })

  it('keeps both fields optional', () => {
    expect(castTransaction(FIELDS, {})).toEqual({})
  })

  it('rejects a value that is neither', () => {
    expect(() => castTransaction(FIELDS, { blobGasPrice: 'nothex' })).toThrow()
  })
})

describe('TransactionFields null receipt fields', () => {
  it('accepts null effectiveGasPrice and type, which Portal carries for Mantle network transactions in blocks <= 29,459', () => {
    expect(castTransaction({ effectiveGasPrice: true, type: true }, { effectiveGasPrice: null, type: null })).toEqual(
      {},
    )
  })

  it('still decodes them when present', () => {
    expect(castTransaction({ effectiveGasPrice: true, type: true }, { effectiveGasPrice: '0x7', type: 2 })).toEqual({
      effectiveGasPrice: 7n,
      type: 2,
    })
  })
})
