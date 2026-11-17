import { cast } from '@subsquid/util-internal-validation'
import { describe, expect, it } from 'vitest'

import { getBlockSchema } from './hyperliquid-replica-cmds.js'

const ALL_HEADER_FIELDS = {
  number: true,
  hash: true,
  parentHash: true,
  round: true,
  parentRound: true,
  proposer: true,
  timestamp: true,
  hardfork: true,
} as const

const ALL_ACTION_FIELDS = {
  actionIndex: true,
  user: true,
  action: true,
  signature: true,
  nonce: true,
  vaultAddress: true,
  status: true,
  response: true,
} as const

const header = {
  number: 1223800001,
  hash: '0x0000000000000000000000000000000000000000000000000000000000000000',
  parentHash: '0x0000000000000000000000000000000000000000000000000000000000000000',
  round: 1529432966,
  parentRound: 1529432965,
  proposer: '0x9dd85fb6cf95a9d755af0853b36fb05e972c71e1',
  timestamp: 1794861532843,
  hardfork: { version: 113, round: 1526234267 },
}

const orderAction = {
  actionIndex: 570,
  user: '0x6978e7917dffd1f1815c858ca1528e51a3af1cb5',
  action: {
    type: 'order',
    orders: [
      {
        a: 223,
        b: true,
        p: '2.6816',
        s: '5',
        r: false,
        t: { limit: { tif: 'Alo' } },
        c: '0x9803ba672ed600000000000007639425',
      },
    ],
    grouping: 'na',
  },
  signature: { r: '0x01', s: '0x02', v: 27 },
  nonce: 1794861532573,
  vaultAddress: '0x6978e7917dffd1f1815c858ca1528e51a3af1cb5',
  status: 'ok',
  response: {
    type: 'order',
    data: { statuses: [{ resting: { oid: 607436620785, cloid: '0x9803ba672ed600000000000007639425' } }] },
  },
}

describe('Hyperliquid replica commands block header', () => {
  const schema = getBlockSchema({ block: ALL_HEADER_FIELDS })

  it('decodes every header field', () => {
    expect(cast(schema, { header }).header).toEqual(header)
  })

  it('decodes a null hardfork to null', () => {
    expect(cast(schema, { header: { ...header, hardfork: null } }).header.hardfork).toBeNull()
  })

  it('rejects a hardfork without version and round', () => {
    const hardforkOnly = getBlockSchema({ block: { hardfork: true } })

    expect(() => cast(hardforkOnly, { header: { hardfork: {} } })).toThrow()
  })
})

describe('Hyperliquid replica commands action', () => {
  it('decodes the signature and passes action and response through unchanged', () => {
    const schema = getBlockSchema({ action: ALL_ACTION_FIELDS })

    expect(cast(schema, { header: {}, actions: [orderAction] }).actions).toEqual([orderAction])
  })

  it('rejects a signature without r, s and v', () => {
    const schema = getBlockSchema({ action: { signature: true } })

    expect(() => cast(schema, { header: {}, actions: [{ signature: {} }] })).toThrow()
  })

  it('rejects a signature whose v is not a number', () => {
    const schema = getBlockSchema({ action: { signature: true } })

    expect(() => cast(schema, { header: {}, actions: [{ signature: { r: '0x01', s: '0x02', v: '27' } }] })).toThrow()
  })

  it('rejects an action that is not an object', () => {
    const schema = getBlockSchema({ action: { action: true } })

    expect(() => cast(schema, { header: {}, actions: [{ action: 'order' }] })).toThrow()
  })
})
