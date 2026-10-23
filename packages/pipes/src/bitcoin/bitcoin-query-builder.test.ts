import { describe, expect, it } from 'vitest'

import { stubPortal } from '~/testing/stub-portal.js'

import { BitcoinQueryBuilder } from './bitcoin-query-builder.js'

describe('BitcoinQueryBuilder', () => {
  describe('from: latest', () => {
    it('should resolve to the latest', async () => {
      const builder = new BitcoinQueryBuilder()
      builder.addRange({ from: 'latest' })

      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({
          getHead: async () => ({ number: 800_000, hash: '0x' }),
        }),
      })

      expect(bounded).toMatchInlineSnapshot(`
        [
          {
            "range": {
              "from": 800000,
            },
            "request": {},
          },
        ]
      `)
    })
  })

  describe('addRequest', () => {
    it('registers transaction, input and output requests', () => {
      const builder = new BitcoinQueryBuilder()
        .addTransactionRequest({ range: { from: 0 }, request: { inputs: true, outputs: true } })
        .addInputRequest({ range: { from: 100, to: 200 }, request: { type: ['witness_v0_keyhash'] } })
        .addOutputRequest({ range: { from: 300 }, request: { scriptPubKeyType: ['scripthash'] } })

      expect(builder.getRequests()).toMatchInlineSnapshot(`
        [
          {
            "range": {
              "from": 0,
              "to": undefined,
            },
            "request": {
              "transactions": [
                {
                  "inputs": true,
                  "outputs": true,
                },
              ],
            },
          },
          {
            "range": {
              "from": 100,
              "to": 200,
            },
            "request": {
              "inputs": [
                {
                  "type": [
                    "witness_v0_keyhash",
                  ],
                },
              ],
            },
          },
          {
            "range": {
              "from": 300,
              "to": undefined,
            },
            "request": {
              "outputs": [
                {
                  "scriptPubKeyType": [
                    "scripthash",
                  ],
                },
              ],
            },
          },
        ]
      `)
    })
  })

  describe('mergeDataRequests', () => {
    it('concatenates transaction/input/output lists and propagates includeAllBlocks', () => {
      const builder = new BitcoinQueryBuilder()

      const merged = builder.mergeDataRequests(
        { transactions: [{ inputs: true }], includeAllBlocks: false },
        { inputs: [{ prevoutGenerated: true }], includeAllBlocks: true },
        { outputs: [{ scriptPubKeyType: ['pubkey'] }] },
      )

      expect(merged).toEqual({
        transactions: [{ inputs: true }],
        inputs: [{ prevoutGenerated: true }],
        outputs: [{ scriptPubKeyType: ['pubkey'] }],
        includeAllBlocks: true,
      })
    })
  })

  describe('includeAllBlocks', () => {
    it('adds an includeAllBlocks request', () => {
      const builder = new BitcoinQueryBuilder().includeAllBlocks({ from: 10, to: 20 })

      expect(builder.getRequests()).toEqual([{ range: { from: 10, to: 20 }, request: { includeAllBlocks: true } }])
    })
  })

  it('reports the bitcoin query type', () => {
    expect(new BitcoinQueryBuilder().getType()).toBe('bitcoin')
  })
})
