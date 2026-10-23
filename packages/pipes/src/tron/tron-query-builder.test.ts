import { describe, expect, it } from 'vitest'

import { stubPortal } from '~/testing/stub-portal.js'

import { TronQueryBuilder } from './tron-query-builder.js'

describe('TronQueryBuilder', () => {
  describe('from: latest', () => {
    it('should resolve to the latest', async () => {
      const builder = new TronQueryBuilder()
      builder.addRange({ from: 'latest' })

      const { bounded } = await builder.calculateRanges({
        portal: stubPortal({
          getHead: async () => ({ number: 84_000_000, hash: '0x' }),
        }),
      })

      expect(bounded).toMatchInlineSnapshot(`
        [
          {
            "range": {
              "from": 84000000,
            },
            "request": {},
          },
        ]
      `)
    })
  })

  describe('addRequest', () => {
    it('registers every TRON request kind under its own key', () => {
      const builder = new TronQueryBuilder()
        .addTransactionRequest({ range: { from: 0 }, request: { type: ['TriggerSmartContract'], logs: true } })
        .addTransferTransactionRequest({ range: { from: 100, to: 200 }, request: { to: ['41abc'] } })
        .addTransferAssetTransactionRequest({ range: { from: 300 }, request: { asset: ['1002000'] } })
        .addTriggerSmartContractTransactionRequest({ range: { from: 400 }, request: { sighash: ['a9059cbb'] } })
        .addLogRequest({ range: { from: 500 }, request: { topic0: ['ddf252ad'] } })
        .addInternalTransactionRequest({ range: { from: 600 }, request: { caller: ['4111'] } })

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
                  "logs": true,
                  "type": [
                    "TriggerSmartContract",
                  ],
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
              "transferTransactions": [
                {
                  "to": [
                    "41abc",
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
              "transferAssetTransactions": [
                {
                  "asset": [
                    "1002000",
                  ],
                },
              ],
            },
          },
          {
            "range": {
              "from": 400,
              "to": undefined,
            },
            "request": {
              "triggerSmartContractTransactions": [
                {
                  "sighash": [
                    "a9059cbb",
                  ],
                },
              ],
            },
          },
          {
            "range": {
              "from": 500,
              "to": undefined,
            },
            "request": {
              "logs": [
                {
                  "topic0": [
                    "ddf252ad",
                  ],
                },
              ],
            },
          },
          {
            "range": {
              "from": 600,
              "to": undefined,
            },
            "request": {
              "internalTransactions": [
                {
                  "caller": [
                    "4111",
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
    it('concatenates all request lists and propagates includeAllBlocks', () => {
      const builder = new TronQueryBuilder()

      const merged = builder.mergeDataRequests(
        { transactions: [{ type: ['TransferContract'] }], includeAllBlocks: false },
        { transferTransactions: [{ owner: ['4111'] }] },
        { transferAssetTransactions: [{ asset: ['1002000'] }] },
        { triggerSmartContractTransactions: [{ contract: ['41abc'] }], includeAllBlocks: true },
        { logs: [{ address: ['41def'] }] },
        { internalTransactions: [{ transferTo: ['41aaa'] }] },
      )

      expect(merged).toEqual({
        transactions: [{ type: ['TransferContract'] }],
        transferTransactions: [{ owner: ['4111'] }],
        transferAssetTransactions: [{ asset: ['1002000'] }],
        triggerSmartContractTransactions: [{ contract: ['41abc'] }],
        logs: [{ address: ['41def'] }],
        internalTransactions: [{ transferTo: ['41aaa'] }],
        includeAllBlocks: true,
      })
    })
  })

  describe('includeAllBlocks', () => {
    it('adds an includeAllBlocks request', () => {
      const builder = new TronQueryBuilder().includeAllBlocks({ from: 10, to: 20 })

      expect(builder.getRequests()).toEqual([{ range: { from: 10, to: 20 }, request: { includeAllBlocks: true } }])
    })
  })

  it('reports the tron query type', () => {
    expect(new TronQueryBuilder().getType()).toBe('tron')
  })
})
