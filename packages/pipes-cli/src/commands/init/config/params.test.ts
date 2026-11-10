import { describe, expect, it } from 'vitest'

import { Config } from '~/types/init.js'

import { getTemplate } from '../templates/registry.js'
import { configJsonSchema, configJsonSchemaRaw } from './params.js'

describe('--config params schema', () => {
  const strictConfigSchema = {
    projectFolder: './morpho-blue-markets',
    networkType: 'evm',
    defaultNetwork: 'ethereum-mainnet',
    packageManager: 'bun',
    target: 'clickhouse',
    templates: [
      {
        templateId: 'custom',
        params: {
          contracts: [
            {
              contractName: 'MorphoBlue',
              contractEvents: [
                {
                  name: 'Supply',
                  type: 'event',
                  inputs: [
                    { name: 'id', type: 'bytes32' },
                    { name: 'caller', type: 'address' },
                    { name: 'onBehalf', type: 'address' },
                    { name: 'assets', type: 'uint256' },
                    { name: 'shares', type: 'uint256' },
                  ],
                },
                {
                  name: 'Withdraw',
                  type: 'event',
                  inputs: [
                    { name: 'id', type: 'bytes32' },
                    { name: 'caller', type: 'address' },
                    { name: 'onBehalf', type: 'address' },
                    { name: 'receiver', type: 'address' },
                    { name: 'assets', type: 'uint256' },
                    { name: 'shares', type: 'uint256' },
                  ],
                },
                {
                  name: 'Borrow',
                  type: 'event',
                  inputs: [
                    { name: 'id', type: 'bytes32' },
                    { name: 'caller', type: 'address' },
                    { name: 'onBehalf', type: 'address' },
                    { name: 'receiver', type: 'address' },
                    { name: 'assets', type: 'uint256' },
                    { name: 'shares', type: 'uint256' },
                  ],
                },
                {
                  name: 'Repay',
                  type: 'event',
                  inputs: [
                    { name: 'id', type: 'bytes32' },
                    { name: 'caller', type: 'address' },
                    { name: 'onBehalf', type: 'address' },
                    { name: 'assets', type: 'uint256' },
                    { name: 'shares', type: 'uint256' },
                  ],
                },
                {
                  name: 'AccrueInterest',
                  type: 'event',
                  inputs: [
                    { name: 'id', type: 'bytes32' },
                    { name: 'prevBorrowRate', type: 'uint256' },
                    { name: 'interest', type: 'uint256' },
                    { name: 'feeShares', type: 'uint256' },
                  ],
                },
                {
                  name: 'Liquidate',
                  type: 'event',
                  inputs: [
                    { name: 'id', type: 'bytes32' },
                    { name: 'caller', type: 'address' },
                    { name: 'borrower', type: 'address' },
                    { name: 'repaidAssets', type: 'uint256' },
                    { name: 'repaidShares', type: 'uint256' },
                    { name: 'seizedAssets', type: 'uint256' },
                    { name: 'badDebtAssets', type: 'uint256' },
                    { name: 'badDebtShares', type: 'uint256' },
                  ],
                },
              ],
              deployments: [{ address: '0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb', range: { from: 'latest' } }],
            },
          ],
        },
      },
    ],
  }

  it('should validate the config matching the strict schema', () => {
    expect(() => configJsonSchemaRaw.parse(strictConfigSchema)).to.not.throw()
  })

  it('should transform the raw config into Config<NetworkType> interface with tuples', () => {
    const config = configJsonSchema.parse(strictConfigSchema)
    const customTemplate = getTemplate('evm', 'custom')

    expect(config.projectFolder).toBe('./morpho-blue-markets')
    expect(config.networkType).toBe('evm')
    expect(config.defaultNetwork).toBe('ethereum-mainnet')
    expect(config.packageManager).toBe('bun')
    expect(config.target).toBe('clickhouse')
    expect(config.templates).toHaveLength(1)
    expect(config.templates[0].template).toBe(customTemplate)
    expect(config.templates[0].params).toEqual(strictConfigSchema.templates[0].params)
  })

  it('should be able to parse the config, ignoring unknown fields', () => {
    const configJson = {
      projectFolder: 'test',
      networkType: 'evm',
      packageManager: 'pnpm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [
        {
          templateId: 'custom',
          params: {
            contracts: [
              {
                contractName: 'Morpho',
                contractEvents: [
                  {
                    anonymous: false,
                    inputs: [
                      {
                        indexed: true,
                        internalType: 'Id',
                        name: 'id',
                        type: 'bytes32',
                      },
                      {
                        indexed: false,
                        internalType: 'address',
                        name: 'caller',
                        type: 'address',
                      },
                      {
                        indexed: true,
                        internalType: 'address',
                        name: 'onBehalf',
                        type: 'address',
                      },
                      {
                        indexed: true,
                        internalType: 'address',
                        name: 'receiver',
                        type: 'address',
                      },
                      {
                        indexed: false,
                        internalType: 'uint256',
                        name: 'assets',
                        type: 'uint256',
                      },
                      {
                        indexed: false,
                        internalType: 'uint256',
                        name: 'shares',
                        type: 'uint256',
                      },
                    ],
                    name: 'Borrow',
                    type: 'event',
                  },
                  {
                    anonymous: false,
                    inputs: [
                      {
                        indexed: true,
                        internalType: 'Id',
                        name: 'id',
                        type: 'bytes32',
                      },
                      {
                        components: [
                          {
                            internalType: 'address',
                            name: 'loanToken',
                            type: 'address',
                          },
                          {
                            internalType: 'address',
                            name: 'collateralToken',
                            type: 'address',
                          },
                          {
                            internalType: 'address',
                            name: 'oracle',
                            type: 'address',
                          },
                          {
                            internalType: 'address',
                            name: 'irm',
                            type: 'address',
                          },
                          {
                            internalType: 'uint256',
                            name: 'lltv',
                            type: 'uint256',
                          },
                        ],
                        indexed: false,
                        internalType: 'struct MarketParams',
                        name: 'marketParams',
                        type: 'tuple',
                      },
                    ],
                    name: 'CreateMarket',
                    type: 'event',
                  },
                  {
                    anonymous: false,
                    inputs: [
                      {
                        indexed: false,
                        internalType: 'uint256',
                        name: 'lltv',
                        type: 'uint256',
                      },
                    ],
                    name: 'EnableLltv',
                    type: 'event',
                  },
                ],
                deployments: [{ address: '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb', range: { from: 'latest' } }],
              },
            ],
          },
        },
      ],
      target: 'clickhouse',
    }

    expect(() => configJsonSchemaRaw.parse(configJson)).to.not.throw()
  })

  it('should reject configs with an unknown templateId', () => {
    const configJson = {
      ...strictConfigSchema,
      templates: [{ templateId: 'doesNotExist', params: {} }],
    }

    expect(() => configJsonSchemaRaw.parse(configJson)).toThrow()
  })

  it('should reject configs with extra top-level keys on a template entry', () => {
    const configJson = {
      ...strictConfigSchema,
      templates: [
        {
          templateId: 'custom',
          params: strictConfigSchema.templates[0]!.params,
          rogueKey: 'should not be allowed',
        },
      ],
    }

    expect(() => configJsonSchemaRaw.parse(configJson)).toThrow()
  })

  it('should validate params against the schema for the selected templateId, not fall through to another templates option', () => {
    const configJson = {
      ...strictConfigSchema,
      templates: [
        {
          templateId: 'erc20Transfers',
          params: { deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' }] },
        },
      ],
    }

    const parsed = configJsonSchemaRaw.parse(configJson)
    expect(parsed.templates[0]).toMatchObject({
      templateId: 'erc20Transfers',
      params: {
        // A deployment without an explicit range gets the schema default.
        deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: 'latest' } }],
      },
    })
  })

  it('should reject the removed memory target as an invalid enum value', () => {
    const configJson = { ...strictConfigSchema, target: 'memory' }

    expect(() => configJsonSchemaRaw.parse(configJson)).toThrow()
  })

  describe('pipeId', () => {
    it('accepts a generated id and a readable one', () => {
      for (const pipeId of ['cce73a95', 'usdc-transfers', 'pipe_1.v2']) {
        expect(configJsonSchemaRaw.parse({ ...strictConfigSchema, pipeId }).pipeId).toBe(pipeId)
      }
    })

    // `id: '{{pipeId}}'` is an escaped Mustache tag, so these would render as a
    // different id than the user wrote and silently orphan the target cursor.
    it('rejects characters that Mustache would HTML-escape', () => {
      for (const pipeId of ['a&b', "a'b", 'a<b', 'a"b', 'a b']) {
        expect(() => configJsonSchemaRaw.parse({ ...strictConfigSchema, pipeId })).toThrow()
      }
    })

    it('rejects an empty or over-long id', () => {
      expect(() => configJsonSchemaRaw.parse({ ...strictConfigSchema, pipeId: '' })).toThrow()
      expect(() => configJsonSchemaRaw.parse({ ...strictConfigSchema, pipeId: 'a'.repeat(65) })).toThrow()
    })
  })

  describe('rpcFallback', () => {
    const svmConfig = {
      projectFolder: './sol-balances',
      networkType: 'svm',
      defaultNetwork: 'solana-mainnet',
      packageManager: 'pnpm',
      target: 'clickhouse',
      templates: [{ templateId: 'tokenBalances' }],
    }

    it('accepts the flag on an EVM config and carries it through the transform', () => {
      for (const rpcFallback of [true, false]) {
        expect(configJsonSchemaRaw.parse({ ...strictConfigSchema, rpcFallback })).toMatchObject({ rpcFallback })
        expect(configJsonSchema.parse({ ...strictConfigSchema, rpcFallback })).toMatchObject({ rpcFallback })
      }
    })

    it('rejects the flag on an SVM config (no fallback support)', () => {
      expect(() => configJsonSchemaRaw.parse(svmConfig)).to.not.throw()
      expect(() => configJsonSchemaRaw.parse({ ...svmConfig, rpcFallback: true })).toThrow()
    })

    // The endpoint URL may embed an API key and must never live in a committed
    // config file — it belongs in the generated .env only.
    it('rejects a top-level rpcUrl key', () => {
      expect(() => configJsonSchemaRaw.parse({ ...strictConfigSchema, rpcUrl: 'https://rpc.example.com' })).toThrow()
      expect(() =>
        configJsonSchemaRaw.parse({ ...strictConfigSchema, rpcFallback: true, rpcUrl: 'https://rpc.example.com' }),
      ).toThrow()
    })
  })
})
