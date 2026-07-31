import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { Config, NetworkType } from '~/types/init.js'

import { getTemplate } from '../templates/registry.js'
import { prepareConfig } from './prepare-config.js'

const transferEvent = {
  name: 'Transfer',
  type: 'event',
  inputs: [
    { name: 'from', type: 'address' },
    { name: 'to', type: 'address' },
    { name: 'value', type: 'uint256' },
  ],
}

const approvalEvent = {
  name: 'Approval',
  type: 'event',
  inputs: [
    { name: 'owner', type: 'address' },
    { name: 'spender', type: 'address' },
    { name: 'value', type: 'uint256' },
  ],
}

type TestContract = {
  contractName: string
  contractEvents?: Array<typeof transferEvent>
  deployments: Array<{ address: string; range?: { from: string; to?: string } }>
}

function configWithContracts(contracts: TestContract[]): Config<NetworkType> {
  const custom = getTemplate('evm', 'custom')!
  return {
    projectFolder: '/tmp/proj',
    networkType: 'evm',
    defaultNetwork: 'ethereum-mainnet',
    target: 'clickhouse',
    packageManager: 'pnpm',
    templates: [
      {
        template: custom,
        params: { contracts },
      },
    ],
  }
}

function contractsOf(config: Config<NetworkType>): TestContract[] {
  return (config.templates[0]!.params as { contracts: TestContract[] }).contracts
}

describe('prepareConfig — pipe id', () => {
  it('mints a pipeId when the config carries none', async () => {
    const config = configWithContracts([{ contractName: 'TokenA', deployments: [{ address: '0x1111' }] }])
    expect(config.pipeId).toBeUndefined()

    await prepareConfig(config, { resolveContracts: async () => {} })

    expect(config.pipeId).toMatch(/^[0-9a-f]{8}$/)
  })

  it('keeps an existing pipeId, so regenerating reuses the target cursor', async () => {
    const config = configWithContracts([{ contractName: 'TokenA', deployments: [{ address: '0x1111' }] }])
    config.pipeId = 'cce73a95'

    await prepareConfig(config, { resolveContracts: async () => {} })

    expect(config.pipeId).toBe('cce73a95')
  })

  it('mints a distinct pipeId per project', async () => {
    const first = configWithContracts([{ contractName: 'TokenA', deployments: [{ address: '0x1111' }] }])
    const second = configWithContracts([{ contractName: 'TokenA', deployments: [{ address: '0x1111' }] }])

    await prepareConfig(first, { resolveContracts: async () => {} })
    await prepareConfig(second, { resolveContracts: async () => {} })

    expect(first.pipeId).not.toBe(second.pipeId)
  })
})

describe('prepareConfig', () => {
  it('invokes the resolver with reference-address views of the contracts', async () => {
    const config = configWithContracts([
      { contractName: 'TokenA', deployments: [{ address: '0x1111' }] },
      { contractName: 'TokenB', deployments: [{ address: '0x2222' }, { address: '0x3333' }] },
    ])
    const resolveContracts = vi.fn(async () => {})

    await prepareConfig(config, { resolveContracts })

    expect(resolveContracts).toHaveBeenCalledTimes(1)
    expect(resolveContracts).toHaveBeenCalledWith([
      { contractAddress: '0x1111', contractName: 'TokenA' },
      { contractAddress: '0x2222', contractName: 'TokenB' },
    ])
  })

  it('lets the resolver mutate contract names to make them unique', async () => {
    const config = configWithContracts([
      { contractName: 'Token', deployments: [{ address: '0x1111aaaa' }] },
      { contractName: 'Token', deployments: [{ address: '0x2222bbbb' }] },
    ])
    const resolveContracts = vi.fn(async (contracts: Array<{ contractAddress: string; contractName: string }>) => {
      const seen = new Set<string>()
      for (const c of contracts) {
        if (seen.has(c.contractName)) {
          c.contractName = `${c.contractName}_${c.contractAddress.slice(0, 6)}`
        }
        seen.add(c.contractName)
      }
    })

    await prepareConfig(config, { resolveContracts })

    const renamed = contractsOf(config).map((c) => c.contractName)
    expect(new Set(renamed).size).toBe(renamed.length)
  })

  describe('deployment and contract merging', () => {
    let warnSpy: ReturnType<typeof vi.spyOn>

    beforeEach(() => {
      warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    })

    afterEach(() => {
      warnSpy.mockRestore()
    })

    it('dedupes an address listed twice within one contract, keeping the oldest range', async () => {
      const config = configWithContracts([
        {
          contractName: 'WETH',
          contractEvents: [transferEvent],
          deployments: [
            { address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '20000000' } },
            { address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } },
          ],
        },
      ])
      await prepareConfig(config, { resolveContracts: vi.fn(async () => {}) })

      const merged = contractsOf(config)
      expect(merged).toHaveLength(1)
      expect(merged[0]!.deployments).toEqual([
        { address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } },
      ])
      expect(warnSpy).toHaveBeenCalledOnce()
    })

    it('collapses contract entries sharing a deployment address into one entry', async () => {
      const config = configWithContracts([
        {
          contractName: 'WETH',
          contractEvents: [transferEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } }],
        },
        {
          contractName: 'WETH',
          contractEvents: [transferEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } }],
        },
      ])
      await prepareConfig(config, { resolveContracts: vi.fn(async () => {}) })

      const merged = contractsOf(config)
      expect(merged).toHaveLength(1)
      expect(merged[0]!.contractEvents).toHaveLength(1)
      expect(merged[0]!.deployments).toHaveLength(1)
      expect(warnSpy).toHaveBeenCalledOnce()
    })

    it('unions contractEvents when merged contract entries have disjoint event sets', async () => {
      const config = configWithContracts([
        {
          contractName: 'WETH',
          contractEvents: [transferEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } }],
        },
        {
          contractName: 'WETH2',
          contractEvents: [approvalEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } }],
        },
      ])
      await prepareConfig(config, { resolveContracts: vi.fn(async () => {}) })

      const merged = contractsOf(config)
      expect(merged).toHaveLength(1)
      expect(merged[0]!.contractName).toBe('WETH')
      expect(merged[0]!.contractEvents!.map((e) => e.name).sort()).toEqual(['Approval', 'Transfer'])
    })

    it('dedups overlapping events by signature when merging contract entries', async () => {
      const config = configWithContracts([
        {
          contractName: 'WETH',
          contractEvents: [transferEvent, approvalEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } }],
        },
        {
          contractName: 'WETH2',
          contractEvents: [approvalEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } }],
        },
      ])
      await prepareConfig(config, { resolveContracts: vi.fn(async () => {}) })

      const merged = contractsOf(config)
      expect(merged).toHaveLength(1)
      expect(merged[0]!.contractEvents).toHaveLength(2)
    })

    it('keeps the oldest numeric range when merging a shared deployment across contract entries', async () => {
      const config = configWithContracts([
        {
          contractName: 'WETH',
          contractEvents: [transferEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '20000000' } }],
        },
        {
          contractName: 'WETH2',
          contractEvents: [transferEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: '4719568' } }],
        },
      ])
      await prepareConfig(config, { resolveContracts: vi.fn(async () => {}) })

      const merged = contractsOf(config)
      expect(merged[0]!.deployments[0]!.range!.from).toBe('4719568')
    })

    it('unions deployments when two contract entries share one address but not the others', async () => {
      const config = configWithContracts([
        {
          contractName: 'WETH',
          contractEvents: [transferEvent],
          deployments: [{ address: '0x1111', range: { from: '100' } }],
        },
        {
          contractName: 'AlsoWeth',
          contractEvents: [transferEvent],
          deployments: [
            { address: '0x1111', range: { from: '100' } },
            { address: '0x2222', range: { from: '200' } },
          ],
        },
      ])
      await prepareConfig(config, { resolveContracts: vi.fn(async () => {}) })

      const merged = contractsOf(config)
      expect(merged).toHaveLength(1)
      expect(merged[0]!.contractName).toBe('WETH')
      expect(merged[0]!.deployments).toEqual([
        { address: '0x1111', range: { from: '100' } },
        { address: '0x2222', range: { from: '200' } },
      ])
    })

    it('treats case-insensitive addresses as duplicates', async () => {
      const config = configWithContracts([
        {
          contractName: 'WETH',
          contractEvents: [transferEvent],
          deployments: [{ address: '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', range: { from: 'latest' } }],
        },
        {
          contractName: 'weth',
          contractEvents: [transferEvent],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: 'latest' } }],
        },
      ])
      await prepareConfig(config, { resolveContracts: vi.fn(async () => {}) })

      expect(contractsOf(config)).toHaveLength(1)
    })
  })

  it('dedupes bare deployments (fixed-ABI templates) without invoking the resolver', async () => {
    const erc20 = getTemplate('evm', 'erc20Transfers')!
    const config: Config<NetworkType> = {
      projectFolder: '/tmp/proj',
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      target: 'clickhouse',
      packageManager: 'pnpm',
      templates: [
        {
          template: erc20,
          params: {
            deployments: [
              { address: '0xaaaa', range: { from: '200' } },
              { address: '0xAAAA', range: { from: '100' } },
            ],
          },
        },
      ],
    }
    const resolveContracts = vi.fn(async () => {})
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await prepareConfig(config, { resolveContracts })

    expect(resolveContracts).not.toHaveBeenCalled()
    expect((config.templates[0]!.params as { deployments: unknown[] }).deployments).toEqual([
      { address: '0xaaaa', range: { from: '100' } },
    ])
    warnSpy.mockRestore()
  })
})

describe('prepareConfig — Copilot review regressions', () => {
  it('keeps a duplicate deployment range when the first occurrence has none', async () => {
    const contract = {
      contractName: 'Weth',
      contractEvents: [],
      deployments: [{ address: '0xAAA' }, { address: '0xaaa', range: { from: '100' } }],
    }
    const config = configWithContracts([contract])

    await prepareConfig(config, { resolveContracts: async () => {} })

    expect(contract.deployments).toHaveLength(1)
    expect(contract.deployments[0]!.range).toEqual({ from: '100' })
  })

  it('collapses transitive contract overlaps (bridging entry) to a single contract', async () => {
    const a = { contractName: 'A', contractEvents: [], deployments: [{ address: '0x1', range: { from: '1' } }] }
    const b = { contractName: 'B', contractEvents: [], deployments: [{ address: '0x2', range: { from: '2' } }] }
    const bridge = {
      contractName: 'C',
      contractEvents: [],
      deployments: [
        { address: '0x1', range: { from: '3' } },
        { address: '0x2', range: { from: '4' } },
      ],
    }
    const config = configWithContracts([a, b, bridge])

    await prepareConfig(config, { resolveContracts: async () => {} })

    const contracts = (config.templates[0]!.params as { contracts: TestContract[] }).contracts
    expect(contracts).toHaveLength(1)
    const survivor = contracts[0]!
    const addresses = survivor.deployments.map((d) => d.address).sort()
    expect(addresses).toEqual(['0x1', '0x2'])
  })
})

describe('prepareConfig — SVM address case sensitivity', () => {
  it('does not merge base58 deployments differing only by case', async () => {
    const contract = {
      contractName: 'jupiter',
      contractEvents: [],
      deployments: [
        { address: 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', range: { from: '1' } },
        { address: 'jup6lkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', range: { from: '2' } },
      ],
    }
    const svmCustom = getTemplate('svm', 'custom')!
    const config: Config<NetworkType> = {
      projectFolder: '/tmp/proj',
      networkType: 'svm',
      defaultNetwork: 'solana-mainnet',
      target: 'clickhouse',
      packageManager: 'pnpm',
      templates: [{ template: svmCustom, params: { contracts: [contract] } }],
    }

    await prepareConfig(config, { resolveContracts: async () => {} })

    expect(contract.deployments).toHaveLength(2)
  })
})
