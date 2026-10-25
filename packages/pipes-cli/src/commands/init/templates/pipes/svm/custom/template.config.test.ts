import { describe, expect, it, vi } from 'vitest'

import { customTemplate } from './template.config.js'

const ctx = {
  network: 'solana-mainnet',
  projectPath: '/tmp/project',
  networkType: 'svm' as const,
}

const whirlpool = {
  contractName: 'whirpool',
  contractEvents: [
    {
      name: 'Swap',
      type: 'event',
      inputs: [
        { name: 'amount0', type: 'i128' },
        { name: 'amount1', type: 'i128' },
      ],
    },
  ],
  deployments: [{ address: '0x0000000000000000000000000000000000000000', range: { from: 'latest' } }],
}

describe('svm customTemplate', () => {
  it('identity metadata', () => {
    expect(customTemplate.id).toBe('custom')
    expect(customTemplate.networkType).toBe('svm')
  })

  it('render() produces artifacts for svm custom contract', () => {
    const artifacts = customTemplate.render({ contracts: [whirlpool] }, ctx)
    expect(artifacts.transformer).toContain('0x0000000000000000000000000000000000000000')
    expect(artifacts.decoderIds).toContain('custom')
  })

  it('render() imports typegen output from the reference deployment for every decoder group', () => {
    const twoDeployments = {
      ...whirlpool,
      deployments: [
        { address: '0x0000000000000000000000000000000000000000', range: { from: '100' } },
        { address: '0x1111111111111111111111111111111111111111', range: { from: '200' } },
      ],
    }
    const artifacts = customTemplate.render({ contracts: [twoDeployments] }, ctx)

    expect(artifacts.decoderIds).toEqual(['customWhirpool', 'customWhirpool2'])

    const importLines = artifacts.transformer.split('\n').filter((l) => l.includes('from "./contracts/'))
    expect(importLines).toEqual([
      'import * as whirpoolProgram from "./contracts/0x0000000000000000000000000000000000000000/index.js"',
    ])
  })

  it('prompt() fetches svm contract metadata and collects deployments', async () => {
    const referenceAddress = whirlpool.deployments[0]!.address
    const fetchedMeta = [
      {
        contractAddress: referenceAddress,
        contractName: whirlpool.contractName,
        contractEvents: whirlpool.contractEvents,
      },
    ]
    const abiService = {
      getContractData: vi.fn(async () => fetchedMeta),
    }
    const promptCtx = {
      text: vi.fn(async () => referenceAddress),
      confirm: vi.fn(async () => false),
      select: vi.fn(),
      checkbox: vi.fn(async (_msg: string, choices: Array<{ value: any }>) => [choices[0].value]),
      blockRange: vi.fn(async () => ({ from: '0' })),
      abiService: abiService as any,
      network: 'solana-mainnet',
    }

    const params = await customTemplate.prompt!(promptCtx)

    expect(abiService.getContractData).toHaveBeenCalledWith('svm', 'solana-mainnet', [referenceAddress])
    expect(params.contracts).toHaveLength(1)
    expect(params.contracts[0].contractEvents).toEqual([whirlpool.contractEvents[0]])
    expect(params.contracts[0].deployments).toEqual([{ address: referenceAddress, range: { from: '0' } }])
  })
})
