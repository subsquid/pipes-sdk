import { describe, expect, it } from 'vitest'

import { CustomTemplateParams } from '../template.config.js'
import { buildDecoderGroups, renderTransformer } from './transformer.js'

const swap = { name: 'Swap', type: 'event', inputs: [{ name: 'user', type: 'publicKey' }] }
const claim = { name: 'Claim', type: 'event', inputs: [{ name: 'user', type: 'publicKey' }] }

const jupiterAddress = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
const raydiumAddress = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'

function jupiter(range: { from: string; to?: string }, events = [swap]) {
  return {
    contractName: 'Jupiter',
    contractEvents: events,
    deployments: [{ address: jupiterAddress, range }],
  }
}

function raydium(range: { from: string; to?: string }, events = [swap]) {
  return {
    contractName: 'Raydium',
    contractEvents: events,
    deployments: [{ address: raydiumAddress, range }],
  }
}

function params(contracts: CustomTemplateParams['contracts']): CustomTemplateParams {
  return { contracts }
}

describe('SVM custom transformer', () => {
  it('emits one decoder per program even when ranges match (never merges programs)', () => {
    const groups = buildDecoderGroups(params([jupiter({ from: '200000000' }), raydium({ from: '200000000' })]))
    expect(groups.map((g) => g.decoderId)).toEqual(['customJupiter', 'customRaydium'])
    expect(groups.every((g) => g.programs.length === 1)).toBe(true)
  })

  it('emits per-program decoders when ranges diverge', () => {
    const groups = buildDecoderGroups(params([jupiter({ from: '200000000' }), raydium({ from: 'latest' })]))
    expect(groups).toHaveLength(2)
    const byId = Object.fromEntries(groups.map((g) => [g.decoderId, g]))
    expect(byId['customJupiter']!.rangeFrom).toBe('200000000')
    expect(byId['customRaydium']!.rangeFrom).toBe('latest')
  })

  it('suffixes decoder ids when deployments of one program get their own decoders', () => {
    const twoDeployments = {
      contractName: 'Jupiter',
      contractEvents: [swap],
      deployments: [
        { address: jupiterAddress, range: { from: '100' } },
        { address: raydiumAddress, range: { from: '200' } },
      ],
    }
    const groups = buildDecoderGroups(params([twoDeployments]))
    expect(groups.map((g) => g.decoderId)).toEqual(['customJupiter', 'customJupiter2'])
  })

  it('preserves `latest` verbatim (no NaN coercion)', () => {
    const code = renderTransformer(params([jupiter({ from: 'latest' }), raydium({ from: '200000000' })]))
    expect(code).toContain("range: { from: 'latest' }")
    expect(code).toContain("range: { from: '200000000' }")
  })

  it('imports each program once, from its reference deployment', () => {
    const twoDeployments = {
      contractName: 'Jupiter',
      contractEvents: [swap],
      deployments: [
        { address: jupiterAddress, range: { from: '100' } },
        { address: raydiumAddress, range: { from: '200' } },
      ],
    }
    const code = renderTransformer(params([twoDeployments]))

    const importLines = code.split('\n').filter((l) => l.includes('from "./contracts/'))
    expect(importLines).toEqual([`import * as jupiterProgram from "./contracts/${jupiterAddress}/index.js"`])
  })

  it('keeps same-named instructions in separate per-program decoders (no cross-decode)', () => {
    const groups = buildDecoderGroups(params([jupiter({ from: '200000000' }), raydium({ from: '200000000' })]))
    expect(groups).toHaveLength(2)
    expect(groups.map((g) => g.instructions.map((i) => i.uniqueKey))).toEqual([['Swap'], ['Swap']])
  })

  it('gives each program its own decoder with its own instructions', () => {
    const groups = buildDecoderGroups(
      params([jupiter({ from: '200000000' }, [swap]), raydium({ from: '200000000' }, [claim])]),
    )
    const byId = Object.fromEntries(groups.map((g) => [g.decoderId, g.instructions.map((i) => i.uniqueKey)]))
    expect(byId).toEqual({ customJupiter: ['Swap'], customRaydium: ['Claim'] })
  })

  it('never merges distinct programs whose names normalize alike', () => {
    const contracts = [
      {
        contractName: 'Foo Bar',
        contractEvents: [swap],
        deployments: [{ address: jupiterAddress, range: { from: '10' } }],
      },
      {
        contractName: 'foo_bar',
        contractEvents: [claim],
        deployments: [{ address: raydiumAddress, range: { from: '10' } }],
      },
    ]
    const groups = buildDecoderGroups(params(contracts))

    expect(groups).toHaveLength(2)
    expect(groups.every((g) => g.programs.length === 1)).toBe(true)
    expect(new Set(groups.map((g) => g.decoderId)).size).toBe(2)
    expect(groups.flatMap((g) => g.instructions.map((i) => i.name)).sort()).toEqual(['Claim', 'Swap'])

    // each program keeps its own typegen import, at its own address
    const importLines = renderTransformer(params(contracts))
      .split('\n')
      .filter((l) => l.includes('from "./contracts/'))
    expect(importLines).toHaveLength(2)
    expect(importLines.some((l) => l.includes(jupiterAddress))).toBe(true)
    expect(importLines.some((l) => l.includes(raydiumAddress))).toBe(true)
  })
})

describe('multi-deployment programs (Copilot round-2 regression)', () => {
  const params = {
    contracts: [
      {
        contractName: 'jupiter',
        contractEvents: [{ name: 'swap', type: 'event', inputs: [] }],
        deployments: [
          { address: 'Addr1111111111111111111111111111', range: { from: '10' } },
          { address: 'Addr2222222222222222222222222222', range: { from: '10' } },
        ],
      },
    ],
  }

  it('gives each same-range deployment its own decoder, with each instruction key once', () => {
    const decoders = buildDecoderGroups(params).map((g) => ({
      id: g.decoderId,
      addresses: g.programs.map((p) => p.contractAddress),
      keys: g.instructions.map((i) => i.uniqueKey),
    }))

    expect(decoders).toEqual([
      { id: 'customJupiter', addresses: ['Addr1111111111111111111111111111'], keys: ['swap'] },
      { id: 'customJupiter2', addresses: ['Addr2222222222222222222222222222'], keys: ['swap'] },
    ])
  })

  it('binds every deployment to its configured address, the reference one included', () => {
    const programLines = renderTransformer(params)
      .split('\n')
      .filter((l) => l.startsWith('  program:'))

    expect(programLines).toEqual([
      '  program: { ...jupiterProgram, programId: "Addr1111111111111111111111111111" },',
      '  program: { ...jupiterProgram, programId: "Addr2222222222222222222222222222" },',
    ])
  })
})
