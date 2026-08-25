import { describe, expect, it } from 'vitest'

import type { Config, NetworkType, PackageManager, Target } from '~/types/init.js'

import { planConfigFiles } from './plan-config-files.js'

function makeConfig(overrides: Partial<Config<NetworkType>> = {}): Config<NetworkType> {
  return {
    projectFolder: '/tmp/proj',
    networkType: 'evm',
    defaultNetwork: 'ethereum-mainnet',
    templates: [],
    target: 'clickhouse',
    packageManager: 'pnpm',
    ...overrides,
  }
}

type Slice = {
  packageManager: PackageManager
  target: Target
  networkType: NetworkType
}

const slices: Slice[] = [
  { packageManager: 'pnpm', target: 'clickhouse', networkType: 'evm' },
  { packageManager: 'pnpm', target: 'postgresql', networkType: 'evm' },
  { packageManager: 'npm', target: 'clickhouse', networkType: 'evm' },
  { packageManager: 'pnpm', target: 'clickhouse', networkType: 'svm' },
]

const basePaths = [
  'biome.json',
  'tsconfig.json',
  '.gitignore',
  'AGENTS.md',
  'package.json',
  'Dockerfile',
  'docker-compose.yml',
  'README.md',
  'src/utils/index.ts',
]

function expectedPaths(packageManager: PackageManager): string[] {
  if (packageManager !== 'pnpm') return basePaths
  return [...basePaths.slice(0, 4), 'pnpm-workspace.yaml', ...basePaths.slice(4)]
}

describe('planConfigFiles', () => {
  describe.each(slices)('$packageManager + $target + $networkType', ({ packageManager, target, networkType }) => {
    const specs = planConfigFiles(makeConfig({ packageManager, target, networkType }), 'proj')
    const paths = specs.map((s) => s.path)

    it('returns the expected ordered list of file paths', () => {
      expect(paths).toEqual(expectedPaths(packageManager))
    })

    it('includes pnpm-workspace.yaml iff packageManager is pnpm', () => {
      if (packageManager === 'pnpm') {
        expect(paths).toContain('pnpm-workspace.yaml')
      } else {
        expect(paths).not.toContain('pnpm-workspace.yaml')
      }
    })

    it('does not include drizzle.config.ts', () => {
      expect(paths).not.toContain('drizzle.config.ts')
    })

    it('does not include .env', () => {
      expect(paths).not.toContain('.env')
    })
  })

  describe('package.json snapshots per (packageManager, target)', () => {
    const pairs: Array<{ packageManager: PackageManager; target: Target }> = [
      { packageManager: 'pnpm', target: 'clickhouse' },
      { packageManager: 'npm', target: 'clickhouse' },
      { packageManager: 'pnpm', target: 'postgresql' },
    ]

    it.each(pairs)('matches snapshot for $packageManager + $target', ({ packageManager, target }) => {
      const specs = planConfigFiles(makeConfig({ packageManager, target }), 'proj')
      const pkg = specs.find((s) => s.path === 'package.json')!
      expect(pkg.contents).toMatchSnapshot()
    })
  })

  describe('src/utils/index.ts snapshots per (networkType, target)', () => {
    const pairs: Array<{ networkType: NetworkType; target: Target }> = [
      { networkType: 'evm', target: 'clickhouse' },
      { networkType: 'evm', target: 'postgresql' },
      { networkType: 'svm', target: 'clickhouse' },
    ]

    it.each(pairs)('matches snapshot for $networkType + $target', ({ networkType, target }) => {
      const specs = planConfigFiles(makeConfig({ networkType, target }), 'proj')
      const utils = specs.find((s) => s.path === 'src/utils/index.ts')!
      expect(utils.contents).toMatchSnapshot()
    })
  })

  it('package.json contains postgres drizzle scripts when target is postgresql', () => {
    const specs = planConfigFiles(makeConfig({ target: 'postgresql' }), 'proj')
    const pkg = specs.find((s) => s.path === 'package.json')!
    expect(pkg.contents).toContain('"db:migrate": "drizzle-kit migrate"')
  })

  it('package.json omits postgres drizzle scripts when target is clickhouse', () => {
    const specs = planConfigFiles(makeConfig({ target: 'clickhouse' }), 'proj')
    const pkg = specs.find((s) => s.path === 'package.json')!
    expect(pkg.contents).not.toContain('"db:migrate": "drizzle-kit migrate"')
  })

  describe('rpcFallback', () => {
    const rpcPeers = [
      '@subsquid/evm-rpc',
      '@subsquid/evm-normalization',
      '@subsquid/rpc-client',
      '@subsquid/http-client',
    ]

    const contentsOf = (specs: ReturnType<typeof planConfigFiles>, path: string) =>
      specs.find((s) => s.path === path)!.contents

    it('package.json gains the optional RPC peers of @subsquid/pipes when on, none when off', () => {
      const on = planConfigFiles(makeConfig({ rpcFallback: true }), 'proj')
      const off = planConfigFiles(makeConfig(), 'proj')
      for (const peer of rpcPeers) {
        expect(contentsOf(on, 'package.json')).toContain(`"${peer}"`)
        expect(contentsOf(off, 'package.json')).not.toContain(`"${peer}"`)
      }
    })

    it('README documents the fallback and RPC_URL only when on', () => {
      expect(contentsOf(planConfigFiles(makeConfig({ rpcFallback: true }), 'proj'), 'README.md')).toContain('RPC_URL')
      expect(contentsOf(planConfigFiles(makeConfig(), 'proj'), 'README.md')).not.toContain('RPC_URL')
    })

    it('docker-compose passes RPC_URL through to the indexer container only when on', () => {
      const composeOn = contentsOf(planConfigFiles(makeConfig({ rpcFallback: true }), 'proj'), 'docker-compose.yml')
      expect(composeOn).toContain('RPC_URL: ${RPC_URL:-}')
      expect(contentsOf(planConfigFiles(makeConfig(), 'proj'), 'docker-compose.yml')).not.toContain('RPC_URL')
    })
  })
})
