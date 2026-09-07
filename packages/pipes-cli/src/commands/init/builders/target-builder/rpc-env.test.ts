import { describe, expect, it } from 'vitest'

import type { Config, NetworkType } from '~/types/init.js'

import { rpcEnvFileLine, rpcEnvSchemaField } from './rpc-env.js'

function makeConfig(rpcFallback: boolean | undefined): Config<NetworkType> {
  return {
    projectFolder: 'mock-folder',
    networkType: 'evm',
    templates: [],
    defaultNetwork: 'ethereum-mainnet',
    target: 'postgresql',
    packageManager: 'pnpm',
    ...(rpcFallback !== undefined ? { rpcFallback } : {}),
  }
}

describe('rpcEnvSchemaField', () => {
  it('emits a required RPC_URL field when the fallback is on', () => {
    expect(rpcEnvSchemaField(makeConfig(true))).toBe('  RPC_URL: z.string().min(1),\n')
  })

  it('emits nothing when the fallback is off or unset', () => {
    expect(rpcEnvSchemaField(makeConfig(false))).toBe('')
    expect(rpcEnvSchemaField(makeConfig(undefined))).toBe('')
  })
})

describe('rpcEnvFileLine', () => {
  it('emits the provided URL when the fallback is on', () => {
    expect(rpcEnvFileLine(makeConfig(true), 'https://rpc.example.com/v2/KEY')).toBe(
      'RPC_URL=https://rpc.example.com/v2/KEY\n',
    )
  })

  it('emits a blank placeholder when the URL was skipped', () => {
    expect(rpcEnvFileLine(makeConfig(true), undefined)).toBe('RPC_URL=\n')
  })

  it('emits nothing when the fallback is off or unset', () => {
    expect(rpcEnvFileLine(makeConfig(false), 'https://rpc.example.com')).toBe('')
    expect(rpcEnvFileLine(makeConfig(undefined), undefined)).toBe('')
  })
})
