import type { Config, NetworkType, PackageManager, Target } from '~/types/init.js'

import type { InitContext } from '../types.js'
import { FakeProjectWriter } from './fake-project-writer.js'

export type MakeContextOverrides = {
  packageManager?: PackageManager
  target?: Target
  networkType?: NetworkType
  defaultNetwork?: string
  projectFolder?: string
  templates?: Config<NetworkType>['templates']
  writer?: FakeProjectWriter
  regenerate?: boolean
  rpcFallback?: boolean
  rpcUrl?: string
}

export function makeTestContext(overrides: MakeContextOverrides = {}): {
  ctx: InitContext
  writer: FakeProjectWriter
} {
  const writer = overrides.writer ?? new FakeProjectWriter(overrides.projectFolder ?? '/tmp/proj')
  const config: Config<NetworkType> = {
    projectFolder: overrides.projectFolder ?? '/tmp/proj',
    networkType: overrides.networkType ?? 'evm',
    defaultNetwork: overrides.defaultNetwork ?? 'ethereum-mainnet',
    templates: overrides.templates ?? [],
    target: overrides.target ?? 'clickhouse',
    packageManager: overrides.packageManager ?? 'pnpm',
    ...(overrides.rpcFallback !== undefined ? { rpcFallback: overrides.rpcFallback } : {}),
  }
  return {
    writer,
    ctx: {
      config,
      projectName: 'proj',
      projectPath: writer.getAbsolutePath(),
      projectWriter: writer.asProjectWriter(),
      regenerate: overrides.regenerate,
      rpcUrl: overrides.rpcUrl,
    },
  }
}
