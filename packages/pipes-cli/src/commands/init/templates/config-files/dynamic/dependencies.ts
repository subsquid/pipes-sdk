import type { NetworkType, Target } from '~/types/init.js'

const baseDependencies: Record<string, string> = {
  '@subsquid/pipes': '^1.0.0-beta.10',
  dotenv: '^16.4.5',
  zod: '^4.3.4',
}

const networkDependencies: Record<NetworkType, Record<string, string>> = {
  evm: {
    '@subsquid/evm-codec': '0.3.0',
    '@subsquid/evm-abi': '0.3.1',
    // contractFactorySqliteStore (dynamic contract tracking) is SQLite-backed
    'better-sqlite3': '^12.4.5',
  },
  svm: {
    '@subsquid/borsh': '^0.3.0',
  },
}

// Optional peerDependencies of @subsquid/pipes required by the `{ type: 'rpc' }`
// source. Keep in sync with packages/pipes/package.json peerDependencies.
const rpcFallbackDependencies: Record<string, string> = {
  '@subsquid/evm-rpc': '^0.0.2',
  '@subsquid/evm-normalization': '^0.0.2',
  '@subsquid/rpc-client': '^4.15.1',
  '@subsquid/http-client': '^1.8.1',
}

const baseDevDependencies: Record<string, string> = {
  typescript: '^5.9.2',
  '@biomejs/biome': '^2.3.4',
  tsx: '^4.20.6',
  tsup: '^8.5.0',
  '@types/node': '^22.14.1',
}

const targetDependencies: Record<Target, Record<string, string>> = {
  clickhouse: {
    '@clickhouse/client': '^1.14.0',
  },
  postgresql: {
    'drizzle-kit': '^0.30.0',
    'drizzle-orm': '^0.44.7',
    pg: '^8.16.3',
  },
}

export function renderDependencies(
  target: Target,
  networkType: NetworkType,
  options: { rpcFallback?: boolean } = {},
): {
  dependencies: Record<string, string>
  devDependencies: Record<string, string>
} {
  const dependencies = {
    ...baseDependencies,
    ...networkDependencies[networkType],
    ...targetDependencies[target],
    ...(options.rpcFallback ? rpcFallbackDependencies : {}),
  }
  const devDependencies = { ...baseDevDependencies }

  return {
    dependencies,
    devDependencies,
  }
}
