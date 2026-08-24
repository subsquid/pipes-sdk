import type { NetworkType, Target } from '~/types/init.js'

const baseDependencies: Record<string, string> = {
  // Exact pin, deliberately. The generated code uses `evmStream({ source })`, which only the
  // alpha line has so far — a range like '^1.0.0-alpha.22' would resolve to the highest 1.0.0
  // prerelease (currently a beta without `source`) and the project would not compile. Widen to a
  // range once a beta or the stable 1.0.0 carries the option.
  '@subsquid/pipes': '1.0.0-alpha.22',
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
): {
  dependencies: Record<string, string>
  devDependencies: Record<string, string>
} {
  const dependencies = { ...baseDependencies, ...networkDependencies[networkType], ...targetDependencies[target] }
  const devDependencies = { ...baseDevDependencies }

  return {
    dependencies,
    devDependencies,
  }
}
