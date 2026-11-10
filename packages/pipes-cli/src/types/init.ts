import type { ConfiguredTemplate } from '~/commands/init/templates/template.js'

export const packageManagerTypes = [
  { name: 'pnpm', value: 'pnpm', lockFile: 'pnpm-lock.yaml' },
  { name: 'yarn', value: 'yarn', lockFile: 'yarn.lock' },
  { name: 'npm', value: 'npm', lockFile: 'package-lock.json' },
  { name: 'bun', value: 'bun', lockFile: 'bun.lock' },
] as const
export type PackageManager = (typeof packageManagerTypes)[number]['value']

export const networkTypes = [
  { name: 'EVM', value: 'evm' },
  { name: 'SVM', value: 'svm' },
] as const
export type NetworkType = (typeof networkTypes)[number]['value']

export const targetTypes = [
  { name: 'ClickHouse', value: 'clickhouse' },
  { name: 'PostgreSQL', value: 'postgresql' },
] as const
export type Target = (typeof targetTypes)[number]['value']

export interface Config<N extends NetworkType> {
  projectFolder: string
  networkType: N
  /**
   * The network every template indexes. Named "default" because it is the
   * project-wide fallback: when per-deployment networks land, a deployment
   * without an explicit network inherits this one.
   */
  defaultNetwork: string
  templates: ConfiguredTemplate<N, any>[]
  target: Target
  packageManager: PackageManager
  /**
   * Identifier of the generated pipe's stream, and the key its target cursor is
   * stored under. Persisted to pipes.config.json so regenerating a project
   * reuses it instead of minting a new one and orphaning the cursor.
   * `prepareConfig()` fills it in when absent.
   */
  pipeId?: string
  /**
   * EVM only: generate the pipe with an RPC fallback source — the Portal stays
   * primary and an RPC endpoint takes over while it is unavailable. The
   * endpoint URL is NOT part of this config (it may embed an API key); it
   * lives only in the generated .env as RPC_URL.
   */
  rpcFallback?: boolean
}
