import type { Config, NetworkType } from '~/types/init.js'

export interface TargetFile {
  path: string
  content: string
  /**
   * Keep the user's copy on regeneration instead of overwriting it. Used for
   * files that carry user state (e.g. `.env` secrets); on a fresh init the file
   * is absent, so it's still written.
   */
  preserveExisting?: boolean
}

export interface TargetPostStep {
  kind: 'exec'
  command: string
}

export interface TargetArtifacts {
  targetCode: string
  envSchema: string
  files: TargetFile[]
  postSteps: TargetPostStep[]
}

export interface TargetBuildOptions {
  /** See `InitContext.rpcUrl` — in-memory only, lands in the .env as RPC_URL. */
  rpcUrl?: string
}

export type TargetHandler = (config: Config<NetworkType>, options?: TargetBuildOptions) => TargetArtifacts
