import type { Config, NetworkType } from '~/types/init.js'
import type { ProjectWriter } from '~/utils/project-writer.js'

export type InitContext = {
  config: Config<NetworkType>
  projectName: string
  projectPath: string
  projectWriter: ProjectWriter
  /**
   * The target folder already exists and is a pipes project (has a saved
   * config): regenerate into it instead of erroring, and never delete it on
   * failure since it predates this run.
   */
  regenerate?: boolean
  /**
   * RPC endpoint URL from the interactive prompt. In-memory only: written to
   * the generated .env, never serialized to pipes.config.json (it may embed
   * an API key). Absent on the --config / --config-id paths — the .env then
   * gets a blank RPC_URL= placeholder when config.rpcFallback is on.
   */
  rpcUrl?: string
}

export type InitStage = {
  id: string
  label: string
  /**
   * When true, a failure is non-fatal: the error is collected and surfaced, the
   * generated project is kept, and the pipeline continues. Used for stages that
   * shell out to the package manager (install, lint), which can fail for
   * environmental or registry reasons the user can resolve afterwards.
   */
  optional?: boolean
  run: (ctx: InitContext) => Promise<void>
}
