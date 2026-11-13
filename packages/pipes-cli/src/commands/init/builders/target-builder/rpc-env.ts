import type { Config, NetworkType } from '~/types/init.js'

/**
 * Extra field for the generated zod env schema; empty when the RPC fallback is
 * off. Required (`min(1)`) on purpose: the user opted into the fallback, so a
 * silently portal-only run behind a blank RPC_URL would be misleading.
 */
export function rpcEnvSchemaField(config: Config<NetworkType>): string {
  return config.rpcFallback ? '  RPC_URL: z.string().min(1),\n' : ''
}

/**
 * Extra `.env` line. Blank when the user skipped the URL prompt (or came in
 * via --config) — the generated env schema then refuses to start the pipe
 * until RPC_URL is filled in.
 *
 * On regeneration the existing .env is preserved (preserveExisting), so
 * enabling the option on an existing project does NOT add this line; the
 * generated schema's "RPC_URL required" zod error tells the user what to add.
 */
export function rpcEnvFileLine(config: Config<NetworkType>, rpcUrl: string | undefined): string {
  return config.rpcFallback ? `RPC_URL=${rpcUrl ?? ''}\n` : ''
}
