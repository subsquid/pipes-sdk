import type { Config, NetworkType } from '~/types/init.js'

/** Filename of the reproducible config written into every generated project. */
export const PIPE_CONFIG_FILENAME = 'pipes.config.json'

/**
 * Serializes a resolved {@link Config} back into the `--config` input shape
 * (`templates` as `{ templateId, params }`), so a generated project carries a
 * faithful, re-runnable record of how it was created.
 */
export function serializePipeConfig(config: Config<NetworkType>): string {
  const raw = {
    projectFolder: config.projectFolder,
    networkType: config.networkType,
    defaultNetwork: config.defaultNetwork,
    packageManager: config.packageManager,
    target: config.target,
    // Written back so a regenerate keeps the same stream id, and with it the
    // target's cursor.
    ...(config.pipeId !== undefined ? { pipeId: config.pipeId } : {}),
    // The fallback flag is committed; the RPC URL itself never is (it may
    // embed an API key) — it lives only in the generated .env. Omitted when
    // off so saved configs stay parseable by older CLIs (schemas are strict).
    ...(config.rpcFallback ? { rpcFallback: true } : {}),
    templates: config.templates.map((configured) => ({
      templateId: configured.template.id,
      ...(configured.params !== undefined ? { params: configured.params } : {}),
    })),
  }

  return `${JSON.stringify(raw, null, 2)}\n`
}
