import { registerFallbackMetrics } from '~/core/fallback/metrics.js'
import { pipeLogger } from '~/core/index.js'

import { BlockStreamClient } from '../portal-client/index.js'
import { EvmFallbackOptions, EvmSourceSpec, createEvmFallbackClient, resolveEvmSource } from './evm-fallback.js'
import { EvmOutputs, EvmPortalStreamOptions, evmPortalStream } from './evm-portal-source.js'

/**
 * Where an EVM stream reads its blocks from:
 *
 * - one source: a portal dataset URL or portal client options, a `{type: 'rpc', url}` JSON-RPC
 *   endpoint, or a ready client (a `PortalClient` or any custom {@link BlockStreamClient});
 * - an **array of sources in preference order**. The stream then drives the first available source
 *   and fails over (and switches back) between them; see {@link EvmSourceSpec} and the `fallback`
 *   option.
 */
export type EvmStreamSource = EvmSourceSpec | BlockStreamClient | EvmSourceSpec[]

export interface EvmStreamOptions<Out extends EvmOutputs> extends Omit<EvmPortalStreamOptions<Out>, 'portal'> {
  /** Where to read blocks from — see {@link EvmStreamSource}. */
  source: EvmStreamSource
  /**
   * Fallback behavior when `source` is a source list — two halves: `detection` senses failure and
   * recovery (probes, head polls, thresholds — it defines the events), `strategy` decides what to
   * do about it (stock-strategy options, or a custom function). Rejected for a single source.
   */
  fallback?: EvmFallbackOptions
}

/**
 * Streams EVM data from a portal, a JSON-RPC endpoint, or an ordered fallback list of both.
 *
 * @experimental The source-list and RPC paths are new; `evmPortalStream` stays the stable entry
 * point for a single portal.
 */
export function evmStream<Out extends EvmOutputs>({ source, fallback, logger, ...options }: EvmStreamOptions<Out>) {
  if (source == null) {
    throw new Error('`source` is required')
  }
  if (fallback && !Array.isArray(source)) {
    throw new Error(
      '`fallback` requires `source` to be an array of sources — a single source has nothing to fall back to',
    )
  }

  // One logger instance for the whole pipe. Resolving it here rather than letting the stream and
  // the fallback each resolve the same level would leave the pipe logging through two instances.
  const log = pipeLogger(options.id, logger)

  if (!Array.isArray(source)) {
    return evmPortalStream({ ...options, portal: resolveEvmSource(source), logger: log })
  }

  // The fallback logs source switches and health transitions; it is part of this pipe, so it
  // logs through the pipe's logger (and carries its id) unless the caller overrode it.
  const client = createEvmFallbackClient(source, { ...fallback, logger: fallback?.logger ?? log })
  if (options.metrics) {
    registerFallbackMetrics(options.metrics.metrics, client, options.id)
  }

  return evmPortalStream({ ...options, portal: client, logger: log })
}
