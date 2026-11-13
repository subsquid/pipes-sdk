import { redactUrl } from '~/core/fallback/diagnostics.js'
import {
  FallbackClient,
  FallbackClientSource,
  FallbackDetectionOptions,
  FallbackStrategyOptions,
  Logger,
} from '~/core/index.js'
import { HttpClient } from '~/http-client/index.js'
import { once } from '~/internal/function.js'
import {
  ApiDataset,
  BlockRef,
  BlockStreamClient,
  GetBlock,
  PortalBlockStream,
  PortalBlockStreamOptions,
  PortalClient,
  PortalClientOptions,
  Query,
  StreamData,
  isBlockStreamClient,
} from '~/portal-client/index.js'

import type { EvmRpcConnectionOptions, RpcMethodOptions } from './rpc/options.js'

export type { EvmRpcConnectionOptions, RpcMethodOptions } from './rpc/options.js'

/**
 * One EVM source in a fallback list, in the order of preference (first entry is the primary).
 *
 * - a `string` is a portal dataset URL with default settings;
 * - an object without `type` (or with `type: 'portal'`) is a portal source with
 *   {@link PortalClientOptions} settings;
 * - `type: 'rpc'` is a JSON-RPC source — plain connection options, no `@subsquid/evm-rpc` import
 *   needed (the RPC stack is loaded lazily when the source is first used, and the
 *   `@subsquid/evm-rpc` + `@subsquid/evm-normalization` optional peers must be installed by then);
 * - `type: 'custom'` plugs in any ready {@link BlockStreamClient}.
 */
export type EvmSourceSpec =
  | string
  | ({ type?: 'portal'; name?: string } & PortalClientOptions)
  | ({
      type: 'rpc'
      name?: string
      method?: RpcMethodOptions
      strideSize?: number
      strideConcurrency?: number
      finalized?: boolean
    } & EvmRpcConnectionOptions)
  | { type: 'custom'; name?: string; client: BlockStreamClient }

export interface EvmFallbackOptions {
  /** How source failure and recovery are detected. See {@link FallbackDetectionOptions}. */
  detection?: FallbackDetectionOptions
  /** What to do about the detected state, e.g. `{ preferPrimary: 'onFailureOnly' }`. */
  strategy?: FallbackStrategyOptions
  /**
   * How many times a source retries a retryable transport failure before the fallback takes over.
   * Default 3. Per-source transport settings win over this.
   */
  sourceRetries?: number
  /**
   * Stream finalized blocks only. Applies to every source that does not set it itself. Mixed
   * settings give "cheap bulk, then follow the tip": a finalized-only portal first, a hot RPC
   * behind it; `detection.maxStalenessMs` hands off at the finality frontier.
   */
  finalized?: boolean
  logger?: Logger
}

/** A lone portal retries forever; in a list another source can serve, so the budget is short. */
const DEFAULT_SOURCE_RETRIES = 3

/** A ready-made `HttpClient` is passed through: its owner has configured it. */
function portalHttp(options: EvmFallbackOptions, http: PortalClientOptions['http']): PortalClientOptions['http'] {
  if (http instanceof HttpClient) {
    return http
  }

  return {
    retryAttempts: options.sourceRetries ?? DEFAULT_SOURCE_RETRIES,
    ...(options.logger ? { logger: options.logger } : {}),
    ...http,
  }
}

/** Build a {@link FallbackClient} over an ordered list of EVM source specs. */
export function createEvmFallbackClient(specs: EvmSourceSpec[], options: EvmFallbackOptions = {}): FallbackClient {
  return new FallbackClient({
    sources: specs.map((spec, i) => createSource(spec, i, options)),
    detection: options.detection,
    strategy: options.strategy,
    logger: options.logger,
  })
}

/**
 * What `evmStream` reads from for a lone source. A portal spec stays plain portal options, so the
 * portal keeps its unbounded retries; an `rpc` or `custom` spec is built as a list entry is.
 */
export function resolveEvmSource(
  source: EvmSourceSpec | BlockStreamClient,
): string | PortalClientOptions | BlockStreamClient {
  if (typeof source === 'string' || isBlockStreamClient(source)) {
    return source
  }

  if (source.type === 'rpc' || source.type === 'custom') {
    return createSource(source, 0, {}).client
  }

  const { type: _type, name: _name, ...portalOptions } = source

  return portalOptions
}

function createSource(spec: EvmSourceSpec, i: number, options: EvmFallbackOptions): FallbackClientSource {
  if (typeof spec === 'string') {
    spec = { url: spec }
  }

  if (spec.type === 'custom') {
    if (!isBlockStreamClient(spec.client)) {
      throw new Error(`fallback source ${i}: 'custom' spec requires a BlockStreamClient in \`client\``)
    }

    return { name: spec.name ?? `custom-${i}`, client: spec.client }
  }

  if (spec.type === 'rpc') {
    const { type: _type, name = `rpc-${i}`, method, strideSize, strideConcurrency, finalized, ...rpc } = spec

    return {
      name,
      client: lazyEvmRpcBlockClient({
        rpc,
        name,
        finalized: finalized ?? options.finalized ?? false,
        method,
        strideSize,
        strideConcurrency,
      }),
    }
  }

  const { type: _type, name, ...portalOptions } = spec

  return {
    name: name ?? `portal-${i}`,
    client: new PortalClient({
      finalized: options.finalized,
      ...portalOptions,
      http: portalHttp(options, portalOptions.http),
    }),
  }
}

// evm-rpc's own peers are listed too: a consumer who installs only the first two still misses them.
const RPC_PEERS = ['@subsquid/evm-rpc', '@subsquid/evm-normalization', '@subsquid/http-client', '@subsquid/rpc-client']

/**
 * A module-not-found for an RPC peer becomes an error that names the whole stack. Any other error,
 * a missing transitive module included, is returned unchanged.
 */
export function translateMissingRpcPeer(e: unknown, sourceName = 'rpc'): unknown {
  const err = e as NodeJS.ErrnoException
  const isModuleNotFound = err?.code === 'ERR_MODULE_NOT_FOUND' || err?.code === 'MODULE_NOT_FOUND'
  if (isModuleNotFound && RPC_PEERS.some((p) => err.message?.includes(`'${p}'`))) {
    const list = RPC_PEERS.map((p) => `"${p}"`).join(', ')
    return new Error(
      `RPC fallback source "${sourceName}" requires the optional peer dependencies ${list} — ` +
        `install them to use RPC sources, or use only 'portal' sources.`,
    )
  }

  return err
}

/**
 * An RPC {@link BlockStreamClient} that imports the optional RPC peers on first use only.
 * `config` restates the `EvmRpcBlockClient` options: importing their type would pull the peers
 * into the type graph.
 */
function lazyEvmRpcBlockClient(config: {
  rpc: EvmRpcConnectionOptions
  name: string
  finalized: boolean
  method?: RpcMethodOptions
  strideSize?: number
  strideConcurrency?: number
}): BlockStreamClient {
  // A head poll and a capability probe race here; two clients would mean two rate limiters.
  const load = once(async (): Promise<BlockStreamClient> => {
    let mod: typeof import('./rpc/block-client.js')
    try {
      mod = await import('./rpc/block-client.js')
    } catch (e) {
      throw translateMissingRpcPeer(e, config.name)
    }

    return new mod.EvmRpcBlockClient(config)
  })

  return {
    finalized: config.finalized,
    getUrl: () => redactUrl(config.rpc.url) ?? config.name,
    getMetadata: async (): Promise<ApiDataset> => (await load()).getMetadata(),
    getHead: async (options?: { finalized: boolean }): Promise<BlockRef | undefined> => (await load()).getHead(options),
    // Optional on the contract: without it here, the fallback falls back to the `getHead` block lookup.
    getHeight: async (options?: { finalized: boolean }): Promise<number | undefined> =>
      await (await load()).getHeight?.(options),
    resolveTimestamp: async (seconds: number): Promise<number> => (await load()).resolveTimestamp(seconds),
    async *getStream<Q extends Query>(query: Q, options?: PortalBlockStreamOptions): PortalBlockStream<GetBlock<Q>> {
      const client = await load()
      yield* client.getStream(query, options)
    },
  }
}
