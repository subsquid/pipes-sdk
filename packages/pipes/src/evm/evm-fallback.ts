import {
  DefaultFallbackStrategyOptions,
  FallbackClient,
  FallbackClientSource,
  FallbackDetectionOptions,
  FallbackStrategy,
  Logger,
  redactUrl,
} from '~/core/index.js'
import { HttpClient } from '~/http-client/index.js'
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

// Re-export the RPC config types so consumers can type an `rpc` source spec. Type-only, so they are
// erased from the JS and never pull the optional peers at *runtime*; they also reference no
// evm-rpc types, so a Portal-only TS consumer typechecks without the peers installed.
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
  /**
   * How source failure and recovery are *detected*: capability probes, head polls, liveness
   * thresholds, cooldowns, and the freshness conditions whose verdicts reach the strategy as
   * events (`stall.stale`, `batch.lagging`). See {@link FallbackDetectionOptions}.
   */
  detection?: FallbackDetectionOptions
  /**
   * What to *do* about the detected state: plain options tune the stock strategy
   * ({@link DefaultFallbackStrategyOptions} — e.g. `{ preferPrimary: 'onFailureOnly' }`), a
   * function replaces its decisions per event (the stock decision arrives as
   * `ctx.defaultCommand`; returning `undefined` lets it stand).
   */
  strategy?: FallbackStrategy | DefaultFallbackStrategyOptions
  /**
   * Stream finalized blocks only. Applied to every source that doesn't set it itself — sources may
   * differ, which enables the "cheap bulk, then follow the tip" topology: a finalized-only portal
   * first, a hot RPC behind it. The portal serves until its finalized head, its request then sits
   * outstanding at the frontier, and `detection.maxStalenessMs` hands off to the source that is
   * genuinely ahead. (Lag cannot do this — it is only evaluated when a batch arrives, and an
   * exhausted source delivers none.) A set containing any hot source reports itself as hot, so the
   * target keeps its fork handling.
   */
  finalized?: boolean
  logger?: Logger
}

/**
 * Build a {@link FallbackClient} over an ordered list of EVM source specs. The result is a
 * {@link BlockStreamClient} — it slots into a `PortalStream` (and thus `evmStream`) exactly where
 * a single portal client goes, all sources serving the same query so their output is
 * interchangeable.
 */
/**
 * Blocks of retrying a source's transport before the fallback is allowed to do its job. A lone
 * portal retries a retryable status indefinitely because there is nothing else to read; inside a
 * list there is, so the budget is short — long enough to ride out a blip, short enough that a
 * struggling source is handed over rather than waited on (ADR-27).
 */
const SOURCE_RETRY_ATTEMPTS = 3 // P-FB-SOURCE-RETRIES

/**
 * Portal client settings for a source in a list: the pipe's logger and the bounded retry budget,
 * with anything the caller specified winning. A ready-made client is passed through untouched —
 * its owner has already configured it.
 */
function portalHttp(options: EvmFallbackOptions, http: PortalClientOptions['http']): PortalClientOptions['http'] {
  if (http instanceof HttpClient) return http

  return { retryAttempts: SOURCE_RETRY_ATTEMPTS, ...(options.logger ? { logger: options.logger } : {}), ...http }
}

export function createEvmFallbackClient(specs: EvmSourceSpec[], options: EvmFallbackOptions = {}): FallbackClient {
  const sources: FallbackClientSource[] = specs.map((spec, i) => {
    if (typeof spec === 'string') {
      return {
        name: `portal-${i}`,
        client: new PortalClient({ url: spec, finalized: options.finalized, http: portalHttp(options, undefined) }),
      }
    }
    if (spec.type === 'custom') {
      if (!isBlockStreamClient(spec.client)) {
        throw new Error(`fallback source ${i}: 'custom' spec requires a BlockStreamClient in \`client\``)
      }
      return { name: spec.name ?? `custom-${i}`, client: spec.client }
    }
    if (spec.type === 'rpc') {
      const { type: _type, name, method, strideSize, strideConcurrency, finalized, ...connection } = spec
      return {
        name: name ?? `rpc-${i}`,
        client: lazyEvmRpcBlockClient({
          connection,
          name: name ?? `rpc-${i}`,
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
  })

  return new FallbackClient({
    sources,
    detection: options.detection,
    strategy: options.strategy,
    logger: options.logger,
  })
}

// The full optional "RPC stack": `@subsquid/evm-rpc` + `@subsquid/evm-normalization` and evm-rpc's
// own peers (`@subsquid/http-client`, `@subsquid/rpc-client`). A consumer installing only the first
// two can still hit a module-not-found for the transitive peers, so all four are detected and named.
const RPC_PEERS = ['@subsquid/evm-rpc', '@subsquid/evm-normalization', '@subsquid/http-client', '@subsquid/rpc-client']

/**
 * If `e` is a module-not-found for one of the optional RPC-stack peers, return an actionable error
 * naming the whole stack; otherwise return `e` unchanged. Matches the missing module by its exact
 * quoted name, so a module-not-found for a *different* module (a broken transitive dep) — and any
 * other fault thrown while loading the RPC stack (a syntax/init error) — surfaces as-is rather than
 * being masked as "peers missing". Handles both the ESM (`ERR_MODULE_NOT_FOUND`) and CJS
 * (`MODULE_NOT_FOUND`) loader codes, since the package ships both builds.
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
 * An RPC {@link BlockStreamClient} whose `@subsquid/evm-rpc` dependency is loaded **lazily** — on
 * the first call that actually needs the endpoint. A multi-Portal fallback therefore never imports
 * the RPC stack, and a misconfigured RPC source fails with a clear, actionable error instead of an
 * opaque module-not-found at startup. (The RPC stack is declared as optional peer dependencies — a
 * Portal-only consumer never installs it.)
 */
function lazyEvmRpcBlockClient(config: {
  connection: EvmRpcConnectionOptions
  name: string
  finalized: boolean
  method?: RpcMethodOptions
  strideSize?: number
  strideConcurrency?: number
}): BlockStreamClient {
  let inner: BlockStreamClient | undefined

  const load = async (): Promise<BlockStreamClient> => {
    if (inner) return inner
    let mod: typeof import('./evm-rpc-block-client.js')
    try {
      mod = await import('./evm-rpc-block-client.js')
    } catch (e) {
      throw translateMissingRpcPeer(e, config.name)
    }
    inner = new mod.EvmRpcBlockClient({
      rpc: config.connection,
      name: config.name,
      finalized: config.finalized,
      method: config.method,
      strideSize: config.strideSize,
      strideConcurrency: config.strideConcurrency,
    })
    return inner
  }

  return {
    finalized: config.finalized,
    getUrl: () => redactUrl(config.connection.url) ?? config.name,
    getMetadata: async (): Promise<ApiDataset> => (await load()).getMetadata(),
    // Head-polling a standby RPC source loads the RPC stack — that is fine/desirable: it is exactly
    // when we want to confirm the source is loadable and viable before switching up to it.
    getHead: async (options?: { finalized: boolean }): Promise<BlockRef | undefined> => (await load()).getHead(options),
    resolveTimestamp: async (seconds: number): Promise<number> => (await load()).resolveTimestamp(seconds),
    getStream<Q extends Query>(query: Q, options?: PortalBlockStreamOptions): PortalBlockStream<GetBlock<Q>> {
      async function* stream(): AsyncGenerator<StreamData<GetBlock<Q>>> {
        const client = await load()
        yield* client.getStream(query, options)
      }
      return {
        [Symbol.asyncIterator]: () => stream()[Symbol.asyncIterator](),
      }
    },
  }
}
