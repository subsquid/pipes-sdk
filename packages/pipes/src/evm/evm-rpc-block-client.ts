import { EvmRpcClient, EvmRpcDataSource, Rpc, DataRequest as RpcDataRequest } from '@subsquid/evm-rpc'

import { redactUrl } from '~/core/fallback-diagnostics.js'
import { ApiDataset, BlockRef, PortalBlockStreamOptions } from '~/portal-client/client.js'
import { ForkException, GetBlock, PortalBlockStream, Query, StreamData } from '~/portal-client/index.js'
import { Query as EvmQuery } from '~/portal-client/query/evm.js'

import { dropEmptyBlocks } from './rpc/project.js'
import { createWireBlockMapper } from './rpc/wire.js'

/** RPC method-selection toggles (the per-chain "C1" config) merged into the coarse fetch request. */
export interface RpcMethodOptions {
  useTraceApi?: boolean
  useDebugTraceBlockByNumber?: boolean
  useDebugApiForStateDiffs?: boolean
  debugTraceTimeout?: string
}

/** Plain connection config for the JSON-RPC endpoint (no `@subsquid/evm-rpc` import needed). */
export interface EvmRpcConnectionOptions {
  url: string
  /** Maximum number of concurrent in-flight requests. */
  capacity?: number
  /** Maximum requests per second. */
  rateLimit?: number
  /** Request timeout in ms. */
  requestTimeout?: number
  /** Whether HTTP 500 / RPC internal errors should be treated as retryable. */
  retryInternalServerErrors?: boolean
}

export interface EvmRpcBlockClientOptions {
  /** A ready `Rpc` instance, or plain connection options to build one from. */
  rpc: Rpc | EvmRpcConnectionOptions
  name?: string
  /** Serve finalized blocks only. Default `false` — stream to the head and report forks. */
  finalized?: boolean
  method?: RpcMethodOptions
  strideSize?: number
  strideConcurrency?: number
}

/**
 * An RPC-backed EVM {@link BlockStreamClient}: it slots into a `PortalStream` (directly or as a
 * fallback source) exactly where a portal client goes, and serves **portal-wire-shaped** blocks
 * for any EVM portal query. Per stream it delegates fetching, finality, continuity and fork
 * detection to `@subsquid/evm-rpc`'s `EvmRpcDataSource`, normalizes each raw block with the same
 * `mapRpcBlock` the portal's own dataset producers use, applies the ported client-side filter, and
 * yields the surviving items still in wire shape — the downstream normalize/cast step then decodes
 * them exactly as it would decode portal output. evm-rpc's fork exception is translated to Pipes'
 * `ForkException`.
 *
 * The query (fields + request + range) arrives per `getStream` call, so one client instance serves
 * any number of ranges/queries — including the multi-range plans a query builder produces.
 */
export class EvmRpcBlockClient {
  readonly finalized: boolean
  readonly #rpc: Rpc
  readonly #name: string
  readonly #method: RpcMethodOptions
  readonly #strideSize?: number
  readonly #strideConcurrency?: number
  /** For head polls only — head calls ignore the data request. */
  readonly #headSource: EvmRpcDataSource

  constructor(options: EvmRpcBlockClientOptions) {
    this.#rpc = options.rpc instanceof Rpc ? options.rpc : new Rpc({ client: new EvmRpcClient({ ...options.rpc }) })
    this.#name = options.name ?? 'evm-rpc'
    this.finalized = options.finalized ?? false
    this.#method = options.method ?? {}
    this.#strideSize = options.strideSize
    this.#strideConcurrency = options.strideConcurrency
    this.#headSource = new EvmRpcDataSource({ rpc: this.#rpc, req: { transactions: true } })
  }

  getUrl(): string {
    return redactUrl(this.#rpc.endpoint) ?? this.#name
  }

  /** RPC endpoints have no dataset registry — synthesize the minimum a `PortalStream` consumes. */
  async getMetadata(): Promise<ApiDataset> {
    return {
      dataset: this.#name,
      aliases: [],
      real_time: true,
      start_block: 0,
    }
  }

  async getHead(options?: { finalized: boolean }): Promise<BlockRef | undefined> {
    const finalized = options?.finalized ?? this.finalized
    const head = finalized ? await this.#headSource.getFinalizedHead() : await this.#headSource.getHead()

    return head ? { number: head.number, hash: head.hash } : undefined
  }

  async resolveTimestamp(_seconds: number): Promise<number> {
    throw new Error(
      'an RPC source cannot resolve Date/timestamp block-range bounds — use numeric block ranges, or configure a portal source',
    )
  }

  getStream<Q extends Query>(query: Q, options?: PortalBlockStreamOptions): PortalBlockStream<GetBlock<Q>> {
    const self = this
    return {
      [Symbol.asyncIterator]() {
        return self.#stream(query as EvmQuery, options)[Symbol.asyncIterator]() as AsyncIterator<
          StreamData<GetBlock<Q>>
        >
      },
    }
  }

  async *#stream(query: EvmQuery, options?: PortalBlockStreamOptions): AsyncGenerator<StreamData<any>> {
    if (query.type !== 'evm') {
      // The client implements the chain-generic contract but can only serve EVM queries; failing
      // here names the mistake instead of surfacing it as a confusing mapping/decoding error.
      throw new Error(`EvmRpcBlockClient can only serve EVM queries, got type "${query.type}"`)
    }
    const { type: _type, fields = {}, fromBlock = 0, toBlock, parentBlockHash, ...request } = query
    const mapper = createWireBlockMapper(fields, request)

    const coarse = mapper.requiredData
    const req: RpcDataRequest = {
      // mapRpcBlock always maps the block's transactions, so full tx objects must be fetched. This
      // is why `RequiredData` carries no `transactions` toggle — it could never be false.
      transactions: true,
      logs: coarse.logs,
      receipts: coarse.receipts,
      traces: coarse.traces,
      stateDiffs: coarse.stateDiffs,
      useTraceApi: this.#method.useTraceApi,
      useDebugTraceBlockByNumber: this.#method.useDebugTraceBlockByNumber,
      useDebugApiForStateDiffs: this.#method.useDebugApiForStateDiffs,
      debugTraceTimeout: this.#method.debugTraceTimeout,
    }

    const inner = new EvmRpcDataSource({
      rpc: this.#rpc,
      req,
      strideSize: this.#strideSize,
      strideConcurrency: this.#strideConcurrency,
    })

    const streamReq = { from: fromBlock, to: toBlock, parentHash: parentBlockHash }
    const finalized = options?.finalized ?? this.finalized
    const stream = finalized ? inner.getFinalizedStream(streamReq) : inner.getStream(streamReq)

    try {
      for await (const { blocks, finalizedHead } of stream) {
        const wire = blocks.map((raw) => mapper.map(raw))
        // Match the Portal: a block left empty by filtering is dropped (boundary blocks kept).
        const data = dropEmptyBlocks(wire, request.includeAllBlocks)
        if (data.length === 0) continue

        const head = finalizedHead ? { number: finalizedHead.number, hash: finalizedHead.hash } : undefined

        yield {
          blocks: data,
          // `latest` is the CHAIN head (the portal fills it from its head header) and drives the
          // progress denominator, so it must not be fabricated from the finalized head: that reads
          // as "the chain ends here" and would report an unbounded run complete ~a finality window
          // early. The stream gives no cheap latest head, so it is left absent — exactly what a
          // portal response without a head header does. Freshness/lag come from `getHead()`, which
          // the fallback polls independently.
          head: { finalized: head },
          meta: {
            // A real measurement, deliberately: the serialize is a few ms per batch while the RPC
            // fetch it accompanies takes seconds, and 0 would render throughput as broken. The
            // wire JSON is ASCII (hex strings + numbers), so string length equals byte length.
            bytes: JSON.stringify(data).length,
            requestedFromBlock: fromBlock,
            lastBlockReceivedAt: new Date(),
            requests: {},
          },
        }
      }
    } catch (e) {
      if (isSqdForkException(e)) {
        throw new ForkException(
          e.previousBlocks.map((b) => ({ number: b.number, hash: b.hash })),
          { fromBlock: e.blockNumber, parentBlockHash: e.expectedParentHash },
        )
      }

      throw e
    }
  }
}

interface SqdForkException {
  isSqdForkException: true
  blockNumber: number
  expectedParentHash: string
  previousBlocks: { number: number; hash: string }[]
}

function isSqdForkException(e: unknown): e is SqdForkException {
  return e instanceof Error && (e as any).isSqdForkException === true
}
