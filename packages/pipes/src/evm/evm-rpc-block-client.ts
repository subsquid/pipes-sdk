import { mapRpcBlock } from '@subsquid/evm-normalization'
import {
  EvmRpcClient,
  EvmRpcDataSource,
  Rpc,
  Block as RpcBlock,
  DataRequest as RpcDataRequest,
} from '@subsquid/evm-rpc'
import { toJSON } from '@subsquid/util-internal-json'
import { cast } from '@subsquid/util-internal-validation'

import { redactUrl } from '~/core/fallback-diagnostics.js'
import { ApiDataset, BlockRef, PortalBlockStreamOptions } from '~/portal-client/client.js'
import { ForkException, GetBlock, PortalBlockStream, Query, StreamData } from '~/portal-client/index.js'
import { DataRequest, FieldSelection, getBlockSchema } from '~/portal-client/query/evm.js'

import { withRequiredFields } from './rpc/decode.js'
import { Relations, filterBlock, setUpRelations } from './rpc/filter.js'
import { augmentFields, dropEmptyBlocks, keptByPosition } from './rpc/project.js'
import { toRequiredData } from './rpc/request.js'
import { shimWireBlock } from './rpc/shim.js'

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
        return self.#stream(query as EvmQueryShape, options)[Symbol.asyncIterator]() as AsyncIterator<
          StreamData<GetBlock<Q>>
        >
      },
    }
  }

  async *#stream(query: EvmQueryShape, options?: PortalBlockStreamOptions): AsyncGenerator<StreamData<any>> {
    const { type: _type, fields = {}, fromBlock = 0, toBlock, parentBlockHash, ...request } = query
    const outputFields = withRequiredFields(fields)
    // The filter needs the where-clause fields decoded even when not selected for output; the wire
    // blocks keep every field regardless — the downstream cast prunes to the user's selection.
    const augmented = augmentFields(outputFields, request)
    const schema = getBlockSchema(augmented)

    const coarse = toRequiredData(request, fields)
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
        const wire = blocks.map((raw) => this.#toWireBlock(raw, coarse, augmented, schema, request))
        // Match the Portal: a block left empty by filtering is dropped (boundary blocks kept).
        const data = dropEmptyBlocks(wire, request.includeAllBlocks as boolean | undefined)
        if (data.length === 0) continue

        const head = finalizedHead ? { number: finalizedHead.number, hash: finalizedHead.hash } : undefined

        yield {
          blocks: data,
          head: { finalized: head, latest: head ? { number: head.number } : undefined },
          meta: {
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

  /**
   * Normalize a raw RPC block to the portal wire shape and filter its item arrays down to what the
   * request matches. The predicates run on a throwaway *decoded* copy (`cast` at the augmented
   * fields); the surviving positions are then mapped back onto the wire arrays — decoded and wire
   * arrays align 1:1 by construction, and position + object identity (never a synthesized key)
   * keeps structurally identical items apart.
   */
  #toWireBlock(
    raw: RpcBlock,
    coarse: { traces: boolean; stateDiffs: boolean },
    augmented: FieldSelection,
    schema: ReturnType<typeof getBlockSchema>,
    request: DataRequest,
  ): FilterableWireBlock {
    const normalized = mapRpcBlock(raw, { withTraces: coarse.traces, withStateDiffs: coarse.stateDiffs })
    const wire = shimWireBlock(toJSON(normalized)) as FilterableWireBlock
    wire.logs ??= []
    wire.transactions ??= []
    wire.traces ??= []
    wire.stateDiffs ??= []

    const decoded: any = cast(schema, wire)
    const preLogs = decoded.logs ?? []
    const preTransactions = decoded.transactions ?? []
    const preTraces = decoded.traces ?? []
    const preStateDiffs = decoded.stateDiffs ?? []

    const relations: Relations = setUpRelations(decoded)
    filterBlock(decoded, request, relations)

    wire.logs = keptByPosition(wire.logs, preLogs, decoded.logs ?? [])
    wire.transactions = keptByPosition(wire.transactions, preTransactions, decoded.transactions ?? [])
    wire.traces = keptByPosition(wire.traces, preTraces, decoded.traces ?? [])
    wire.stateDiffs = keptByPosition(wire.stateDiffs, preStateDiffs, decoded.stateDiffs ?? [])

    return wire
  }
}

/** The EVM portal query fields this client consumes (a structural view of `EvmQuery`). */
type EvmQueryShape = {
  type?: string
  fields?: FieldSelection
  fromBlock?: number
  toBlock?: number
  parentBlockHash?: string
  includeAllBlocks?: boolean
  [key: string]: unknown
}

type FilterableWireBlock = {
  header: { number: number; hash: string; timestamp?: number }
  logs: unknown[]
  transactions: unknown[]
  traces: unknown[]
  stateDiffs: unknown[]
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
