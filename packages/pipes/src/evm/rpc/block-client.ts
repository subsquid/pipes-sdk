import { EvmRpcClient, EvmRpcDataSource, Rpc, DataRequest as RpcDataRequest } from '@subsquid/evm-rpc'

import { nextUnlessAborted, safeReturn } from '~/core/fallback/async.js'
import { redactUrl } from '~/core/fallback/diagnostics.js'
import { ApiDataset, BlockRef, BlockStreamClient, PortalBlockStreamOptions } from '~/portal-client/client.js'
import { ForkException, GetBlock, PortalBlockStream, Query, StreamData } from '~/portal-client/index.js'
import { Query as EvmQuery } from '~/portal-client/query/evm.js'

import { EvmRpcConnectionOptions, RpcMethodOptions } from './options.js'
import { createWireBlockMapper, dropEmptyBlocks } from './wire.js'

export type { EvmRpcConnectionOptions, RpcMethodOptions } from './options.js'

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
 * An RPC-backed EVM {@link BlockStreamClient} that serves portal-wire-shaped blocks for any EVM
 * portal query. `@subsquid/evm-rpc` does the fetching, finality and fork detection; the blocks are
 * normalized and filtered in `wire.ts`.
 */
export class EvmRpcBlockClient implements BlockStreamClient {
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

  /** `eth_blockNumber`; the finalized commitment has no such call, so it looks the block up. */
  async getHeight(options?: { finalized: boolean }): Promise<number | undefined> {
    const finalized = options?.finalized ?? this.finalized
    if (finalized) {
      return (await this.#headSource.getFinalizedHead())?.number
    }

    return await this.#rpc.getHeight()
  }

  async resolveTimestamp(_seconds: number): Promise<number> {
    throw new Error(
      'an RPC source cannot resolve Date/timestamp block-range bounds — use numeric block ranges, or configure a portal source',
    )
  }

  getStream<Q extends Query>(query: Q, options?: PortalBlockStreamOptions): PortalBlockStream<GetBlock<Q>> {
    return this.#stream(query as EvmQuery, options)
  }

  async *#stream(query: EvmQuery, options?: PortalBlockStreamOptions): AsyncGenerator<StreamData<any>> {
    if (query.type !== 'evm') {
      throw new Error(`EvmRpcBlockClient can only serve EVM queries, got type "${query.type}"`)
    }
    const { type: _type, fields = {}, fromBlock = 0, toBlock, parentBlockHash, ...request } = query
    const mapper = createWireBlockMapper(fields, request)

    const req: RpcDataRequest = {
      // mapRpcBlock always maps the block's transactions, so full tx objects must be fetched.
      transactions: true,
      ...mapper.requiredData,
      ...this.#method,
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
    const iterator = stream[Symbol.asyncIterator]()

    try {
      while (true) {
        // evm-rpc cannot cancel a request, so a stop must not wait for the next block.
        const next = await nextUnlessAborted(iterator, options?.request?.abort)
        if (next === undefined || next.done) {
          return
        }

        const { blocks, finalizedHead } = next.value
        const wire = blocks.map((raw) => mapper.map(raw))
        const data = dropEmptyBlocks(wire, request.includeAllBlocks)

        // evm-rpc reports finality progress only in batches without blocks, so those must pass.
        if (data.length === 0 && !finalizedHead) {
          continue
        }

        const head = finalizedHead ? { number: finalizedHead.number, hash: finalizedHead.hash } : undefined

        yield {
          blocks: data,
          // No `latest`: it is the chain head and drives progress, and the finalized head in its
          // place would report an unbounded run complete a finality window early.
          head: { finalized: head },
          meta: {
            // The wire JSON is ASCII, so its string length is its byte length.
            bytes: JSON.stringify(data).length,
            requestedFromBlock: fromBlock,
            lastBlockReceivedAt: new Date(),
            requests: {},
          },
        }
      }
    } catch (e) {
      if (isSqdForkException(e)) {
        // evm-rpc does not keep the fork block number; it is the child of the last previous block.
        const forkBlock = e.previousBlocks[e.previousBlocks.length - 1].number + 1

        throw new ForkException(
          e.previousBlocks.map((b) => ({ number: b.number, hash: b.hash })),
          { fromBlock: forkBlock, parentBlockHash: e.expectedParentHash },
        )
      }

      throw e
    } finally {
      safeReturn(iterator)
    }
  }
}

interface SqdForkException {
  isSqdForkException: true
  expectedParentHash: string
  previousBlocks: { number: number; hash: string }[]
}

function isSqdForkException(e: unknown): e is SqdForkException {
  return e instanceof Error && (e as any).isSqdForkException === true
}
