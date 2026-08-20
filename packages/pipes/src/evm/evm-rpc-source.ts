import { mapRpcBlock } from '@subsquid/evm-normalization'
import { EvmRpcDataSource, Rpc, Block as RpcBlock, DataRequest as RpcDataRequest } from '@subsquid/evm-rpc'

import {
  BatchContext,
  BlockCursor,
  Logger,
  PortalBatch,
  cursorFromHeader,
  defaultLogger,
  extractRollbackChain,
  noopMetricsServer,
} from '~/core/index.js'
import { Span } from '~/core/profiling.js'
import { ApiDataset } from '~/portal-client/client.js'
import { ForkException } from '~/portal-client/index.js'
import { Block, DataRequest, FieldSelection } from '~/portal-client/query/evm.js'

import { decodeBlock, withRequiredFields } from './rpc/decode.js'
import { Relations, filterBlock, setUpRelations } from './rpc/filter.js'
import { augmentFields, dropEmptyBlocks, keptByPosition, selectionGrew } from './rpc/project.js'
import { toRequiredData } from './rpc/request.js'

/** RPC method-selection toggles (the per-chain "C1" config) merged into the coarse fetch request. */
export interface RpcMethodOptions {
  useTraceApi?: boolean
  useDebugTraceBlockByNumber?: boolean
  useDebugApiForStateDiffs?: boolean
  debugTraceTimeout?: string
}

export interface EvmRpcSourceOptions<F extends FieldSelection> {
  id?: string
  rpc: Rpc
  fields: F
  request: DataRequest
  from: number
  to?: number
  /** Stream finalized blocks only (default). Hot blocks resume-safety is the target's concern. */
  finalized?: boolean
  method?: RpcMethodOptions
  strideSize?: number
  strideConcurrency?: number
  logger?: Logger
}

/**
 * An RPC-backed EVM source whose output matches the Portal source's. It delegates fetching,
 * finality, continuity and fork detection to `@subsquid/evm-rpc`'s `EvmRpcDataSource`, maps each
 * raw block through the reused Portal decoder + the ported client-side filter, and emits
 * `PortalBatch`es with a populated `BatchContext` — so it drops into the fallback (or a target)
 * exactly like a `PortalSource`'s stream. evm-rpc's `ForkException` is translated to Pipes'.
 */
export class EvmRpcSource<F extends FieldSelection> {
  readonly #inner: EvmRpcDataSource
  /** F + structural required fields — the Portal output shape; the projection target. */
  readonly #outputFields: FieldSelection
  /** #outputFields + the fields the request's where-clauses need to be evaluated. */
  readonly #augmentedFields: FieldSelection
  readonly #needsProjection: boolean
  readonly #request: DataRequest
  readonly #from: number
  readonly #to?: number
  readonly #finalized: boolean
  readonly #withTraces: boolean
  readonly #withStateDiffs: boolean
  readonly #id: string
  readonly #logger: Logger
  readonly #metrics = noopMetricsServer().metrics
  readonly #rawQuery: unknown

  constructor(options: EvmRpcSourceOptions<F>) {
    this.#outputFields = withRequiredFields(options.fields)
    this.#augmentedFields = augmentFields(this.#outputFields, options.request)
    this.#needsProjection = selectionGrew(this.#augmentedFields, this.#outputFields)
    this.#request = options.request
    this.#from = options.from
    this.#to = options.to
    this.#finalized = options.finalized ?? true
    this.#id = options.id ?? 'evm-rpc'
    this.#logger = options.logger ?? defaultLogger({ id: this.#id })
    this.#rawQuery = { type: 'evm', fields: options.fields, ...options.request }

    const coarse = toRequiredData(options.request, options.fields)
    this.#withTraces = coarse.traces
    this.#withStateDiffs = coarse.stateDiffs

    const req: RpcDataRequest = {
      // mapRpcBlock always maps the block's transactions, so full tx objects must be fetched. This
      // is why `RequiredData` carries no `transactions` toggle — it could never be false.
      transactions: true,
      logs: coarse.logs,
      receipts: coarse.receipts,
      traces: coarse.traces,
      stateDiffs: coarse.stateDiffs,
      useTraceApi: options.method?.useTraceApi,
      useDebugTraceBlockByNumber: options.method?.useDebugTraceBlockByNumber,
      useDebugApiForStateDiffs: options.method?.useDebugApiForStateDiffs,
      debugTraceTimeout: options.method?.debugTraceTimeout,
    }

    this.#inner = new EvmRpcDataSource({
      rpc: options.rpc,
      req,
      strideSize: options.strideSize,
      strideConcurrency: options.strideConcurrency,
    })
  }

  get name(): string {
    return this.#id
  }

  /** Independent head poll (no stream) — the same commitment the stream reads toward. */
  async getHead(): Promise<BlockCursor | undefined> {
    const head = this.#finalized ? await this.#inner.getFinalizedHead() : await this.#inner.getHead()
    return head ? { number: head.number, hash: head.hash } : undefined
  }

  async *read(cursor?: BlockCursor): AsyncIterable<PortalBatch<Block<F>[]>> {
    const from = cursor ? cursor.number + 1 : this.#from
    const streamReq = { from, to: this.#to, parentHash: cursor?.hash }
    const initial = from

    const stream = this.#finalized ? this.#inner.getFinalizedStream(streamReq) : this.#inner.getStream(streamReq)

    try {
      for await (const { blocks, finalizedHead } of stream) {
        const mapped = blocks.map((raw) => this.#mapBlock(raw))
        // Match the Portal: a block left empty by filtering is dropped (boundary blocks kept). The
        // last kept block is also the batch's progress cursor.
        const data = dropEmptyBlocks(mapped as any, this.#request.includeAllBlocks) as typeof mapped
        if (data.length === 0) continue

        const current = cursorFromHeader(data[data.length - 1] as any)
        const finalized = finalizedHead ? { number: finalizedHead.number, hash: finalizedHead.hash } : undefined

        yield { data, ctx: this.#buildContext(data, current, finalized, initial) }
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

  #mapBlock(raw: RpcBlock): Block<F> {
    const normalized = mapRpcBlock(raw, { withTraces: this.#withTraces, withStateDiffs: this.#withStateDiffs })
    const filtered: any = decodeBlock(normalized, this.#augmentedFields)

    // The augmented decode's item arrays align 1:1 by index with a decode at exactly the output
    // fields of the *same* normalized block; snapshot them before filtering so the surviving
    // positions can be recovered by object identity (used by the projection below).
    const preLogs = filtered.logs
    const preTransactions = filtered.transactions
    const preTraces = filtered.traces
    const preStateDiffs = filtered.stateDiffs

    const relations: Relations = setUpRelations(filtered)
    filterBlock(filtered, this.#request, relations)

    if (!this.#needsProjection) {
      return filtered as Block<F>
    }

    // A where-clause referenced a field not selected for output; decode again at exactly the output
    // shape and keep the items whose pre-filter position survived. Position/identity — not a
    // synthesized structural key — so structurally identical items can't collide and project the
    // wrong one.
    const projected: any = decodeBlock(normalized, this.#outputFields)
    projected.logs = keptByPosition(projected.logs, preLogs, filtered.logs)
    projected.transactions = keptByPosition(projected.transactions, preTransactions, filtered.transactions)
    projected.traces = keptByPosition(projected.traces, preTraces, filtered.traces)
    projected.stateDiffs = keptByPosition(projected.stateDiffs, preStateDiffs, filtered.stateDiffs)

    return projected as Block<F>
  }

  #buildContext(
    data: Block<F>[],
    current: BlockCursor,
    finalized: BlockCursor | undefined,
    initial: number,
  ): BatchContext {
    return {
      id: this.#id,
      profiler: Span.root('batch', false),
      metrics: this.#metrics,
      logger: this.#logger,
      stream: {
        dataset: {} as ApiDataset,
        head: { finalized, latest: current },
        state: {
          initial,
          last: current.number,
          current,
          ranges: [{ from: this.#from, to: this.#to }],
          rollbackChain: extractRollbackChain({ blocks: data as any, head: finalized }),
        },
        query: { url: '', hash: '', raw: this.#rawQuery },
      },
      batch: { blocksCount: data.length, bytesSize: 0, requests: {}, lastBlockReceivedAt: new Date() },
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
