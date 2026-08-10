import { promisify } from 'node:util'
import zlib from 'node:zlib'

import { BlockCursor, Logger, PortalCache, cursorFromHeader } from '~/core/index.js'
import { hashQuery } from '~/core/query-builder.js'
import { last } from '~/internal/array.js'
import { GetBlock, PortalBlockStream, PortalClient, Query, StreamData } from '~/portal-client/index.js'

// @ts-ignore
const compressAsync = promisify('zstdCompress' in zlib ? (zlib.zstdCompress as any) : zlib.gzip)
// @ts-ignore
const decompressAsync = promisify('zstdDecompress' in zlib ? (zlib.zstdDecompress as any) : zlib.gunzip)

export type Options<ImplOptions> = {
  /**
   * Enable or disable data compression.
   * Uses zstd compression algorithm.
   * @default true
   */
  compress?: boolean
} & ImplOptions

export type SaveBatch = { queryHash: string; cursors: { first: number; last: number }; data: Buffer }
export type StreamBatch = { queryHash: string; fromBlock: number }

export abstract class PortalCacheNodeJs<ImplOptions> implements PortalCache {
  protected readonly options: Options<ImplOptions>

  protected constructor(options: Options<ImplOptions>) {
    this.options = {
      compress: true,
      ...options,
    }
  }

  #initialized = false

  protected abstract initialize?(): Promise<void>
  protected abstract stream(request: StreamBatch): AsyncIterable<Buffer>
  protected abstract save(batch: SaveBatch): Promise<void>

  protected async compress(value: string): Promise<Buffer> {
    if (!this.options.compress) return Buffer.from(value)

    return await compressAsync(value)
  }

  protected async decompress(value: Buffer): Promise<string> {
    if (!this.options.compress) return value.toString('utf-8')

    const buffer = await decompressAsync(value)
    return buffer.toString('utf8')
  }

  protected async ensureInitialized() {
    if (this.#initialized) return

    await this.initialize?.()

    this.#initialized = true
  }

  async *getStream<Q extends Query>({
    portal,
    query,
    logger,
    perBlockUnfinalized,
    finalized,
  }: {
    portal: PortalClient
    query: Query
    logger: Logger
    perBlockUnfinalized?: boolean
    finalized?: boolean
  }): PortalBlockStream<GetBlock<Q>> {
    const queryHash = await hashQuery(query)

    await this.ensureInitialized()

    let cursor: BlockCursor = {
      number: query.fromBlock,
      hash: query.parentBlockHash,
    }
    logger.debug(`loading data from cache from ${cursor.number} block`)

    for await (const message of this.stream({
      fromBlock: cursor.number,
      queryHash,
    })) {
      const decoded: StreamData<GetBlock<Q>> = JSON.parse(await this.decompress(message))

      yield decoded

      cursor = cursorFromHeader(last(decoded.blocks))
      cursor.number += 1
    }

    if (query.toBlock && cursor.number >= query.toBlock) return

    logger.debug(`switching to the portal from ${cursor.number} block`)
    for await (const batch of portal.getStream(
      {
        ...query,
        fromBlock: cursor.number,
        parentBlockHash: cursor.hash,
      } as Q,
      { perBlockUnfinalized, finalized },
    )) {
      const finalizedHead = batch.head.finalized?.number
      // TODO add warning
      if (!finalizedHead) {
        yield batch
        continue
      }

      const finalizedBlocks = batch.blocks.filter((b) => b.header.number <= finalizedHead)
      if (finalizedBlocks.length) {
        await this.save({
          queryHash,
          cursors: {
            first: batch.meta.requestedFromBlock,
            last: last(finalizedBlocks).header.number,
          },
          data: await this.compress(
            JSON.stringify({
              ...batch,
              blocks: finalizedBlocks,
            }),
          ),
        })
      }

      yield batch
      // TODO check next batch in cache
    }
  }
}
