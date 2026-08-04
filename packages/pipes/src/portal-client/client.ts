import { Readable } from 'stream'

import { unexpectedCase, wait, withErrorContext } from '@subsquid/util-internal'

import {
  HttpBody,
  HttpClient,
  HttpClientOptions,
  HttpError,
  HttpResponse,
  RequestOptions,
} from '~/http-client/index.js'
import { partition } from '~/internal/array.js'
import { npmVersion } from '~/version.js'

import { ForkException } from './fork-exception.js'
import { GetBlock, PortalQuery, Query } from './query/index.js'
import { splitLines } from './split-lines.js'
import { type BlockRef, type PortalHead, StreamBuffer, type StreamData } from './stream-buffer.js'

export type { BlockRef, PortalHead, StreamData } from './stream-buffer.js'

export type ApiDataset = {
  dataset: string
  aliases: string[]
  real_time: boolean
  start_block: number
  metadata?: {
    kind: string
    display_name?: string
    logo_url?: string
    type?: string
    evm?: {
      chain_id: number
    }
  }
}

const USER_AGENT = `@subsquid/pipes:${npmVersion}`

export interface PortalClientOptions {
  /**
   * The URL of the portal dataset.
   */
  url: string

  /**
   *  If true, queries will only return finalized blocks.
   */
  finalized?: boolean

  /**
   * Optional custom HTTP client to use.
   */
  http?: HttpClient | HttpClientOptions

  /**
   * Maximum number of bytes to buffer before flushing.
   * @default 10_485_760 (10MB)
   */
  maxBytes?: number

  /**
   * Maximum time between stream data in milliseconds for return.
   * @default 300
   */
  maxIdleTimeMs?: number

  /**
   * Maximum wait time in milliseconds for return.
   * @default 5_000
   */
  maxWaitTimeMs?: number

  /**
   * Interval for polling the head in milliseconds.
   * @default 0
   */
  headPollIntervalMs?: number
}

export type PortalRequestOptions = Pick<
  RequestOptions,
  'headers' | 'retryAttempts' | 'retrySchedule' | 'httpTimeout' | 'bodyTimeout'
>

export interface PortalBlockStreamOptions {
  request?: PortalRequestOptions
  maxBytes?: number
  maxIdleTimeMs?: number
  maxWaitTimeMs?: number
  headPollIntervalMs?: number
  finalized?: boolean
  /**
   * Give every unfinalized block its own batch, and never mix finalized blocks into an unfinalized
   * block's batch. Only targets that key a rollback snapshot by the batch's last block (the Postgres
   * target) need this; for everyone else it just costs extra round-trips, so it is off by default
   * and a response is delivered as a single batch.
   */
  perBlockUnfinalized?: boolean
}

/**
 * The raw block-batch stream for a single portal query, as returned by
 * {@link PortalClient.getStream}.
 */
export interface PortalBlockStream<B> extends AsyncIterable<StreamData<B>> {}

/** The portal can answer 409 with the error envelope alone, leaving no chain to walk back to. */
function isForkHttpError(err: unknown): err is HttpError {
  if (!(err instanceof HttpError)) return false
  if (err.response.status !== 409) return false

  const previousBlocks = err.response.body?.previousBlocks
  if (!Array.isArray(previousBlocks) || previousBlocks.length === 0) return false

  return true
}

export class PortalClient {
  readonly #client: HttpClient
  readonly #url: URL
  readonly #options: {
    finalized: boolean
    headPollInterval: number
    maxBytes: number
    maxIdleTime: number
    maxWaitTime: number
  }

  constructor(options: PortalClientOptions) {
    this.#client = options.http instanceof HttpClient ? options.http : new HttpClient(options.http)

    this.#url = new URL(options.url)
    this.#options = {
      finalized: options.finalized ?? false,
      headPollInterval: options.headPollIntervalMs ?? 0,
      maxBytes: options.maxBytes ?? 10 * 1024 * 1024,
      maxIdleTime: options.maxIdleTimeMs ?? 300,
      maxWaitTime: options.maxWaitTimeMs ?? 5_000,
    }
  }

  /** Whether this client reads `/finalized-stream`, i.e. never sees a fork. */
  get finalized(): boolean {
    return this.#options.finalized
  }

  getUrl() {
    return this.#url.toString()
  }

  private getDatasetUrl(path: string): string {
    let u = new URL(this.#url)
    if (this.#url.pathname.endsWith('/')) {
      u.pathname += path
    } else {
      u.pathname += '/' + path
    }
    return u.toString()
  }

  async getMetadata(options?: PortalRequestOptions): Promise<ApiDataset> {
    const url = this.getDatasetUrl('metadata') + '?expand[]=metadata'
    const res = await this.request('GET', url, options)

    return res.body
  }

  async resolveTimestamp(seconds: number, options?: PortalRequestOptions): Promise<number> {
    const res = await this.request<{ block_number: number }>(
      'GET',
      this.getDatasetUrl(`timestamps/${seconds}/block`),
      options,
    )

    return res.body.block_number
  }

  async getHead(options?: PortalRequestOptions & { finalized: boolean }): Promise<BlockRef | undefined> {
    const res = await this.request<BlockRef>(
      'GET',
      this.getDatasetUrl((options?.finalized ?? this.#options.finalized) ? 'finalized-head' : 'head'),
      options,
    )
    return res.body ?? undefined
  }

  getStream<Q extends Query>(query: Q, options?: PortalBlockStreamOptions): PortalBlockStream<GetBlock<Q>> {
    const settings = {
      request: options?.request ?? {},
      finalized: options?.finalized ?? this.#options.finalized,
      perBlockUnfinalized: options?.perBlockUnfinalized ?? false,
      maxBytes: options?.maxBytes ?? this.#options.maxBytes,
      maxIdleTime: options?.maxIdleTimeMs ?? this.#options.maxIdleTime,
      maxWaitTime: options?.maxWaitTimeMs ?? this.#options.maxWaitTime,
      headPollInterval: options?.headPollIntervalMs ?? this.#options.headPollInterval,
    }

    return createPortalStream(
      query,
      settings,
      async (q, o) =>
        this.getStreamRequest((options?.finalized ?? this.#options.finalized) ? 'finalized-stream' : 'stream', q, o),
      async () => this.getHead({ ...settings.request, finalized: true }),
    )
  }

  private async getStreamRequest(path: string, query: PortalQuery, options?: RequestOptions) {
    try {
      let res = await this.request<Readable | undefined>('POST', this.getDatasetUrl(path), {
        ...options,
        json: query,
        stream: true,
      }).catch(withErrorContext({ query }))

      switch (res.status) {
        case 200:
        case 204:
          return {
            status: res.status,
            head: getHeadFromHeaders(res.headers),
            stream: res.body && res.status === 200 ? splitLines(res.body) : undefined,
          }
        default:
          throw unexpectedCase(res.status)
      }
    } catch (e: unknown) {
      if (isForkHttpError(e)) {
        throw new ForkException(e.response.body.previousBlocks, {
          fromBlock: query.fromBlock,
          parentBlockHash: query.parentBlockHash,
        })
      }

      throw e
    }
  }

  private request<T = any>(method: string, url: string, options: RequestOptions & HttpBody = {}) {
    return this.#client.request<T>(url, {
      ...options,
      method,
      headers: {
        'User-Agent': USER_AGENT,
        ...options?.headers,
      },
    })
  }
}

function createPortalStream<Q extends Query>(
  query: Q,
  options: {
    request: PortalRequestOptions
    finalized: boolean
    perBlockUnfinalized: boolean
    maxBytes: number
    maxIdleTime: number
    maxWaitTime: number
    headPollInterval: number
  },
  requestStream: (
    query: Q,
    options?: RequestOptions,
  ) => Promise<{
    head: PortalHead
    status: number
    stream?: AsyncIterable<string[]> | null | undefined
  }>,
  getFinalizedHead: () => Promise<BlockRef | undefined>,
): PortalBlockStream<GetBlock<Q>> {
  const { headPollInterval, request, perBlockUnfinalized, ...bufferOptions } = options
  const buffer = new StreamBuffer<GetBlock<Q>>(bufferOptions)

  let { fromBlock = 0, toBlock, parentBlockHash } = query

  // Outside the loop: a response carrying no blocks hands its counters to no batch, so they must
  // survive into the next iteration.
  let requests: Record<number, number> = {}

  // Highest finalized head any response has reported. Stays undefined for a no-finality dataset,
  // which is also the answer to whether there is anything to wait for at the end of a range.
  let finalizedSeen: number | undefined

  // Cadence for re-asking the portal when it has nothing for us yet — finality still below
  // `toBlock`, or a bounded range answered with no data. `headPollInterval` keeps its opt-in
  // role for the 204 head-tail; this one must never hot-loop, so it defaults on.
  const retryInterval = headPollInterval > 0 ? headPollInterval : 1_000

  const ingest = async () => {
    while (!buffer.signal.aborted) {
      if (toBlock != null && fromBlock > toBlock) {
        if (finalizedSeen == null || finalizedSeen >= toBlock) break

        // Every requested block is delivered, but finality is still below `toBlock`. Ending here
        // would strand the tail: a finalized-only consumer holds rows back until some batch
        // reports them final, and after the last block no such batch would ever come — the stream
        // "completes" while the consumer silently discards what it was never allowed to commit.
        // So poll the finalized head until it reaches `toBlock` and deliver the catch-up as one
        // final empty batch. This waits exactly as long as finality itself does — for a range
        // ending above the chain's current finalized head, minutes, not forever.
        const head = await getFinalizedHead()
        if (head != null && head.number > finalizedSeen) {
          finalizedSeen = head.number
          if (head.number >= toBlock) {
            await buffer.put({
              blocks: [],
              head: { finalized: head },
              meta: { bytes: 0, requestedFromBlock: fromBlock, lastBlockReceivedAt: new Date(), requests },
            })
            requests = {}
            buffer.flush()
            break
          }
        }
        await wait(retryInterval, buffer.signal)
        continue
      }

      const requestedFrom = fromBlock
      const res = await requestStream(
        {
          ...query,
          fromBlock,
          parentBlockHash,
        },
        {
          ...request,
          abort: buffer.signal,
          hooks: {
            onAfterResponse: (_, res) => {
              requests[res.status] = (requests[res.status] || 0) + 1
            },
          },
        },
      )

      if (res.head.finalized != null) {
        finalizedSeen = Math.max(finalizedSeen ?? -1, res.head.finalized.number)
      }

      // We are on head, we need to wait a little bit until new dta arrives
      if (res.status === 204) {
        await buffer.put({
          blocks: [],
          meta: { bytes: 0, requestedFromBlock: fromBlock, lastBlockReceivedAt: new Date(), requests },
          head: res.head,
        })
        requests = {}
        buffer.flush()
        if (headPollInterval > 0) {
          await wait(headPollInterval, buffer.signal)
        }
        continue
      }

      // A 200 with no body: the portal has no data at `fromBlock` right now. For an open-ended
      // stream that is the end of the dataset. For a bounded range it is a promise not yet kept —
      // sealed chunks and hotblocks both lag the chain — so keep asking until the data shows up
      // rather than silently ending `fromBlock..toBlock` short.
      if (res.stream == null) {
        if (toBlock == null) break
        await wait(retryInterval, buffer.signal)
        continue
      }

      try {
        for await (let data of res.stream) {
          const lastBlockReceivedAt = new Date()
          const blocks: { block: GetBlock<Q>; bytes: number }[] = []
          const requestedFromBlock = fromBlock

          for (let line of data) {
            try {
              const block = JSON.parse(line)

              // Update for next request
              fromBlock = block.header.number + 1
              parentBlockHash = block.header.hash

              // Collect a block and its size
              blocks.push({ block, bytes: line.length })
            } catch (e: any) {
              // FIXME we need to catch JSON parse errors here
              // we hit an incomplete line, we should break and wait for more data
              // we need to find the RC first
              // if (e.message?.includes?.('Unterminated string')) break

              throw e
            }
          }

          const finalizedHead = res.head.finalized?.number

          // `!= null`: a head of 0 is real, and reading it as absent files every hot block as finalized.
          const [finalizedBlocks, unfinalizedBlocks] =
            finalizedHead != null
              ? partition(blocks, ({ block }) => block.header?.number <= finalizedHead)
              : [blocks, []]

          // Carried by the first batch out, so one response counts as one request.
          let pendingRequests = requests

          if (perBlockUnfinalized) {
            // Postgres tags undo rows with the batch's last block, so every rollback-able block
            // needs its own batch, never shared with a finalized one.
            if (finalizedBlocks.length > 0) {
              await buffer.put(
                {
                  blocks: finalizedBlocks.map((b) => b.block),
                  head: res.head,
                  meta: {
                    bytes: finalizedBlocks.reduce((a, b) => a + b.bytes, 0),
                    requestedFromBlock,
                    lastBlockReceivedAt,
                    requests: pendingRequests,
                  },
                },
                unfinalizedBlocks.length > 0,
              )
              pendingRequests = {}
            } else if (unfinalizedBlocks.length > 0) {
              // Or blocks pending from earlier all-finalized responses inherit this batch's number.
              await buffer.cut()
            }

            for (let { block, bytes } of unfinalizedBlocks) {
              await buffer.put(
                {
                  blocks: [block],
                  head: res.head,
                  meta: {
                    bytes,
                    requestedFromBlock,
                    lastBlockReceivedAt,
                    requests: pendingRequests,
                  },
                },
                true,
              )
              pendingRequests = {}
            }
          } else {
            // One batch per response. Targets that resolve a rollback from a block number carried on
            // the rows themselves (ClickHouse) don't need per-block batches, and splitting them only
            // costs round-trips. Flush once when the response carries unfinalized blocks, so head
            // latency stays the same as the per-block path.
            await buffer.put(
              {
                blocks: blocks.map((b) => b.block),
                head: res.head,
                meta: {
                  bytes: blocks.reduce((a, b) => a + b.bytes, 0),
                  requestedFromBlock,
                  lastBlockReceivedAt,
                  requests: pendingRequests,
                },
              },
              unfinalizedBlocks.length > 0,
            )
            pendingRequests = {}
          }

          requests = pendingRequests
        }
      } catch (err) {
        if (buffer.signal.aborted) break
        if (!isStreamAbortedError(err)) {
          throw err
        }
      }

      // A response that advanced nothing (a 200 whose body carried no blocks) would otherwise be
      // re-asked in a hot loop; pace it like the other empty answers.
      if (fromBlock === requestedFrom) {
        await wait(retryInterval, buffer.signal)
      }
    }
  }

  ingest().then(
    () => buffer.close(),
    (err) => buffer.fail(err),
  )

  return buffer.iterate()
}

function getHeadFromHeaders(headers: HttpResponse['headers']) {
  const finalizedHeadHash = headers.get('X-Sqd-Finalized-Head-Hash')
  const finalizedHeadNumber = headers.get('X-Sqd-Finalized-Head-Number')
  const headNumber = headers.get('X-Sqd-Head-Number')

  return {
    finalized:
      finalizedHeadHash && finalizedHeadNumber
        ? {
            hash: finalizedHeadHash,
            number: parseInt(finalizedHeadNumber, 10),
          }
        : undefined,
    latest: headNumber
      ? {
          number: parseInt(headNumber, 10),
        }
      : undefined,
  }
}

function isStreamAbortedError(err: unknown) {
  if (!(err instanceof Error)) return false

  const code = (err.cause as any)?.code || (err as any).code
  if (!code) return false

  switch (code) {
    // Explicitly canceled via AbortController
    case 'ABORT_ERR':
    // The remote server ended the connection
    // before Node finished reading the response body.
    case 'ERR_STREAM_PREMATURE_CLOSE':
    // The other side hung up unexpectedly
    case 'ECONNRESET':
    // A low-level socket problem —
    // the TCP connection between your app and the remote server failed
    // or behaved unexpectedly.
    case 'UND_ERR_SOCKET':
      return true
    default:
      return false
  }
}
