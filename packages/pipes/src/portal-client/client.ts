import { Readable } from 'stream'

import { unexpectedCase, wait, withErrorContext } from '@subsquid/util-internal'

import { formatDuration } from '~/core/formatters.js'
import { MalformedPortalResponseError, MissingForkAncestorError, PortalBlockOrderError } from '~/core/index.js'
import { type Logger } from '~/core/logger.js'
import {
  HttpBody,
  HttpClient,
  HttpClientOptions,
  HttpError,
  HttpResponse,
  RequestOptions,
} from '~/http-client/index.js'
import { npmVersion } from '~/version.js'

import { ForkException } from './fork-exception.js'
import { GetBlock, PortalQuery, Query } from './query/index.js'
import { Lines, splitLines } from './split-lines.js'
import { type BlockRef, type PortalHead, StreamBuffer, type StreamData } from './stream-buffer.js'
import { isBodyTimeoutError, streamBodyTimeout } from './stream-timeout.js'

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
   * Gets a warning each time a stalled stream response is cut and requested again.
   * Defaults to the logger of the `http` options.
   */
  logger?: Logger

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
  /**
   * `bodyTimeout` defaults to the one of the HTTP client. Without one, a response gets 30 s when
   * the portal serves it from real-time data and 150 s otherwise. A stream requests a response that
   * times out again from the block after the last one it delivered.
   */
  request?: PortalRequestOptions
  maxBytes?: number
  maxIdleTimeMs?: number
  maxWaitTimeMs?: number
  headPollIntervalMs?: number
  finalized?: boolean
}

/**
 * The raw block-batch stream for a single portal query, as returned by
 * {@link PortalClient.getStream}.
 */
export interface PortalBlockStream<B> extends AsyncIterable<StreamData<B>> {}

export class PortalClient {
  readonly #client: HttpClient
  readonly #url: URL
  readonly #logger?: Logger
  readonly #options: {
    finalized: boolean
    headPollInterval: number
    maxBytes: number
    maxIdleTime: number
    maxWaitTime: number
  }

  constructor(options: PortalClientOptions) {
    this.#client = options.http instanceof HttpClient ? options.http : new HttpClient(options.http)

    const httpOptions = options.http instanceof HttpClient ? undefined : options.http
    this.#logger = options.logger ?? httpOptions?.logger ?? undefined

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
      request: {
        ...options?.request,
        bodyTimeout: options?.request?.bodyTimeout ?? this.#client.bodyTimeout ?? streamBodyTimeout,
      },
      finalized: options?.finalized ?? this.#options.finalized,
      maxBytes: options?.maxBytes ?? this.#options.maxBytes,
      maxIdleTime: options?.maxIdleTimeMs ?? this.#options.maxIdleTime,
      maxWaitTime: options?.maxWaitTimeMs ?? this.#options.maxWaitTime,
      headPollInterval: options?.headPollIntervalMs ?? this.#options.headPollInterval,
      onStall: ({ fromBlock, stalledMs }: Stall) => {
        this.#logger?.warn({
          message: `portal response stalled: no block for ${formatDuration(stalledMs)}, requesting again from block ${fromBlock}`,
          url: withoutCredentials(this.#url),
          fromBlock,
          stalledMs,
        })
      },
      onCut: ({ fromBlock }: Cut) => {
        this.#logger?.warn({
          message: `portal response ended inside a block, requesting again from block ${fromBlock}`,
          url: withoutCredentials(this.#url),
          fromBlock,
        })
      },
    }

    return createPortalStream(query, settings, async (q, o) =>
      this.getStreamRequest((options?.finalized ?? this.#options.finalized) ? 'finalized-stream' : 'stream', q, o),
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
      const fork = e instanceof HttpError && e.response.status === 409
      if (!fork) {
        throw e
      }

      // The portal can answer 409 with the error envelope alone, leaving no chain to walk back to.
      const previousBlocks = e.response.body?.previousBlocks
      const noAncestor = !Array.isArray(previousBlocks) || previousBlocks.length === 0
      if (noAncestor) {
        throw new MissingForkAncestorError({ block: query.fromBlock, portalMessage: e.response.body?.error?.message })
      }

      throw new ForkException(previousBlocks, {
        fromBlock: query.fromBlock,
        parentBlockHash: query.parentBlockHash,
      })
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

/** A response that went silent: `fromBlock` is where the next request starts. */
type Stall = { fromBlock: number; stalledMs: number }

/** A response that ended inside a block: `fromBlock` is where the next request starts. */
type Cut = { fromBlock: number }

/** Returned for a line the body ended inside of: the response was cut, not malformed. */
const CUT = Symbol('cut')

function createPortalStream<Q extends Query>(
  query: Q,
  options: {
    request: PortalRequestOptions
    finalized: boolean
    maxBytes: number
    maxIdleTime: number
    maxWaitTime: number
    headPollInterval: number
    onStall: (stall: Stall) => void
    onCut: (cut: Cut) => void
  },
  requestStream: (
    query: Q,
    options?: RequestOptions,
  ) => Promise<{
    head: PortalHead
    status: number
    stream?: AsyncIterable<Lines> | null | undefined
  }>,
): PortalBlockStream<GetBlock<Q>> {
  const { headPollInterval, request, onStall, onCut, ...bufferOptions } = options
  const buffer = new StreamBuffer<GetBlock<Q>>(bufferOptions)

  let { fromBlock = 0, toBlock, parentBlockHash } = query
  const start = fromBlock

  // Outside the loop: a response carrying no blocks hands its counters to no batch, so they must
  // survive into the next iteration.
  let requests: Record<number, number> = {}
  // The block a response last ended inside of. A second cut there is not a transport fault.
  let cutAt: number | undefined

  const ingest = async () => {
    while (!buffer.signal.aborted) {
      if (toBlock != null && fromBlock > toBlock) break

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

      // If data is missing for a particular range,
      // portal responds with 200 status and empty body
      if (res.stream == null) break

      let lastDataAt = Date.now()

      try {
        for await (const { lines, unterminated } of res.stream) {
          const lastBlockReceivedAt = new Date()
          lastDataAt = lastBlockReceivedAt.getTime()

          const blocks: { block: GetBlock<Q>; bytes: number }[] = []
          const requestedFromBlock = fromBlock

          for (const line of lines) {
            const block = parseBlockLine(line, { fromBlock, unterminated })

            // The body has ended, so the loop requests again from `fromBlock`.
            if (block === CUT) {
              if (cutAt === fromBlock) {
                throw new MalformedPortalResponseError({ problem: 'cut-twice', fromBlock, line })
              }

              cutAt = fromBlock
              onCut({ fromBlock })
              break
            }

            const number = block.header.number

            // `fromBlock` is the request start, then one past the last block delivered.
            const outOfWindow = number < fromBlock || (toBlock != null && number > toBlock)
            if (outOfWindow) {
              const previous = fromBlock > start ? fromBlock - 1 : undefined

              throw new PortalBlockOrderError({ block: number, previous, range: { from: start, to: toBlock } })
            }

            // Update for next request
            fromBlock = number + 1
            parentBlockHash = block.header.hash

            // Collect a block and its size
            blocks.push({ block, bytes: line.length })
          }

          // Only a cut line leaves nothing to deliver; its counters go with the next batch.
          if (!blocks.length) {
            continue
          }

          // `!= null`: a head of 0 is real, and reading it as absent would hold hot blocks back.
          const finalizedHead = res.head.finalized?.number
          const hasUnfinalized =
            finalizedHead != null && blocks.some(({ block }) => block.header?.number > finalizedHead)

          // One batch per response: the source splits it by finality for a target that needs that.
          // Flush at once when the response carries unfinalized blocks, so head latency stays low.
          await buffer.put(
            {
              blocks: blocks.map((b) => b.block),
              head: res.head,
              meta: {
                bytes: blocks.reduce((a, b) => a + b.bytes, 0),
                requestedFromBlock,
                lastBlockReceivedAt,
                requests,
              },
            },
            hasUnfinalized,
          )
          requests = {}
        }
      } catch (err) {
        if (buffer.signal.aborted) break

        // The loop requests again from `fromBlock`, which only a fully parsed block moves.
        if (isBodyTimeoutError(err)) {
          onStall({ fromBlock, stalledMs: Date.now() - lastDataAt })
        } else if (!isStreamAbortedError(err)) {
          throw err
        }
      }
    }
  }

  ingest().then(
    () => buffer.close(),
    (err) => buffer.fail(err),
  )

  return buffer.iterate()
}

/**
 * Reads one line of a stream response as a block. A line that does not parse is a cut when the body
 * ended inside it, and malformed when a newline ended it.
 */
function parseBlockLine(line: string, { fromBlock, unterminated }: { fromBlock: number; unterminated: boolean }) {
  let block: any
  try {
    block = JSON.parse(line)
  } catch {
    if (unterminated) {
      return CUT
    }

    throw new MalformedPortalResponseError({ problem: 'invalid-json', fromBlock, line })
  }

  if (!Number.isSafeInteger(block?.header?.number)) {
    throw new MalformedPortalResponseError({ problem: 'no-block-number', fromBlock, line })
  }

  return block
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

function withoutCredentials(url: URL): string {
  const copy = new URL(url)
  copy.username = ''
  copy.password = ''

  return copy.toString()
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
