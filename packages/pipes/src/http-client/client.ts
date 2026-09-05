import { addErrorContext, ensureError, wait } from '@subsquid/util-internal'
import { addTimeout } from '@subsquid/util-timeout'

import { type Logger } from '~/core/logger.js'
import { isNodeVersionGreaterOrEqual } from '~/internal/runtime.js'

import type { HttpBody } from './body.js'

export type { HttpBody }

const USER_AGENT = 'subsquid-pipes/http-client (sqd.ai)'

export type HeadersInit = string[][] | Record<string, string | readonly string[]> | Headers

export interface RequestHooks {
  /**
   * Called before the request is sent.
   * Can be used for logging, metrics, or modifying the request.
   */
  onBeforeRequest?: (req: FetchRequest) => void | Promise<void>

  /**
   * Called after response headers are received but before body is read.
   * Useful for early response inspection or streaming setup.
   */
  onAfterResponseHeaders?: (req: FetchRequest, res: Response) => void | Promise<void>

  /**
   * Called after the full response is received and processed.
   * The response body has been read at this point.
   */
  onAfterResponse?: (req: FetchRequest, res: HttpResponse) => void | Promise<void>

  /**
   * Called before a retry is attempted.
   * Provides the reason for retry (error or response) and pause duration.
   */
  onBeforeRetry?: (
    req: FetchRequest,
    reason: Error | HttpResponse,
    pauseMs: number,
    attempt: number,
  ) => void | Promise<void>

  /**
   * Called when a request fails with a non-retryable error.
   */
  onError?: (req: FetchRequest, error: Error) => void | Promise<void>

  /**
   * Called when a request completes successfully (after all retries if any).
   */
  onSuccess?: (req: FetchRequest, res: HttpResponse) => void | Promise<void>
}

export interface HttpClientOptions {
  /**
   * Base URL for all requests. If provided, relative URLs will be resolved against this base.
   * The base URL should be an absolute URL including protocol and host, e.g. 'https://api.example.com'.
   */
  baseUrl?: string

  /**
   * Default headers to be included with every request.
   * These can be overridden by request-specific headers.
   */
  headers?: Record<string, string | number | bigint>

  /**
   * Default request timeout in milliseconds.
   * This is the maximum time allowed for the entire HTTP request to complete, including connection time.
   *
   * Note: This timeout is only related to individual HTTP requests.
   * Overall request processing time might be much larger due to retries.
   *
   * Default: 20_000 (20 seconds)
   */
  httpTimeout?: number

  /**
   * Maximum time in milliseconds allowed for reading the response body.
   * This timeout is separate from httpTimeout and starts after response headers are received.
   *
   * When reading streaming responses, the timeout applies to the interval between chunks.
   * If no data is received within the timeout period, the request fails with HttpBodyTimeoutError.
   *
   * Default: undefined (no timeout)
   */
  bodyTimeout?: number
  retryAttempts?: number
  retrySchedule?: number[]
  keepalive?: boolean

  logger?: Logger | null

  /**
   * Lifecycle hooks for request processing.
   * These hooks are called at various stages of the request lifecycle.
   */
  hooks?: RequestHooks
}

export interface RequestOptions {
  method?: string
  query?: Record<string, string | number | bigint>
  headers?: HeadersInit
  retryAttempts?: number
  retrySchedule?: number[]
  httpTimeout?: number
  bodyTimeout?: number
  abort?: AbortSignal
  stream?: boolean
  keepalive?: boolean
  /**
   * Request-specific hooks that combine with the default hooks from HttpClientOptions.
   * Both hooks will execute: client-level hook first, then request-level hook.
   */
  hooks?: RequestHooks
}

export interface FetchRequest extends RequestInit {
  id: number
  url: string
  headers: Headers
  timeout?: number
  bodyTimeout?: number
  signal?: AbortSignal
  stream?: boolean
  hooks?: RequestHooks
}

export interface BaseHttpClient {
  request<T>(url: string, options?: RequestOptions & HttpBody): Promise<HttpResponse<T>>
}

const statusTexts: Record<number, string> = {
  429: 'rate limit exceeded',
  500: 'internal server error',
  502: 'bad gateway',
  503: 'service unavailable',
  504: 'gateway timeout',
  521: 'web server is down',
  522: 'connection timed out',
  523: 'origin is unreachable',
  524: 'timeout occurred',
  529: 'server is overloaded',
}

const MAX_LOG_BODY_SIZE_BYTES = 1024 * 1024 // 1MB

export class HttpClient implements BaseHttpClient {
  protected logger?: Logger
  protected headers?: Record<string, string | number | bigint>
  private baseUrl?: string
  private requestCounter = 0

  private readonly retrySchedule: number[]
  private readonly retryAttempts: number
  private readonly httpTimeout: number
  private readonly bodyTimeout?: number
  private readonly keepalive?: boolean
  private readonly hooks?: RequestHooks

  constructor(options: HttpClientOptions = {}) {
    this.logger = options.logger?.child({ module: 'http-client' })
    this.headers = options.headers
    this.setBaseUrl(options.baseUrl)
    this.retrySchedule = options.retrySchedule || [10, 100, 500, 2000, 10000, 20000]
    this.retryAttempts = options.retryAttempts || 0
    this.httpTimeout = options.httpTimeout ?? 20000
    this.bodyTimeout = options.bodyTimeout
    this.keepalive = options.keepalive ?? true
    this.hooks = options.hooks
  }

  async get<T = any>(url: string, options?: Omit<RequestOptions, 'method'>): Promise<T> {
    const res = await this.request(url, { ...options, method: 'get' })
    return res.body
  }

  async post<T = any>(url: string, options?: Omit<RequestOptions, 'method'> & HttpBody): Promise<T> {
    const res = await this.request(url, { ...options, method: 'post' })
    return res.body
  }

  async request<T = any>(url: string, options: RequestOptions & HttpBody = {}): Promise<HttpResponse<T>> {
    let req = await this.prepareRequest(url, options)

    await this.beforeRequest(req)

    let retryAttempts = options.retryAttempts ?? this.retryAttempts
    let retrySchedule = options.retrySchedule ?? this.retrySchedule
    let retries = 0

    while (true) {
      let res: HttpResponse | Error = await this.performRequestWithTimeout(req).catch(ensureError)
      if (res instanceof Error || !res.ok) {
        if (retryAttempts > retries && isRetryableError(res, req)) {
          let pause = asRetryAfterPause(res)
          if (pause == null) {
            pause = retrySchedule[Math.min(retries, retrySchedule.length - 1)] ?? 1000
          }
          retries += 1
          await this.beforeRetryPause(req, res, pause, retries)
          await wait(pause, req.signal)
        } else if (res instanceof Error) {
          await req.hooks?.onError?.(req, res)
          throw addErrorContext(res, { httpRequestId: req.id })
        } else {
          const error = new HttpError(res)
          await req.hooks?.onError?.(req, error)
          throw error
        }
      } else {
        await req.hooks?.onSuccess?.(req, res)
        return res
      }
    }
  }

  protected async beforeRequest(req: FetchRequest): Promise<void> {
    this.logger?.debug(
      {
        request_id: req.id,
        http_request: {
          url: req.url,
          method: req.method,
          body: req.body,
        },
      },
      'http request started',
    )

    await req.hooks?.onBeforeRequest?.(req)
  }

  protected async beforeRetryPause(
    req: FetchRequest,
    reason: Error | HttpResponse,
    pause: number,
    attempt: number,
  ): Promise<void> {
    let info: any = {
      retry_reason:
        reason instanceof Error
          ? describeError(reason)
          : `got ${reason.status} response status${statusTexts[reason.status] ? `: ${statusTexts[reason.status]}` : ''}`,
      request_id: req.id,
      http_request: {
        url: req.url,
        method: req.method,
        body: req.body,
      },
    }

    if (!(reason instanceof Error)) {
      info.http_response = {
        status: reason.status,
        body: reason.body,
      }
    }

    this.logger?.warn(info, `HTTP request id:${req.id} will be retried in ${pause} ms`)

    await req.hooks?.onBeforeRetry?.(req, reason, pause, attempt)
  }

  protected async afterResponseHeaders(req: FetchRequest, res: Response): Promise<void> {
    this.logger?.debug(
      {
        request_id: req.id,
        http_response: {
          url: res.url,
          status: res.status,
          headers: Object.fromEntries(res.headers),
        },
      },
      'http response headers received',
    )

    await req.hooks?.onAfterResponseHeaders?.(req, res)
  }

  protected async afterResponse(req: FetchRequest, res: HttpResponse): Promise<void> {
    if (!res.stream && this.logger?.isLevelEnabled('debug')) {
      let httpResponseBody: any = res.body
      if (
        (typeof res.body === 'string' || res.body instanceof Uint8Array) &&
        res.body.length > MAX_LOG_BODY_SIZE_BYTES
      ) {
        // Truncate response body logging if it exceeds 1MB to prevent excessive memory usage and log file size growth.
        // The full response body is still available in res.body
        httpResponseBody = `...response body truncated: size ${(res.body.length / (1024 * 1024)).toFixed(2)}MB exceeds 1MB limit...`
      }

      this.logger.debug(
        {
          request_id: req.id,
          http_response: {
            body: httpResponseBody,
          },
        },
        'http response finished',
      )
    }

    await req.hooks?.onAfterResponse?.(req, res)
  }

  protected async prepareRequest(url: string, options: RequestOptions & HttpBody): Promise<FetchRequest> {
    let req: FetchRequest = {
      id: ++this.requestCounter,
      method: options.method,
      headers: new Headers(options.headers),
      url: this.getAbsUrl(url),
      signal: options.abort,
      timeout: options.httpTimeout ?? this.httpTimeout,
      bodyTimeout: options.bodyTimeout ?? this.bodyTimeout,
      stream: options.stream,
      keepalive: options.keepalive ?? this.keepalive,
    }

    this.handleBasicAuth(req)

    if (options.query) {
      let qs = new URLSearchParams(options.query as any).toString()
      if (req.url.includes('?')) {
        req.url += `&${qs}`
      } else {
        req.url += `?${qs}`
      }
    }

    // If Node.js version >= 24.4.0, include 'zstd' in the Accept-Encoding header
    // to leverage built-in zstd support for better compression.
    if (isNodeVersionGreaterOrEqual(24, 4, 0)) {
      req.headers.set('accept-encoding', 'gzip, zstd')
    }

    if (!req.headers.has('user-agent')) {
      req.headers.set('user-agent', USER_AGENT)
    }

    if (options.content !== undefined) {
      if (typeof options.content === 'string') {
        req.body = options.content
        if (!req.headers.has('content-type')) {
          req.headers.set('content-type', 'text/plain')
        }
      } else {
        req.body = options.content
      }
    }

    if (options.json !== undefined) {
      req.body = JSON.stringify(options.json)
      if (!req.headers.has('content-type')) {
        req.headers.set('content-type', 'application/json')
      }
    }

    for (let name in this.headers) {
      if (!req.headers.has(name)) {
        req.headers.set(name, `${this.headers[name]}`)
      }
    }

    req.hooks = this.mergeHooks(this.hooks, options.hooks)

    return req
  }

  private mergeHooks(clientHooks?: RequestHooks, requestHooks?: RequestHooks): RequestHooks | undefined {
    if (!clientHooks && !requestHooks) return undefined
    if (!clientHooks) return requestHooks
    if (!requestHooks) return clientHooks

    return {
      onBeforeRequest: this.combineHook(clientHooks.onBeforeRequest, requestHooks.onBeforeRequest),
      onAfterResponseHeaders: this.combineHook(clientHooks.onAfterResponseHeaders, requestHooks.onAfterResponseHeaders),
      onAfterResponse: this.combineHook(clientHooks.onAfterResponse, requestHooks.onAfterResponse),
      onBeforeRetry: this.combineHook(clientHooks.onBeforeRetry, requestHooks.onBeforeRetry),
      onError: this.combineHook(clientHooks.onError, requestHooks.onError),
      onSuccess: this.combineHook(clientHooks.onSuccess, requestHooks.onSuccess),
    }
  }

  private combineHook<T extends (...args: any[]) => void | Promise<void>>(a?: T, b?: T): T | undefined {
    if (!a && !b) return undefined
    if (!a) return b
    if (!b) return a

    return (async (...args: any[]) => {
      await a(...args)
      await b(...args)
    }) as T
  }

  private handleBasicAuth(req: FetchRequest): void {
    let u = new URL(req.url)
    if (u.username || u.password) {
      req.headers.set('Authorization', `Basic ${btoa(`${u.username}:${u.password}`)}`)
      u.username = ''
      u.password = ''
      req.url = u.toString()
    }
  }

  private async performRequestWithTimeout(req: FetchRequest): Promise<HttpResponse> {
    if (!req.timeout) return this.performRequest(req)

    let ac = new AbortController()

    function abort() {
      ac.abort()
    }

    req.signal?.addEventListener('abort', abort)

    let timer: any | null = setTimeout(() => {
      timer = null
      abort()
    }, req.timeout)

    let res: HttpResponse | undefined
    try {
      res = await this.performRequest({ ...req, signal: ac.signal })
      return res
    } catch (err: any) {
      if (timer == null) {
        throw new HttpTimeoutError(req.timeout)
      }
      throw err
    } finally {
      clearTimeout(timer)
      req.signal?.removeEventListener('abort', abort)
    }
  }

  private async performRequest(req: FetchRequest): Promise<HttpResponse> {
    let res = await fetch(req.url, req)
    await this.afterResponseHeaders(req, res)
    let body = await this.handleResponseBody(req, res)
    let httpResponse = new HttpResponse(req.id, res.url, res.status, res.headers, body, body instanceof ReadableStream)
    await this.afterResponse(req, httpResponse)
    return httpResponse
  }

  protected async handleResponseBody(req: FetchRequest, res: Response): Promise<any> {
    let contentType = (res.headers.get('content-type') || '').split(';')[0]

    if (req.bodyTimeout != null && res.body != null) {
      let body = addStreamTimeout(res.body, req.bodyTimeout, () => new HttpBodyTimeoutError(req.bodyTimeout ?? 0))
      res = new Response(body, res)
    }

    if (req.stream && res.ok && res.body != null) {
      return res.body
    }

    if (contentType === 'application/json') {
      return res.json()
    }

    if (contentType?.startsWith('text/')) {
      return res.text()
    }

    let arrayBuffer = await res.arrayBuffer()
    if (arrayBuffer.byteLength === 0) return undefined
    return arrayBuffer
  }

  private getQueryUrlAndAuth(url: string): { url: string; basic?: string } {
    let u = new URL(this.getAbsUrl(url))
    if (u.username || u.password) {
      let basic = btoa(`${u.username}:${u.password}`)
      u.username = ''
      u.password = ''
      return { url: u.toString(), basic }
    }

    return { url: u.toString() }
  }

  getAbsUrl(url: string): string {
    if (!this.baseUrl) return url
    if (url.includes('://')) return url
    if (url === '/') return this.baseUrl
    if (url[0] === '/') return this.baseUrl + url
    return `${this.baseUrl}/${url}`
  }

  private setBaseUrl(url?: string): void {
    if (url) {
      let u = new URL(url)
      u.hash = ''
      u.search = ''
      url = u.toString()
      if (url.endsWith('/')) {
        url = url.slice(0, url.length - 1)
      }
      this.baseUrl = url
    } else {
      this.baseUrl = undefined
    }
  }
}

export class HttpResponse<T = any> {
  constructor(
    public readonly requestId: number,
    public readonly url: string,
    public readonly status: number,
    public readonly headers: Headers,
    public readonly body: T,
    public readonly stream: boolean,
  ) {}

  get ok(): boolean {
    return this.status >= 200 && this.status < 300
  }

  assert(): void {
    if (this.ok) return
    throw new HttpError(this)
  }

  toJSON() {
    return {
      status: this.status,
      headers: Array.from(this.headers),
      body: this.stream ? undefined : this.body,
      url: this.url,
    }
  }
}

export class HttpError extends Error {
  constructor(public readonly response: HttpResponse) {
    super(`Got ${response.status} from ${response.url}`)
  }

  override get name(): string {
    return 'HttpError'
  }
}

export class HttpTimeoutError extends Error {
  constructor(public readonly ms: number) {
    super(`request timed out after ${ms} ms`)
  }

  override get name(): string {
    return 'HttpTimeoutError'
  }
}

export class HttpBodyTimeoutError extends Error {
  constructor(ms: number) {
    super(`request body timed out after ${ms} ms`)
  }

  override get name(): string {
    return 'HttpBodyTimeoutError'
  }
}

export function describeError(error: Error): string {
  const parts = [error.toString()]

  let cause: unknown = (error as { cause?: unknown }).cause
  let depth = 0
  while (cause && depth < 5) {
    const c = cause as { code?: string; name?: string; message?: string; errors?: unknown[]; cause?: unknown }
    parts.push(`cause: ${c.code ?? c.name ?? c.message ?? String(cause)}`)

    if (cause instanceof AggregateError) {
      const inner = cause.errors.map((e: any) => e?.code ?? e?.message ?? String(e)).join(', ')
      parts.push(`errors: [${inner}]`)
    }

    cause = c.cause
    depth += 1
  }

  return parts.join(' | ')
}

function isRetryableError(error: HttpResponse | Error, req?: FetchRequest): boolean {
  if (isHttpConnectionError(error)) return true
  if (error instanceof HttpTimeoutError) return true
  if (error instanceof HttpError) {
    error = error.response
  }
  if (error instanceof HttpResponse) {
    switch (error.status) {
      case 429:
      case 502:
      case 503:
      case 504:
      case 521:
      case 522:
      case 523:
      case 524:
      case 529:
        return true
      default:
        return error.headers.has('retry-after')
    }
  }
  return false
}

export function asRetryAfterPause(res: HttpResponse | Error): number | undefined {
  if (res instanceof HttpError) {
    res = res.response
  }
  if (res instanceof HttpResponse) {
    let retryAfter = res.headers.get('retry-after')
    if (retryAfter == null) return undefined
    if (/^\d+$/.test(retryAfter)) return Number.parseInt(retryAfter, 10) * 1000
    if (HTTP_DATE_REGEX.test(retryAfter)) return Math.max(0, new Date(retryAfter).getTime() - Date.now())
  }

  return undefined
}

const HTTP_DATE_REGEX =
  /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), (\d{2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$/

// ref: https://github.com/sindresorhus/is-network-error/blob/main/index.js
const errorMessages = new Set([
  'network error', // Chrome
  'failed to fetch', // Chrome
  'networkerror when attempting to fetch resource.', // Firefox
  'the internet connection appears to be offline.', // Safari 16
  'load failed', // Safari 17+
  'network request failed', // `cross-fetch`
  'fetch failed', // Undici (Node.js)
  'terminated', // Undici (Node.js)
])

export function isHttpConnectionError(error: unknown) {
  if (error instanceof TypeError) {
    if (error.message === 'Load failed') {
      return error.stack === undefined
    }
    return errorMessages.has(error.message.toLowerCase())
  }
  return false
}

function addStreamTimeout<T>(
  stream: ReadableStream<T>,
  ms: number,
  onTimeout?: () => Error | undefined | void,
): ReadableStream<T> {
  if (!ms) return stream

  let reader = stream.getReader()
  return new ReadableStream({
    pull: async (c) => {
      try {
        let data = await addTimeout(reader.read(), ms, onTimeout)

        if (data.done) {
          c.close()
        } else {
          c.enqueue(data.value)
        }
      } catch (e) {
        c.error(e)
        await reader.cancel()
      }
    },
    cancel: async (reason) => {
      await reader.cancel(reason)
    },
  })
}
