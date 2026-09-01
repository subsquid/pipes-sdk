import { PollingClient, RpcLatencyWatcher, rpcLatencyWatcher } from '~/monitoring/index.js'

import { bitcoinQuery } from './bitcoin-query-builder.js'

export type { LatencySample, RpcObservation } from '~/monitoring/index.js'

const DEFAULT_INTERVAL_MS = 4_000

type BlockHeader = {
  hash: string
  height: number
  // unix seconds
  time: number
}

type Endpoint = {
  url: string
  authHeader?: string
}

/**
 * `bitcoind`'s JSON-RPC interface uses HTTP Basic auth. Node's `fetch`
 * does NOT honor `user:pass@host` URL credentials (that is browser-only
 * behavior), so we extract them and emit an `Authorization` header.
 */
function parseEndpoint(raw: string): Endpoint {
  const u = new URL(raw)
  if (!u.username && !u.password) {
    return { url: raw }
  }
  const credentials = `${decodeURIComponent(u.username)}:${decodeURIComponent(u.password)}`
  u.username = ''
  u.password = ''
  return {
    url: u.toString(),
    authHeader: `Basic ${Buffer.from(credentials).toString('base64')}`,
  }
}

export class BitcoinRpcLatencyWatcher extends RpcLatencyWatcher {
  /**
   * Maps the **sanitized** URL (no `user:pass@`) — the value passed to
   * `super()` and used as the key for the base class's `nodes` map — to the
   * full `Endpoint` (URL + Authorization header). Keying by the sanitized URL
   * ensures credentials never appear in `lookup()` output, the resulting
   * `Latency.rpc[].url`, log lines, or downstream metrics.
   */
  readonly #endpointsByUrl: Map<string, Endpoint>
  readonly #intervalMs: number
  readonly #requestTimeoutMs: number

  constructor(
    rpcUrl: string | string[],
    intervalMs: number = DEFAULT_INTERVAL_MS,
    requestTimeoutMs: number = Math.max(1_000, intervalMs),
  ) {
    const inputs = (Array.isArray(rpcUrl) ? rpcUrl : [rpcUrl]).map(parseEndpoint)
    super(inputs.map((e) => e.url))
    // Subscribing is deferred to `start()`, so these fields are initialized
    // BEFORE any tick fires.
    this.#endpointsByUrl = new Map(inputs.map((e) => [e.url, e]))
    this.#intervalMs = intervalMs
    this.#requestTimeoutMs = requestTimeoutMs
  }

  watch(url: string) {
    let lastHash: string | undefined
    const endpoint = this.#endpointsByUrl.get(url) ?? { url }

    return new PollingClient(this.#intervalMs, async () => {
      const hash = await this.rpcCall<string>(endpoint, 'getbestblockhash', [])
      if (!hash || hash === lastHash) return
      lastHash = hash

      const header = await this.rpcCall<BlockHeader>(endpoint, 'getblockheader', [hash, true])
      if (!header) return

      // `url` here is already the sanitized form (the key the base class stored
      // when iterating `this.rpcUrl`).
      this.addBlock(url, {
        number: header.height,
        hash,
        timestamp: new Date(header.time * 1000),
        receivedAt: new Date(),
      })
    })
  }

  private async rpcCall<T>(endpoint: Endpoint, method: string, params: unknown[]): Promise<T | undefined> {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' }
    if (endpoint.authHeader) headers['Authorization'] = endpoint.authHeader

    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), this.#requestTimeoutMs)

    try {
      const res = await fetch(endpoint.url, {
        method: 'POST',
        headers,
        body: JSON.stringify({ jsonrpc: '1.0', id: 'pipes', method, params }),
        signal: controller.signal,
      })
      if (!res.ok) return undefined

      const json = (await res.json()) as { result?: T; error?: unknown }
      if (json.error) return undefined
      return json.result
    } finally {
      clearTimeout(timer)
    }
  }
}

export function bitcoinRpcLatencyWatcher({
  rpcUrl,
  intervalMs = DEFAULT_INTERVAL_MS,
  requestTimeoutMs,
  resolveTimeoutMs,
}: {
  rpcUrl: string[]
  intervalMs?: number
  /** Per-RPC request timeout. Defaults to `max(1000, intervalMs)` so a stalled
   *  endpoint can never block the polling loop indefinitely. */
  requestTimeoutMs?: number
  /** How long to wait for the node to report a head before recording it as `rpc-behind`. */
  resolveTimeoutMs?: number
}) {
  const transformer = rpcLatencyWatcher({
    watcher: new BitcoinRpcLatencyWatcher(rpcUrl, intervalMs, requestTimeoutMs),
    resolveTimeoutMs,
  })

  return bitcoinQuery()
    .addRange({
      from: 'latest',
    })
    .addFields({
      block: {
        number: true,
        timestamp: true,
      },
    })
    .build()
    .pipe(transformer.options)
}
