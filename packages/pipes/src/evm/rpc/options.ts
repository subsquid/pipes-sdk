// Apart from the client on purpose: the public barrel re-exports these types, so nothing here may
// import an optional peer, or a Portal-only consumer could not typecheck.

/** RPC method-selection toggles (the per-chain "C1" config) merged into the coarse fetch request. */
export interface RpcMethodOptions {
  useTraceApi?: boolean
  useDebugTraceBlockByNumber?: boolean
  useDebugApiForStateDiffs?: boolean
  debugTraceTimeout?: string
}

/** Connection settings for the JSON-RPC endpoint. */
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
