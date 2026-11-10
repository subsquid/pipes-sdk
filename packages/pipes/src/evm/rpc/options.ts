/**
 * Plain configuration shapes for an RPC source.
 *
 * They live apart from the client that consumes them on purpose: the client imports
 * `@subsquid/evm-rpc`, and these types are re-exported from the public `./evm` barrel. Declaring
 * them beside the client would drag that optional peer into the barrel's *type* graph, so a
 * Portal-only consumer could not typecheck without installing an RPC stack it never uses. Nothing
 * here may import an optional peer.
 */

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
