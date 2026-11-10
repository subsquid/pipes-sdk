import type { ChainBlock, PortalChain } from './portal-chain.js'

/**
 * The JSON-RPC face of a mock portal: an EVM node over the same chain, so a fork, a head move or a
 * fault reaches a portal reader and an RPC reader alike. It answers what an RPC source reads for a
 * logs query: the chain id, the height, blocks by number or tag, and logs by block range.
 */

export type RpcRequest = { jsonrpc?: string; id?: number | string | null; method: string; params?: unknown[] }

export type RpcError = { code: number; message: string }

/** A JSON-RPC call the mock portal received, and what it answered. */
export type MockRpcCall = {
  method: string
  params: unknown[]
  /** Set when the call was answered; a fault or a stall leaves both unset. */
  result?: unknown
  error?: RpcError
  /** The fault the HTTP request carrying this call got instead of an answer. */
  fault?: string
}

const ZERO_HASH = `0x${'0'.repeat(64)}`
const ZERO_ADDRESS = `0x${'0'.repeat(40)}`
const EMPTY_BLOOM = `0x${'0'.repeat(512)}`
const METHOD_NOT_FOUND = -32601
const INVALID_PARAMS = -32602

type PortalTransaction = { transactionIndex: number; hash: string; from?: string; to?: string; sighash?: string }
type PortalLog = {
  logIndex: number
  transactionIndex: number
  transactionHash: string
  address: string
  topics: string[]
  data: string
}

function toQty(n: number): string {
  return `0x${n.toString(16)}`
}

/** A block number from a quantity or a tag; `undefined` for a tag the chain cannot answer. */
function blockNumberOf(chain: PortalChain, tag: unknown): number | undefined {
  if (tag === 'latest' || tag === 'pending') {
    return chain.latest
  }

  // A dataset without finality treats every block as final, as the portal does (DEF-15).
  if (tag === 'finalized' || tag === 'safe') {
    return chain.finalized ?? chain.latest
  }

  if (tag === 'earliest') {
    return 0
  }

  if (typeof tag === 'string' && /^0x[0-9a-f]+$/i.test(tag)) {
    return Number.parseInt(tag, 16)
  }

  return undefined
}

function transactionsOf(block: ChainBlock): PortalTransaction[] {
  return (block['transactions'] as PortalTransaction[] | undefined) ?? []
}

function logsOf(block: ChainBlock): PortalLog[] {
  return (block['logs'] as PortalLog[] | undefined) ?? []
}

function rpcTransaction(tx: PortalTransaction, number: number, blockHash: string) {
  return {
    hash: tx.hash,
    blockHash,
    blockNumber: toQty(number),
    transactionIndex: toQty(tx.transactionIndex),
    from: tx.from ?? ZERO_ADDRESS,
    to: tx.to ?? null,
    value: '0x0',
    gas: '0x5208',
    gasPrice: '0x0',
    input: tx.sighash ?? '0x',
    nonce: toQty(tx.transactionIndex),
  }
}

/**
 * A block as `eth_getBlockByNumber` returns it. The parent hash comes from the chain, not the given
 * header, so a branch links the way the chain holds it. Roots and the bloom are zero: the RPC
 * source verifies none of them unless told to.
 */
function rpcBlock(chain: PortalChain, number: number, fullTransactions: boolean) {
  const block = chain.block(number)
  const hash = chain.hash(number)
  const transactions = transactionsOf(block)

  return {
    number: toQty(number),
    hash,
    parentHash: number === 0 ? ZERO_HASH : chain.hash(number - 1),
    timestamp: toQty(chain.header(number).timestamp ?? 0),
    nonce: '0x0000000000000000',
    difficulty: '0x0',
    gasLimit: '0x1c9c380',
    gasUsed: '0x0',
    miner: ZERO_ADDRESS,
    mixHash: ZERO_HASH,
    size: '0x0',
    sha3Uncles: ZERO_HASH,
    stateRoot: ZERO_HASH,
    transactionsRoot: ZERO_HASH,
    receiptsRoot: ZERO_HASH,
    logsBloom: EMPTY_BLOOM,
    extraData: '0x',
    transactions: fullTransactions
      ? transactions.map((tx) => rpcTransaction(tx, number, hash))
      : transactions.map((tx) => tx.hash),
    uncles: [],
  }
}

type LogFilter = { fromBlock?: unknown; toBlock?: unknown; address?: string | string[]; topics?: unknown[] }

function matchesFilter(log: PortalLog, filter: LogFilter): boolean {
  const addresses = filter.address === undefined ? [] : [filter.address].flat().map((a) => a.toLowerCase())
  if (addresses.length > 0 && !addresses.includes(log.address.toLowerCase())) {
    return false
  }

  const topics = filter.topics ?? []

  return topics.every((wanted, position) => {
    if (wanted == null) {
      return true
    }

    const options = [wanted].flat().map((topic) => String(topic).toLowerCase())

    return options.includes(log.topics[position]?.toLowerCase())
  })
}

/** `eth_getLogs` over the chain as it is now, clamped to the head. */
function rpcLogs(chain: PortalChain, filter: LogFilter) {
  const from = blockNumberOf(chain, filter.fromBlock ?? 'latest')
  const to = Math.min(blockNumberOf(chain, filter.toBlock ?? 'latest') ?? -1, chain.latest)
  if (from === undefined) {
    return []
  }

  const logs = []
  for (let number = from; number <= to; number++) {
    const blockHash = chain.hash(number)
    for (const log of logsOf(chain.block(number))) {
      if (matchesFilter(log, filter)) {
        logs.push({
          address: log.address,
          topics: log.topics,
          data: log.data,
          blockNumber: toQty(number),
          blockHash,
          transactionHash: log.transactionHash,
          transactionIndex: toQty(log.transactionIndex),
          logIndex: toQty(log.logIndex),
          removed: false,
        })
      }
    }
  }

  return logs
}

/** Answers one JSON-RPC call from the chain as it is at this moment. */
export function answerRpc(chain: PortalChain, call: RpcRequest): { result: unknown } | { error: RpcError } {
  const params = call.params ?? []

  switch (call.method) {
    case 'eth_chainId':
      return { result: '0x1' }

    case 'eth_blockNumber':
      return { result: toQty(chain.latest) }

    case 'eth_getBlockByNumber': {
      const number = blockNumberOf(chain, params[0])
      if (number === undefined) {
        return { error: { code: INVALID_PARAMS, message: `unknown block tag ${JSON.stringify(params[0])}` } }
      }

      const beyondHead = number > chain.latest

      return { result: beyondHead ? null : rpcBlock(chain, number, params[1] === true) }
    }

    case 'eth_getLogs':
      return { result: rpcLogs(chain, (params[0] ?? {}) as LogFilter) }

    default:
      return { error: { code: METHOD_NOT_FOUND, message: `the method ${call.method} does not exist` } }
  }
}
