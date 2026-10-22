import { IncomingMessage, Server, ServerResponse, createServer } from 'http'

import type { MockPortal } from '../test-portal.js'
import type { PortalBlock } from './mock-block.js'

type LogFilter = {
  address?: string[]
  topic0?: string[]
  topic1?: string[]
  topic2?: string[]
  topic3?: string[]
  transaction?: boolean
}

export type ChainPortalRequest = {
  endpoint: 'stream' | 'finalized-stream'
  query: {
    fromBlock: number
    toBlock?: number
    parentBlockHash?: string
    logs?: LogFilter[]
    includeAllBlocks?: boolean
    [key: string]: unknown
  }
  status: number
}

export type MockEvmChainPortal = MockPortal & {
  /** Every stream request, in the order the portal received them */
  requests: ChainPortalRequest[]
  setHead(head: { finalized?: number | null; latest?: number }): void
}

export type MockEvmChainPortalOptions = {
  /** Blocks with data. Every other block number up to the head exists with an empty body. */
  blocks: PortalBlock[]
  /** The finalized head. `null` makes the dataset report none. Defaults to the latest head. */
  finalized?: number | null
  /** Defaults to the last block in `blocks` */
  latest?: number
  /** The most blocks one response covers, like a worker chunk. Defaults to the whole request. */
  blocksPerResponse?: number
  /** Answers 400 to a query whose log requests carry more addresses than this in total */
  maxAddresses?: number
  /** Called before a stream request is answered, to hold it back */
  delay?: (request: Omit<ChainPortalRequest, 'status'>) => Promise<void> | void
}

/**
 * A portal that answers stream queries from a chain the way the real one does, instead of
 * replaying canned responses: it filters logs by address and topics, includes the transactions
 * a request asks for, and sends the first and last block of every range it covers.
 *
 * Use it where the queries a pipe sends depend on timing or on data, so their order cannot be
 * scripted in advance.
 *
 * @example
 * ```ts
 * const portal = await mockEvmChainPortal({
 *   blocks: [mockBlock({ number: 10, transactions: [{ logs: [event] }] })],
 *   finalized: 5,
 *   latest: 20,
 * })
 * ```
 */
export async function mockEvmChainPortal({
  blocks,
  finalized,
  latest,
  blocksPerResponse = Number.POSITIVE_INFINITY,
  maxAddresses,
  delay,
}: MockEvmChainPortalOptions): Promise<MockEvmChainPortal> {
  const byNumber = new Map(blocks.map((block) => [block.header.number, block]))
  const requests: ChainPortalRequest[] = []

  let latestHead = latest ?? Math.max(0, ...blocks.map((b) => b.header.number))
  let finalizedHead = finalized === undefined ? latestHead : finalized

  const header = (number: number) => byNumber.get(number)?.header ?? emptyHeader(number)

  const headHeaders = () => {
    const headers: Record<string, string | number> = { 'X-Sqd-Head-Number': latestHead }

    if (finalizedHead !== null) {
      headers['X-Sqd-Finalized-Head-Number'] = finalizedHead
      headers['X-Sqd-Finalized-Head-Hash'] = header(finalizedHead).hash
    }

    return headers
  }

  const handleStream = async (endpoint: ChainPortalRequest['endpoint'], body: string, res: ServerResponse) => {
    const query = JSON.parse(body) as ChainPortalRequest['query']
    await delay?.({ endpoint, query })

    const answer = (status: number, extraHeaders: Record<string, string | number> = {}) => {
      requests.push({ endpoint, query, status })
      res.writeHead(status, { ...headHeaders(), ...extraHeaders })
    }

    const addressCount = (query.logs ?? []).reduce((sum, request) => sum + (request.address?.length ?? 0), 0)
    if (maxAddresses !== undefined && addressCount > maxAddresses) {
      answer(400)
      res.end(`the query carries ${addressCount} addresses, more than ${maxAddresses}`)

      return
    }

    const available = endpoint === 'finalized-stream' ? finalizedHead : latestHead
    if (available === null || query.fromBlock > available) {
      answer(204)
      res.end()

      return
    }

    if (query.parentBlockHash != null && query.fromBlock > 0) {
      const parent = header(query.fromBlock - 1)

      if (parent.hash !== query.parentBlockHash) {
        answer(409, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ previousBlocks: [{ number: parent.number, hash: parent.hash }] }))

        return
      }
    }

    const last = Math.min(query.toBlock ?? Number.POSITIVE_INFINITY, available, query.fromBlock + blocksPerResponse - 1)

    answer(200, { 'Content-Type': 'application/jsonl' })

    for (let number = query.fromBlock; number <= last; number++) {
      const boundary = number === query.fromBlock || number === last
      const selected = select(byNumber.get(number), query)

      if (selected || boundary || query.includeAllBlocks) {
        res.write(JSON.stringify({ header: header(number), ...selected }) + '\n')
      }
    }

    res.end()
  }

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let body = ''
    req.on('data', (chunk) => {
      body += chunk
    })

    req.on('end', () => {
      const url = req.url ?? ''

      if (url.startsWith('/metadata')) {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(
          JSON.stringify({
            dataset: 'mock-chain',
            aliases: [],
            real_time: true,
            start_block: 0,
            metadata: { kind: 'evm' },
          }),
        )
      } else if (url === '/head') {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ number: latestHead, hash: header(latestHead).hash }))
      } else if (url === '/finalized-head') {
        if (finalizedHead === null) {
          res.writeHead(404)
          res.end()

          return
        }

        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ number: finalizedHead, hash: header(finalizedHead).hash }))
      } else if (url === '/stream' || url === '/finalized-stream') {
        handleStream(url === '/stream' ? 'stream' : 'finalized-stream', body, res).catch((error) => {
          res.writeHead(500)
          res.end(String(error))
        })
      } else {
        res.writeHead(404)
        res.end()
      }
    })
  })

  await new Promise<void>((resolve, reject) => {
    server.on('error', reject)
    server.listen(0, () => resolve())
  })

  const address = server.address()
  if (!address || typeof address === 'string') {
    throw new Error('Invalid server address')
  }

  return {
    server,
    url: `http://127.0.0.1:${address.port}`,
    requests,
    setHead(head) {
      if (head.latest !== undefined) latestHead = head.latest
      if (head.finalized !== undefined) finalizedHead = head.finalized
    },
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.closeAllConnections()
        server.close((err) => (err ? reject(err) : resolve()))
      }),
  }
}

function emptyHeader(number: number): PortalBlock['header'] {
  return {
    number,
    hash: `0x${number.toString(16).padStart(64, '0')}`,
    parentHash: `0x${Math.max(number - 1, 0)
      .toString(16)
      .padStart(64, '0')}`,
    timestamp: number * 1000,
  }
}

function matches(values: string[] | undefined, value: string | undefined): boolean {
  if (values === undefined) return true
  if (value === undefined) return false

  return values.some((v) => v.toLowerCase() === value.toLowerCase())
}

/** The items of a block the query selects, or `undefined` when it selects none. */
function select(block: PortalBlock | undefined, query: ChainPortalRequest['query']) {
  if (!block || !query.logs?.length) return undefined

  const logs: PortalBlock['logs'] = []
  const transactionIndexes = new Set<number>()

  for (const log of block.logs) {
    const requests = query.logs.filter(
      (r) =>
        matches(r.address, log.address) &&
        matches(r.topic0, log.topics[0]) &&
        matches(r.topic1, log.topics[1]) &&
        matches(r.topic2, log.topics[2]) &&
        matches(r.topic3, log.topics[3]),
    )
    if (requests.length === 0) continue

    logs.push(log)

    if (requests.some((r) => r.transaction)) {
      transactionIndexes.add(log.transactionIndex)
    }
  }

  if (logs.length === 0) return undefined

  const transactions = block.transactions.filter((tx) => transactionIndexes.has(tx.transactionIndex))

  return transactions.length > 0 ? { logs, transactions } : { logs }
}
