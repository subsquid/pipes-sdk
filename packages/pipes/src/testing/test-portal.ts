import { IncomingMessage, Server, ServerResponse, createServer } from 'http'

import {
  type ChainAnswer,
  type ChainBlock,
  type ChainFault,
  type ChainRequest,
  PortalChain,
  type StreamEndpoint,
} from './portal-chain.js'
import {
  LOOPBACK,
  WireHead,
  getServerAddress,
  headHeaders,
  listen,
  readBody,
  writeBlocks,
  writeMetadata,
} from './portal-wire.js'

type ValidateRequest = (res: any) => unknown

export type MockResponse =
  | {
      statusCode: 204
      validateRequest?: ValidateRequest
    }
  | {
      statusCode: 200
      data: {
        header: {
          number: number
          hash: string
          timestamp?: number
        }
        logs?: any[]
        instructions?: any[]
        transactions?: any[]
        inputs?: any[]
        outputs?: any[]
        internalTransactions?: any[]
      }[]
      head?: {
        finalized?: { number: number; hash: string }
        latest?: { number: number }
      }
      validateRequest?: ValidateRequest
    }
  | {
      statusCode: 409
      /** The body verbatim: since ADR-011 `error` sits beside `previousBlocks`, or stands alone. */
      data: {
        previousBlocks?: {
          number: number
          hash: string
        }[]
        error?: {
          type: string
          code: string
          message: string
        }
      }
      validateRequest?: ValidateRequest
    }
  | {
      statusCode: 500 | 503 | 529
      validateRequest?: ValidateRequest
    }

export type MockPortal = {
  server: Server
  url: string
  close(): Promise<void>
}

/** A block of the mock chain: a header plus the item lists of its network (`logs`, `instructions`, …). */
export type MockPortalBlock = ChainBlock

/** A stream request the mock portal received, with what it answered and the head it reported. */
export type MockPortalRequest = ChainRequest

/** What the mock portal answered a stream request: blocks, at-head, a fork, a refusal or a fault. */
export type MockPortalAnswer = ChainAnswer

/** A transport failure the mock portal serves instead of the next answers. */
export type MockPortalFault = ChainFault

export type MockPortalOptions = {
  /** Blocks with data. Every other block number up to the head exists as an empty block. */
  blocks?: MockPortalBlock[]
  /** The latest head. Defaults to the highest block in `blocks`. */
  latest?: number
  /** The finalized head. `null` makes the dataset report none. Defaults to the latest head. */
  finalized?: number | null
  /** `metadata.kind` of the dataset. Defaults to `evm`. */
  kind?: string
  /** The most blocks one response covers, like a worker chunk. Defaults to the whole request. */
  blocksPerResponse?: number
  /** How many canonical blocks a 409 carries in `previousBlocks`. Defaults to 10. */
  previousBlocks?: number
  /** Answers 400 to a query whose log requests carry more addresses than this in total. */
  maxAddresses?: number
  /** Called before a stream request is answered, to hold it back. */
  delay?: (request: { endpoint: StreamEndpoint; query: MockPortalRequest['query'] }) => Promise<void> | void
}

export type MockChainPortal = MockPortal & {
  /** Every stream request, in the order the portal received them, with the status it got. */
  requests: MockPortalRequest[]
  /** Extends the chain above the head and moves the head to the last new block. */
  append(...blocks: MockPortalBlock[]): void
  /**
   * Replaces every block above `at` with `branch` and new empty blocks. A request anchored on a
   * replaced block then gets a 409. Blocks in `branch` need hashes of their own.
   */
  fork(at: number, branch?: MockPortalBlock[]): void
  setHead(head: { latest?: number; finalized?: number | null }): void
  /** Answers the next stream requests with a failure instead of data. */
  fail(fault: MockPortalFault): void
}

/**
 * @deprecated Use `mockPortal({ blocks, ... })`: it answers both `/stream` and `/finalized-stream`.
 * For an exact response list, use `mockPortal(responses, { finalized: true })`.
 */
export async function finalizedMockPortal(mockResponses: MockResponse[]) {
  return mockPortal(mockResponses, {
    finalized: true,
  })
}

// The list form comes first and again last, so code written against the list-only signature keeps
// compiling: inference inside an argument (`mockPortal(items.map(...))`) settles on the first
// overload, while `Parameters<typeof mockPortal>` and `ReturnType` read the last one.
/**
 * Starts a local portal.
 *
 * Given options, it answers every request from a chain it holds, the way the real portal does:
 * it filters EVM logs, transactions, traces and state diffs by the query and its relation flags,
 * reports heads, answers 204 at the head and 409 to a parent hash that is not on the chain. A
 * restarted pipe, a fork, or a re-request therefore needs no scripted response.
 *
 * Given a list of responses, it answers stream requests with them in order, and with 500 after the
 * list ends. Use it to send an exact response, such as a malformed one.
 *
 * @example
 * ```ts
 * const portal = await mockPortal({
 *   blocks: [mockBlock({ number: 10, transactions: [{ logs: [event] }] })],
 *   finalized: 5,
 *   latest: 20,
 * })
 *
 * portal.fork(7)
 * portal.append(mockBlock({ number: 21 }))
 * ```
 */
export function mockPortal(mockResponses: MockResponse[], options?: { finalized?: boolean }): Promise<MockPortal>
export function mockPortal(options: MockPortalOptions): Promise<MockChainPortal>
export function mockPortal(mockResponses: MockResponse[], options?: { finalized?: boolean }): Promise<MockPortal>
export function mockPortal(
  input: MockResponse[] | MockPortalOptions,
  options: { finalized?: boolean } = {},
): Promise<MockPortal | MockChainPortal> {
  return Array.isArray(input) ? scriptedPortal(input, options) : chainPortal(input)
}

async function chainPortal(options: MockPortalOptions): Promise<MockChainPortal> {
  const chain = new PortalChain(options)

  const handle = async (req: IncomingMessage, res: ServerResponse) => {
    const body = await readBody(req)
    const url = req.url ?? ''

    if (url.startsWith('/metadata')) {
      writeMetadata(res, options.kind ?? 'evm')

      return
    }

    if (url === '/head' || url === '/finalized-head') {
      const number = url === '/head' ? chain.latest : chain.finalized
      // The client reads a `null` body as "no head"; a 404 would fail the request instead.
      const head = number === null ? null : { number, hash: chain.hash(number) }

      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(head))

      return
    }

    if (url === '/stream' || url === '/finalized-stream') {
      const endpoint: StreamEndpoint = url === '/stream' ? 'stream' : 'finalized-stream'
      const query = JSON.parse(body)

      const request = chain.receive(endpoint, query)
      await options.delay?.({ endpoint, query })
      const answer = chain.answer(request)
      writeAnswer(res, answer, request.head as WireHead)

      return
    }

    res.writeHead(404)
    res.end()
  }

  const server = await listen((req, res) => {
    handle(req, res).catch((error) => {
      res.writeHead(500)
      res.end(String(error))
    })
  })

  return {
    server,
    url: getServerAddress(server),
    requests: chain.requests,
    append: (...blocks) => chain.append(blocks),
    fork: (at, branch) => chain.fork(at, branch),
    setHead: (head) => chain.setHead(head),
    fail: (fault) => chain.fail(fault),
    close: () =>
      new Promise<void>((resolve, reject) => {
        // A held-back request would otherwise keep the server open.
        server.closeAllConnections()
        server.close((err) => (err ? reject(err) : resolve()))
      }),
  }
}

function writeAnswer(res: ServerResponse, answer: ChainAnswer, head: WireHead) {
  switch (answer.kind) {
    case 'blocks':
      writeBlocks(res, answer.blocks, head)
      res.end()
      break

    case 'at-head':
      res.writeHead(204, headHeaders(head))
      res.end()
      break

    case 'fork':
      res.writeHead(409, { 'Content-Type': 'application/json', ...headHeaders(head) })
      res.end(JSON.stringify({ previousBlocks: answer.previousBlocks }))
      break

    case 'refused':
      res.writeHead(400, headHeaders(head))
      res.end(answer.message)
      break

    case 'fault':
      res.writeHead(answer.status, answer.retryAfter != null ? { 'Retry-After': answer.retryAfter } : {})
      res.end()
      break
  }
}

async function scriptedPortal(
  mockResponses: MockResponse[],
  { finalized = false }: { finalized?: boolean },
): Promise<MockPortal> {
  const promise = new Promise<Server>((resolve, reject) => {
    let requestCount = 0

    const streamUrl = finalized ? '/finalized-stream' : '/stream'

    const server = createServer((req: IncomingMessage, res: ServerResponse) => {
      if (req.url?.startsWith('/metadata')) {
        writeMetadata(res, 'evm')

        return
      } else if (req.url !== streamUrl) {
        res.statusCode = 404
        res.end()
        return
      }

      const mockResp: MockResponse | undefined = mockResponses[requestCount]
      if (!mockResp) {
        let body = ''
        req.on('data', (chunk) => {
          body += chunk
        })
        req.on('end', () => {
          res.statusCode = 500
          res.end()
        })

        return
      }

      let body = ''
      req.on('data', (chunk) => {
        body += chunk
      })
      req.on('end', () => {
        mockResp.validateRequest?.(body ? JSON.parse(body) : undefined)

        switch (mockResp.statusCode) {
          case 200:
            writeBlocks(res, mockResp.data, mockResp.head)
            break

          case 409:
            res.writeHead(mockResp.statusCode, { 'Content-Type': 'application/json' })
            res.write(JSON.stringify(mockResp.data))
            break
          default:
            res.writeHead(mockResp.statusCode)
            break
        }

        requestCount++

        res.end()
      })
    })

    server.listen(0, LOOPBACK, () => {
      resolve(server)
    })

    server.on('error', (e) => {
      reject(e)
    })
  })

  const server = await promise

  const portal: MockPortal = {
    server,
    url: getServerAddress(server),
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => {
          if (err) return reject(err)
          resolve()
        })
      }),
  }

  return portal
}

/** @internal */
export async function readAll<T>(stream: AsyncIterable<{ data: T[] }>): Promise<T[]> {
  const res: T[] = []

  for await (const chunk of stream) {
    res.push(...chunk.data)
  }

  return res
}
