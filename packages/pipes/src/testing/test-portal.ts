import { IncomingMessage, Server, ServerResponse, createServer } from 'http'

import { Portal } from '~/core/query-builder.js'

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
      statusCode: 500 | 503
      validateRequest?: ValidateRequest
    }

export type MockPortal = {
  server: Server
  url: string
  close(): Promise<void>
}

export async function finalizedMockPortal(mockResponses: MockResponse[]) {
  return mockPortal(mockResponses, {
    finalized: true,
  })
}

export async function mockPortal(
  mockResponses: MockResponse[],
  {
    finalized = false,
    finalizedHead,
  }: {
    finalized?: boolean
    /**
     * Answer for GET `/finalized-head`, polled by a bounded stream whose delivered blocks are not
     * yet reported final. Defaults to "everything served so far is final" — the highest block
     * number any 200 response has carried — so existing scenarios complete without scripting it.
     */
    finalizedHead?: () => { number: number; hash: string } | undefined
  } = {},
): Promise<MockPortal> {
  const promise = new Promise<Server>((resolve, reject) => {
    let requestCount = 0
    let maxServedBlock: { number: number; hash: string } | undefined

    const streamUrl = finalized ? '/finalized-stream' : '/stream'

    const server = createServer((req: IncomingMessage, res: ServerResponse) => {
      if (req.url === '/finalized-head') {
        const head = finalizedHead ? finalizedHead() : maxServedBlock
        // No head → 204: a JSON content-type with an empty body would make the client's
        // res.json() throw instead of resolving to undefined.
        if (!head) {
          res.writeHead(204)
          res.end()
          return
        }
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(head))
        res.end()
        return
      }
      if (req.url?.startsWith('/metadata')) {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(
          JSON.stringify({
            dataset: 'mock-dataset',
            aliases: [],
            real_time: true,
            start_block: 0,
            metadata: {
              kind: 'evm',
            },
          }),
        )
        res.end()
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
            const headers: Record<string, string | number> = {
              'Content-Type': 'application/jsonl',
            }
            // `!= null`: block 0 is a valid head; dropping the header would hide finality entirely.
            if (mockResp.head?.finalized?.number != null) {
              headers['X-Sqd-Finalized-Head-Number'] = mockResp.head.finalized.number
            }
            if (mockResp.head?.finalized?.hash != null) {
              headers['X-Sqd-Finalized-Head-Hash'] = mockResp.head.finalized.hash
            }
            if (mockResp.head?.latest?.number != null) {
              headers['X-Sqd-Head-Number'] = mockResp.head.latest.number
            }

            res.writeHead(mockResp.statusCode, headers)
            // Send each mock data item as a JSON line
            mockResp.data.forEach((data) => {
              if (maxServedBlock == null || data.header.number > maxServedBlock.number) {
                maxServedBlock = { number: data.header.number, hash: data.header.hash }
              }
              res.write(JSON.stringify(data) + '\n')
            })
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

    server.listen(0, () => {
      // console.log(`Listening ${getServerAddress(server)}`);
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

function getServerAddress(server: Server): string {
  const address = server.address()
  if (!address || typeof address === 'string') {
    throw new Error('Invalid server address')
  }
  return `http://127.0.0.1:${address.port}`
}

/** @internal */
export async function readAll<T>(stream: AsyncIterable<{ data: T[] }>): Promise<T[]> {
  const res: T[] = []

  for await (const chunk of stream) {
    res.push(...chunk.data)
  }

  return res
}

/**
 * @internal
 */
export function mockPortalRestApi(overrides: Partial<Portal> = {}): Portal {
  return {
    getHead: async () => ({ number: 1, hash: '0x' }),
    resolveTimestamp: async () => 0,
    ...overrides,
  }
}
