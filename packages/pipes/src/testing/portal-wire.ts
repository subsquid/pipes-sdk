import { IncomingMessage, Server, ServerResponse, createServer } from 'http'

/** The head a stream response reports in its headers. */
export type WireHead = {
  finalized?: { number: number; hash: string }
  latest?: { number: number }
}

/** `!= null`: block 0 is a valid head; dropping the header would hide finality entirely. */
export function headHeaders(head: WireHead | undefined): Record<string, string | number> {
  const headers: Record<string, string | number> = {}

  if (head?.finalized?.number != null) {
    headers['X-Sqd-Finalized-Head-Number'] = head.finalized.number
  }
  if (head?.finalized?.hash != null) {
    headers['X-Sqd-Finalized-Head-Hash'] = head.finalized.hash
  }
  if (head?.latest?.number != null) {
    headers['X-Sqd-Head-Number'] = head.latest.number
  }

  return headers
}

/** A 200 stream response: one JSON block per line. Leaves the response open. */
export function writeBlocks(res: ServerResponse, blocks: unknown[], head: WireHead | undefined) {
  res.writeHead(200, { 'Content-Type': 'application/jsonl', ...headHeaders(head) })

  for (const block of blocks) {
    res.write(JSON.stringify(block) + '\n')
  }
}

export function writeMetadata(res: ServerResponse, kind: string) {
  res.writeHead(200, { 'Content-Type': 'application/json' })
  res.end(
    JSON.stringify({
      dataset: 'mock-dataset',
      aliases: [],
      real_time: true,
      start_block: 0,
      metadata: { kind },
    }),
  )
}

export function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = ''
    req.on('data', (chunk) => {
      body += chunk
    })
    req.on('end', () => resolve(body))
    req.on('error', reject)
  })
}

/**
 * The URL names this address, so the server binds it and nothing wider: bound to every interface,
 * it shares its port with whatever process holds the loopback one, and that process answers.
 */
export const LOOPBACK = '127.0.0.1'

export function listen(handler: (req: IncomingMessage, res: ServerResponse) => void): Promise<Server> {
  return new Promise((resolve, reject) => {
    const server = createServer(handler)

    server.listen(0, LOOPBACK, () => resolve(server))
    server.on('error', reject)
  })
}

export function getServerAddress(server: Server): string {
  const address = server.address()
  if (!address || typeof address === 'string') {
    throw new Error('Invalid server address')
  }

  return `http://${LOOPBACK}:${address.port}`
}
