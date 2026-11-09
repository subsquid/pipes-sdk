import net from 'node:net'

const DEFAULT_PORTS: Record<string, number> = { 'http:': 80, 'https:': 443, 'postgres:': 5432, 'postgresql:': 5432 }

/** The port a client connects to for `url`: URLs leave a default port out, and `http:` drops an explicit one. */
export function portOf(url: URL): number {
  if (url.port) return Number(url.port)

  const port = DEFAULT_PORTS[url.protocol]
  if (port === undefined) throw new Error(`No default port for ${url.protocol}//${url.host}; give one`)

  return port
}

/**
 * A TCP proxy to `host:port` that holds every chunk for half of `rttMs` each way, so connection
 * setup, requests and responses all pay the distance. Equal delays keep a socket's chunks in order.
 * Returns the proxy's port.
 */
export async function latencyProxy({ host, port, rttMs }: { host: string; port: number; rttMs: number }) {
  const oneWay = rttMs / 2

  const relay = (from: net.Socket, to: net.Socket) => {
    from.on('data', (chunk) => setTimeout(() => to.write(chunk), oneWay))
    from.on('end', () => setTimeout(() => to.end(), oneWay))
    from.on('error', () => to.destroy())
    from.on('close', () => setTimeout(() => to.destroy(), oneWay))
  }

  const server = net.createServer((client) => {
    const upstream = net.connect(port, host)
    upstream.setNoDelay(true)
    client.setNoDelay(true)
    relay(client, upstream)
    relay(upstream, client)
  })

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  server.unref()

  return (server.address() as net.AddressInfo).port
}
