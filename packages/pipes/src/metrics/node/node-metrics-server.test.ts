import { readFile } from 'node:fs/promises'
import net from 'node:net'
import { fileURLToPath } from 'node:url'

import client from 'prom-client'
import { describe, expect, it, vi } from 'vitest'

import { MetricsServer } from '~/core/index.js'
import { evmPortalStream } from '~/evm/index.js'
import { blockDecoder, readAll } from '~/testing/index.js'
import { StubPortalClient, stubBatch } from '~/testing/stub-portal.js'

import { MetricsServerOptions, metricsServer, packPreview } from './node-metrics-server.js'

/** Wrap `leaf` in `depth` nested `{ v: ... }` objects. */
function nest(leaf: unknown, depth: number): unknown {
  let node: unknown = leaf
  for (let i = 0; i < depth; i++) {
    node = { v: node }
  }

  return node
}

describe('packPreview', () => {
  it('does not overflow the stack on a cyclic object', () => {
    const a: any = {}
    a.self = a

    expect(() => packPreview(a)).not.toThrow()
    expect(JSON.stringify(packPreview(a))).toContain('[Truncated]')
  })

  it('bounds a deeply nested object instead of throwing', () => {
    // 100k levels — overflowed the stack before the depth guard (the reported crash-loop).
    const root: any = {}
    let cur = root
    for (let i = 0; i < 100_000; i++) {
      cur.next = {}
      cur = cur.next
    }
    cur.leaf = 'end'

    expect(() => packPreview(root)).not.toThrow()
    expect(JSON.stringify(packPreview(root))).toContain('[Truncated]')
  })

  it('keeps values up to the depth bound and truncates just past it', () => {
    const withinBound = JSON.stringify(packPreview(nest('LEAF', 8)))
    expect(withinBound).toContain('LEAF')
    expect(withinBound).not.toContain('[Truncated]')

    const pastBound = JSON.stringify(packPreview(nest('LEAF', 9)))
    expect(pastBound).not.toContain('LEAF')
    expect(pastBound).toContain('[Truncated]')
  })

  it('leaves realistic shallow preview data untouched', () => {
    const value = {
      header: { number: 100, hash: '0xabc' },
      transactions: [{ from: '0x1', to: '0x2', logs: [{ topics: ['0xa', '0xb'] }] }],
    }

    expect(packPreview(value)).toEqual(value)
  })

  it('passes primitives, null and Date through unchanged', () => {
    const date = new Date(0)

    expect(packPreview(42)).toBe(42)
    expect(packPreview('x')).toBe('x')
    expect(packPreview(null)).toBe(null)
    expect(packPreview(date)).toBe(date)
  })

  it('returns short primitive arrays verbatim', () => {
    expect(packPreview([1, 2, 3])).toEqual([1, 2, 3])
  })

  it('collapses long arrays to head plus a remainder marker', () => {
    const arr = Array.from({ length: 25 }, (_, i) => ({ i }))

    expect(packPreview(arr)).toEqual([{ i: 0 }, '... 24 more ...'])
  })
})

async function freePort(): Promise<number> {
  const probe = net.createServer()
  await new Promise<void>((resolve) => probe.listen(0, '127.0.0.1', resolve))

  const { port } = probe.address() as net.AddressInfo
  await new Promise((resolve) => probe.close(resolve))

  return port
}

type Served = { server: MetricsServer; url: string }

/** A test with a `serve` fixture: it starts metrics servers on free ports and stops them after the test. */
const withServer = it.extend<{ serve: (options?: Omit<MetricsServerOptions, 'port'>) => Promise<Served> }>({
  serve: async ({}, use) => {
    const opened: MetricsServer[] = []

    await use(async (options) => {
      const port = await freePort()
      const server = metricsServer({ ...options, port })
      server.start()
      opened.push(server)

      return { server, url: `http://127.0.0.1:${port}` }
    })

    await Promise.all(opened.map((server) => server.stop()))
  },
})

/** Beside `serve`, a metric the application registers in the prom-client global registry. */
const withForeignMetric = withServer.extend<{ foreign: string }>({
  foreign: async ({}, use) => {
    const name = 'app_foreign_total'
    new client.Counter({ name, help: 'A metric the application registers itself' })

    await use(name)
    client.register.removeSingleMetric(name)
  },
})

async function allowedOrigin(url: string, origin: string): Promise<string | null> {
  const res = await fetch(`${url}/health`, { headers: { origin } })

  return res.headers.get('access-control-allow-origin')
}

async function profilerEnabled(url: string, id: string): Promise<boolean> {
  const res = await fetch(`${url}/profiler?id=${id}`)
  const body = (await res.json()) as { payload: { enabled: boolean } }

  return body.payload.enabled
}

async function statsProgress(url: string): Promise<{ current: number; to: number }> {
  const res = await fetch(`${url}/stats`)
  const body = (await res.json()) as { payload: { pipes: { progress: { current: number; to: number } }[] } }

  return body.payload.pipes[0].progress
}

async function scrape(url: string): Promise<string> {
  const res = await fetch(`${url}/metrics`)

  return res.text()
}

describe('metrics server', () => {
  withServer.for<[string, boolean]>([
    ['http://localhost:3000', true],
    ['http://127.0.0.1:3000', true],
    ['http://[::1]:3000', true],
    ['http://localhost.evil.com', false],
    ['http://evil-localhost.com', false],
    ['null', false],
  ])('allows the origin %s cross-origin: %s', async ([origin, allowed], { serve }) => {
    const { url } = await serve()

    expect(await allowedOrigin(url, origin)).toBe(allowed ? origin : null)
  })

  type CorsCase = { name: string; cors: MetricsServerOptions['cors']; origin: string; allowed: boolean }
  const dashboard = { origin: ['https://dash.example.com'] }
  const subdomains = { origin: (origin: string) => origin.endsWith('.example.com') }

  withServer.for<CorsCase>([
    { name: 'a listed origin', cors: dashboard, origin: 'https://dash.example.com', allowed: true },
    {
      name: 'loopback when a list replaces the default',
      cors: dashboard,
      origin: 'http://localhost:3000',
      allowed: false,
    },
    { name: 'an origin the predicate accepts', cors: subdomains, origin: 'https://a.example.com', allowed: true },
    { name: 'an origin the predicate refuses', cors: subdomains, origin: 'https://example.org', allowed: false },
    { name: 'loopback with CORS off', cors: false, origin: 'http://localhost:3000', allowed: false },
  ])('answers $name as configured', async ({ cors, origin, allowed }, { serve }) => {
    const { url } = await serve({ cors })

    expect(await allowedOrigin(url, origin)).toBe(allowed ? origin : null)
  })

  withServer('reports whether each pipe collects profiles', async ({ serve }) => {
    const { server, url } = await serve()

    for (const [id, profiler] of [
      ['profiled', true],
      ['unprofiled', false],
    ] as const) {
      evmPortalStream({
        id,
        portal: 'http://127.0.0.1:1',
        logger: 'silent',
        outputs: blockDecoder({ from: 0 }),
        metrics: server,
        profiler,
      })
    }

    expect(await profilerEnabled(url, 'profiled')).toBe(true)
    expect(await profilerEnabled(url, 'unprofiled')).toBe(false)
  })

  withForeignMetric('keeps the application metrics when it stops', async ({ serve, foreign }) => {
    const { server } = await serve()
    server.metrics.counter({ name: 'sqd_test_stop_total', help: 'test' })

    await server.stop()

    expect(client.register.getSingleMetric(foreign)).toBeDefined()
  })

  withServer('reports the progress of a head-only batch in the stats (WP-13)', async ({ serve }) => {
    const { server, url } = await serve()
    // The pipe stops the server when it ends, so the stats are read while it waits for the next batch.
    const headSeen = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const portal = new StubPortalClient(async function* () {
      yield stubBatch([1], { latest: { number: 10 } }, { 200: 1 })
      yield stubBatch([], {}, { 204: 1 })
      headSeen.resolve()
      await release.promise
    })

    const run = readAll(
      evmPortalStream({ id: 'test', portal, logger: 'silent', outputs: blockDecoder({ from: 1 }), metrics: server }),
    )
    await headSeen.promise
    const progress = await statsProgress(url)
    release.resolve()
    await run

    expect(progress).toMatchObject({ current: 1, to: 1 })
  })

  withServer('matches the golden stats, metrics and transformation preview for one scripted run', async ({ serve }) => {
    const { server, url } = await serve()
    const release = Promise.withResolvers<void>()
    const portal = new StubPortalClient(async function* () {
      yield stubBatch([1, 2], { latest: { number: 2 }, finalized: { number: 2, hash: '0x2' } }, { 200: 1 })
      await release.promise
    })
    const run = readAll(
      evmPortalStream({
        id: 'golden',
        portal,
        logger: 'silent',
        outputs: blockDecoder({ from: 1, to: 2 }),
        metrics: server,
        profiler: true,
      }),
    )

    try {
      await vi.waitFor(async () => {
        const res = await fetch(`${url}/preview/transformation?id=golden`)
        const body = (await res.json()) as { payload: { transformation?: unknown; batch?: unknown } }
        expect(body.payload.transformation).toBeDefined()
        expect(body.payload.batch).toBeDefined()
      })

      const stats = (await (await fetch(`${url}/stats`)).json()) as any
      // An unconditional assignment would also create a field the endpoint stopped sending,
      // so prove the shape first, then mask only the environment-dependent values.
      expect(stats.payload.sdk.version).toBeTypeOf('string')
      expect(stats.payload.runtime.version).toBeTypeOf('string')
      expect(stats.payload.entrypoint).toBeTypeOf('string')
      expect(stats.payload.usage.memory).toBeTypeOf('number')

      stats.payload.sdk.version = '<sdk-version>'
      stats.payload.runtime.version = '<runtime-version>'
      stats.payload.entrypoint = '<entrypoint>'
      stats.payload.usage.memory = 0

      for (const pipe of stats.payload.pipes) {
        // Zero the fields in place: a replaced object would hide a renamed or extra key.
        expect(pipe.speed?.blocksPerSecond).toBeTypeOf('number')
        expect(pipe.speed?.bytesPerSecond).toBeTypeOf('number')
        pipe.speed.blocksPerSecond = 0
        pipe.speed.bytesPerSecond = 0
      }

      const preview = (await (await fetch(`${url}/preview/transformation?id=golden`)).json()) as any
      const normalizeTimings = (node: any) => {
        if (!node) return
        if (node.elapsed !== undefined) node.elapsed = 0
        if (node.startOffset !== undefined) node.startOffset = 0
        for (const child of node.children ?? []) normalizeTimings(child)
      }
      normalizeTimings(preview.payload.transformation)

      const requiredMetrics = new Set([
        'sqd_processed_block',
        'sqd_end_block',
        'sqd_progress_ratio',
        'sqd_eta_seconds',
        'sqd_blocks_processed_total',
        'sqd_bytes_downloaded_total',
        'sqd_forks_total',
        'sqd_portal_requests_total',
        'sqd_batch_size_blocks',
        'sqd_batch_size_bytes',
      ])
      const metrics = (await scrape(url))
        .split('\n')
        .filter((line) => {
          const name = line.match(/sqd_[a-z0-9_]+/)?.[0]
          if (!name) return false
          return requiredMetrics.has(name.replace(/_(?:bucket|sum|count)$/, ''))
        })
        .join('\n')

      const fixture = (name: string) => fileURLToPath(new URL(`./__fixtures__/${name}`, import.meta.url))
      expect(stats).toEqual(JSON.parse(await readFile(fixture('golden-stats.json'), 'utf8')))
      expect(preview).toEqual(JSON.parse(await readFile(fixture('golden-preview.json'), 'utf8')))
      expect(metrics + '\n').toBe(await readFile(fixture('golden-metrics.prom'), 'utf8'))
    } finally {
      release.resolve()
      await run
    }
  })

  withServer('lets two servers in one process register the same metric', async ({ serve }) => {
    const servers = [await serve(), await serve()]

    for (const { server } of servers) {
      server.metrics.counter({ name: 'sqd_test_shared_total', help: 'test' }).inc(1)
    }

    for (const { url } of servers) {
      expect(await scrape(url)).toContain('sqd_test_shared_total 1')
    }
  })
})
