import { Server } from 'http'

import express, { Application } from 'express'
import client from 'prom-client'

import { Logger, Metrics } from '~/core/index.js'
import {
  Counter,
  CounterConfiguration,
  Gauge,
  GaugeConfiguration,
  Histogram,
  HistogramConfiguration,
  MetricsServer,
  Summary,
  SummaryConfiguration,
} from '~/core/metrics-server.js'
import { BatchContext } from '~/core/portal-source.js'
import { Profiler } from '~/core/profiling.js'
import { npmVersion } from '~/version.js'

export type MetricsServerOptions = {
  port?: number
  enabled?: boolean
  logger?: Logger
}

export type Stats = {
  sdk: {
    version: string
  }
  runtime: {
    name: 'bun' | 'node' | 'deno' | 'unknown'
    version: string
  }
  /** Path of the process entrypoint (process.argv[1]). */
  entrypoint: string
  pipes: {
    id: string
    dataset: BatchContext['stream']['dataset'] | null
    portal: {
      url: string
      query: any
    }
    progress: {
      from: number
      current: number
      to: number
      percent: number
      etaSeconds: number
    }
    speed: {
      blocksPerSecond: number
      bytesPerSecond: number
    }
  }[]

  usage: {
    memory: number
  }
}

function detectRuntime(): Stats['runtime'] {
  const versions = (process as any)?.versions ?? {}
  if (typeof versions.bun === 'string' && versions.bun) {
    return { name: 'bun', version: versions.bun }
  }
  if (typeof versions.deno === 'string' && versions.deno) {
    return { name: 'deno', version: versions.deno }
  }
  if (typeof versions.node === 'string' && versions.node) {
    return { name: 'node', version: versions.node }
  }
  return { name: 'unknown', version: '' }
}

function parseDate(date: any): Date | null {
  date = Array.isArray(date) ? date[0] : date

  if (typeof date !== 'string') {
    return null
  }

  const parsed = new Date(date)
  if (isNaN(parsed.getTime())) {
    return null
  }

  return parsed
}

type ProfilerResult = {
  name: string
  totalTime: number
  /** Offset (ms) of this span's start relative to the root of the profiler tree. */
  startOffset: number
  children: ProfilerResult[]
}

function transformProfiler(profiler: Profiler): ProfilerResult {
  const rootStarted = profiler.started
  return profiler.transform((span, children) => ({
    name: span.name,
    totalTime: span.elapsed,
    startOffset: span.started - rootStarted,
    children,
  }))
}

type TransformationResult = {
  name: string
  data: any
  elapsed?: number
  startOffset?: number
  dataSize?: number
  labels?: string[]
  children: TransformationResult[]
}

export function packPreview(value: any, depth = 0): any {
  if (depth > 8) return '[Truncated]'

  if (Array.isArray(value)) {
    if (
      value.length <= 10 &&
      !value.some((v) => typeof v !== 'string' && typeof v !== 'number' && typeof v !== 'boolean')
    ) {
      return value
    }

    if (value.length > 10) {
      return [packPreview(value[0], depth + 1), `... ${value.length - 1} more ...`]
    }

    return value.map((v) => packPreview(v, depth + 1))
  }

  if (value === null || value instanceof Date) return value

  if (typeof value === 'object') {
    const res: any = {}
    for (const key in value) {
      res[key] = packPreview(value[key], depth + 1)
    }
    return res
  }

  return value
}

/**
 * The `metrics processing` span is started after the root batch span has
 * already ended, so its real wall-clock offset exceeds `root.elapsed`. We
 * reposition it to start right at the root's previous end and extend the
 * root's duration so the tree stays internally consistent (child offsets
 * always fall within [0, parent.totalTime]).
 */
function patchMetricsProcessing(root: ProfilerResult, metricsElapsed: number): void {
  const lastChild = root.children.at(-1)
  if (!lastChild || lastChild.name !== 'metrics processing') return

  lastChild.totalTime = metricsElapsed
  lastChild.startOffset = root.totalTime
  root.totalTime += metricsElapsed
}

function patchMetricsProcessingPreview(root: TransformationResult, metricsElapsed: number): void {
  const lastChild = root.children.at(-1)
  if (!lastChild || lastChild.name !== 'metrics processing') return

  const rootElapsed = root.elapsed ?? 0
  lastChild.elapsed = metricsElapsed
  lastChild.startOffset = rootElapsed
  root.elapsed = rootElapsed + metricsElapsed
}

function transformPreview(profiler: Profiler): TransformationResult {
  const rootStarted = profiler.started
  return profiler.transform((span, children) => {
    const data =
      span.data != null
        ? JSON.stringify(packPreview(span.data), (k: string, v: any) => {
            if (typeof v === 'bigint') return v.toString() + 'n'
            if (v instanceof Date) return v.toISOString()
            return v
          })
        : null

    return {
      name: span.name,
      data,
      elapsed: span.elapsed || undefined,
      startOffset: span.started - rootStarted,
      dataSize: data ? data.length : undefined,
      labels: span.labels.length > 0 ? span.labels : undefined,
      children,
    }
  })
}

const MAX_HISTORY = 50
const DEFAULT_PORT = 9090

type PipeData = {
  lastBatch?: BatchContext
  profilers: { profiler: ProfilerResult; collectedAt: Date }[]
  transformationPreview?: TransformationResult
}

class ExpressMetricServer implements MetricsServer {
  readonly #options: { port: number; enabled: boolean }
  readonly #app: Application
  readonly #metrics: Metrics

  #started: boolean = false
  #server?: Server
  #logger?: Logger
  #pipes: Map<string, PipeData> = new Map()

  constructor({ port = DEFAULT_PORT, enabled = true, logger }: MetricsServerOptions = {}) {
    this.#options = {
      port,
      enabled,
    }
    this.#logger = logger

    const registry = new client.Registry()
    client.collectDefaultMetrics({ register: registry })

    const metricsCache = new Map<string, any>()

    this.#app = express()

    this.#metrics = {
      counter<T extends string>(options: CounterConfiguration<T>): Counter<T> {
        const exits = metricsCache.get(options.name)
        if (exits) return exits

        const metric = new client.Counter(options)
        metricsCache.set(options.name, metric)
        registry.registerMetric(metric)

        return metric
      },

      gauge<T extends string>(options: GaugeConfiguration<T>): Gauge<T> {
        const exits = metricsCache.get(options.name)
        if (exits) return exits

        const metric = new client.Gauge(options)
        metricsCache.set(options.name, metric)
        registry.registerMetric(metric)

        return metric
      },

      histogram<T extends string>(options: HistogramConfiguration<T>): Histogram<T> {
        const exits = metricsCache.get(options.name)
        if (exits) return exits

        const metric = new client.Histogram(options)
        metricsCache.set(options.name, metric)
        registry.registerMetric(metric)

        return metric
      },

      summary<T extends string>(options: SummaryConfiguration<T>): Summary<T> {
        const exits = metricsCache.get(options.name)
        if (exits) return exits

        const metric = new client.Summary(options)
        metricsCache.set(options.name, metric)
        registry.registerMetric(metric)

        return metric
      },
    }

    this.#app.use((req, res, next): any => {
      const origin = req.headers.origin

      // Allow requests only from localhost
      if (origin && origin.includes('localhost')) {
        res.setHeader('Access-Control-Allow-Origin', origin)
        res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        res.setHeader('Access-Control-Allow-Credentials', 'true') // if needed
      }

      // Handle preflight requests
      if (req.method === 'OPTIONS') {
        return res.sendStatus(204) // No Content
      }

      next()
    })

    this.#app.get('/stats', async (req, res) => {
      const memory = await registry.getSingleMetric('process_resident_memory_bytes')?.get()

      const data: Stats = {
        sdk: {
          version: npmVersion,
        },
        runtime: detectRuntime(),
        usage: {
          memory: memory?.values?.[0]?.value || 0,
        },
        entrypoint: process.argv[1],
        pipes: Array.from(this.#pipes.keys()).map((id) => {
          const pipeData = this.getPipe(id)
          const lastBatch = pipeData?.lastBatch

          return {
            id,
            dataset: lastBatch?.stream.dataset || null,
            portal: {
              url: lastBatch?.stream.query.url || '',
              query: lastBatch?.stream.query.raw || {},
            },
            progress: {
              from: lastBatch?.stream.state.initial || 0,
              current: lastBatch?.stream.state.current.number || 0,
              to: lastBatch?.stream.state.last || 0,
              percent: lastBatch?.stream.progress?.state.percent || 0,
              etaSeconds: lastBatch?.stream.progress?.state.etaSeconds || 0,
            },
            speed: {
              blocksPerSecond: lastBatch?.stream.progress?.intervalStats.processedBlocks.perSecond || 0,
              bytesPerSecond: lastBatch?.stream.progress?.intervalStats.bytesDownloaded.perSecond || 0,
            },
          }
        }),
      }

      res.json({ payload: data })
    })

    this.#app.get('/profiler', async (req, res) => {
      const from = parseDate(req.query['from']) || new Date(0)
      const pipeData = this.getPipe(req.query['id'] as string | undefined)
      const profilers = pipeData?.profilers || []

      const profiles = profilers.filter((p) => p.collectedAt >= from).map((p) => p.profiler)

      return res.json({
        // FIXME: remove hardcoded field
        payload: {
          enabled: true,
          profiles,
        },
      })
    })

    const transformationPreviewHandler: express.RequestHandler = async (req, res) => {
      const pipeData = this.getPipe(req.query['id'] as string | undefined)
      const lastBatch = pipeData?.lastBatch

      res.json({
        payload: {
          transformation: pipeData?.transformationPreview,
          batch: lastBatch
            ? {
                from:
                  lastBatch.batch.blocksCount > 0
                    ? lastBatch.stream.state.current.number - lastBatch.batch.blocksCount + 1
                    : lastBatch.stream.state.current.number,
                to: lastBatch.stream.state.current.number,
                blocksCount: lastBatch.batch.blocksCount,
                bytesSize: lastBatch.batch.bytesSize,
              }
            : undefined,
        },
      })
    }

    this.#app.get('/preview/transformation', transformationPreviewHandler)

    this.#app.get('/metrics', async (req, res) => {
      res.set('Content-Type', registry.contentType)
      res.end(await registry.metrics())
    })

    this.#app.get('/health', async (req, res) => {
      res.send('ok')
    })
  }

  async start() {
    if (!this.#options.enabled) return
    if (this.#started) return

    this.#started = true
    this.#server = this.#app.listen(this.#options.port, () => {
      this.#logger?.info(`🦑 Metrics server started at http://localhost:${this.#options.port}`)
    })
  }

  async stop() {
    this.#started = false
    client.register.clear()

    return new Promise<void>((done) => {
      if (!this.#server) return done()

      this.#server.close((_) => done())
    })
  }

  private getPipe(id?: string): PipeData | undefined {
    if (id) return this.#pipes.get(id)
    // Default to the first pipe for backward compatibility
    const first = this.#pipes.keys().next()
    return first.done ? undefined : this.#pipes.get(first.value)
  }

  registerPipe(id: string) {
    let data = this.#pipes.get(id)
    if (!data) {
      data = { profilers: [] }
      this.#pipes.set(id, data)
    }
    return data
  }

  batchProcessed(ctx: BatchContext) {
    const span = ctx.profiler.start('metrics processing').addLabels('core')
    const data = this.registerPipe(ctx.id)

    data.lastBatch = ctx
    const preview = transformPreview(ctx.profiler)
    const profiler = transformProfiler(ctx.profiler)

    data.profilers.push({
      profiler,
      collectedAt: new Date(),
    })
    data.profilers = data.profilers.slice(-MAX_HISTORY)
    span.end()

    // `metrics processing` is started AFTER the root batch span ends
    // (see `PortalSource.batchEnd`). That means:
    //  - its `elapsed` wasn't captured at transform time (patched below), and
    //  - its real `startOffset` is beyond `root.elapsed`, which would push it
    //    off the right edge of the flame chart canvas.
    //
    // Reposition the child at the end of the root and extend the root's
    // duration so both visualizations (profiler tree and preview) stay
    // internally consistent.
    patchMetricsProcessing(profiler, span.elapsed)
    patchMetricsProcessingPreview(preview, span.elapsed)

    data.transformationPreview = preview
  }

  get metrics() {
    return this.#metrics
  }

  setLogger(newLogger: Logger, override = false) {
    if (!override && this.#logger) return

    this.#logger = newLogger
  }
}

const servers = new Map<number, ExpressMetricServer>()

export function metricsServer(options: MetricsServerOptions = {}): MetricsServer {
  const port = options.port || DEFAULT_PORT

  const existed = servers.get(port)
  if (existed) return existed

  const server = new ExpressMetricServer(options)
  servers.set(port, server)

  return server
}
