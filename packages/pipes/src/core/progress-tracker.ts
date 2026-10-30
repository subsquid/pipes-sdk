import { Counter, Gauge, Histogram } from '~/core/index.js'

import { formatBlock, formatEta, formatNumber, humanBytes } from './formatters.js'
import { Logger } from './logger.js'
import { BatchContext } from './portal-source.js'
import { createTransformer } from './transformer.js'
import { BlockCursor } from './types.js'

type HistoryState = {
  ts: number
  bytesDownloaded: number
  blockNumber: number
  requests: Record<number, number>
}
type LastCursorState = { initial: number; last: number; current: BlockCursor }

export type StartEvent = {
  state: {
    initial: number
    current?: BlockCursor
  }
  logger: Logger
}

export type ProgressEvent = {
  progress: {
    state: {
      /** First block of the indexed range. */
      from: number
      /** End of the indexed range: the configured `to` bound, or the chain head when unbounded. */
      to: number
      current: number
      percent: number
      etaSeconds: number
    }
    /** Activity during the last reporting interval. */
    intervalStats: {
      requests: {
        total: {
          count: number
        }
        successful: {
          count: number
          percent: number
        }
        rateLimited: {
          count: number
          percent: number
        }
        failed: {
          count: number
          percent: number
        }
      }
      processedBlocks: {
        count: number
        perSecond: number
      }
      bytesDownloaded: {
        count: number
        perSecond: number
      }
    }
  }
  logger: Logger
}

type ProgressHistoryOptions = {
  maxHistory?: number
  maxStaleSeconds?: number
}

enum Classification {
  Successful = 'successful',
  RateLimited = 'rateLimited',
  Failed = 'failed',
}

const METRIC_LABELS: Record<Classification, string> = {
  [Classification.Successful]: 'success',
  [Classification.RateLimited]: 'rate_limited',
  [Classification.Failed]: 'error',
}

function mapRequestStatus(statusCode: number): Classification {
  if (statusCode >= 200 && statusCode < 300) {
    return Classification.Successful
  } else if (statusCode === 429) {
    return Classification.RateLimited
  } else {
    return Classification.Failed
  }
}

class ProgressHistory {
  #options: Required<ProgressHistoryOptions>
  #states: HistoryState[] = []
  #lastCursorState?: LastCursorState

  constructor(options?: ProgressHistoryOptions) {
    this.#options = {
      maxHistory: 100,
      maxStaleSeconds: 10,

      ...options,
    }
  }

  addState({ bytes, state, requests }: { bytes: number; state: LastCursorState; requests: Record<number, number> }) {
    // if (!state.current?.number) return

    this.#states.push({
      ts: Date.now(),
      bytesDownloaded: bytes,
      blockNumber: state.current.number || 0,
      requests,
    })

    this.#lastCursorState = state

    // Keep only the last N states for X seconds
    this.#states = this.#states.slice(-this.#options.maxHistory)
  }

  private validateHistory(states: HistoryState[]) {
    const lastTs = states[states.length - 1]?.ts

    // If the last state is too old, reset the history
    // This can happen if the stream got stuck
    if (lastTs && Date.now() - lastTs > this.#options.maxStaleSeconds * 1000) {
      this.#states = []
      return {
        blocks: 0,
        bytes: 0,
        requests: {
          total: 0,
          successful: 0,
          rateLimited: 0,
          failed: 0,
        },
      }
    }

    return {
      blocks: states.length >= 2 ? states[states.length - 1].blockNumber - states[0].blockNumber : 0,
      bytes: states.reduce((acc, state) => acc + state.bytesDownloaded, 0),
      requests: states.reduce(
        (acc, state) => {
          for (const [status, value] of Object.entries(state.requests)) {
            acc.total += value
            acc[mapRequestStatus(Number(status))] += value
          }

          return acc
        },
        {
          total: 0,
          successful: 0,
          rateLimited: 0,
          failed: 0,
        },
      ),
    }
  }

  calculate(): ProgressEvent['progress'] {
    const stat = this.validateHistory(this.#states)

    const last = this.#lastCursorState?.last || 0
    const initial = this.#lastCursorState?.initial || 0
    const current = this.#lastCursorState?.current?.number || 0

    const blocksTotal = Math.max(last - initial, 0)
    const blocksProcessed = Math.max(current - initial, 0)
    const blocksRemaining = Math.max(last - current, 0)

    const secsDiff = this.#states[0] ? (Date.now() - this.#states[0].ts) / 1000 : 0
    const blockPerSecond = secsDiff > 0 ? stat.blocks / secsDiff : 0

    const intervalStats = {
      requests: {
        total: {
          count: stat.requests.total,
        },
        successful: {
          count: stat.requests.successful,
          percent: stat.requests.total > 0 ? (stat.requests.successful / stat.requests.total) * 100 : 0,
        },
        rateLimited: {
          count: stat.requests.rateLimited,
          percent: stat.requests.total > 0 ? (stat.requests.rateLimited / stat.requests.total) * 100 : 0,
        },
        failed: {
          count: stat.requests.failed,
          percent: stat.requests.total > 0 ? (stat.requests.failed / stat.requests.total) * 100 : 0,
        },
      },
      processedBlocks: {
        count: stat.blocks,
        perSecond: blockPerSecond,
      },
      bytesDownloaded: {
        count: stat.bytes,
        perSecond: secsDiff > 0 ? stat.bytes / secsDiff : 0,
      },
    }

    return {
      state: {
        from: initial,
        to: last,
        current,
        percent: blocksTotal > 0 ? (blocksProcessed / blocksTotal) * 100 : 0,
        etaSeconds: blockPerSecond > 0 ? blocksRemaining / blockPerSecond : 0,
      },
      intervalStats,
    }
  }
}

export type ProgressTrackerOptions = {
  onStart?: (state: StartEvent) => void
  onProgress?: (state: ProgressEvent) => void
  interval?: number
}

const DEFAULT_BLOCKS_BUCKETS = [1, 5, 10, 50, 100, 500, 1000, 2000, 3000, 5000]
const DEFAULT_BYTES_BUCKETS = [
  1024, // 1kb
  10240, // 10kb
  102400, // 100kb
  524288, // 512kb
  1048576, // 1mb
  5242880, // 5mb
  10485760, // 10mb
  11534336, // 11mb
]

export function progressTracker<T>({ onProgress, onStart, interval = 5000 }: ProgressTrackerOptions) {
  let ticker: NodeJS.Timeout | null = null
  let lastProgress: ProgressEvent['progress'] | null = null

  let pipeId = ''
  let processedBlock: Gauge<'id'> | null = null
  let endBlock: Gauge<'id'> | null = null
  let progressRatio: Gauge<'id'> | null = null
  let etaSeconds: Gauge<'id'> | null = null
  let blocksProcessedTotal: Counter<'id'> | null = null
  let bytesDownloaded: Counter<'id'> | null = null
  let reorgsTotal: Counter<'id'> | null = null
  let portalRequestsTotal: Counter<'id' | 'classification' | 'status'> | null = null
  let batchSizeBlocks: Histogram<'id'> | null = null
  let batchSizeBytes: Histogram<'id'> | null = null

  const history = new ProgressHistory()

  if (!onStart) {
    onStart = ({ state, logger }) => {
      if (state.current) {
        logger.info(`Resuming indexing from ${formatBlock(state.current.number)} block`)
        return
      }

      logger.info(`Start indexing from ${formatBlock(state.initial)} block`)
    }
  }

  if (!onProgress) {
    onProgress = ({ progress: { state, intervalStats }, logger }) => {
      if (state.current === 0 && state.to === 0) {
        logger.info({ message: 'Initializing...' })
        return
      }

      const bps =
        intervalStats.processedBlocks.perSecond > 1
          ? formatNumber(intervalStats.processedBlocks.perSecond, 0)
          : intervalStats.processedBlocks.perSecond.toFixed(2)

      const msg: Record<string, string> = {
        message: `${formatNumber(state.current)} / ${formatNumber(state.to)} (${formatNumber(state.percent)}%), ${formatEta(state.etaSeconds)}`,
        blocks: `${bps} blocks/second`,
        bytes: `${humanBytes(intervalStats.bytesDownloaded.perSecond)}/second`,
      }

      if (intervalStats.requests.total.count > 0) {
        msg['requests'] = [
          intervalStats.requests.successful.percent > 0
            ? `${formatNumber(intervalStats.requests.successful.percent)}% successful`
            : false,
          intervalStats.requests.rateLimited.percent
            ? `${formatNumber(intervalStats.requests.rateLimited.percent)}% rate limited`
            : false,
          intervalStats.requests.failed.percent > 0
            ? `${formatNumber(intervalStats.requests.failed.percent)}% failed`
            : false,
        ]
          .filter(Boolean)
          .join(', ')
      }

      logger.info(msg)
    }
  }

  const countRequests = (id: string, requests: Record<number, number>) => {
    for (const [statusCode, count] of Object.entries(requests)) {
      portalRequestsTotal?.inc(
        {
          id,
          classification: METRIC_LABELS[mapRequestStatus(Number(statusCode))],
          status: statusCode,
        },
        count,
      )
    }
  }

  const record = (ctx: BatchContext) => {
    history.addState({
      state: ctx.stream.state,
      bytes: ctx.batch.bytesSize,
      requests: ctx.batch.requests,
    })

    countRequests(ctx.id, ctx.batch.requests)

    if (ctx.stream.state.current?.number) {
      processedBlock?.set({ id: ctx.id }, ctx.stream.state.current.number)
    }

    lastProgress = history.calculate()

    endBlock?.set({ id: ctx.id }, lastProgress.state.to)
    progressRatio?.set({ id: ctx.id }, lastProgress.state.percent / 100)
    etaSeconds?.set({ id: ctx.id }, lastProgress.state.etaSeconds)
    blocksProcessedTotal?.inc({ id: ctx.id }, ctx.batch.blocksCount)
    bytesDownloaded?.inc({ id: ctx.id }, ctx.batch.bytesSize)

    ctx.stream.progress = lastProgress
  }

  return createTransformer<T, T>({
    profiler: { name: 'track progress' },
    start: ({ id, metrics, state, logger }) => {
      pipeId = id

      if (interval > 0) {
        ticker = setInterval(() => {
          if (!lastProgress) return

          onProgress({ progress: lastProgress, logger })
        }, interval)
      }

      onStart({ state, logger })

      processedBlock = metrics.gauge({
        name: 'sqd_processed_block',
        help: 'Highest block number processed so far',
        labelNames: ['id'] as const,
      })

      endBlock = metrics.gauge({
        name: 'sqd_end_block',
        help: 'End of the indexed range: the configured `to` bound, or the chain head when unbounded',
        labelNames: ['id'] as const,
      })

      progressRatio = metrics.gauge({
        name: 'sqd_progress_ratio',
        help: 'Indexing progress as a ratio from 0 to 1',
        labelNames: ['id'] as const,
      })

      etaSeconds = metrics.gauge({
        name: 'sqd_eta_seconds',
        help: 'Estimated time to full sync in seconds',
        labelNames: ['id'] as const,
      })

      blocksProcessedTotal = metrics.counter({
        name: 'sqd_blocks_processed_total',
        help: 'Total number of blocks processed',
        labelNames: ['id'] as const,
      })

      bytesDownloaded = metrics.counter({
        name: 'sqd_bytes_downloaded_total',
        help: 'Total bytes downloaded from portal',
        labelNames: ['id'] as const,
      })

      reorgsTotal = metrics.counter({
        name: 'sqd_forks_total',
        help: 'Total number of chain forks detected',
        labelNames: ['id'] as const,
      })

      portalRequestsTotal = metrics.counter({
        name: 'sqd_portal_requests_total',
        help: 'Total number of requests to the portal',
        labelNames: ['id', 'classification', 'status'] as const,
      })

      batchSizeBlocks = metrics.histogram({
        name: 'sqd_batch_size_blocks',
        help: 'Number of blocks per batch',
        labelNames: ['id'] as const,
        // TODO are these buckets good by default?
        // TODO make it configurable!
        buckets: DEFAULT_BLOCKS_BUCKETS,
      })

      batchSizeBytes = metrics.histogram({
        name: 'sqd_batch_size_bytes',
        help: 'Size of each batch in bytes',
        labelNames: ['id'] as const,
        // TODO are these buckets good by default?
        // TODO make it configurable
        buckets: DEFAULT_BYTES_BUCKETS,
      })

      processedBlock.set({ id }, -1)
    },
    transform: async (data, ctx) => {
      batchSizeBlocks?.observe({ id: ctx.id }, ctx.batch.blocksCount)
      batchSizeBytes?.observe({ id: ctx.id }, ctx.batch.bytesSize)

      record(ctx)

      return data
    },
    // A head-only response is no batch, so it stays out of the batch-size histograms.
    head: (ctx) => {
      // Before the first block there is no cursor to report progress at.
      if (!ctx.stream) {
        countRequests(ctx.id, ctx.batch.requests)

        return
      }

      record({ ...ctx, stream: ctx.stream })
    },
    rollback: () => {
      reorgsTotal?.inc({ id: pipeId }, 1)
    },
    stop: () => {
      if (ticker) {
        clearInterval(ticker)
      }
    },
  })
}
