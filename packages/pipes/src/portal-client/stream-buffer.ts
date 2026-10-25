import { Future, createFuture } from '@subsquid/util-internal'

import { maxFinalized } from '~/core/finalized-watermark.js'

export type BlockRef = {
  hash: string
  number: number
}

export type PortalHead = {
  finalized?: BlockRef
  latest?: { number: number }
}

export type StreamData<B> = {
  blocks: B[]
  head: PortalHead
  meta: {
    bytes: number
    requestedFromBlock: number
    lastBlockReceivedAt: Date
    requests: Record<number, number>
  }
}

export class StreamBuffer<B> {
  private buffer: StreamData<B> | undefined
  private state: 'pending' | 'ready' | 'failed' | 'closed' = 'pending'
  private error: unknown

  // Signals that buffer is ready to be consumed (threshold hit or timeout)
  private flushSignal: Future<void> = createFuture()
  // Signals that consumer has taken the buffer (backpressure)
  private consumedSignal: Future<void> = createFuture()
  // Signals that at least one put() has been called
  private hasDataSignal: Future<void> = createFuture()

  private idleTimeout: ReturnType<typeof setTimeout> | undefined
  private waitTimeout: ReturnType<typeof setTimeout> | undefined

  private maxBytes: number
  private maxIdleTime: number
  private maxWaitTime: number

  private abortController = new AbortController()

  get signal() {
    return this.abortController.signal
  }

  constructor(options: {
    maxWaitTime: number
    maxBytes: number
    maxIdleTime: number
  }) {
    this.maxWaitTime = options.maxWaitTime
    this.maxBytes = options.maxBytes
    this.maxIdleTime = options.maxIdleTime
  }

  async take(): Promise<StreamData<B> | undefined> {
    if (this.state === 'pending') {
      this.waitTimeout = setTimeout(() => this._ready(), this.maxWaitTime)
    }

    await Promise.all([this.flushSignal.promise(), this.hasDataSignal.promise()])

    if (this.waitTimeout != null) {
      clearTimeout(this.waitTimeout)
      this.waitTimeout = undefined
    }

    const result = this.buffer
    this.buffer = undefined

    switch (this.state) {
      case 'failed':
        if (result != null) return result
        throw this.error
      case 'closed':
        return result
      default:
        if (result == null) {
          throw new Error('Buffer is empty')
        }
        this.consumedSignal.resolve()
        this._reset()
        return result
    }
  }

  async put(data: StreamData<B>, flushImmediate = false): Promise<void> {
    if (this.state === 'closed' || this.state === 'failed') {
      throw new Error('Buffer is closed')
    }

    if (this.idleTimeout != null) {
      clearTimeout(this.idleTimeout)
      this.idleTimeout = undefined
    }

    if (this.buffer == null) {
      this.buffer = {
        blocks: [],
        head: {},
        meta: {
          bytes: 0,
          lastBlockReceivedAt: new Date(),
          requestedFromBlock: Infinity,
          requests: {},
        },
      }
    }

    this.buffer.blocks.push(...data.blocks)
    // A merged response can come without the finalized header, and finality never moves back.
    this.buffer.head = { ...data.head, finalized: maxFinalized(this.buffer.head.finalized, data.head.finalized) }
    this.buffer.meta.bytes += data.meta.bytes
    this.buffer.meta.requests = mergeRequests(this.buffer.meta.requests, data.meta.requests)
    this.buffer.meta.requestedFromBlock = Math.min(this.buffer.meta.requestedFromBlock, data.meta.requestedFromBlock)
    this.buffer.meta.lastBlockReceivedAt = data.meta.lastBlockReceivedAt

    this.hasDataSignal.resolve()

    if (flushImmediate || this.buffer.meta.bytes >= this.maxBytes) {
      this.flushSignal.resolve()
      await this.consumedSignal.promise()
    }

    if (this.state === 'pending' && this.buffer != null) {
      this.idleTimeout = setTimeout(() => this._ready(), this.maxIdleTime)
    }
  }

  flush() {
    if (this.buffer == null) return
    this._ready()
  }

  close() {
    if (this.state === 'closed' || this.state === 'failed') return
    this.state = 'closed'
    this._cleanup()
  }

  fail(err: any) {
    if (this.state === 'closed' || this.state === 'failed') return
    this.state = 'failed'
    this.error = err
    this._cleanup()
  }

  iterate() {
    return {
      [Symbol.asyncIterator]: (): AsyncIterator<StreamData<B>> => {
        return {
          next: async (): Promise<IteratorResult<StreamData<B>>> => {
            const value = await this.take()
            if (value == null) {
              return { done: true, value: undefined }
            }
            return { done: false, value }
          },
          return: async (): Promise<IteratorResult<StreamData<B>>> => {
            this.close()
            return { done: true, value: undefined }
          },
          throw: async (error?: any): Promise<IteratorResult<StreamData<B>>> => {
            this.fail(error)
            throw error
          },
        }
      },
    }
  }

  private _reset() {
    this.flushSignal = createFuture()
    this.hasDataSignal = createFuture()
    this.consumedSignal = createFuture()
    this.state = 'pending'
  }

  private _ready() {
    if (this.state === 'pending') {
      this.state = 'ready'
      this.flushSignal.resolve()
    }
    if (this.idleTimeout != null) {
      clearTimeout(this.idleTimeout)
      this.idleTimeout = undefined
    }
    if (this.waitTimeout != null) {
      clearTimeout(this.waitTimeout)
      this.waitTimeout = undefined
    }
  }

  private _cleanup() {
    if (this.idleTimeout != null) {
      clearTimeout(this.idleTimeout)
      this.idleTimeout = undefined
    }
    if (this.waitTimeout != null) {
      clearTimeout(this.waitTimeout)
      this.waitTimeout = undefined
    }
    this.flushSignal.resolve()
    this.hasDataSignal.resolve()
    this.consumedSignal.resolve()
    this.abortController.abort()
  }
}

function mergeRequests(a: Record<number, number>, b: Record<number, number>) {
  for (let code in b) {
    if (a[code] == null) {
      a[code] = 0
    }

    a[code] += b[code]
  }

  return a
}
