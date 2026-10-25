import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { StreamBuffer, StreamData } from './stream-buffer.js'

function makeData<B>(blocks: B[], bytes = 100, overrides?: Partial<StreamData<B>['meta']>): StreamData<B> {
  return {
    blocks,
    head: { finalized: { hash: '0x1', number: 1 } },
    meta: {
      bytes,
      requestedFromBlock: 0,
      lastBlockReceivedAt: new Date(0),
      requests: {},
      ...overrides,
    },
  }
}

async function collectAll<B>(buffer: StreamBuffer<B>): Promise<StreamData<B>[]> {
  const results: StreamData<B>[] = []
  for await (const data of buffer.iterate()) {
    results.push(data)
  }
  return results
}

describe('StreamBuffer', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('basic put + take', () => {
    it('should return data after idle timeout', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      await buffer.put(makeData(['a', 'b'], 50))
      // idle timeout triggers flush
      vi.advanceTimersByTime(100)

      const result = await takePromise
      expect(result?.blocks).toEqual(['a', 'b'])
      expect(result?.meta.bytes).toBe(50)

      buffer.close()
    })

    it('should return data after maxWaitTime', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 300, maxWaitTime: 200 })

      const takePromise = buffer.take()

      await buffer.put(makeData(['a'], 50))

      // maxWaitTime fires before idleTime
      vi.advanceTimersByTime(200)

      const result = await takePromise
      expect(result?.blocks).toEqual(['a'])

      buffer.close()
    })

    it('should flush immediately when bytes exceed maxBytes', async () => {
      const buffer = new StreamBuffer({ maxBytes: 100, maxIdleTime: 300, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      // put() with bytes >= maxBytes resolves flush and waits for consumed
      const putPromise = buffer.put(makeData(['a'], 150))

      const result = await takePromise
      expect(result?.blocks).toEqual(['a'])
      expect(result?.meta.bytes).toBe(150)

      // put() should have resolved after take consumed
      await putPromise

      buffer.close()
    })

    it('should flush immediately when flushImmediate=true', async () => {
      const buffer = new StreamBuffer({ maxBytes: 10000, maxIdleTime: 300, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      const putPromise = buffer.put(makeData(['a'], 10), true)

      const result = await takePromise
      expect(result?.blocks).toEqual(['a'])

      await putPromise

      buffer.close()
    })
  })

  describe('accumulation', () => {
    it('should accumulate blocks from multiple puts', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      await buffer.put(makeData(['a'], 50))
      await buffer.put(makeData(['b'], 60))

      vi.advanceTimersByTime(100)

      const result = await takePromise
      expect(result?.blocks).toEqual(['a', 'b'])
      expect(result?.meta.bytes).toBe(110)

      buffer.close()
    })

    it('should merge request counts', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      await buffer.put(makeData([], 10, { requests: { 200: 1, 204: 2 } }))
      await buffer.put(makeData([], 10, { requests: { 200: 3, 500: 1 } }))

      vi.advanceTimersByTime(100)

      const result = await takePromise
      expect(result?.meta.requests).toEqual({ 200: 4, 204: 2, 500: 1 })

      buffer.close()
    })

    it('should take minimum requestedFromBlock', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      await buffer.put(makeData([], 10, { requestedFromBlock: 100 }))
      await buffer.put(makeData([], 10, { requestedFromBlock: 50 }))
      await buffer.put(makeData([], 10, { requestedFromBlock: 200 }))

      vi.advanceTimersByTime(100)

      const result = await takePromise
      expect(result?.meta.requestedFromBlock).toBe(50)

      buffer.close()
    })

    it('should use latest head', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      await buffer.put({
        blocks: [],
        head: { finalized: { hash: '0x1', number: 1 } },
        meta: { bytes: 0, requestedFromBlock: 0, lastBlockReceivedAt: new Date(0), requests: {} },
      })
      await buffer.put({
        blocks: [],
        head: { finalized: { hash: '0x5', number: 5 } },
        meta: { bytes: 0, requestedFromBlock: 0, lastBlockReceivedAt: new Date(0), requests: {} },
      })

      vi.advanceTimersByTime(100)

      const result = await takePromise
      expect(result?.head).toEqual({ finalized: { hash: '0x5', number: 5 } })

      buffer.close()
    })

    // The portal can drop the finalized header for a while. The merged batch keeps the head it had,
    // or its blocks look final to the source.
    it('keeps the highest finalized head when a later put has none', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      await buffer.put({
        blocks: [],
        head: { finalized: { hash: '0x5', number: 5 }, latest: { number: 6 } },
        meta: { bytes: 0, requestedFromBlock: 0, lastBlockReceivedAt: new Date(0), requests: {} },
      })
      await buffer.put({
        blocks: [],
        head: { latest: { number: 8 } },
        meta: { bytes: 0, requestedFromBlock: 0, lastBlockReceivedAt: new Date(0), requests: {} },
      })

      vi.advanceTimersByTime(100)

      const result = await takePromise
      expect(result?.head).toEqual({ finalized: { hash: '0x5', number: 5 }, latest: { number: 8 } })

      buffer.close()
    })
  })

  describe('backpressure', () => {
    it('should block put when maxBytes exceeded until take consumes', async () => {
      const buffer = new StreamBuffer({ maxBytes: 100, maxIdleTime: 300, maxWaitTime: 5000 })

      let putResolved = false

      const takePromise = buffer.take()
      const putPromise = buffer.put(makeData(['a'], 200)).then(() => {
        putResolved = true
      })

      // put should not resolve until take consumes
      expect(putResolved).toBe(false)

      await takePromise
      await putPromise

      expect(putResolved).toBe(true)

      buffer.close()
    })

    it('should allow sequential take+put cycles', async () => {
      const buffer = new StreamBuffer({ maxBytes: 50, maxIdleTime: 300, maxWaitTime: 5000 })

      // First cycle
      const take1 = buffer.take()
      const put1 = buffer.put(makeData(['a'], 100))
      const result1 = await take1
      await put1
      expect(result1?.blocks).toEqual(['a'])

      // Second cycle
      const take2 = buffer.take()
      const put2 = buffer.put(makeData(['b'], 100))
      const result2 = await take2
      await put2
      expect(result2?.blocks).toEqual(['b'])

      buffer.close()
    })
  })

  describe('close', () => {
    it('should return undefined from take after close', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      buffer.close()

      const result = await buffer.take()
      expect(result).toBeUndefined()
    })

    it('should return buffered data then undefined on close', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      await buffer.put(makeData(['a'], 50))
      buffer.close()

      const result1 = await buffer.take()
      expect(result1?.blocks).toEqual(['a'])

      const result2 = await buffer.take()
      expect(result2).toBeUndefined()
    })

    it('should throw on put after close', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      buffer.close()

      await expect(buffer.put(makeData(['a']))).rejects.toThrow('Buffer is closed')
    })

    it('should abort the signal on close', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      expect(buffer.signal.aborted).toBe(false)
      buffer.close()
      expect(buffer.signal.aborted).toBe(true)
    })
  })

  describe('fail', () => {
    it('should throw error from take when failed with no buffered data', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      buffer.fail(new Error('test error'))

      await expect(buffer.take()).rejects.toThrow('test error')
    })

    it('should return buffered data first, then throw on next take', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      await buffer.put(makeData(['a'], 10))
      buffer.fail(new Error('test error'))

      const result = await buffer.take()
      expect(result?.blocks).toEqual(['a'])

      await expect(buffer.take()).rejects.toThrow('test error')
    })

    it('should throw on put after fail', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      buffer.fail(new Error('test'))

      await expect(buffer.put(makeData(['a']))).rejects.toThrow('Buffer is closed')
    })

    it('should abort the signal on fail', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      expect(buffer.signal.aborted).toBe(false)
      buffer.fail(new Error('fail'))
      expect(buffer.signal.aborted).toBe(true)
    })
  })

  describe('flush', () => {
    it('should flush buffered data immediately', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 300, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      await buffer.put(makeData(['a'], 10))
      buffer.flush()

      const result = await takePromise
      expect(result?.blocks).toEqual(['a'])

      buffer.close()
    })

    it('should be a no-op when buffer is empty', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 300, maxWaitTime: 5000 })

      // Should not throw
      buffer.flush()

      buffer.close()
    })
  })

  describe('iterate', () => {
    it('should yield all data until close', async () => {
      const buffer = new StreamBuffer<string>({
        maxBytes: 1000,
        maxIdleTime: 50,
        maxWaitTime: 5000,
      })

      const results: string[][] = []

      const iteratePromise = (async () => {
        for await (const data of buffer.iterate()) {
          results.push(data.blocks)
        }
      })()

      await buffer.put(makeData(['a', 'b'], 10))
      vi.advanceTimersByTime(50)
      // Let the microtask queue flush
      await vi.advanceTimersByTimeAsync(0)

      await buffer.put(makeData(['c'], 10))
      vi.advanceTimersByTime(50)
      await vi.advanceTimersByTimeAsync(0)

      buffer.close()
      await iteratePromise

      expect(results).toEqual([['a', 'b'], ['c']])
    })

    it('should stop iteration on return()', async () => {
      const buffer = new StreamBuffer<string>({
        maxBytes: 100,
        maxIdleTime: 50,
        maxWaitTime: 5000,
      })

      const iter = buffer.iterate()[Symbol.asyncIterator]()

      const putPromise = buffer.put(makeData(['a'], 200))
      const result = await iter.next()
      await putPromise

      expect(result.done).toBe(false)
      expect(result.value?.blocks).toEqual(['a'])

      const ret = await iter.return!()
      expect(ret.done).toBe(true)
    })

    it('should propagate errors via throw()', async () => {
      const buffer = new StreamBuffer<string>({
        maxBytes: 1000,
        maxIdleTime: 50,
        maxWaitTime: 5000,
      })

      const iter = buffer.iterate()[Symbol.asyncIterator]()

      await expect(iter.throw!(new Error('thrown'))).rejects.toThrow('thrown')
    })
  })

  describe('idle timeout reset', () => {
    it('should reset idle timeout on each put', async () => {
      const buffer = new StreamBuffer({
        maxBytes: 1000,
        maxIdleTime: 100,
        maxWaitTime: 5000,
      })

      const takePromise = buffer.take()

      await buffer.put(makeData(['a'], 10))
      // 80ms — not yet idle
      vi.advanceTimersByTime(80)
      await buffer.put(makeData(['b'], 10))
      // Another 80ms (160ms total, but only 80ms since last put)
      vi.advanceTimersByTime(80)

      // Should NOT have flushed yet — idle timer was reset
      await buffer.put(makeData(['c'], 10))

      // Now let it idle out
      vi.advanceTimersByTime(100)

      const result = await takePromise
      expect(result?.blocks).toEqual(['a', 'b', 'c'])

      buffer.close()
    })
  })

  describe('take with no data then close', () => {
    it('should resolve take with undefined when close is called while waiting', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      buffer.close()

      const result = await takePromise
      expect(result).toBeUndefined()
    })

    it('should resolve take with error when fail is called while waiting', async () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })

      const takePromise = buffer.take()

      buffer.fail(new Error('stream error'))

      await expect(takePromise).rejects.toThrow('stream error')
    })
  })

  describe('close / fail idempotency', () => {
    it('close is idempotent', () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })
      buffer.close()
      buffer.close() // should not throw
    })

    it('fail is idempotent', () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })
      buffer.fail(new Error('first'))
      buffer.fail(new Error('second')) // should not throw
    })

    it('fail after close is a no-op', () => {
      const buffer = new StreamBuffer({ maxBytes: 1000, maxIdleTime: 100, maxWaitTime: 5000 })
      buffer.close()
      buffer.fail(new Error('nope')) // should not throw or change state
    })
  })
})
