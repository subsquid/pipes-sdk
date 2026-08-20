import { describe, expect, it } from 'vitest'

import { safeReturn, withTimeout } from './fallback-async.js'

const wait = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

/**
 * Run `fn` while capturing any `unhandledRejection` — the promise-hygiene edges (an abandoned
 * timed-out promise, a rejecting `return()`) must never leak one.
 */
async function withoutUnhandledRejections(fn: () => Promise<void>): Promise<unknown[]> {
  const unhandled: unknown[] = []
  const onUnhandled = (e: unknown) => unhandled.push(e)
  process.on('unhandledRejection', onUnhandled)
  try {
    await fn()
    await wait(30) // let any abandoned promise settle
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
  return unhandled
}

describe('withTimeout', () => {
  it('resolves with the value when it settles before the timeout', async () => {
    await expect(withTimeout(Promise.resolve(42), 1000, () => new Error('nope'))).resolves.toBe(42)
  })

  it('rejects with makeError() when the timeout fires first', async () => {
    const never = new Promise<never>(() => {})
    await expect(withTimeout(never, 5, () => new Error('timed out'))).rejects.toThrow('timed out')
  })

  it('carries an arbitrary (non-Error) rejection value from makeError', async () => {
    const cause = { reason: 'timeout' as const, code: 1 } // e.g. a classified SourceErrorInfo
    await expect(withTimeout(new Promise<never>(() => {}), 5, () => cause)).rejects.toBe(cause)
  })

  it('returns the promise unchanged when ms is null (guard disabled)', () => {
    const p = Promise.resolve('v')
    expect(withTimeout(p, null, () => new Error('nope'))).toBe(p) // identity: no race wrapper
  })

  it('does not surface an unhandled rejection from the promise abandoned after a timeout', async () => {
    const unhandled = await withoutUnhandledRejections(async () => {
      // rejects only AFTER the timeout already fired — withTimeout must have silenced it
      const rejectsLate = new Promise((_r, reject) => setTimeout(() => reject(new Error('late')), 5))
      await expect(withTimeout(rejectsLate, 1, () => new Error('timed out'))).rejects.toThrow('timed out')
    })
    expect(unhandled).toHaveLength(0)
  })
})

describe('safeReturn', () => {
  it('closes an iterator via return()', async () => {
    let closed = false
    const it: AsyncIterator<unknown> = {
      next: async () => ({ done: true, value: undefined }),
      return: async () => {
        closed = true
        return { done: true, value: undefined }
      },
    }
    safeReturn(it)
    await wait(0)
    expect(closed).toBe(true)
  })

  it('swallows a rejecting return() (never throws, never leaks)', async () => {
    const unhandled = await withoutUnhandledRejections(async () => {
      const it: AsyncIterator<unknown> = {
        next: async () => ({ done: true, value: undefined }),
        return: async () => {
          throw new Error('return boom')
        },
      }
      expect(() => safeReturn(it)).not.toThrow()
    })
    expect(unhandled).toHaveLength(0)
  })

  it('tolerates an iterator without a return() method', () => {
    const it = { next: async () => ({ done: true, value: undefined }) } as AsyncIterator<unknown>
    expect(() => safeReturn(it)).not.toThrow()
  })
})
