import { describe, expect, it } from 'vitest'

import { once, sleep } from './function.js'

describe('once', () => {
  it('runs the function once for concurrent callers and shares the result', async () => {
    // Guarding on the resolved value instead would let every caller that arrives before the first
    // one resolves start its own run — which for a lazily constructed client means several clients
    // for one configured endpoint.
    let runs = 0
    const load = once(async () => {
      runs++
      await sleep(20)
      return { id: runs }
    })

    const [a, b, c] = await Promise.all([load(), load(), load()])

    expect(runs).toBe(1)
    expect(a).toBe(b)
    expect(b).toBe(c)
  })

  it('keeps returning the same result afterwards', async () => {
    let runs = 0
    const load = once(async () => ({ id: ++runs }))

    expect(await load()).toBe(await load())
    expect(runs).toBe(1)
  })

  it('does not cache a failure', async () => {
    // A poisoned helper would make one transient failure permanent for the life of the process.
    let attempts = 0
    const load = once(async () => {
      attempts++
      if (attempts === 1) throw new Error('first attempt fails')
      return 'ok'
    })

    await expect(load()).rejects.toThrowError('first attempt fails')
    await expect(load()).resolves.toBe('ok')
    expect(attempts).toBe(2)
  })

  it('shares one failure with everyone already waiting', async () => {
    let attempts = 0
    const load = once(async () => {
      attempts++
      await sleep(10)
      throw new Error('boom')
    })

    const results = await Promise.allSettled([load(), load()])

    expect(results.every((r) => r.status === 'rejected')).toBe(true)
    expect(attempts).toBe(1)
  })
})
