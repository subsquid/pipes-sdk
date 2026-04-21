import { describe, expect, it, vi } from 'vitest'

import { devRunner } from './runner.js'

// The runner builds its own logger from LOG_LEVEL, and every restart logs an error.
const quietTest = it.extend<{ quiet: void }>({
  quiet: [
    async ({}, use) => {
      vi.stubEnv('LOG_LEVEL', 'silent')
      await use()
      vi.unstubAllEnvs()
    },
    { auto: true },
  ],
})

function failingPipe(id: string, error: Error) {
  const handler = vi.fn(async () => {
    throw error
  })

  return { pipe: { id, params: {}, handler }, handler }
}

describe('devRunner', () => {
  quietTest('fails on the first error when retry is 0', async () => {
    const boom = new Error('boom')
    const { pipe, handler } = failingPipe('fail-fast', boom)

    const runner = devRunner([pipe], { retry: 0 })

    await expect(runner.start()).rejects.toBe(boom)
    expect(handler).toHaveBeenCalledTimes(1)
  })

  quietTest('makes 5 attempts when retry is not set', async () => {
    const boom = new Error('boom')
    const { pipe, handler } = failingPipe('default-retries', boom)

    const runner = devRunner([pipe])

    await expect(runner.start()).rejects.toBe(boom)
    expect(handler).toHaveBeenCalledTimes(5)
  })
})
