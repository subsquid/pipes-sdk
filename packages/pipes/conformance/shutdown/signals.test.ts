import { fileURLToPath } from 'node:url'

import { describe, expect } from 'vitest'

import { mockBlock } from '~/testing/evm/index.js'
import { type MockChainPortal, mockPortal } from '~/testing/index.js'

import { type StartChild, withChild } from './spawn.js'
import type { RunEntry, RunMode } from './stopped-run.js'

const STOPPED_RUN = fileURLToPath(new URL('./stopped-run.ts', import.meta.url))

const LAST_SERVED = 3
const RANGE_END = 8

/** Serves blocks up to LAST_SERVED, then holds every further request, so the child sits in an idle read. */
const it = withChild.extend<{ chain: MockChainPortal }>({
  chain: async ({}, use) => {
    const chain = await mockPortal({
      blocks: Array.from({ length: RANGE_END }, (_, i) => mockBlock({ number: i + 1 })),
      blocksPerResponse: 1,
      delay: ({ query }) => (query.fromBlock > LAST_SERVED ? new Promise<void>(() => {}) : undefined),
    })

    await use(chain)
    await chain.close()
  },
})

function startStopped(
  startChild: StartChild,
  chain: MockChainPortal,
  { to = RANGE_END, ...mode }: RunMode & { to?: number },
) {
  return startChild<RunEntry>(STOPPED_RUN, { url: chain.url, to, mode })
}

const committedOf = (entries: RunEntry[]) => entries.flatMap((entry) => ('committed' in entry ? [entry.committed] : []))

const reachedIdle = (entry: RunEntry) => 'committed' in entry && entry.committed === LAST_SERVED

describe('pipeTo under real signals', () => {
  it.for(['SIGTERM', 'SIGINT'] as const)(
    '%s: the application handles the stop, and the process ends by itself',
    async (signal, { startChild, chain }) => {
      const run = await startStopped(startChild, chain, { application: 'catching', shutdown: '30s' })

      await run.entry(reachedIdle)
      run.kill(signal)

      const exit = await run.exit
      const entries = await run.journal()

      expect(exit.code, exit.output).toBe(0)
      expect(committedOf(entries).at(-1)).toBe(LAST_SERVED)
      expect(entries.slice(-2)).toEqual([{ callback: true }, { rejected: 'E3001', signal }])
    },
  )

  it.for(['SIGTERM', 'SIGINT'] as const)(
    '%s: a plain application exits 1 with E3001, and skips its completion code',
    async (signal, { startChild, chain }) => {
      const run = await startStopped(startChild, chain, { application: 'plain', shutdown: '30s' })

      await run.entry(reachedIdle)
      run.kill(signal)

      const exit = await run.exit
      const entries = await run.journal()

      expect(exit.code, exit.output).toBe(1)
      expect(exit.output).toContain('E3001')
      expect(entries).not.toContainEqual({ completed: true })
      expect(entries).toContainEqual({ callback: true })
    },
  )

  it('a repeated signal does not disturb the drain, and the callback runs once', async ({ startChild, chain }) => {
    const run = await startStopped(startChild, chain, { application: 'catching', shutdown: '30s' })

    await run.entry(reachedIdle)
    run.kill('SIGTERM')
    run.kill('SIGTERM')
    run.kill('SIGINT')

    const exit = await run.exit
    const callbacks = (await run.journal()).filter((entry) => 'callback' in entry)

    expect(exit.code, exit.output).toBe(0)
    expect(callbacks).toHaveLength(1)
  })

  it('a range that completes resolves, and the callback does not run', async ({ startChild, chain }) => {
    const run = await startStopped(startChild, chain, { application: 'plain', shutdown: '30s', to: LAST_SERVED })

    const exit = await run.exit
    const entries = await run.journal()

    expect(exit.code, exit.output).toBe(0)
    expect(entries.at(-1)).toEqual({ completed: true })
    expect(entries).not.toContainEqual({ callback: true })
  })

  it('`shutdown: false` leaves the signal its default action', async ({ startChild, chain }) => {
    const run = await startStopped(startChild, chain, { application: 'plain', shutdown: false })

    await run.entry(reachedIdle)
    run.kill('SIGTERM')

    const exit = await run.exit
    const entries = await run.journal()

    expect(exit.signal, exit.output).toBe('SIGTERM')
    expect(entries).not.toContainEqual({ callback: true })
    expect(entries).not.toContainEqual({ completed: true })
  })

  it('a commit past the deadline: a plain application exits 1 with E3002', async ({ startChild, chain }) => {
    const run = await startStopped(startChild, chain, { application: 'plain', shutdown: '100ms', wedged: true })

    await run.entry((entry) => 'committed' in entry)
    run.kill('SIGTERM')

    const exit = await run.exit
    const entries = await run.journal()

    expect(exit.code, exit.output).toBe(1)
    expect(exit.output).toContain('E3002')
    expect(entries).not.toContainEqual({ callback: true })
    expect(entries).not.toContainEqual({ completed: true })
  })

  it('after the deadline the SDK has left: a process that stays alive dies on the next signal', async ({
    startChild,
    chain,
  }) => {
    const run = await startStopped(startChild, chain, { application: 'staying-alive', shutdown: '100ms', wedged: true })

    await run.entry((entry) => 'committed' in entry)
    run.kill('SIGTERM')

    await run.entry((entry) => 'rejected' in entry && entry.rejected === 'E3002')
    expect(await run.journal()).not.toContainEqual({ callback: true })
    run.kill('SIGTERM')

    const exit = await run.exit

    expect(exit.signal, exit.output).toBe('SIGTERM')
  })
})
