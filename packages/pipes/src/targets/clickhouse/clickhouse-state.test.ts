import { describe, expect, it, vi } from 'vitest'

import { BatchContext, BlockCursor } from '~/core/index.js'
import { testLogger } from '~/testing/index.js'

import { ClickhouseState, isStartCursor, rollbackCursorOf } from './clickhouse-state.js'

/**
 * Unit tests for the finalized-head wiring in ClickhouseState, using a mocked store (no live
 * ClickHouse). `getCursor` hands the persisted finalized head back as resume state so the source
 * can seed its monotonic watermark; `saveCursor` persists the (already source-clamped) finalized
 * head + rollback chain verbatim. The clamp itself lives in the source now.
 */

function block(number: number, hash = `0x${number}`) {
  return { number, hash }
}

function fakeSpan(): any {
  const span: any = {
    measure: (_name: any, fn: any) => fn(span),
    start: () => span,
    end: () => {},
  }
  return span
}

function ctxFor(
  current: { number: number; hash: string },
  finalized: { number: number; hash: string } | undefined,
  rollbackChain: { number: number; hash: string }[],
): BatchContext {
  return {
    logger: testLogger(),
    profiler: fakeSpan(),
    stream: {
      head: { finalized },
      state: { current, rollbackChain },
    },
  } as unknown as BatchContext
}

function makeStore(persistedFinalized: { number: number; hash: string }) {
  const inserts: any[] = []
  const store = {
    client: { connectionParams: { database: 'default' } },
    query: async () => ({
      json: async () => [
        {
          current: JSON.stringify(block(50)),
          finalized: JSON.stringify(persistedFinalized),
          timestamp: '2026-10-28T10:00:00.000Z',
        },
      ],
    }),
    insert: async (args: any) => {
      inserts.push(args)
    },
    removeAllRowsByQuery: async () => {},
    command: async () => {},
  }

  return { store, inserts }
}

describe('ClickhouseState — options', () => {
  it('falls back to the default id when an explicit `undefined` is passed', () => {
    const { store } = makeStore(block(100))
    const state = new ClickhouseState(store as any, { id: undefined })

    // getCursor/cleanup/fork bind {id:String}; an undefined id would break that binding.
    expect(state.options.id).toBe('stream')
  })
})

describe('ClickhouseState — resume state & cursor persistence', () => {
  it('returns the persisted cursor and finalized head as TargetState', async () => {
    const { store } = makeStore(block(100))
    const state = new ClickhouseState(store as any, {})

    const resume = await state.getCursor()

    expect(resume).toEqual({ latest: block(50), finalized: block(100) })
  })

  it('persists the finalized head + rollback chain verbatim (the source already clamped them)', async () => {
    const { store, inserts } = makeStore(block(100))
    const state = new ClickhouseState(store as any, {})

    await state.saveCursor(ctxFor(block(130), block(120), [block(121)]))

    const row = inserts[0].values[0]
    expect(row.current).toBe(JSON.stringify(block(130)))
    expect(row.finalized).toBe(JSON.stringify(block(120)))
    expect(row.rollback_chain).toBe(JSON.stringify([block(121)]))
  })

  it('persists the empty sentinel when there is no finalized head', async () => {
    const { store, inserts } = makeStore(block(100))
    const state = new ClickhouseState(store as any, {})

    await state.saveCursor(ctxFor(block(130), undefined, []))

    const row = inserts[0].values[0]
    expect(row.finalized).toBe('')
    expect(row.rollback_chain).toBe(JSON.stringify([]))
  })
})

describe('ClickhouseState — fork', () => {
  function forkStore(rows: { rollback_chain: string; finalized: string }[]) {
    const queries: any[] = []
    const store = {
      client: { connectionParams: { database: 'default' } },
      query: async (args: any) => {
        queries.push(args)
        return {
          stream: async function* () {
            yield rows.map((r) => ({ json: () => r }))
          },
        }
      },
    }
    return { store, queries }
  }

  it('scopes the fork scan to its own stream id', async () => {
    const { store, queries } = forkStore([
      { rollback_chain: JSON.stringify([block(5), block(6)]), finalized: JSON.stringify(block(4)) },
    ])
    const state = new ClickhouseState(store as any, { id: 'pipe-a' })

    await state.fork([block(5), block(6, '0x6a')])

    // Must filter by id, like getCursor/cleanup — otherwise other streams sharing the table
    // would mix their rollback chains into this fork resolution.
    expect(queries[0].query).toMatch(/WHERE id = \{id:String\}/)
    expect(queries[0].query_params).toEqual({ id: 'pipe-a' })
  })

  it('skips offsets with no finalized head instead of crashing (matches postgres/bigquery)', async () => {
    // A source that never reported a finalized head persists finalized as '' with an empty
    // rollback chain; fork resolution must skip it gracefully, not crash on JSON.parse('').
    const { store } = forkStore([{ rollback_chain: '[]', finalized: '' }])
    const state = new ClickhouseState(store as any, {})

    await expect(state.fork([block(5)])).resolves.toBeNull()
  })

  it('resolves normally when finalized heads are present', async () => {
    const { store } = forkStore([
      { rollback_chain: JSON.stringify([block(5), block(6)]), finalized: JSON.stringify(block(4)) },
    ])
    const state = new ClickhouseState(store as any, {})

    const safe = await state.fork([block(5), block(6, '0x6a')])
    expect(safe).toEqual(block(5))
  })
})

// Store whose `query` answers for the id it was queried with — as the JSON latest-row read
// (getCursor) and as the streaming full-row read the legacy migration uses to re-key rows.
function keyedStore(rowsById: Record<string, { current: string; finalized: string; timestamp: string }>) {
  const inserts: any[] = []
  const removals: any[] = []
  const store = {
    client: { connectionParams: { database: 'default' } },
    query: async ({ query_params }: any) => {
      const row = rowsById[query_params?.id]

      return {
        json: async () => (row ? [row] : []),
        stream: async function* () {
          if (row) yield [{ json: () => ({ ...row, id: query_params.id }) }]
        },
      }
    },
    insert: async (args: any) => {
      inserts.push(args)
    },
    removeAllRowsByQuery: async (args: any) => {
      removals.push(args)
    },
    command: async () => {},
  }

  return { store, inserts, removals }
}

describe('ClickhouseState — cursor key binding & legacy migration', () => {
  const row = (current: number, finalized: number) => ({
    current: JSON.stringify(block(current)),
    finalized: JSON.stringify(block(finalized)),
    timestamp: '2026-10-28T10:00:00.000Z',
  })

  it('keys by the source id when no explicit settings.id is set', async () => {
    const { store, inserts } = keyedStore({ 'pipe-x': row(50, 40) })
    const state = new ClickhouseState(store as any, {})
    state.bindCursorKey('pipe-x')

    expect(state.cursorKey).toBe('pipe-x')
    await expect(state.getCursor()).resolves.toEqual({ latest: block(50), finalized: block(40) })

    await state.saveCursor(ctxFor(block(60), block(55), []))
    expect(inserts[0].values[0].id).toBe('pipe-x')
  })

  it('lets an explicit settings.id override the source id', async () => {
    const { store } = keyedStore({ pinned: row(10, 5) })
    const state = new ClickhouseState(store as any, { id: 'pinned' })
    state.bindCursorKey('pipe-x')

    expect(state.cursorKey).toBe('pinned')
    await expect(state.getCursor()).resolves.toEqual({ latest: block(10), finalized: block(5) })
  })

  it('automatically migrates a legacy "stream" cursor to the pipe id, and warns', async () => {
    const { store, inserts, removals } = keyedStore({ stream: row(99, 90) })
    const warn = vi.fn()
    const state = new ClickhouseState(store as any, {})
    state.bindCursorKey('pipe-x', { warn } as any)

    await expect(state.getCursor()).resolves.toEqual({ latest: block(99), finalized: block(90) })
    expect(warn).toHaveBeenCalledOnce()

    // The legacy rows are physically re-keyed in ONE atomic insert per batch: the copy under the
    // pipe id plus the sign=-1 cancellation of the original, so fork resolution and later starts
    // see only the pipe's key and a crash cannot leave the rows duplicated under both keys.
    expect(inserts).toHaveLength(1)
    expect(inserts[0].values).toEqual([
      expect.objectContaining({ id: 'pipe-x' }),
      expect.objectContaining({ id: 'stream', sign: -1 }),
    ])
    expect(removals).toHaveLength(0)
  })

  it("prefers the pipe's own cursor and leaves the legacy rows alone once it exists", async () => {
    const { store, inserts, removals } = keyedStore({ 'pipe-x': row(200, 190), stream: row(99, 90) })
    const state = new ClickhouseState(store as any, {})
    state.bindCursorKey('pipe-x')

    await expect(state.getCursor()).resolves.toEqual({ latest: block(200), finalized: block(190) })
    expect(inserts).toHaveLength(0)
    expect(removals).toHaveLength(0)
  })

  it('does not migrate the legacy cursor onto an explicit settings.id', async () => {
    // An explicitly pinned key deliberately names its own cursor — inheriting the shared
    // legacy cursor could resume a fresh pipe from a foreign position.
    const { store, removals } = keyedStore({ stream: row(99, 90) })
    const state = new ClickhouseState(store as any, { id: 'pinned' })
    state.bindCursorKey('pipe-x')

    await expect(state.getCursor()).resolves.toBeUndefined()
    expect(removals).toHaveLength(0)
  })

  it('reads the legacy key in place when the cursor key IS the legacy id', async () => {
    const { store, inserts, removals } = keyedStore({ stream: row(99, 90) })
    const state = new ClickhouseState(store as any, {})
    state.bindCursorKey('stream')

    expect(state.cursorKey).toBe('stream')
    await expect(state.getCursor()).resolves.toEqual({ latest: block(99), finalized: block(90) })
    expect(inserts).toHaveLength(0)
    expect(removals).toHaveLength(0)
  })

  it('starts fresh when neither the pipe key nor the legacy key holds a cursor', async () => {
    const { store, removals } = keyedStore({})
    const state = new ClickhouseState(store as any, {})
    state.bindCursorKey('pipe-x')

    await expect(state.getCursor()).resolves.toBeUndefined()
    expect(removals).toHaveLength(0)
  })
})

describe('ClickhouseState — row stamps', () => {
  /** The stamp of a stored row 4 s ahead of the clock, as on a host whose clock went back across a restart. */
  const withClockBehind = it.extend<{ stored: number }>({
    stored: async ({}, use) => {
      const clock = vi.spyOn(Date, 'now').mockReturnValue(1_000)

      await use(5_000)

      clock.mockRestore()
    },
  })

  const rowAt = (stamp: number) => ({
    current: JSON.stringify(block(50)),
    finalized: JSON.stringify(block(40)),
    timestamp: new Date(stamp).toISOString(),
  })

  withClockBehind('stamps a resumed run past the newest stored row, though the clock went back', async ({ stored }) => {
    const { store, inserts } = keyedStore({ 'pipe-x': rowAt(stored) })
    const state = new ClickhouseState(store as any, {})
    state.bindCursorKey('pipe-x')

    await state.getCursor()
    await state.saveCursor(ctxFor(block(60), block(55), []))

    expect(inserts[0].values[0].timestamp).toBe(stored + 1)
  })

  withClockBehind('stamps a run past the legacy rows it migrated', async ({ stored }) => {
    const { store, inserts } = keyedStore({ stream: rowAt(stored) })
    const state = new ClickhouseState(store as any, {})
    state.bindCursorKey('pipe-x', { warn: () => {} } as any)

    await state.getCursor()
    await state.saveCursor(ctxFor(block(60), block(55), []))
    const [, cursorInsert] = inserts

    expect(cursorInsert.values[0].timestamp).toBe(stored + 1)
  })
})

describe('ClickhouseState — start cursor', () => {
  /** A clock stopped at one millisecond, so every row a test writes would share its timestamp. */
  const withStoppedClock = it.extend<{ now: number }>({
    now: async ({}, use) => {
      const clock = vi.spyOn(Date, 'now').mockReturnValue(1_000)

      await use(1_000)

      clock.mockRestore()
    },
  })

  it('writes one block below the range start, marked with the first batch end', async () => {
    const { store, inserts } = makeStore(block(100))
    const state = new ClickhouseState(store as any, { id: 'pipe' })

    await state.saveStartCursor({ from: 0, to: 5 })

    expect(inserts[0].values[0]).toMatchObject({
      id: 'pipe',
      current: '{"kind":"start","number":-1,"batchEnd":5}',
      finalized: '',
      rollback_chain: '[]',
      sign: 1,
    })
  })

  it('tells a start cursor by its kind, not by a missing hash or a bound', () => {
    const { store } = makeStore(block(100))
    const state = new ClickhouseState(store as any, {})
    const start = state.decodeCursor('{"kind":"start","number":-1,"batchEnd":5}')

    expect(isStartCursor(start)).toBe(true)
    expect(isStartCursor(block(5))).toBe(false)
    expect(isStartCursor({ number: 5 })).toBe(false)
    expect(isStartCursor({ number: -1, upTo: 5 } as BlockCursor)).toBe(false)
  })

  it('hands the hook a start cursor as its number with the batch end as upTo, and a batch cursor as is', () => {
    const batch = block(5)

    expect(rollbackCursorOf({ kind: 'start', number: -1, batchEnd: 5 })).toStrictEqual({ number: -1, upTo: 5 })
    expect(rollbackCursorOf(batch)).toBe(batch)
  })

  it('resuming from a start cursor, cancels it in the insert of its own', async () => {
    const stored = {
      id: 'pipe',
      current: '{"kind":"start","number":-1,"batchEnd":3}',
      finalized: '',
      rollback_chain: '[]',
      timestamp: '2026-10-28T10:00:00.000Z',
      sign: 1,
    }
    const inserts: any[] = []
    const store = {
      client: { connectionParams: { database: 'default' } },
      query: async () => ({ json: async () => [stored] }),
      insert: async (args: any) => {
        inserts.push(args)
      },
    }
    const state = new ClickhouseState(store as any, { id: 'pipe' })

    await state.getCursor()
    await state.saveStartCursor({ from: 0, to: 7 })
    await state.saveStartCursor({ from: 0, to: 9 })
    const [replaced, next] = inserts.map((insert) => insert.values)

    expect(replaced).toMatchObject([
      { ...stored, sign: -1 },
      { current: '{"kind":"start","number":-1,"batchEnd":7}', sign: 1 },
    ])
    expect(next).toHaveLength(1)
  })

  withStoppedClock('stamps each row later than the one before, within one millisecond too', async ({ now }) => {
    const { store, inserts } = makeStore(block(100))
    const state = new ClickhouseState(store as any, {})

    await state.saveStartCursor({ from: 0, to: 5 })
    await state.saveCursor(ctxFor(block(5), block(2), []))
    await state.saveCursor(ctxFor(block(9), block(2), []))
    const stamps = inserts.map((insert) => insert.values[0].timestamp)

    expect(stamps).toEqual([now, now + 1, now + 2])
  })
})
