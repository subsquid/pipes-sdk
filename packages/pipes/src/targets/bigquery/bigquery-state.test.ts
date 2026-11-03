import { describe, expect, it, vi } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { BigQuerySyncState } from './bigquery-state.js'

/**
 * Unit tests for the finalized-head wiring in BigQuerySyncState, using a mocked store (no live
 * BigQuery). `getCursor` hands the persisted finalized head back as resume state so the source can
 * seed its monotonic watermark — the clamp itself now lives in the source (see portal-source and
 * finalized-watermark tests), not in the target.
 */

function block(number: number, hash = `0x${number}`) {
  return { number, hash }
}

function committedRow(finalized: { number: number; hash: string } | null) {
  return {
    id: 'stream',
    op: 'commit',
    current: JSON.stringify(block(50)),
    finalized: finalized ? JSON.stringify(finalized) : null,
    rollback_chain: '[]',
    range_low: null,
    range_high: null,
    committed: true,
    timestamp: 1,
    timestamp_micros: 1,
  }
}

function stateWith(row: ReturnType<typeof committedRow>) {
  const store = {
    query: async () => [row],
  }

  return new BigQuerySyncState({
    store: store as any,
    bigquery: {} as any,
    trackedTables: [],
    options: { projectId: 'p', dataset: 'd' },
  })
}

describe('BigQuerySyncState — resume state', () => {
  it('returns the persisted cursor and finalized head as TargetState', async () => {
    const state = stateWith(committedRow(block(100)))

    const resume = await state.getCursor({ logger: testLogger() })

    expect(resume).toEqual({ latest: block(50), finalized: block(100) })
  })

  it('returns finalized: null when no finalized head was persisted', async () => {
    const state = stateWith(committedRow(null))

    const resume = await state.getCursor({ logger: testLogger() })

    expect(resume).toEqual({ latest: block(50), finalized: null })
  })
})

describe('BigQuerySyncState — row stamps', () => {
  /** The stamp of a stored row 4 s ahead of the clock, as on a host whose clock went back across a restart. */
  const withClockBehind = it.extend<{ stored: number }>({
    stored: async ({}, use) => {
      const clock = vi.spyOn(Date, 'now').mockReturnValue(1_000)

      await use(5_000_000)

      clock.mockRestore()
    },
  })

  function resumedState(latest: Record<string, unknown>) {
    const walRows: Record<string, unknown>[] = []
    const store = {
      query: async () => [latest],
      commitSyncRow: async (_schema: unknown, row: Record<string, unknown>) => {
        walRows.push(row)
      },
      executeDml: async () => ({ rowCount: 0 }),
    }

    const state = new BigQuerySyncState({
      store: store as any,
      bigquery: {} as any,
      trackedTables: [],
      options: { projectId: 'p', dataset: 'd' },
    })

    return { state, walRows }
  }

  withClockBehind('stamps a resumed run past the newest stored row, though the clock went back', async ({ stored }) => {
    const { state, walRows } = resumedState({ ...committedRow(block(40)), timestamp_micros: stored })

    await state.getCursor({ logger: testLogger() })
    await state.saveCommitPre({
      cursor: block(50),
      finalized: undefined,
      rollbackChain: [],
      range: { low: 51, high: 60 },
    })
    await state.saveCommitPost({ logger: testLogger(), cursor: block(60), finalized: undefined, rollbackChain: [] })
    const stamps = walRows.map((row) => row['timestamp'])

    expect(stamps).toEqual([stored + 1, stored + 2])
  })

  withClockBehind('stamps the recovery row of an unfinished write past it', async ({ stored }) => {
    const inFlight = { ...committedRow(block(40)), committed: false, range_low: 51, range_high: 60 }
    const { state, walRows } = resumedState({ ...inFlight, timestamp_micros: stored })

    await state.getCursor({ logger: testLogger() })

    expect(walRows).toMatchObject([{ op: 'rollback', committed: true, timestamp: stored + 1 }])
  })
})

describe('BigQuerySyncState — cursor key binding', () => {
  function keyedState(options: { id?: string } = {}) {
    const queryParams: any[] = []
    const walRows: any[] = []
    const store = {
      query: async (_sql: string, params?: Record<string, unknown>) => {
        queryParams.push(params)

        return []
      },
      commitSyncRow: async (_schema: unknown, row: Record<string, unknown>) => {
        walRows.push(row)
      },
      executeDml: async () => ({ rowCount: 0 }),
    }

    const state = new BigQuerySyncState({
      store: store as any,
      bigquery: {} as any,
      trackedTables: [],
      options: { projectId: 'p', dataset: 'd', ...options },
    })

    return { state, queryParams, walRows }
  }

  it('keys reads and WAL writes by the source id when no explicit id is set', async () => {
    const { state, queryParams, walRows } = keyedState()
    state.bindCursorKey('pipe-x')

    expect(state.cursorKey).toBe('pipe-x')

    await state.getCursor({ logger: testLogger() })
    expect(queryParams[0]).toEqual({ id: 'pipe-x' })

    await state.saveCommitPost({
      logger: testLogger(),
      cursor: block(1),
      finalized: undefined,
      rollbackChain: [],
    })
    expect(walRows[0].id).toBe('pipe-x')
  })

  it('lets an explicit options.id override the source id', () => {
    const { state } = keyedState({ id: 'pinned' })
    state.bindCursorKey('pipe-x')

    expect(state.cursorKey).toBe('pinned')
  })

  it('falls back to the default key when no source id is bound', () => {
    const { state } = keyedState()
    state.bindCursorKey(undefined)

    expect(state.cursorKey).toBe('stream')
  })
})

describe('BigQuerySyncState — fork records', () => {
  function syncRow(
    rollbackChain: { number: number; hash: string }[],
    finalized: { number: number; hash: string },
    timestamp: number,
  ) {
    return { ...committedRow(finalized), rollback_chain: JSON.stringify(rollbackChain), timestamp }
  }

  // Newest first: the match, block 5, comes before the record that holds the highest floor.
  const rows = [syncRow([block(5), block(7)], block(4), 2), syncRow([], block(6), 1)]

  /** Answers like BigQuery: newest first, below the cutoff, at most `limit` rows. */
  function pagedState(maxRows?: number) {
    const queries: Record<string, unknown>[] = []
    const store = {
      query: async (_sql: string, params: Record<string, unknown>) => {
        queries.push(params)
        const cutoff = params['cutoff'] as number | undefined
        const page = rows.filter((row) => cutoff === undefined || row.timestamp < cutoff)

        return page.slice(0, params['limit'] as number)
      },
    }

    const state = new BigQuerySyncState({
      store: store as any,
      bigquery: {} as any,
      trackedTables: [],
      options: { projectId: 'p', dataset: 'd', maxRows },
    })

    return { state, queries }
  }

  it('reads the floor from a later page before it resolves the fork', async () => {
    const { state, queries } = pagedState(1)

    const { safeCursor } = await state.fork([block(5), block(6, '0x6b'), block(7, '0x7b')])

    expect(safeCursor).toBeNull()
    expect(queries).toHaveLength(3)
  })

  it('reads every record in one query when they fit in maxRows', async () => {
    const { state, queries } = pagedState()

    const { safeCursor } = await state.fork([block(5), block(6, '0x6b'), block(7, '0x7b')])

    expect(safeCursor).toBeNull()
    expect(queries).toHaveLength(1)
  })
})
