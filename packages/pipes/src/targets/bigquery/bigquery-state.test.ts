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

  it('refuses to bind without a source id or an explicit key', () => {
    const { state } = keyedState()

    expect(() => state.bindCursorKey(undefined)).toThrow(expect.objectContaining({ code: 'E0001' }))
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

  // A floor low enough that a match at block 5 stays above it.
  const lowFloorRows = [syncRow([block(5), block(7)], block(4), 2), syncRow([], block(4), 1)]

  /** Answers like BigQuery: newest first, below the cutoff, at most `limit` rows. */
  function pagedState({
    maxRows,
    records = rows,
    commitSyncRow = async () => {},
  }: {
    maxRows?: number
    records?: ReturnType<typeof syncRow>[]
    commitSyncRow?: (schema: unknown, row: Record<string, unknown>) => Promise<void>
  } = {}) {
    const queries: Record<string, unknown>[] = []
    const store = {
      query: async (_sql: string, params: Record<string, unknown>) => {
        queries.push(params)
        const cutoff = params['cutoff'] as number | undefined
        const page = records.filter((row) => cutoff === undefined || row.timestamp < cutoff)

        return page.slice(0, params['limit'] as number)
      },
      commitSyncRow,
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
    const { state, queries } = pagedState({ maxRows: 1 })

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

  it('bounds the delete by the retained top when the canonical set ends below it', async () => {
    const { state } = pagedState({ records: lowFloorRows })

    // The window ends at 6, the records hold block 7: rows in (6, 7] must still go.
    const { safeCursor, upper } = await state.fork([block(5), block(6, '0x6b')])

    expect(safeCursor).toEqual(block(5))
    expect(upper).toBe(7)
  })

  it('counts the committed cursor in the delete bound', async () => {
    const { state } = pagedState({ records: lowFloorRows })

    await state.saveCommitPost({
      logger: testLogger(),
      cursor: block(9),
      finalized: block(4),
      rollbackChain: [block(9)],
    })

    const { upper } = await state.fork([block(5), block(6, '0x6b')])

    expect(upper).toBe(9)
  })

  it('rides the own top through the rollback WAL, so recovery re-deletes to it after a crash', async () => {
    const walRows: Record<string, unknown>[] = []
    const live = pagedState({
      records: lowFloorRows,
      commitSyncRow: async (_schema: unknown, row: Record<string, unknown>) => {
        walRows.push(row)
      },
    })

    await live.state.saveCommitPost({
      logger: testLogger(),
      cursor: block(9),
      finalized: block(4),
      rollbackChain: [block(9)],
    })

    // A crash between the rollback WAL row and the per-table DELETEs leaves the row in flight.
    const { safeCursor, upper } = await live.state.fork([block(5), block(6, '0x6b')])
    await live.state.saveRollbackPre({
      cursor: safeCursor!,
      finalized: undefined,
      rollbackChain: [],
      range: { low: safeCursor!.number + 1, high: upper },
    })

    const inFlight = walRows.at(-1)
    expect(inFlight).toMatchObject({ op: 'rollback', committed: false, range_low: 6, range_high: 9 })

    // The restart sees the unfinished rollback and must delete every row up to the own top, 9.
    const deletes: Record<string, unknown>[] = []
    const restarted = new BigQuerySyncState({
      store: {
        query: async () => [inFlight],
        commitSyncRow: async () => {},
        executeDml: async (_sql: string, params: Record<string, unknown>) => {
          deletes.push(params)
          return { rowCount: 0 }
        },
      } as any,
      bigquery: {} as any,
      trackedTables: [{ fqn: 'p.d.events', blockNumberColumn: 'block_number' }] as any,
      options: { projectId: 'p', dataset: 'd' },
    })

    const resume = await restarted.getCursor({ logger: testLogger() })

    expect(deletes).toEqual([{ low: 6, high: 9 }])
    expect(resume).toEqual({ latest: block(5), finalized: null })
  })
})

describe('BigQuerySyncState — corrupt recovery ranges', () => {
  it.each([null, '12garbage', '', '1.5', -1, 1.5, Number.MAX_SAFE_INTEGER + 1])(
    'refuses invalid low bound %j before any DELETE or recovery marker',
    async (low) => {
      const executeDml = vi.fn()
      const commitSyncRow = vi.fn()
      const row = { ...committedRow(block(40)), committed: false, range_low: low, range_high: 60 }
      const store = { query: async () => [row], executeDml, commitSyncRow }
      const state = new BigQuerySyncState({
        store: store as any,
        bigquery: {} as any,
        trackedTables: [{ fqn: 'p.d.events', blockNumberColumn: 'blockNumber' }] as any,
        options: { projectId: 'p', dataset: 'd' },
      })
      await expect(state.getCursor({ logger: testLogger() })).rejects.toMatchObject({ code: 'E2211' })
      expect(executeDml).not.toHaveBeenCalled()
      expect(commitSyncRow).not.toHaveBeenCalled()
    },
  )
})

describe('BigQuerySyncState — persisted format round-trip', () => {
  it.each([undefined, block(40)])('reads a commit from a fresh state instance (floor %j)', async (floor) => {
    const rows: Record<string, unknown>[] = []
    const store = {
      query: async () => rows.slice(-1),
      commitSyncRow: async (_schema: unknown, row: Record<string, unknown>) =>
        rows.push({ ...row, timestamp_micros: row['timestamp'] }),
      executeDml: async () => ({ rowCount: 0 }),
    }
    const open = () =>
      new BigQuerySyncState({
        store: store as any,
        bigquery: {} as any,
        trackedTables: [],
        options: { projectId: 'p', dataset: 'd' },
      })
    const current = { ...block(50), timestamp: 1_700_000_000 }
    const chain = floor ? [block(41), current] : []
    await open().saveCommitPost({ logger: testLogger(), cursor: current, finalized: floor, rollbackChain: chain })
    expect(rows[0]).toMatchObject({
      id: 'stream',
      op: 'commit',
      committed: true,
      current: JSON.stringify(current),
      finalized: floor ? JSON.stringify(floor) : null,
      rollback_chain: JSON.stringify(chain),
      range_low: null,
      range_high: null,
    })
    expect(await open().getCursor({ logger: testLogger() })).toEqual({ latest: current, finalized: floor ?? null })
  })
})

describe('bigquery — malformed persisted cursor', () => {
  it.each(['not-json', '{}', '{"number":-2}', '{"number":1.5}', '{"number":"7"}', '{"number":7,"hash":42}'])(
    'refuses %s before mutating the store',
    async (raw) => {
      const executeDml = vi.fn()
      const commitSyncRow = vi.fn()
      const store = { query: async () => [{ ...committedRow(block(40)), current: raw }], executeDml, commitSyncRow }
      const state = new BigQuerySyncState({
        store: store as any,
        bigquery: {} as any,
        trackedTables: [],
        options: { projectId: 'p', dataset: 'd' },
      })
      await expect(state.getCursor({ logger: testLogger() })).rejects.toMatchObject({ code: 'E2214' })
      expect(executeDml).not.toHaveBeenCalled()
      expect(commitSyncRow).not.toHaveBeenCalled()
    },
  )
})
