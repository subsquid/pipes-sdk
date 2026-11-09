import { PgDialect } from 'drizzle-orm/pg-core'
import { describe, expect, it, vi } from 'vitest'

import { BatchContext } from '~/core/index.js'
import { testLogger } from '~/testing/index.js'

import { PostgresState } from './postgres-state.js'

/**
 * Unit tests for the finalized-head wiring in PostgresState, using a mocked pg client +
 * transaction (no live Postgres). `getCursor` hands the persisted finalized head back as resume
 * state so the source can seed its monotonic watermark; `saveCursor` persists the (already
 * source-clamped) finalized head + rollback chain verbatim. The clamp itself lives in the source.
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

/** Extract the values bound into a captured drizzle SQL template. */
function paramsOf(sql: any): unknown[] {
  return new PgDialect().sqlToQuery(sql).params
}

/** Render a captured drizzle SQL template to its parameterised text. */
function sqlTextOf(sql: any): string {
  return new PgDialect().sqlToQuery(sql).sql
}

async function seededState(persistedFinalized: unknown) {
  const client = {
    query: async () => ({
      rows: [
        {
          id: 'stream',
          current_number: '50',
          current_hash: '0x50',
          current_timestamp: null,
          finalized: persistedFinalized,
          rollback_chain: [],
        },
      ],
    }),
  }
  const state = new PostgresState(client as any, { unfinalizedBlocksRetention: 1000 })
  await state.getCursor({ logger: testLogger() })

  return state
}

describe('PostgresState — resume state & cursor persistence', () => {
  it('returns the persisted cursor and finalized head as TargetState', async () => {
    const state = await seededState(block(100))

    const resume = await state.getCursor({ logger: testLogger() })

    expect(resume).toEqual({ latest: { number: 50, hash: '0x50' }, finalized: block(100) })
  })

  it('returns finalized: null when no finalized head was persisted', async () => {
    const state = await seededState({})

    const resume = await state.getCursor({ logger: testLogger() })

    expect(resume).toEqual({ latest: { number: 50, hash: '0x50' }, finalized: null })
  })

  it('reads SQL NULL for an optional hash and timestamp as absent cursor fields', async () => {
    const client = {
      query: async () => ({
        rows: [
          {
            current_number: '0',
            current_hash: null,
            current_timestamp: null,
            finalized: {},
            rollback_chain: [],
          },
        ],
      }),
    }
    const state = new PostgresState(client as any, {})
    expect(await state.getCursor({ logger: testLogger() })).toEqual({
      latest: { number: 0, hash: undefined, timestamp: undefined },
      finalized: null,
    })
  })

  it.each([undefined, block(120)])('round-trips bound state through a fresh reader (floor %j)', async (floor) => {
    const state = await seededState(block(100))
    const current = { ...block(130), timestamp: 1_700_000_000 }
    const chain = floor ? [block(121), block(130)] : []
    const executed: any[] = []
    const tx = {
      execute: async (sql: any) => {
        executed.push(sql)
        return { rowCount: 0 }
      },
    }
    await state.saveCursor(tx as any, ctxFor(current, floor, chain))
    const [id, number, hash, timestamp, finalized, rollbackChain] = paramsOf(executed[0])
    expect([id, number, hash, timestamp, finalized, rollbackChain]).toEqual([
      'stream',
      130,
      '0x130',
      new Date(1_700_000_000_000),
      JSON.stringify(floor ?? {}),
      JSON.stringify(chain),
    ])
    const reader = new PostgresState(
      {
        query: async () => ({
          rows: [
            {
              id,
              current_number: String(number),
              current_hash: hash,
              current_timestamp: timestamp,
              finalized: JSON.parse(finalized as string),
              rollback_chain: JSON.parse(rollbackChain as string),
            },
          ],
        }),
      } as any,
      {},
    )
    expect(await reader.getCursor({ logger: testLogger() })).toEqual({ latest: current, finalized: floor ?? null })
  })

  // Falling back to the current block sweeps away the cursor rows a deep fork needs.
  it('prunes nothing when the finalized head is block 0', async () => {
    const client = { query: async () => ({ rows: [] }) }
    const state = new PostgresState(client as any, { unfinalizedBlocksRetention: 1000 })

    const tx = { execute: async () => ({ rowCount: 0 }) }

    // The first save is one of the two that run the retention sweep.
    const { safeBlockNumber } = await state.saveCursor(tx as any, ctxFor(block(5000), block(0), []))

    expect(safeBlockNumber).toBe(0)
  })
})

describe('PostgresState — fork', () => {
  function forkState(rows: { rollback_chain: unknown; finalized: unknown }[]) {
    let call = 0
    const client = {
      // fork() paginates; serve the rows on the first page, then an empty page to terminate.
      query: async () => {
        const page = call === 0 ? rows : []
        call++

        return { rows: page }
      },
    }

    return new PostgresState(client as any, {})
  }

  it('resolves the safe cursor from the persisted rollback chains', async () => {
    const state = forkState([{ rollback_chain: [block(5), block(6)], finalized: block(4) }])

    const safe = await state.fork([block(5), block(6, '0x6a')])

    expect(safe).toEqual(block(5))
  })

  it('skips offsets with no finalized head ({}) without crashing', async () => {
    // A source that never reported a finalized head persists `{}` with an empty rollback chain;
    // fork resolution must skip it gracefully (jsonb, so no JSON.parse crash), like ClickHouse.
    const state = forkState([{ rollback_chain: [], finalized: {} }])

    await expect(state.fork([block(5)])).resolves.toBeNull()
  })

  it('persists the fork ancestor with the current floor and a trimmed rollback chain', async () => {
    const state = await seededState(block(120))
    const executed: any[] = []
    const tx = {
      execute: async (query: any) => {
        executed.push(query)
        if (executed.length === 1) {
          return {
            rows: [
              {
                current_number: '130',
                finalized: block(120),
                rollback_chain: [block(121), block(122), block(130)],
              },
            ],
          }
        }
        return { rows: [] }
      },
    }

    await state.saveForkCursor(tx as any, block(122))

    expect(sqlTextOf(executed[0])).toMatch(/"current_number" >= COALESCE/i)
    expect(paramsOf(executed[0])).toEqual(['stream', 'stream', 122])
    expect(sqlTextOf(executed[1])).toMatch(/ON CONFLICT \(id, current_number\) DO UPDATE/i)
    expect(paramsOf(executed[1])).toEqual([
      'stream',
      122,
      '0x122',
      JSON.stringify(block(120)),
      JSON.stringify([block(121), block(122)]),
    ])
  })
})

describe('PostgresState — cursor key binding', () => {
  it('keys reads and saves by the source id when no explicit id is set', async () => {
    const queries: { params?: unknown[] }[] = []
    const client = {
      query: async (_text: string, params?: unknown[]) => {
        queries.push({ params })

        return { rows: [] }
      },
    }
    const state = new PostgresState(client as any, {})
    state.bindCursorKey('pipe-x')

    expect(state.cursorKey).toBe('pipe-x')

    await state.getCursor({ logger: testLogger() })
    expect(queries[0].params).toEqual(['pipe-x'])

    const executed: any[] = []
    const tx = {
      execute: async (sql: any) => {
        executed.push(sql)

        return { rowCount: 0 }
      },
    }
    await state.saveCursor(tx as any, ctxFor(block(130), block(120), []))
    expect(paramsOf(executed[0])).toContain('pipe-x')
  })

  it('lets an explicit options.id override the source id', () => {
    const state = new PostgresState({ query: async () => ({ rows: [] }) } as any, { id: 'pinned' })
    state.bindCursorKey('pipe-x')

    expect(state.cursorKey).toBe('pinned')
  })

  it('refuses to bind without a source id or an explicit key', () => {
    const state = new PostgresState({ query: async () => ({ rows: [] }) } as any, {})

    expect(() => state.bindCursorKey(undefined)).toThrow(expect.objectContaining({ code: 'E0001' }))
  })
})

describe('PostgresState — legacy cursor migration', () => {
  // In-memory sync table keyed by id. SELECTs answer for the id they were queried with; the
  // migration UPDATE re-keys the legacy rows atomically, honouring its NOT EXISTS guard.
  function keyedClient(rowsById: Record<string, any[]>) {
    const updates: { params?: unknown[] }[] = []
    const client = {
      query: async (text: string, params?: unknown[]) => {
        if (text.startsWith('UPDATE')) {
          const [newId, oldId] = params as [string, string]
          const legacy = rowsById[oldId] ?? []

          if (rowsById[newId]?.length || !legacy.length) return { rowCount: 0 }

          updates.push({ params })
          rowsById[newId] = legacy.map((r) => ({ ...r, id: newId }))
          delete rowsById[oldId]

          return { rowCount: legacy.length }
        }

        const id = params?.[0] as string

        return { rows: rowsById[id] ?? [] }
      },
    }

    return { client, updates }
  }

  const syncRow = (n: number) => ({
    id: 'stream',
    current_number: String(n),
    current_hash: `0x${n}`,
    current_timestamp: null,
    finalized: {},
    rollback_chain: [],
  })

  it('automatically migrates legacy "stream" rows to the pipe id, and warns', async () => {
    const { client, updates } = keyedClient({ stream: [syncRow(50)] })
    const warn = vi.fn()
    const state = new PostgresState(client as any, {})
    state.bindCursorKey('pipe-x')

    const resume = await state.getCursor({ logger: { warn, debug: () => {} } as any })

    expect(resume).toEqual({ latest: { number: 50, hash: '0x50' }, finalized: null })
    expect(updates).toHaveLength(1)
    expect(updates[0].params).toEqual(['pipe-x', 'stream'])
    expect(warn).toHaveBeenCalledOnce()
  })

  it("prefers the pipe's own cursor and leaves the legacy rows alone once it exists", async () => {
    const { client, updates } = keyedClient({ 'pipe-x': [{ ...syncRow(200), id: 'pipe-x' }], stream: [syncRow(50)] })
    const state = new PostgresState(client as any, {})
    state.bindCursorKey('pipe-x')

    const resume = await state.getCursor({ logger: testLogger() })

    expect(resume).toEqual({ latest: { number: 200, hash: '0x200' }, finalized: null })
    expect(updates).toHaveLength(0)
  })

  it('does not migrate the legacy cursor onto an explicit options.id', async () => {
    // An explicitly pinned key deliberately names its own cursor — inheriting the shared
    // legacy cursor could resume a fresh pipe from a foreign position.
    const { client, updates } = keyedClient({ stream: [syncRow(50)] })
    const state = new PostgresState(client as any, { id: 'pinned' })
    state.bindCursorKey('pipe-x')

    await expect(state.getCursor({ logger: testLogger() })).resolves.toBeUndefined()
    expect(updates).toHaveLength(0)
  })

  it('reads the legacy key in place when the cursor key IS the legacy id', async () => {
    const { client, updates } = keyedClient({ stream: [syncRow(50)] })
    const state = new PostgresState(client as any, {})
    state.bindCursorKey('stream')

    const resume = await state.getCursor({ logger: testLogger() })

    expect(resume).toEqual({ latest: { number: 50, hash: '0x50' }, finalized: null })
    expect(updates).toHaveLength(0)
  })

  it('starts fresh when neither the pipe key nor the legacy key holds a cursor', async () => {
    const { client, updates } = keyedClient({})
    const warn = vi.fn()
    const state = new PostgresState(client as any, {})
    state.bindCursorKey('pipe-x')

    await expect(state.getCursor({ logger: { warn, debug: () => {} } as any })).resolves.toBeUndefined()
    expect(updates).toHaveLength(0)
    expect(warn).not.toHaveBeenCalled()
  })
})

// The millisecond fixture includes subsecond precision so restart validation must
// accept fractional epoch seconds while preserving the wall-clock instant.
const EVM_SECONDS = 1_794_173_939
const TRON_MS = 1_794_173_943_123

function ctxWithTimestamp(
  current: { number: number; hash: string; timestamp?: number },
  finalized: { number: number; hash: string } | undefined = undefined,
): BatchContext {
  return {
    logger: testLogger(),
    profiler: fakeSpan(),
    stream: {
      head: { finalized },
      state: { current, rollbackChain: [] },
    },
  } as unknown as BatchContext
}

function stateWithRow(row: Partial<Record<string, unknown>>) {
  const client = {
    query: async () => ({
      rows: [
        {
          id: 'stream',
          current_number: '50',
          current_hash: '0x50',
          finalized: {},
          rollback_chain: [],
          ...row,
        },
      ],
    }),
  }

  return new PostgresState(client as any, {})
}

describe('PostgresState — cursor timestamp units', () => {
  it('binds a tron-style millisecond cursor as its wall-clock instant, not ×1000', async () => {
    const state = stateWithRow({})

    const executed: any[] = []
    const tx = { execute: async (sql: any) => (executed.push(sql), { rowCount: 0 }) }

    await state.saveCursor(tx as any, ctxWithTimestamp({ number: 50, hash: '0x50', timestamp: TRON_MS }))

    const bound = paramsOf(executed[0])[3]
    expect(bound).toBeInstanceOf(Date)
    // Pre-fix code bound `new Date(TRON_MS * 1000)`, storing year ~58825.
    expect((bound as Date).getTime()).toBe(TRON_MS)
  })

  it('keeps an evm-style seconds cursor unchanged', async () => {
    const state = stateWithRow({})

    const executed: any[] = []
    const tx = { execute: async (sql: any) => (executed.push(sql), { rowCount: 0 }) }

    await state.saveCursor(tx as any, ctxWithTimestamp({ number: 50, hash: '0x50', timestamp: EVM_SECONDS }))

    const bound = paramsOf(executed[0])[3] as Date
    expect(bound.getTime()).toBe(EVM_SECONDS * 1000)
  })

  it('stores SQL NULL for a timestamp no Date can hold (microseconds)', async () => {
    const state = stateWithRow({})

    const executed: any[] = []
    const tx = { execute: async (sql: any) => (executed.push(sql), { rowCount: 0 }) }

    // Hyperliquid's native API reports microseconds; a portal passing them through would reach
    // here. `new Date(µs * 1000)` is an Invalid Date that node-postgres serializes as
    // "NaN-NaN-NaN…", failing the whole batch transaction.
    await state.saveCursor(tx as any, ctxWithTimestamp({ number: 50, hash: '0x50', timestamp: TRON_MS * 1000 }))

    expect(paramsOf(executed[0]).some((p) => p instanceof Date)).toBe(false)
    expect(sqlTextOf(executed[0])).toMatch(/NULL/)
  })

  it('reads the resumed cursor timestamp as epoch seconds from a correctly stored row', async () => {
    const state = stateWithRow({ current_timestamp: new Date(TRON_MS) })

    const resume = await state.getCursor({ logger: testLogger() })

    expect(resume?.latest.timestamp).toBe(TRON_MS / 1000)
  })

  it('recovers the true instant from a legacy row stored ×1000', async () => {
    // Rows written by pre-fix code for a millisecond network hold the portal value as if it were
    // seconds — a date in the far future. The read must normalize, not echo the wrong unit.
    const state = stateWithRow({ current_timestamp: new Date(TRON_MS * 1000) })

    const resume = await state.getCursor({ logger: testLogger() })

    expect(resume?.latest.timestamp).toBe(TRON_MS / 1000)
  })

  it('persists the fork ancestor timestamp with the same normalization', async () => {
    const state = stateWithRow({})

    const executed: any[] = []
    const tx = {
      execute: async (query: any) => {
        executed.push(query)
        if (executed.length === 1) {
          return {
            rows: [
              {
                current_number: '130',
                finalized: block(120),
                rollback_chain: [block(121), block(122), block(130)],
              },
            ],
          }
        }
        return { rows: [] }
      },
    }

    await state.saveForkCursor(tx as any, { number: 122, hash: '0x122', timestamp: TRON_MS })

    const bound = paramsOf(executed[1])[3]
    expect(bound).toBeInstanceOf(Date)
    expect((bound as Date).getTime()).toBe(TRON_MS)
  })
})

describe('PostgresState — fork cleanup', () => {
  it('deletes the now-dead sync rows above the fork cursor', async () => {
    const state = await seededState(block(100))

    const executed: any[] = []
    const tx = {
      execute: async (sql: any) => {
        executed.push(sql)
        return { rowCount: 0 }
      },
    }

    await state.removeForkedRows(tx as any, block(997))

    const text = sqlTextOf(executed[0])
    const params = paramsOf(executed[0])
    // Scope to this id and drop only rows strictly above the safe cursor (the dead chain),
    // so the resume row stays the last write and reprocessing can re-insert those numbers.
    expect(text).toMatch(/DELETE FROM/i)
    expect(text).toMatch(/"current_number" > \$\d/)
    expect(params).toEqual(['stream', 997])
  })
})

describe('PostgresState — malformed persisted cursor', () => {
  it.each(['garbage', '-2', '1.5', '9007199254740992'])('refuses numeric cursor %s before a write', async (number) => {
    const query = vi.fn(async (_sql: string) => ({
      rows: [
        {
          current_number: number,
          current_hash: '0x7',
          current_timestamp: null,
          finalized: {},
          rollback_chain: [],
        },
      ],
    }))
    const state = new PostgresState({ query } as any, {})
    await expect(state.getCursor({ logger: testLogger() })).rejects.toMatchObject({ code: 'E2108' })
    expect(query).toHaveBeenCalledTimes(1)
    expect(query.mock.calls[0]?.[0]).toMatch(/^SELECT /)
  })
})

describe('PostgresState — persisted finalized validation', () => {
  it.each(
    [{ number: '40' }, { hash: '0x40' }, [], 'invalid', { number: -1 }, { number: 40, kind: 'unknown' }].map(
      (finalized) => ({ finalized }),
    ),
  )('refuses malformed finalized %j rather than dropping the floor', async ({ finalized }) => {
    const state = stateWithRow({ finalized })
    await expect(state.getCursor({ logger: testLogger() })).rejects.toMatchObject({ code: 'E2108' })
  })
  it('preserves SQL NULL as an absent finalized head', async () => {
    const state = stateWithRow({ finalized: null })
    expect((await state.getCursor({ logger: testLogger() }))?.finalized).toBeNull()
  })
})
