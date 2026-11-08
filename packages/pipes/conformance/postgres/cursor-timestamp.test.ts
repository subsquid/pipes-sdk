import { drizzle } from 'drizzle-orm/node-postgres'
import pg from 'pg'
import { expect } from 'vitest'

import type { BatchContext } from '~/core/index.js'
import { PostgresState } from '~/targets/drizzle/node-postgres/postgres-state.js'
import { testLogger } from '~/testing/index.js'

import { it as baseIt } from './harness.js'

// A tron fixture cursor round-trip asserting the declared unit (IB-21).
// The values below were read from the live portals on 2026-11-09 — tron-mainnet block
// 87833400 reports milliseconds, ethereum-mainnet block 26372900 seconds — and the sync column
// must hold the wall-clock instant under either unit (IB-21).
const EVM_SECONDS = 1_794_173_939
const TRON_MS = 1_794_173_943_000

function fakeSpan(): any {
  const span: any = {
    measure: (_name: any, fn: any) => fn(span),
    start: () => span,
    end: () => {},
  }
  return span
}

function ctxWithTimestamp(timestamp?: number): BatchContext {
  return {
    logger: testLogger(),
    profiler: fakeSpan(),
    stream: {
      head: { finalized: undefined },
      state: {
        current: { number: 87833400, hash: '0x87833400', timestamp },
        rollbackChain: [],
      },
    },
  } as unknown as BatchContext
}

const it = baseIt.extend<{ state: { pool: pg.Pool; state: PostgresState } }>({
  state: async ({ database }, use) => {
    const pool = new pg.Pool({ connectionString: database.dsn, application_name: 'ct-gap24-cursor' })
    const state = new PostgresState(pool as any, { schema: database.schema })

    // The real flow creates the sync table lazily on the first cursor read, before any save.
    await state.getCursor({ logger: testLogger() })

    await use({ pool, state })

    await pool.end()
  },
})

it('stores a tron millisecond cursor as wall-clock time and round-trips it', async ({ database, state }) => {
  const db = drizzle(state.pool)
  const pool = database.admin as pg.Pool

  await db.transaction((tx) => state.state.saveCursor(tx as any, ctxWithTimestamp(TRON_MS)))

  const { rows } = await pool.query(`SELECT "current_timestamp" FROM "${database.schema}"."sync" WHERE id = 'stream'`)
  expect(rows).toHaveLength(1)

  const storedMs = rows[0].current_timestamp.getTime()
  // Pre-fix code stored the portal value as if it were seconds — a date in the year ~58825.
  expect(Math.abs(storedMs - TRON_MS)).toBeLessThanOrEqual(2000)

  const resume = await state.state.getCursor({ logger: testLogger() })
  expect(resume?.latest.number).toBe(87833400)
  expect(resume?.latest.timestamp).toBe(TRON_MS / 1000)
})

it('keeps an evm seconds cursor byte-identical through the round-trip', async ({ database, state }) => {
  const db = drizzle(state.pool)
  const pool = database.admin as pg.Pool

  await db.transaction((tx) => state.state.saveCursor(tx as any, ctxWithTimestamp(EVM_SECONDS)))

  const { rows } = await pool.query(`SELECT "current_timestamp" FROM "${database.schema}"."sync" WHERE id = 'stream'`)
  expect(rows[0].current_timestamp.getTime()).toBe(EVM_SECONDS * 1000)

  const resume = await state.state.getCursor({ logger: testLogger() })
  expect(resume?.latest.timestamp).toBe(EVM_SECONDS)
})

it('stores NULL, not a failed batch, for a microseconds timestamp', async ({ database, state }) => {
  const db = drizzle(state.pool)
  const pool = database.admin as pg.Pool

  // No portal reports microseconds today, but Hyperliquid's native API does; a portal passing
  // them through must not take the batch transaction down with it.
  await expect(
    db.transaction((tx) => state.state.saveCursor(tx as any, ctxWithTimestamp(TRON_MS * 1000))),
  ).resolves.toMatchObject({ safeBlockNumber: expect.any(Number) })

  const { rows } = await pool.query(`SELECT "current_timestamp" FROM "${database.schema}"."sync" WHERE id = 'stream'`)
  expect(rows[0].current_timestamp).toBeNull()

  const resume = await state.state.getCursor({ logger: testLogger() })
  expect(resume?.latest.timestamp).toBeUndefined()
})

it('recovers the true instant from a legacy row stored ×1000', async ({ database, state }) => {
  const pool = database.admin as pg.Pool

  // What pre-fix code wrote for a millisecond network: the portal value interpreted as seconds.
  await pool.query(
    `INSERT INTO "${database.schema}"."sync" (id, current_number, current_hash, "current_timestamp", finalized, rollback_chain)
     VALUES ('stream', 87833400, '0x87833400', $1, '{}', '[]')`,
    [new Date(TRON_MS * 1000)],
  )

  const resume = await state.state.getCursor({ logger: testLogger() })

  expect(resume?.latest.timestamp).toBe(TRON_MS / 1000)
})
