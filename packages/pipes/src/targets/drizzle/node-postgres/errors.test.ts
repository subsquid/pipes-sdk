import { integer, pgTable } from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'

import { PipeError } from '~/core/errors.js'

import { createColumnNameResolver } from './consts.js'
import { drizzleTarget } from './drizzle-target.js'
import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'
import { PostgresState } from './postgres-state.js'
import { buildSnapshotPlan } from './rollback.js'

describe('PostgresTargetError', () => {
  it('carries the given E21xx code and a docs link', () => {
    const err = new PostgresTargetError(POSTGRES_ERROR_CODES.RETENTION_INVALID, 'boom')

    expect(err).toBeInstanceOf(PipeError)
    expect(err.code).toBe('E2102')
    expect(err.message).toContain('boom')
    expect(err.message).toContain('See: https://docs.sqd.dev/en/sdk/pipes-sdk/errors/E2102')
  })

  it('drizzleTarget on a db without a client throws DRIZZLE_CLIENT_MISSING (E2101)', () => {
    try {
      drizzleTarget({ db: {} as any, tables: [], onData: async () => {} })
      throw new Error('expected throw')
    } catch (e) {
      expect(e).toBeInstanceOf(PostgresTargetError)
      expect((e as PostgresTargetError).code).toBe(POSTGRES_ERROR_CODES.DRIZZLE_CLIENT_MISSING)
    }
  })

  it('PostgresState with non-positive retention throws RETENTION_INVALID (E2102)', () => {
    try {
      new PostgresState({} as any, { unfinalizedBlocksRetention: -1 })
      throw new Error('expected throw')
    } catch (e) {
      expect(e).toBeInstanceOf(PostgresTargetError)
      expect((e as PostgresTargetError).code).toBe(POSTGRES_ERROR_CODES.RETENTION_INVALID)
    }
  })

  it('buildSnapshotPlan on a table without a primary key throws MISSING_PRIMARY_KEY (E2105)', () => {
    const table = pgTable('t', { a: integer('a') })
    try {
      buildSnapshotPlan(table, (col) => col.name)
      throw new Error('expected throw')
    } catch (e) {
      expect(e).toBeInstanceOf(PostgresTargetError)
      expect((e as PostgresTargetError).code).toBe(POSTGRES_ERROR_CODES.MISSING_PRIMARY_KEY)
    }
  })

  it('an unnamed column with no dialect to resolve it throws COLUMN_NAME_UNRESOLVED (E2107)', () => {
    const table = pgTable('t', { someKey: integer().primaryKey() })
    try {
      buildSnapshotPlan(table, createColumnNameResolver({}))
      throw new Error('expected throw')
    } catch (e) {
      expect(e).toBeInstanceOf(PostgresTargetError)
      expect((e as PostgresTargetError).code).toBe(POSTGRES_ERROR_CODES.COLUMN_NAME_UNRESOLVED)
    }
  })
})
