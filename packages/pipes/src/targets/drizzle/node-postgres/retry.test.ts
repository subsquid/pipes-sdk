import { DrizzleQueryError } from 'drizzle-orm/errors'
import { describe, expect, it } from 'vitest'

import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'
import { isTransientPgError } from './retry.js'

function pgError(code: string) {
  return Object.assign(new Error(`pg error ${code}`), { code })
}

function wrapped(cause: unknown) {
  return new DrizzleQueryError('select 1', [], cause as Error)
}

describe('isTransientPgError', () => {
  it.each([
    ['serialization failure', '40001'],
    ['deadlock', '40P01'],
    ['too many connections', '53300'],
    ['admin shutdown', '57P01'],
    ['cannot connect now', '57P03'],
    ['connection failure', '08006'],
    ['connection reset', 'ECONNRESET'],
    ['connection refused', 'ECONNREFUSED'],
  ])('retries a %s (%s) that drizzle wrapped', (_, code) => {
    expect(isTransientPgError(wrapped(pgError(code)))).toBe(true)
  })

  it('retries a connection pg dropped without a code', () => {
    expect(isTransientPgError(wrapped(new Error('Connection terminated unexpectedly')))).toBe(true)
  })

  it.each([
    ['unique violation', '23505'],
    ['foreign key violation', '23503'],
    ['invalid text representation', '22P02'],
    ['undefined table', '42P01'],
    ['statement timeout', '57014'],
  ])('does not retry a %s (%s)', (_, code) => {
    expect(isTransientPgError(wrapped(pgError(code)))).toBe(false)
  })

  it('does not retry an error the author threw', () => {
    expect(isTransientPgError(new Error('bad data in onData'))).toBe(false)
  })

  it('does not retry a coded refusal, even over a transient cause', () => {
    const refusal = new PostgresTargetError(POSTGRES_ERROR_CODES.ADVISORY_LOCK_FAILED, 'locked')
    refusal.cause = pgError('40001')

    expect(isTransientPgError(refusal)).toBe(false)
  })

  it('does not retry a non-error value', () => {
    expect(isTransientPgError(undefined)).toBe(false)
    expect(isTransientPgError('40001')).toBe(false)
  })
})
