import { PipeError } from '~/core/errors.js'

/** serialization failure, deadlock, too many connections, admin/crash shutdown, cannot connect now. */
const TRANSIENT_SQLSTATES = new Set(['40001', '40P01', '53300', '57P01', '57P02', '57P03'])
const CONNECTION_EXCEPTION_CLASS = '08'

const TRANSIENT_NODE_CODES = new Set(['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE'])

// `pg` raises these with no code when the connection drops under a query.
const DROPPED_CONNECTION_MESSAGES = [
  'Connection terminated unexpectedly',
  'Client has encountered a connection error and is not queryable',
  'Query read timeout',
]

/**
 * Whether a fresh attempt of the same transaction can succeed (FM-2). An allowlist: a fault it
 * does not name halts the pipe, and the restart resumes from the last commit. A coded `PipeError`
 * is a decision, never retried, whatever its cause. Drizzle wraps the driver error in `cause`.
 */
export function isTransientPgError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false
  if (error instanceof PipeError) return false

  const { code, message, cause } = error as { code?: unknown; message?: unknown; cause?: unknown }

  if (typeof code === 'string') {
    const transientCode =
      TRANSIENT_SQLSTATES.has(code) || code.startsWith(CONNECTION_EXCEPTION_CLASS) || TRANSIENT_NODE_CODES.has(code)
    if (transientCode) return true
  }

  if (typeof message === 'string' && DROPPED_CONNECTION_MESSAGES.includes(message)) return true

  return isTransientPgError(cause)
}
