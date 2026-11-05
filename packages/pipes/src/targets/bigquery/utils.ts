import type { TableField, TableMetadata } from '@google-cloud/bigquery'

import { BIGQUERY_ERROR_CODES, BigQueryTargetError } from './errors.js'

const GRPC_STATUS = {
  INVALID_ARGUMENT: 3,
  DEADLINE_EXCEEDED: 4,
  NOT_FOUND: 5,
  RESOURCE_EXHAUSTED: 8,
  ABORTED: 10,
  INTERNAL: 13,
  UNAVAILABLE: 14,
} as const

const TRANSIENT_CODES = new Set<number>([
  GRPC_STATUS.DEADLINE_EXCEEDED,
  GRPC_STATUS.RESOURCE_EXHAUSTED,
  GRPC_STATUS.ABORTED,
  GRPC_STATUS.INTERNAL,
  GRPC_STATUS.UNAVAILABLE,
  429,
  500,
  502,
  503,
  504,
])

/**
 * Detects "Not Found" errors from the @google-cloud/bigquery client AND the Storage Write
 * API: HTTP 404 from the REST client, gRPC NOT_FOUND (code 5) from Storage Write, or — when
 * no machine-readable code is present — an Error whose message contains "not found". Walks
 * the `cause` chain because both clients wrap lower-level errors at multiple layers.
 *
 * Numeric codes are authoritative: a `{ code: 500, message: "internal: not found in cache" }`
 * must NOT classify as not-found. The message scan only kicks in when no numeric code is
 * present anywhere in the local frame.
 */
export function isNotFoundError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false

  const e = error as { code?: number | string; message?: string; cause?: unknown; errors?: { reason?: string }[] }
  const code = parseLocalCode(error)

  if (code === 404 || code === GRPC_STATUS.NOT_FOUND) return true
  if (code !== undefined) {
    // Some other numeric code — authoritative, don't fall through to message scan. Still
    // walk the cause chain in case the outer is a generic wrapper around a real not-found.
    return e.cause ? isNotFoundError(e.cause) : false
  }
  if (typeof e.message === 'string' && /not found/i.test(e.message)) return true
  if (Array.isArray(e.errors) && e.errors.some((x) => x?.reason === 'notFound')) return true
  if (e.cause) return isNotFoundError(e.cause)

  return false
}

/** BigQuery's "retrying may solve the problem" reasons; a job that fails mid-run reports them under HTTP 400. */
const TRANSIENT_REASONS = new Set([
  'backendError',
  'internalError',
  'jobBackendError',
  'jobInternalError',
  'rateLimitExceeded',
  'jobRateLimitExceeded',
])

/**
 * Detects transient BigQuery errors that should be retried by `doWithRetry`.
 *
 * gRPC error codes:
 *   ABORTED (10), RESOURCE_EXHAUSTED (8), UNAVAILABLE (14), DEADLINE_EXCEEDED (4), INTERNAL (13)
 *
 * Storage Write API surfaces these as `code` numeric properties on the rejected promise.
 * The BigQuery REST client uses HTTP-style codes (429, 500, 502, 503, 504), and `errors[].reason`
 * for a job that failed mid-run (`TRANSIENT_REASONS`, under any HTTP code).
 *
 * Walks the cause chain — finds a transient code ANYWHERE in the chain (different from
 * `readErrorCode`, which returns just the outermost code).
 */
export function isTransientError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false

  const code = parseLocalCode(error)
  if (code !== undefined && TRANSIENT_CODES.has(code)) return true

  const e = error as { cause?: unknown; errors?: { reason?: string }[] }
  if (Array.isArray(e.errors) && e.errors.some((x) => TRANSIENT_REASONS.has(x?.reason ?? ''))) return true
  if (e.cause) return isTransientError(e.cause)

  return false
}

/** Parses just the local `code` field on `error`, normalising string codes to number. */
function parseLocalCode(error: unknown): number | undefined {
  if (!error || typeof error !== 'object') return undefined
  const e = error as { code?: number | string }
  if (e.code === undefined) return undefined
  const code = typeof e.code === 'string' ? Number.parseInt(e.code, 10) : e.code

  return typeof code === 'number' && !Number.isNaN(code) ? code : undefined
}

export type BqErrorKind = 'not_found' | 'invalid_argument' | 'resource_exhausted' | 'transient' | 'unknown'

/**
 * Classifies a BigQuery / Storage Write API error into the small set of `kind` labels used by
 * `sqd_bigquery_append_errors_total`. The label is bounded — gRPC error codes are stable, but
 * we collapse them into operationally distinct buckets so a Grafana panel can split
 * "schema mismatch" from "rate limit" from "everything else" without label cardinality blowing up.
 *
 * - not_found (gRPC 5 / HTTP 404):           dataset/table/stream gone — operator action needed.
 * - invalid_argument (gRPC 3 / HTTP 400):    schema mismatch, NOT NULL violation — code/data bug.
 * - resource_exhausted (gRPC 8 / HTTP 429):  rate/quota — back off, transient but distinct.
 * - transient (gRPC 4/10/13/14, HTTP 5xx):   the rest of the retryable family.
 * - unknown:                                  anything else (don't drop the observation).
 */
export function classifyBqError(error: unknown): BqErrorKind {
  // `isNotFoundError` covers both REST 404 and gRPC NOT_FOUND (5).
  if (isNotFoundError(error)) return 'not_found'
  const code = readErrorCode(error)
  if (code === GRPC_STATUS.INVALID_ARGUMENT || code === 400) return 'invalid_argument'
  if (code === GRPC_STATUS.RESOURCE_EXHAUSTED || code === 429) return 'resource_exhausted'
  if (isTransientError(error)) return 'transient'

  return 'unknown'
}

/** Returns the first code anywhere in the cause chain (outermost wins). */
function readErrorCode(error: unknown): number | undefined {
  if (!error || typeof error !== 'object') return undefined
  const code = parseLocalCode(error)
  if (code !== undefined) return code
  const e = error as { cause?: unknown }

  return e.cause ? readErrorCode(e.cause) : undefined
}

/**
 * Looks up a field by name in a BQ table's schema. Returns undefined if not present.
 */
export function findField(metadata: TableMetadata, name: string): TableField | undefined {
  return metadata.schema?.fields?.find((f) => f.name === name)
}

/**
 * Asserts that `<columnName>` exists, is INT64, and is REQUIRED (NOT NULL).
 *
 * Why these three constraints are non-negotiable for the partition column:
 *
 * - **NULLable**: SQL three-valued logic — `WHERE bn > @safe` does NOT match rows where bn IS NULL.
 *   Such rows would survive every fork DELETE → silent post-fork corruption.
 * - **FLOAT64 / NUMERIC / BIGNUMERIC**: precision loss above 2^53 (Solana slot numbers can exceed
 *   this). `BETWEEN @safe+1 AND @upper` becomes inexact → some rows leak through DELETE.
 * - **STRING / other**: `RANGE_BUCKET(...)` does not work; `BETWEEN` does lexicographic compare,
 *   which is wildly wrong across digit-length transitions ("9" > "10").
 */
export function assertInt64NotNull(metadata: TableMetadata, columnName: string, tableFqn: string): void {
  const field = findField(metadata, columnName)

  if (!field) {
    throw new BigQueryTargetError(
      BIGQUERY_ERROR_CODES.PARTITION_COLUMN_MISSING,
      `Table ${tableFqn} is missing the partition column '${columnName}'. Add it as INT64 NOT NULL, then re-run.`,
    )
  }

  const type = (field.type || '').toUpperCase()
  if (type !== 'INT64' && type !== 'INTEGER') {
    throw new BigQueryTargetError(
      BIGQUERY_ERROR_CODES.PARTITION_COLUMN_TYPE,
      `Table ${tableFqn} has '${columnName}' typed as ${field.type}, but the BigQuery target ` +
        `requires INT64. Reason: ${type === 'FLOAT64' || type === 'FLOAT' || type === 'NUMERIC' || type === 'BIGNUMERIC' ? `${type} loses precision above 2^53 (Solana slot numbers exceed this), making BETWEEN predicates inexact during reorg cleanup.` : `${type} cannot be used with RANGE_BUCKET partitioning, and BETWEEN compares lexicographically — wrong across digit-length transitions.`}`,
    )
  }

  // BQ default mode is NULLABLE when not specified.
  const mode = (field.mode || 'NULLABLE').toUpperCase()
  if (mode !== 'REQUIRED') {
    throw new BigQueryTargetError(
      BIGQUERY_ERROR_CODES.PARTITION_COLUMN_NULLABLE,
      `Table ${tableFqn} has '${columnName}' as ${mode}, but the BigQuery target requires NOT NULL. ` +
        `Reason: SQL three-valued logic means rows with NULL ${columnName} do NOT match ` +
        `the WHERE predicate during fork DELETE, leaving them uncleaned forever.`,
    )
  }
}

/**
 * Asserts the table is range-partitioned on `<columnName>`.
 * Without this, DELETE during fork cleanup scans the entire table → unaffordable on real data.
 */
export function assertRangePartitionedOn(
  metadata: TableMetadata,
  columnName: string,
  tableFqn: string,
  suggestedDdl: string,
): void {
  const field = metadata.rangePartitioning?.field
  if (field !== columnName) {
    throw new BigQueryTargetError(
      BIGQUERY_ERROR_CODES.TABLE_NOT_PARTITIONED,
      `Table ${tableFqn} is not range-partitioned on '${columnName}' ` +
        `(found: ${field ? `range-partitioned on '${field}'` : 'no range partitioning'}). ` +
        `Reorg DELETE without partition pruning scans the whole table — unaffordable at scale. ` +
        `Suggested DDL:\n\n${suggestedDdl}`,
    )
  }
}
