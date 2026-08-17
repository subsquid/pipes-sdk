import { PipeError, SdkErrorName } from '~/core/errors.js'

/**
 * Common error wrapper for everything thrown by the Postgres (Drizzle) target.
 *
 * All target-originated errors extend this class and carry an `E21xx` code so
 * downstream code can pattern-match on `instanceof PostgresTargetError` and react
 * (alerting, retries, etc.) without scraping `.message`. Code bands: E0xxx = source,
 * E1xxx = fork handling, E2xxx = targets (E20xx ClickHouse, E21xx Postgres, E22xx
 * BigQuery, E23xx Parquet, E24xx PubSub).
 */
export class PostgresTargetError extends PipeError {
  constructor(code: string, message: string | string[]) {
    super(code, SdkErrorName.TargetConfiguration, message)
  }
}

// E21xx — Postgres (Drizzle) target codes
export const POSTGRES_ERROR_CODES = {
  /** The provided Drizzle db instance has no underlying client (`$client`). */
  DRIZZLE_CLIENT_MISSING: 'E2101',
  /** `unfinalizedBlocksRetention` is not a positive number. */
  RETENTION_INVALID: 'E2102',
  /** Could not acquire the advisory lock — another process holds this state id. */
  ADVISORY_LOCK_FAILED: 'E2103',
  /** A write targeted a table not registered in `tables` for rollback tracking. */
  UNTRACKED_TABLE: 'E2104',
  /** Cannot build a snapshot trigger for a table without primary key columns. */
  MISSING_PRIMARY_KEY: 'E2105',
  /** Foreign keys form a cycle, so a safe delete order cannot be determined. */
  CIRCULAR_DEPENDENCY: 'E2106',
  /** A column's real database name could not be resolved from the Drizzle dialect. */
  COLUMN_NAME_UNRESOLVED: 'E2107',
} as const
