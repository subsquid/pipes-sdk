import { PipeError, SdkErrorName } from '~/core/errors.js'

/**
 * Common error wrapper for everything thrown by the ClickHouse target.
 *
 * All target-originated errors extend this class and carry an `E20xx` code so
 * downstream code can pattern-match on `instanceof ClickhouseTargetError` and react
 * (alerting, retries, etc.) without scraping `.message`. Code bands: E0xxx = source,
 * E1xxx = fork handling, E2xxx = targets (E20xx ClickHouse, E21xx Postgres, E22xx
 * BigQuery, E23xx Parquet, E24xx PubSub).
 */
export class ClickhouseTargetError extends PipeError {
  constructor(code: string, message: string | string[]) {
    super(code, SdkErrorName.TargetConfiguration, message)
  }
}

// E20xx — ClickHouse target codes
export const CLICKHOUSE_ERROR_CODES = {
  /** The `maxRows` batching option is not a positive number. */
  MAX_ROWS: 'E2001',
  /** A table identifier could not be parsed as `table` or `database.table`. */
  INVALID_TABLE_NAME: 'E2002',
  /** Rollback targeted a Distributed table — it must target the local table instead. */
  DISTRIBUTED_ROLLBACK: 'E2003',
  /** The table's collapsing engine collapses on a column other than `sign`. */
  ROLLBACK_COLLAPSE_COLUMN: 'E2004',
  /** The table has no `sign` column, so cancel-row rollback cannot work. */
  ROLLBACK_MISSING_SIGN: 'E2005',
  /** `ensureRollbackIndex` was given a column name that is not a plain identifier. */
  INVALID_ROLLBACK_INDEX_COLUMN: 'E2006',
  /** A chain fork arrived but no `onRollback` handler is configured to remove the orphaned rows. */
  MISSING_ROLLBACK_ON_FORK: 'E2007',
} as const
