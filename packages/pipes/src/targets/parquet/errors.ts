import { PipeError, SdkErrorName } from '~/core/errors.js'

/**
 * Common error wrapper for everything thrown by the Parquet target.
 *
 * All target-originated errors extend this class and carry an `E23xx` code so
 * downstream code can pattern-match on `instanceof ParquetTargetError` and react
 * (alerting, retries, etc.) without scraping `.message`. Code bands: E0xxx = source,
 * E1xxx = fork handling, E2xxx = targets (E20xx ClickHouse, E21xx Postgres, E22xx
 * BigQuery, E23xx Parquet, E24xx PubSub).
 */
export class ParquetTargetError extends PipeError {
  constructor(code: string, message: string | string[]) {
    super(code, SdkErrorName.TargetConfiguration, message)
  }
}

// E23xx — Parquet target codes
export const PARQUET_ERROR_CODES = {
  /** `tables[]` is empty — the target has nothing to write. */
  NO_TABLES: 'E2301',
  /** Two declared tables share the same name. */
  DUPLICATE_TABLE: 'E2302',
  /** A table schema is empty (no columns declared). */
  EMPTY_SCHEMA: 'E2303',
  /** Table schema is missing the declared block-number column. */
  BLOCK_COLUMN_MISSING: 'E2304',
  /** Block-number column is not an integer type (must be INT64/INT32). */
  BLOCK_COLUMN_TYPE: 'E2305',
  /** A column declared an unsupported compression codec. */
  UNSUPPORTED_COMPRESSION: 'E2306',
  /** A column declared an unsupported Parquet type. */
  UNSUPPORTED_TYPE: 'E2307',
  /** User wrote to a table that wasn't registered in `tables[]`. */
  UNREGISTERED_TABLE: 'E2308',
  /** `publish()` refused to overwrite an existing data file (would lose data). */
  FILE_COLLISION: 'E2309',
  /** Persisted state file exists but could not be parsed. */
  STATE_CORRUPT: 'E2310',
  /** The block-number column is declared `optional` — it must be present on every row. */
  BLOCK_COLUMN_OPTIONAL: 'E2311',
  /** A row carried a missing/non-finite block number (would corrupt attribution and recovery). */
  BLOCK_VALUE_INVALID: 'E2312',
  /** A row value does not match its declared column type (dev-mode value check). */
  VALUE_INVALID: 'E2313',
  /** Crash recovery could not delete an over-cursor data file (leaving it would duplicate data). */
  RECOVERY_DELETE_FAILED: 'E2314',
  /** A nested column declaration is malformed (empty STRUCT fields, LIST without element, over-deep nesting). */
  NESTED_SCHEMA_INVALID: 'E2315',
  /** A segment's coverage range could not be formed or is inverted (internal invariant). */
  COVERAGE_RANGE_INVALID: 'E2316',
  /** The persisted coverage map disagrees with the cursor stored beside it. */
  STATE_COVERAGE_INVALID: 'E2317',
  // E2318–E2319 retired, numbers left unassigned (ADR-4): they were dynamic-load / runtime
  // engine-shape errors — engines statically import their libraries and `settings.engine` is
  // compile-time typed. (The duckdb per-column-compression check moved out with its engine,
  // see ADR-19; coverage naming makes zero-row segments legitimate, so no empty-segment code
  // exists either.)
  /** An engine's finished segment file failed the Parquet magic-bytes check at publication. */
  SEGMENT_NOT_PARQUET: 'E2320',
} as const
