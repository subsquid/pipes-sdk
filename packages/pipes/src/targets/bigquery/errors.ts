import { PipeError, SdkErrorName } from '~/core/errors.js'

/**
 * Common error wrapper for everything thrown by the BigQuery target.
 *
 * All target-originated errors extend this class and carry an `E22xx` code so
 * downstream code can pattern-match on `instanceof BigQueryTargetError` and react
 * (alerting, retries, etc.) without scraping `.message`. Code bands: E0xxx = source,
 * E1xxx = fork handling, E2xxx = targets (E20xx ClickHouse, E21xx Postgres, E22xx
 * BigQuery, E23xx Parquet, E24xx PubSub).
 */
export class BigQueryTargetError extends PipeError {
  constructor(code: string, message: string | string[]) {
    super(code, SdkErrorName.TargetConfiguration, message)
  }
}

// E22xx — BigQuery target codes
export const BIGQUERY_ERROR_CODES = {
  /** Cannot determine GCP project id at target construction. */
  PROJECT_ID: 'E2201',
  /** Tracked table schema is missing the declared partition column. */
  PARTITION_COLUMN_MISSING: 'E2202',
  /** Partition column has the wrong type (must be INT64). */
  PARTITION_COLUMN_TYPE: 'E2203',
  /** Partition column is NULLable (must be REQUIRED). */
  PARTITION_COLUMN_NULLABLE: 'E2204',
  /** Existing live table is not range-partitioned on the declared column. */
  TABLE_NOT_PARTITIONED: 'E2205',
  /** Auto-create cannot generate DDL for REPEATED/RECORD/STRUCT fields. */
  UNSUPPORTED_FIELD_SHAPE: 'E2206',
  /** Declared field missing from the live table. */
  SCHEMA_FIELD_MISSING: 'E2207',
  /** Declared field has a different type in the live table. */
  SCHEMA_TYPE_MISMATCH: 'E2208',
  /** User wrote to a table that wasn't registered in tables[]. */
  UNREGISTERED_TABLE: 'E2209',
  /** Internal: schema map and allowlist disagree (should be unreachable). */
  INTERNAL_SCHEMA_MAP: 'E2210',
  /** Sync row in IN_FLIGHT state lacks the range_low/range_high it needs for recovery. */
  CORRUPT_INFLIGHT_ROW: 'E2211',
  /**
   * Sync table has no rows for this stream id, but tracked tables still hold data — refusing
   * to silently restart from the initial cursor (would re-process and duplicate everything).
   * Surfaces accidental sync truncation / drop / row deletion before it becomes corruption.
   */
  ORPHAN_TRACKED_DATA: 'E2212',
  /**
   * `AppendRows` resolved without a channel error, but the response carries per-row
   * `rowErrors` from BigQuery (proto-schema mismatch, NOT NULL violation, etc.). The SDK
   * does not propagate these as rejections — without an explicit check the row is silently
   * dropped while our code believes the write succeeded.
   */
  APPEND_ROW_REJECTED: 'E2213',
} as const
