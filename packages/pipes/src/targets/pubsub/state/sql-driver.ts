/**
 * The seam `SqlPubsubStateCore` (state-core.ts) talks through instead of a concrete storage
 * engine. One driver per backend (SqliteDriver, PostgresDriver) — the core holds every
 * sequencing, fork-compensation and finality-folding rule exactly once, and a driver supplies
 * only: how to get a connection, how to lock it to one writer, and how to run SQL on it.
 *
 * Every method is async so the same core body drives both a synchronous engine (SQLite, wrapped
 * trivially) and a genuinely async one (Postgres) without two copies of the business logic.
 */
export interface SqlDriver {
  /** Human-readable location for error messages: a file path, or a redacted connection target. */
  readonly location: string

  /** Opens the connection and acquires the single-writer lock. Throws `DriverLockedError` or
   *  `DriverUnavailableError`. Schema creation is the core's job (`schemaDDL`, run inside its
   *  own transaction) so a disk-full mid-bootstrap reports the same STATE_WRITE_FAILED as any
   *  other write, instead of a raw driver error. */
  connect(): Promise<void>

  /** The fully addressable identifier for one of the 8 state tables — same name in DDL and DML. */
  table(name: string): string

  /** `CREATE TABLE`/`CREATE INDEX` statements for this driver's dialect and naming scheme. */
  schemaDDL(): string[]

  get<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T | undefined>
  all<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]>
  exec(sql: string, params?: unknown[]): Promise<void>

  begin(immediate: boolean): Promise<void>
  commit(): Promise<void>
  /** Returns false if the connection may still hold an open transaction. */
  rollback(): Promise<boolean>

  /** A full or unreachable store, as opposed to a fault in the statement itself. */
  isStorageFailure(e: unknown): boolean

  /** Releases the connection and any lock it holds. Idempotent. */
  close(): Promise<void>
}

/** Another writer already holds the single-writer lock this driver's `connect()` needs. */
export class DriverLockedError extends Error {
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause))
    this.cause = cause
  }
}

/** The connection could not be established at all — a bad address, credentials, or a down store. */
export class DriverUnavailableError extends Error {
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause))
    this.cause = cause
  }
}

// ─── shared schema (rendered per dialect by each driver) ──────────────────────────────────────

type ColumnType =
  | 'TEXT'
  /** SQLite's INTEGER affinity is a dynamic 64-bit width; Postgres needs BIGINT to match it —
   *  sequence numbers run up to Number.MAX_SAFE_INTEGER and `updated_at` is a millisecond epoch,
   *  both well past Postgres's 32-bit INTEGER range. */
  | 'BIGINT'
  | 'BLOB'
  /** Auto-incrementing integer primary key. */
  | 'ROWID_PK'

type TableSpec = {
  name: string
  columns: [name: string, type: ColumnType][]
  primaryKey?: string[]
}

type IndexSpec = {
  name: string
  table: string
  columns: string[]
}

export const STATE_TABLES: readonly TableSpec[] = [
  {
    name: 'meta',
    columns: [
      ['key', 'TEXT'],
      ['value', 'TEXT'],
    ],
    primaryKey: ['key'],
  },
  {
    name: 'cursor',
    columns: [
      ['id', 'TEXT'],
      ['latest', 'TEXT'],
      ['finalized', 'TEXT'],
      ['updated_at', 'BIGINT'],
    ],
    primaryKey: ['id'],
  },
  {
    name: 'outbox',
    columns: [
      ['row_id', 'ROWID_PK'],
      ['route', 'TEXT'],
      ['topic', 'TEXT'],
      ['op', 'TEXT'],
      ['id', 'TEXT'],
      ['ordering_key', 'TEXT'],
      ['seq', 'BIGINT'],
      ['attributes', 'TEXT'],
      ['payload', 'BLOB'],
      ['block_number', 'BIGINT'],
    ],
  },
  {
    name: 'ledger_blocks',
    columns: [
      ['number', 'BIGINT'],
      ['hash', 'TEXT'],
      ['timestamp', 'BIGINT'],
    ],
    primaryKey: ['number'],
  },
  {
    name: 'manifest',
    columns: [
      ['route', 'TEXT'],
      ['topic', 'TEXT'],
      ['ordering_key', 'TEXT'],
      ['seq', 'BIGINT'],
      ['block_number', 'BIGINT'],
      ['mode', 'TEXT'],
      ['op', 'TEXT'],
      ['id', 'TEXT'],
      ['attributes', 'TEXT'],
      ['payload', 'BLOB'],
    ],
    primaryKey: ['topic', 'ordering_key', 'seq'],
  },
  {
    name: 'materialized_baseline',
    columns: [
      ['route', 'TEXT'],
      ['topic', 'TEXT'],
      ['ordering_key', 'TEXT'],
      ['id', 'TEXT'],
      ['op', 'TEXT'],
      ['attributes', 'TEXT'],
      ['payload', 'BLOB'],
      ['block_number', 'BIGINT'],
    ],
    primaryKey: ['topic', 'ordering_key', 'id'],
  },
  {
    name: 'materialized_identity',
    columns: [
      ['id', 'TEXT'],
      ['route', 'TEXT'],
      ['topic', 'TEXT'],
      ['ordering_key', 'TEXT'],
      ['attributes', 'TEXT'],
    ],
    primaryKey: ['id'],
  },
  {
    name: 'rollback_inverse',
    columns: [
      ['route', 'TEXT'],
      ['topic', 'TEXT'],
      ['ordering_key', 'TEXT'],
      ['id', 'TEXT'],
      ['op', 'TEXT'],
      ['payload', 'BLOB'],
    ],
    primaryKey: ['topic', 'ordering_key', 'id'],
  },
] as const

export const STATE_INDEXES: readonly IndexSpec[] = [
  { name: 'manifest_block_idx', table: 'manifest', columns: ['block_number'] },
  { name: 'manifest_row_idx', table: 'manifest', columns: ['topic', 'ordering_key', 'id', 'seq'] },
  // Identity is global to a producer, so the stability check looks an id up across partitions.
  { name: 'manifest_id_idx', table: 'manifest', columns: ['id', 'seq'] },
  { name: 'baseline_id_idx', table: 'materialized_baseline', columns: ['id'] },
] as const

/**
 * A driver's naming scheme, shared between its DDL and every DML statement so the two can never
 * name a table differently. `table` is a fully addressable identifier (schema + prefix, where
 * the dialect has either); `index` stays unschema-qualified — Postgres places an index in its
 * table's schema implicitly and rejects a schema-qualified index name outright.
 */
export type SchemaQualifier = {
  table(name: string): string
  index(name: string): string
}

/** Renders `STATE_TABLES`/`STATE_INDEXES` into one dialect's DDL, addressed through `qualifier`. */
export function renderSchemaDDL(dialect: 'sqlite' | 'postgres', qualifier: SchemaQualifier): string[] {
  const rowIdPk =
    dialect === 'sqlite' ? 'INTEGER PRIMARY KEY AUTOINCREMENT' : 'BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY'
  const blob = dialect === 'sqlite' ? 'BLOB' : 'BYTEA'
  const bigint = 'BIGINT'

  const columnDDL = (type: ColumnType): string => {
    switch (type) {
      case 'ROWID_PK':
        return rowIdPk
      case 'BLOB':
        return blob
      case 'BIGINT':
        return bigint
      case 'TEXT':
        return 'TEXT'
    }
  }

  const statements: string[] = []

  for (const table of STATE_TABLES) {
    const columns = table.columns.map(([name, type]) => `${name} ${columnDDL(type)}`)
    const primaryKey = table.primaryKey?.length ? [`PRIMARY KEY (${table.primaryKey.join(', ')})`] : []

    statements.push(
      `CREATE TABLE IF NOT EXISTS ${qualifier.table(table.name)} (${[...columns, ...primaryKey].join(', ')})`,
    )
  }

  for (const index of STATE_INDEXES) {
    statements.push(
      `CREATE INDEX IF NOT EXISTS ${qualifier.index(index.name)} ON ${qualifier.table(index.table)} (${index.columns.join(', ')})`,
    )
  }

  return statements
}

/** SQLite returns BIGINT-affinity columns as JS numbers already; Postgres returns them as text. */
export function toNumber(value: unknown): number {
  return typeof value === 'number' ? value : Number(value)
}
