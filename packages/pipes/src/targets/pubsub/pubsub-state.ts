import { BlockCursor, Logger, TargetState } from '~/core/index.js'
import { SqliteOptions, SqliteSync } from '~/drivers/sqlite/sqlite.js'

import { CommitInput, OutboxRow, PubsubState } from './pubsub-state-types.js'
import { PostgresConnection, PostgresDriver } from './state/postgres-driver.js'
import { SqlDriver } from './state/sql-driver.js'
import { SqliteDriver } from './state/sqlite-driver.js'
import { SqlPubsubStateCore } from './state/state-core.js'

export {
  type CommitInput,
  type OutboxRow,
  type PendingOperation,
  type PubsubState,
  type RouteMode,
  type RowIdSource,
  STATE_SCHEMA_VERSION,
  stableAttributes,
} from './pubsub-state-types.js'
export { type PostgresConnection } from './state/postgres-driver.js'

/**
 * The seam the storage-fault suites inject a driver through — a full or failing volume cannot
 * be staged against a real file. Symbol-keyed so it stays out of the option surface consumers
 * see, and absent from the public entrypoint.
 *
 * @internal
 */
export const INTERNAL_DRIVER: unique symbol = Symbol.for('@subsquid/pipes:pubsub-state:driver')

/**
 * `PubsubState` delegating every call to one `SqlPubsubStateCore` — the shared state machine —
 * over whichever `SqlDriver` a subclass constructs. `SqlitePubsubState` and `PostgresPubsubState`
 * differ only in that driver; every sequencing, fork-compensation and finality-folding rule
 * lives once, in the core.
 */
abstract class SqlBackedPubsubState implements PubsubState {
  readonly #core: SqlPubsubStateCore

  protected constructor(driver: SqlDriver, id?: string) {
    this.#core = new SqlPubsubStateCore(driver, id)
  }

  get cursorKey(): string {
    return this.#core.cursorKey
  }

  open(ctx: { cursorKey: string; logger: Logger; allowColdStart?: boolean }): Promise<{ coldStart: boolean }> {
    return this.#core.open(ctx)
  }

  getCursor(): Promise<TargetState | undefined> {
    return this.#core.getCursor()
  }

  getMeta(key: string): Promise<string | undefined> {
    return this.#core.getMeta(key)
  }

  setMeta(key: string, value: string): Promise<void> {
    return this.#core.setMeta(key, value)
  }

  commit(input: CommitInput): Promise<void> {
    return this.#core.commit(input)
  }

  pending(): Promise<OutboxRow[]> {
    return this.#core.pending()
  }

  confirm(rowIds: number[]): Promise<void> {
    return this.#core.confirm(rowIds)
  }

  fork(canonicalBlocks: BlockCursor[]): Promise<BlockCursor | null> {
    return this.#core.fork(canonicalBlocks)
  }

  stats(): Promise<{ outbox: number; manifest: number }> {
    return this.#core.stats()
  }

  close(): Promise<void> {
    return this.#core.close()
  }
}

export type SqlitePubsubStateOptions = {
  path: string
  /** Cursor key override; defaults to the pipe id once `open` binds it (ADR-2). */
  id?: string
  /** @internal */
  [INTERNAL_DRIVER]?: (options: SqliteOptions) => Promise<SqliteSync>
}

/** SQLite-backed `PubsubState` — one file, one producer. The default `state` backend. */
export class SqlitePubsubState extends SqlBackedPubsubState {
  constructor(options: SqlitePubsubStateOptions) {
    super(new SqliteDriver({ path: options.path, connect: options[INTERNAL_DRIVER] }), options.id)
  }
}

export type PostgresPubsubStateOptions = {
  /** A connection string, a `pg.PoolConfig`, or an already-constructed `pg.Pool` to reuse. A
   *  `Pool` is not consumed wholesale — one dedicated connection is checked out and held for the
   *  state's lifetime, because the single-writer lock is a property of the session. */
  connection: PostgresConnection
  /** Defaults to "public". */
  schema?: string
  /** Prefix for the 8 backing tables (`${prefix}meta`, `${prefix}outbox`, …). Defaults to "pubsub_". */
  tablePrefix?: string
  /** Cursor key override; defaults to the pipe id once `open` binds it (ADR-2). */
  id?: string
}

/** Postgres-backed `PubsubState` — same contract as `SqlitePubsubState`, for a stateless deploy
 *  that would rather not carry a local file. */
export class PostgresPubsubState extends SqlBackedPubsubState {
  constructor(options: PostgresPubsubStateOptions) {
    super(
      new PostgresDriver({ connection: options.connection, schema: options.schema, tablePrefix: options.tablePrefix }),
      options.id,
    )
  }
}

/**
 * The `state` option of `pubsubTarget`. `SqlitePubsubStateOptions` (no `kind`, or `kind:
 * 'sqlite'`) is the default and keeps existing `{ path }` configuration working unchanged;
 * `PostgresPubsubStateOptions` needs `kind: 'postgres'`; a `PubsubState` is used verbatim.
 */
export type PubsubStateConfig =
  | ({ kind?: 'sqlite' } & SqlitePubsubStateOptions)
  | ({ kind: 'postgres' } & PostgresPubsubStateOptions)
  | PubsubState

export function resolvePubsubState(config: PubsubStateConfig): PubsubState {
  if (isPubsubState(config)) return config

  return config.kind === 'postgres' ? new PostgresPubsubState(config) : new SqlitePubsubState(config)
}

/** A `PubsubStateConfig` is a `PubsubState` used verbatim whenever it isn't one of the two
 *  built-in option shapes — both lack `commit`, the one method every `PubsubState` has. */
export function isPubsubState(config: PubsubStateConfig): config is PubsubState {
  return typeof (config as Partial<PubsubState>).commit === 'function'
}

/** A short, credential-free description for logs and error messages. */
export function describePubsubStateConfig(config: PubsubStateConfig): string {
  if (isPubsubState(config)) return 'custom'

  return config.kind === 'postgres'
    ? `postgres (schema "${config.schema ?? 'public'}", prefix "${config.tablePrefix ?? 'pubsub_'}")`
    : config.path
}
