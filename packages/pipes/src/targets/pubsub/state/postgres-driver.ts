import type { Client, Pool, PoolClient, PoolConfig } from 'pg'

import { DriverLockedError, DriverUnavailableError, SchemaQualifier, SqlDriver, renderSchemaDDL } from './sql-driver.js'

export type PostgresConnection = string | PoolConfig | Pool

export type PostgresDriverOptions = {
  connection: PostgresConnection
  /** Defaults to "public". */
  schema?: string
  /** Prefix for the 8 backing tables (`${prefix}meta`, `${prefix}outbox`, …). Defaults to "pubsub_". */
  tablePrefix?: string
}

/** Turns the shared `?` placeholders into Postgres's positional `$1, $2, …`. None of this state's
 *  SQL embeds a literal `?` in a string or comment, so a plain sequential replace is exact. */
function toPositional(sql: string): string {
  let i = 0

  return sql.replace(/\?/g, () => `$${++i}`)
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

const CONNECTION_ERROR_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'ETIMEDOUT',
  'EPIPE',
  'EHOSTUNREACH',
  'ENETUNREACH',
])

/**
 * SQLSTATE class 53 (insufficient resources: disk full, out of memory, too many connections),
 * class 58 (system error: I/O, undefined/duplicate file), 25006 (a replica or otherwise
 * read-only session), and a dropped connection (a Node network error code, or `pg`'s own
 * unstructured "connection terminated" message) — the store failing, not the statement.
 */
function isPostgresStorageFailure(e: unknown): boolean {
  const code = (e as { code?: unknown } | null)?.code
  if (typeof code === 'string' && (/^5[38]/.test(code) || code === '25006' || CONNECTION_ERROR_CODES.has(code))) {
    return true
  }

  const message = e instanceof Error ? e.message : ''

  return /connection.*(terminated|closed|ended)|termina(ting|ted) connection/i.test(message)
}

/** Strips credentials from a DSN, and never assumes a bare Pool/Client exposes any. */
function redact(connection: PostgresConnection): string {
  if (typeof connection === 'string') {
    try {
      const url = new URL(connection)
      if (url.password) url.password = '***'
      if (url.username) url.username = '***'
      // `pg` also accepts credentials as query parameters (?password=..., ?sslpassword=...).
      // The query string is diagnostic noise here regardless, so it is dropped outright rather
      // than allowlisting which keys might carry a secret.
      url.search = ''

      return url.toString()
    } catch {
      return 'the configured Postgres connection'
    }
  }

  if (typeof (connection as { query?: unknown }).query === 'function') {
    return 'the provided pg.Pool'
  }

  const config = connection as PoolConfig

  return `postgres://${config.host ?? 'localhost'}:${config.port ?? 5432}/${config.database ?? ''}`
}

/**
 * One Postgres session, held for the state's lifetime, mirroring the SQLite driver's single
 * open file handle. Single-writer is a session-level advisory lock scoped to `(schema,
 * tablePrefix)` — the same granularity as "one state file, one producer": the lock does not key
 * on the cursor id, because the outbox, manifest and sequence counter are producer-wide, not
 * per-cursor (mirrors `SqlitePubsubState`'s own note on this).
 *
 * A caller-owned `Pool` is supported by checking out exactly one dedicated `PoolClient` and
 * holding it — an advisory lock is a property of the physical session, so it cannot float across
 * a pool's connections the way pooled queries normally do.
 */
export class PostgresDriver implements SqlDriver {
  readonly #options: PostgresDriverOptions
  readonly #schema: string
  readonly #tablePrefix: string
  readonly #lockKey: string
  #client?: Client | PoolClient
  /** Set only when we own the client outright (string/PoolConfig) — then `close()` ends it
   *  instead of releasing it back to a pool we do not own. */
  #owned = false
  /** False once a rollback could not be confirmed — `close()` then destroys the connection
   *  instead of returning a possibly-dirty one to a shared pool. */
  #unwound = true
  /** A dropped connection surfaces here first (see the `error` listener in `connect()`) — `pg`
   *  emits it as an event, and an EventEmitter's `error` event with no listener crashes the
   *  process instead of rejecting whatever query happened to be in flight. */
  #connectionError?: Error
  #onConnectionError?: (e: Error) => void

  constructor(options: PostgresDriverOptions) {
    this.#options = options
    this.#schema = options.schema ?? 'public'
    this.#tablePrefix = options.tablePrefix ?? 'pubsub_'
    this.#lockKey = `pubsub-state:${this.#schema}.${this.#tablePrefix}`
  }

  get location(): string {
    return `${redact(this.#options.connection)} (schema "${this.#schema}", prefix "${this.#tablePrefix}")`
  }

  table(name: string): string {
    return `${quoteIdent(this.#schema)}.${quoteIdent(this.#tablePrefix + name)}`
  }

  #qualifier(): SchemaQualifier {
    return {
      table: (name) => this.table(name),
      // Postgres places an index in its table's schema implicitly and rejects an explicitly
      // schema-qualified index name — only the prefix carries over.
      index: (name) => quoteIdent(this.#tablePrefix + name),
    }
  }

  schemaDDL(): string[] {
    return renderSchemaDDL('postgres', this.#qualifier())
  }

  async connect(): Promise<void> {
    // A retry after a dropped connection reuses this same driver instance (the state above it
    // is reused too): a fresh session must not inherit the previous one's dead-connection error
    // or its unconfirmed-rollback flag, or every operation on the new connection would fail
    // before it ever ran.
    this.#connectionError = undefined
    this.#unwound = true

    let pgModule: typeof import('pg')
    try {
      pgModule = await import('pg')
    } catch (e) {
      throw new DriverUnavailableError(
        new Error('The `pg` package is required for a Postgres PubSub state and is not installed.', { cause: e }),
      )
    }

    // CJS/ESM interop can surface `pg`'s exports either as named bindings or under `.default`.
    const { Pool, Client: PgClient } = (
      'Pool' in pgModule ? pgModule : (pgModule as any).default
    ) as typeof import('pg')

    // A connection error — during the handshake below, or dropped later (network blip, server
    // restart) — surfaces as an `error` event, not only a rejected promise: `pg`'s `Client` is
    // known to emit one even for a failed `connect()`. An EventEmitter throws past all promise
    // handling for an `error` event with no listener, so the listener has to be attached before
    // `connect()` runs, not after it resolves. `#assertConnected` is what turns a later one back
    // into a normal rejection.
    this.#onConnectionError = (e) => {
      this.#connectionError = e
    }

    try {
      if (this.#options.connection instanceof Pool) {
        // The Pool itself owns connection-lifecycle handling for a fresh checkout — a failed
        // handshake here only ever rejects this call, so the listener only needs to cover the
        // checked-out client's life after we have a reference to it.
        this.#client = await this.#options.connection.connect()
        this.#client.on('error', this.#onConnectionError)
        this.#owned = false
      } else {
        const client = new PgClient(
          typeof this.#options.connection === 'string'
            ? { connectionString: this.#options.connection }
            : this.#options.connection,
        )
        client.on('error', this.#onConnectionError)
        await client.connect()
        this.#client = client
        this.#owned = true
      }
    } catch (e) {
      // `this.#client` was never set, so the abandoned `client`/checkout above (with its
      // listener) holds no reference back to this driver and is simply left for GC.
      throw new DriverUnavailableError(e)
    }

    try {
      const result = await this.#client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock(hashtext($1)::bigint) AS locked',
        [this.#lockKey],
      )

      if (!result.rows[0]?.locked) {
        throw new Error(`advisory lock "${this.#lockKey}" is already held by another session`)
      }
    } catch (e) {
      await this.#release()
      throw new DriverLockedError(e)
    }
  }

  async #release(): Promise<void> {
    const client = this.#client
    this.#client = undefined
    if (!client) return

    if (this.#onConnectionError) {
      // Detach before returning a PoolClient to a shared pool — otherwise our listener (and
      // everything it keeps reachable through this closure) outlives this driver on a
      // connection some unrelated later checkout will reuse.
      client.removeListener('error', this.#onConnectionError)
      this.#onConnectionError = undefined
    }

    if (this.#owned) {
      await (client as Client).end().catch(() => {})
    } else {
      // A connection we could not confirm rollback-clean is destroyed instead of returned to
      // the pool, where a later, unrelated checkout would otherwise inherit its state.
      ;(client as PoolClient).release(!this.#unwound)
    }
  }

  /** A connection error is reported once, here, and turns every further operation into a
   *  rejection instead of leaving a doomed query to hang or throw something confusing. */
  #assertConnected(): void {
    if (this.#connectionError) throw this.#connectionError
  }

  async get<T>(sql: string, params: unknown[] = []): Promise<T | undefined> {
    this.#assertConnected()
    const result = await this.#client!.query<T & Record<string, unknown>>(toPositional(sql), params)

    return result.rows[0]
  }

  async all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    this.#assertConnected()
    const result = await this.#client!.query<T & Record<string, unknown>>(toPositional(sql), params)

    return result.rows
  }

  async exec(sql: string, params: unknown[] = []): Promise<void> {
    this.#assertConnected()
    await this.#client!.query(toPositional(sql), params)
  }

  /** Postgres has no "BEGIN IMMEDIATE": no SQLite-style whole-database writer lock to
   *  pre-acquire — write locks are row-level and taken as statements touch rows. */
  async begin(_immediate: boolean): Promise<void> {
    this.#assertConnected()
    await this.#client!.query('BEGIN')
  }

  async commit(): Promise<void> {
    this.#assertConnected()
    await this.#client!.query('COMMIT')
  }

  async rollback(): Promise<boolean> {
    if (this.#connectionError) {
      this.#unwound = false

      return false
    }

    try {
      // Unlike SQLite, issuing ROLLBACK with no open transaction is a harmless no-op in
      // Postgres (a NOTICE, not an error), so this only fails when the session itself is dead.
      await this.#client!.query('ROLLBACK')
      this.#unwound = true
    } catch {
      this.#unwound = false
    }

    return this.#unwound
  }

  isStorageFailure(e: unknown): boolean {
    return isPostgresStorageFailure(e)
  }

  async close(): Promise<void> {
    if (!this.#client) return

    try {
      await this.#client.query('SELECT pg_advisory_unlock(hashtext($1)::bigint)', [this.#lockKey])
    } catch {
      // Best-effort: ending/releasing the connection below drops the session-level lock anyway.
    }

    await this.#release()
  }
}
