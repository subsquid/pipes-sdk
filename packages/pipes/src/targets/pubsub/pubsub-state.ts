import {
  BlockCursor,
  CursorKey,
  Logger,
  RollbackRecord,
  TargetState,
  normalizeFinalized,
  resolveForkCursor,
} from '~/core/index.js'
import { SqliteSync, loadSqlite } from '~/drivers/sqlite/sqlite.js'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import { PubsubOp } from './protocol.js'

export const STATE_SCHEMA_VERSION = '1'

export type DeliveryProfile = 'lww' | 'ordered'

export type RouteMode = 'event' | 'materialized'

/**
 * One wire operation on its way out: everything the state needs to sequence it, publish it,
 * and — when it sits above the finalized watermark — compensate it after a fork.
 */
export type PendingOperation = {
  topic: string
  /** Empty under `lww`, where Pub/Sub ordering keys are not used at all. */
  orderingKey: string
  mode: RouteMode
  op: PubsubOp
  /** Absent on `heartbeat` — a heartbeat is not a row and is never rollbackable. */
  id?: string
  attributes: Record<string, string>
  payload: Uint8Array
  blockNumber: number
  /**
   * Whether a fork could still orphan this operation. False below the finalized watermark,
   * on the finalized stream, and under `assumeNoForks` — those never enter the manifest.
   */
  rollbackable: boolean
  /**
   * The route's `rollbackWhenMissing` inverse, already encoded. Stored on the id's first
   * rollbackable publish, because at fork time the draft that produced it is long gone.
   */
  inverse?: { op: 'upsert' | 'delete'; payload: Uint8Array }
}

export type OutboxRow = {
  rowId: number
  topic: string
  op: PubsubOp
  id?: string
  orderingKey: string
  seq: number
  attributes: Record<string, string>
  payload: Uint8Array
}

export type CommitInput = {
  operations: PendingOperation[]
  /** The source's unfinalized rollback chain — the fork-resolution ledger. */
  ledger: BlockCursor[]
  cursor: BlockCursor
  finalized: BlockCursor | null
  /**
   * Whether a fork can still arrive on this pipe. False on the finalized stream and under
   * `assumeNoForks`, where manifests and baselines would only be write amplification.
   */
  forkCapable: boolean
  /**
   * Bookkeeping that must advance with the batch, not beside it (heartbeat cadence). Applied
   * inside the same transaction, so a failed commit cannot leave the cadence advanced for a
   * heartbeat that was never enqueued.
   */
  meta?: Record<string, string>
}

/**
 * The combined local state: resume cursor, rollback manifest, finalized baselines, publish
 * outbox and sequence counters, all committed in one transaction per batch (CN-17).
 *
 * Kept an interface so a remote backend (GCS/Firestore/Postgres) can replace the default
 * SQLite implementation for stateless deploys — the transactional contract is its core
 * requirement, not the storage engine.
 */
export interface PubsubState {
  /** Opens the backing store, migrates it, and reports whether it started empty (CN-46). */
  open(ctx: { cursorKey: string; logger: Logger }): Promise<{ coldStart: boolean }>
  getCursor(): Promise<TargetState | undefined>
  getMeta(key: string): Promise<string | undefined>
  setMeta(key: string, value: string): Promise<void>
  /** The per-batch transaction of 7.3 step 2: sequence, enqueue, record, advance, prune. */
  commit(input: CommitInput): Promise<void>
  /** Everything enqueued but not yet confirmed published, in publish order. */
  pending(): Promise<OutboxRow[]>
  confirm(rowIds: number[]): Promise<void>
  /**
   * Resolve a fork: fold every orphaned id back to the state that survives at the safe
   * cursor, enqueue the compensations, and rewind. Returns the safe cursor, or `null` on a
   * dead-end fork (the whole rollbackable manifest is compensated first).
   */
  fork(canonicalBlocks: BlockCursor[]): Promise<BlockCursor | null>
  stats(): Promise<{ outbox: number; manifest: number }>
  close(): Promise<void>
}

type MetaRow = { value: string }
type CursorRow = { latest: string; finalized: string | null }
type OutboxDbRow = {
  row_id: number
  topic: string
  op: string
  id: string | null
  ordering_key: string
  seq: number
  attributes: string
  payload: Uint8Array | Buffer | null
}
type ManifestDbRow = {
  topic: string
  ordering_key: string
  seq: number
  block_number: number
  mode: string
  op: string
  id: string
  attributes: string
  payload: Uint8Array | Buffer | null
}

const META_LWW_SEQ = 'lww_seq'
const META_CURSOR_KEY = 'cursor_key'
const META_DELIVERY = 'delivery'

/**
 * An unambiguous key for a composite identity. A separator-joined string is not one:
 * `("a", "b c")` and `("a b", "c")` collide, and for a fork that means two orphaned rows
 * folding into one group — one compensation published where two are owed.
 */
const identityKey = (...parts: string[]) => JSON.stringify(parts)

/**
 * Attributes are stored and compared in one canonical form, so an id whose route emits the
 * same attributes in a different insertion order is not read as having moved.
 */
export function stableAttributes(attributes: Record<string, string>): string {
  return JSON.stringify(
    Object.fromEntries(
      Object.keys(attributes)
        .sort()
        .map((key) => [key, attributes[key]]),
    ),
  )
}

function asBytes(value: Uint8Array | Buffer | null): Uint8Array {
  if (!value) return new Uint8Array()

  return value instanceof Uint8Array ? value : new Uint8Array(value)
}

function asBlob(value: Uint8Array): Buffer {
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength)
}

function isLockError(e: unknown): boolean {
  const message = e instanceof Error ? e.message : String(e)

  return /database is locked|SQLITE_BUSY/i.test(message)
}

function lockedError(path: string, cause: unknown): PubsubTargetError {
  return new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_LOCKED, [
    `Another process holds the Pub/Sub state file "${path}".`,
    'Exactly one producer may own a state file: it is the authoritative sequencer for every id it ' +
      'publishes, and a second writer would hand consumers sequence numbers they have already seen.',
    cause instanceof Error ? cause.message : String(cause),
  ])
}

export type SqlitePubsubStateOptions = {
  path: string
  delivery: DeliveryProfile
  /** Cursor key override; defaults to the pipe id once `open` binds it (ADR-2). */
  id?: string
}

export class SqlitePubsubState implements PubsubState {
  readonly #options: SqlitePubsubStateOptions
  readonly #key: CursorKey
  #db?: SqliteSync
  #logger?: Logger

  constructor(options: SqlitePubsubStateOptions) {
    this.#options = options
    this.#key = new CursorKey(options.id)
  }

  get cursorKey(): string {
    return this.#key.value
  }

  async open({ cursorKey, logger }: { cursorKey: string; logger: Logger }): Promise<{ coldStart: boolean }> {
    this.#key.bind(cursorKey)
    this.#logger = logger

    let db: SqliteSync
    try {
      db = await loadSqlite({ path: this.#options.path })
    } catch (e) {
      // A live producer holds the file in exclusive locking mode, so the second one fails
      // right here — on the driver's own WAL pragma, before any target code runs.
      if (isLockError(e)) throw lockedError(this.#options.path, e)

      throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_UNAVAILABLE, [
        `Cannot open the Pub/Sub state file at "${this.#options.path}": ${e instanceof Error ? e.message : String(e)}`,
        'The state is the producer’s sequencer — it must live on a persistent volume, not ephemeral container storage.',
      ])
    }

    this.#db = db

    // Every refusal below happens with the exclusive lock already held, so it releases the
    // file on the way out: otherwise a caller that retries in the same process meets its own
    // dead connection and reads the mismatch as a second producer.
    try {
      // The shared driver defaults to synchronous = NORMAL, under which an OS crash can roll
      // back the most recent commits. Here a rolled-back commit is a published operation the
      // state has no record of, so the target pays for FULL.
      db.exec('PRAGMA synchronous = FULL;')

      this.#lock()
      this.#migrate()

      const version = this.getMetaSync('schema_version')
      if (version && version !== STATE_SCHEMA_VERSION) {
        throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_SCHEMA_VERSION, [
          `The Pub/Sub state at "${this.#options.path}" was written with schema version ${version}, ` +
            `this build speaks ${STATE_SCHEMA_VERSION}.`,
        ])
      }

      const coldStart = !version
      if (coldStart) {
        this.setMetaSync('schema_version', STATE_SCHEMA_VERSION)
        this.setMetaSync(META_LWW_SEQ, '0')
        this.setMetaSync(META_CURSOR_KEY, this.#key.value)
        this.setMetaSync(META_DELIVERY, this.#options.delivery)
      } else {
        this.#assertSameProducer()
      }

      return { coldStart }
    } catch (e) {
      await this.close()

      throw e
    }
  }

  /**
   * A state file is one producer's sequencer, not a shared cache. The outbox, the manifest
   * and the counters are producer-wide — only the cursor row is keyed — so opening another
   * pipe's file would report a clean warm start while inheriting its unpublished operations,
   * and switching profile would re-issue sequence numbers under the other scoping. Both are
   * refused before anything is drained.
   */
  #assertSameProducer(): void {
    const storedKey = this.getMetaSync(META_CURSOR_KEY)
    if (storedKey !== undefined && storedKey !== this.#key.value) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_IDENTITY_MISMATCH, [
        `The Pub/Sub state at "${this.#options.path}" belongs to producer "${storedKey}", but this pipe ` +
          `binds "${this.#key.value}".`,
        'One state file per producer: its outbox, manifest and sequence counters are producer-wide, so ' +
          'adopting this one would publish another producer’s pending operations under this pipe’s identity.',
      ])
    }

    const storedDelivery = this.getMetaSync(META_DELIVERY)
    if (storedDelivery !== undefined && storedDelivery !== this.#options.delivery) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_PROFILE_MISMATCH, [
        `The Pub/Sub state at "${this.#options.path}" was written under the "${storedDelivery}" delivery ` +
          `profile; this pipe runs "${this.#options.delivery}".`,
        'The profiles scope `_seq` differently — one producer-wide counter versus one dense counter per ' +
          'partition — so continuing would re-issue sequence numbers consumers already hold. Changing ' +
          'profile is a new feed: fresh namespace, fresh state, re-bootstrapped consumers.',
      ])
    }

    // Written on first open of a file that predates these keys, so the checks bind from now on.
    if (storedKey === undefined) this.setMetaSync(META_CURSOR_KEY, this.#key.value)
    if (storedDelivery === undefined) this.setMetaSync(META_DELIVERY, this.#options.delivery)
  }

  /**
   * Exclusive locking mode makes the invariant an OS-level fact rather than a convention:
   * a second producer on the same file fails here, instead of silently double-assigning
   * sequence numbers that a conforming consumer would discard as duplicates.
   */
  #lock(): void {
    const db = this.#db!
    try {
      db.exec('PRAGMA locking_mode = EXCLUSIVE;')
      db.exec('BEGIN IMMEDIATE')
      db.exec('COMMIT')
    } catch (e) {
      throw lockedError(this.#options.path, e)
    }
  }

  #migrate(): void {
    const db = this.#db!

    db.exec('BEGIN')
    try {
      db.exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS cursor (
          id          TEXT PRIMARY KEY,
          latest      TEXT,
          finalized   TEXT,
          updated_at  INTEGER
        )`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS outbox (
          row_id        INTEGER PRIMARY KEY AUTOINCREMENT,
          topic         TEXT,
          op            TEXT,
          id            TEXT,
          ordering_key  TEXT,
          seq           INTEGER,
          attributes    TEXT,
          payload       BLOB,
          block_number  INTEGER
        )`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS partition_seq (
          topic         TEXT,
          ordering_key  TEXT,
          next_seq      INTEGER,
          PRIMARY KEY (topic, ordering_key)
        )`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS ledger_blocks (
          number     INTEGER PRIMARY KEY,
          hash       TEXT,
          timestamp  INTEGER
        )`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS manifest (
          topic         TEXT,
          ordering_key  TEXT,
          seq           INTEGER,
          block_number  INTEGER,
          mode          TEXT,
          op            TEXT,
          id            TEXT,
          attributes    TEXT,
          payload       BLOB,
          PRIMARY KEY (topic, ordering_key, seq)
        )`)
      db.exec(`CREATE INDEX IF NOT EXISTS manifest_block_idx ON manifest (block_number)`)
      db.exec(`CREATE INDEX IF NOT EXISTS manifest_row_idx ON manifest (topic, ordering_key, id, seq)`)
      // Identity is global to a producer, so the stability check looks an id up across partitions.
      db.exec(`CREATE INDEX IF NOT EXISTS manifest_id_idx ON manifest (id, seq)`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS materialized_baseline (
          topic         TEXT,
          ordering_key  TEXT,
          id            TEXT,
          op            TEXT,
          attributes    TEXT,
          payload       BLOB,
          block_number  INTEGER,
          PRIMARY KEY (topic, ordering_key, id)
        )`)
      db.exec(`CREATE INDEX IF NOT EXISTS baseline_id_idx ON materialized_baseline (id)`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS materialized_identity (
          id            TEXT PRIMARY KEY,
          topic         TEXT,
          ordering_key  TEXT,
          attributes    TEXT
        )`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS rollback_inverse (
          topic         TEXT,
          ordering_key  TEXT,
          id            TEXT,
          op            TEXT,
          payload       BLOB,
          PRIMARY KEY (topic, ordering_key, id)
        )`)
      db.exec('COMMIT')
    } catch (e) {
      db.exec('ROLLBACK')
      throw e
    }
  }

  // ─── meta ───────────────────────────────────────────────────────────────────

  getMetaSync(key: string): string | undefined {
    return this.#db!.get<MetaRow>('SELECT value FROM meta WHERE key = ?', [key])?.value
  }

  setMetaSync(key: string, value: string): void {
    this.#db!.exec(
      'INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value',
      [key, value],
    )
  }

  async getMeta(key: string): Promise<string | undefined> {
    return this.getMetaSync(key)
  }

  async setMeta(key: string, value: string): Promise<void> {
    this.setMetaSync(key, value)
  }

  // ─── cursor ─────────────────────────────────────────────────────────────────

  async getCursor(): Promise<TargetState | undefined> {
    const row = this.#db!.get<CursorRow>('SELECT latest, finalized FROM cursor WHERE id = ?', [this.#key.value])
    if (!row?.latest) return

    // The finalized floor is handed back explicitly — the source seeds its monotonic
    // watermark from it, and `null` has to state the absence rather than omit it (RP-3).
    return {
      latest: JSON.parse(row.latest) as BlockCursor,
      finalized: normalizeFinalized(row.finalized ? (JSON.parse(row.finalized) as BlockCursor) : undefined) ?? null,
    }
  }

  #saveCursor(cursor: BlockCursor, finalized: BlockCursor | null): void {
    this.#db!.exec(
      `INSERT INTO cursor (id, latest, finalized, updated_at) VALUES (?, ?, ?, ?)
       ON CONFLICT (id) DO UPDATE SET latest = excluded.latest, finalized = excluded.finalized, updated_at = excluded.updated_at`,
      [this.#key.value, JSON.stringify(cursor), finalized ? JSON.stringify(finalized) : null, Date.now()],
    )
  }

  // ─── sequencing ─────────────────────────────────────────────────────────────

  /**
   * The next `_seq`: one producer-wide counter under `lww` (a per-id version needs neither
   * density nor per-topic scoping), the partition's dense counter under `ordered`. Both never
   * rewind — not across restarts, not across forks — which is what lets a consumer drop a
   * republished operation instead of re-applying it.
   */
  #nextSeq(topic: string, orderingKey: string): number {
    const db = this.#db!

    if (this.#options.delivery === 'lww') {
      const next = Number(this.getMetaSync(META_LWW_SEQ) ?? '0') + 1
      this.setMetaSync(META_LWW_SEQ, String(next))

      return next
    }

    const row = db.get<{ next_seq: number }>(
      'SELECT next_seq FROM partition_seq WHERE topic = ? AND ordering_key = ?',
      [topic, orderingKey],
    )
    const next = (row?.next_seq ?? 0) + 1

    db.exec(
      `INSERT INTO partition_seq (topic, ordering_key, next_seq) VALUES (?, ?, ?)
       ON CONFLICT (topic, ordering_key) DO UPDATE SET next_seq = excluded.next_seq`,
      [topic, orderingKey, next],
    )

    return next
  }

  #enqueue(operation: PendingOperation, seq: number): void {
    this.#db!.exec(
      `INSERT INTO outbox (topic, op, id, ordering_key, seq, attributes, payload, block_number)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        operation.topic,
        operation.op,
        operation.id ?? null,
        operation.orderingKey,
        seq,
        stableAttributes(operation.attributes),
        asBlob(operation.payload),
        operation.blockNumber,
      ],
    )
  }

  // ─── per-batch transaction (CN-17) ──────────────────────────────────────────

  async commit({ operations, ledger, cursor, finalized, forkCapable, meta }: CommitInput): Promise<void> {
    const db = this.#db!

    db.exec('BEGIN IMMEDIATE')
    try {
      for (const operation of operations) {
        const seq = this.#nextSeq(operation.topic, operation.orderingKey)
        this.#enqueue(operation, seq)

        if (operation.id === undefined) continue

        if (operation.mode === 'materialized') {
          this.#assertIdentityStable(operation)
          if (!forkCapable) this.#updateMaterializedIdentity(operation)
        }

        if (!operation.rollbackable) {
          // A materialized row that arrives already finalized never passes through the
          // manifest, but a fork rewinding to it still has to restore its value — so it
          // becomes a baseline directly.
          if (forkCapable && operation.mode === 'materialized') {
            this.#saveBaseline({
              topic: operation.topic,
              ordering_key: operation.orderingKey,
              id: operation.id,
              op: operation.op,
              attributes: stableAttributes(operation.attributes),
              payload: asBlob(operation.payload),
              block_number: operation.blockNumber,
            })
          }

          continue
        }

        db.exec(
          `INSERT INTO manifest (topic, ordering_key, seq, block_number, mode, op, id, attributes, payload)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            operation.topic,
            operation.orderingKey,
            seq,
            operation.blockNumber,
            operation.mode,
            operation.op,
            operation.id,
            stableAttributes(operation.attributes),
            // An orphaned write-once event has nothing to restore, so its bytes are dead weight;
            // a materialized revision keeps them verbatim so a fork can republish it.
            operation.mode === 'materialized' ? asBlob(operation.payload) : null,
          ],
        )

        if (operation.inverse) {
          // First writer wins: the inverse is pure, so a later revision would only rewrite the
          // same bytes, and the id must keep the identity it was first published under.
          db.exec(
            `INSERT OR IGNORE INTO rollback_inverse (topic, ordering_key, id, op, payload) VALUES (?, ?, ?, ?, ?)`,
            [
              operation.topic,
              operation.orderingKey,
              operation.id,
              operation.inverse.op,
              asBlob(operation.inverse.payload),
            ],
          )
        }
      }

      for (const [key, value] of Object.entries(meta ?? {})) {
        this.setMetaSync(key, value)
      }

      for (const block of ledger) {
        db.exec(
          `INSERT INTO ledger_blocks (number, hash, timestamp) VALUES (?, ?, ?)
           ON CONFLICT (number) DO UPDATE SET hash = excluded.hash, timestamp = excluded.timestamp`,
          [block.number, block.hash ?? null, block.timestamp ?? null],
        )
      }

      this.#saveCursor(cursor, finalized)

      if (finalized) {
        this.#foldFinalized(finalized.number)
      }

      db.exec('COMMIT')
    } catch (e) {
      db.exec('ROLLBACK')
      throw e
    }
  }

  /**
   * A materialized row outlives the block that last touched it, so its topic, ordering key and
   * filter attributes are fixed for its lifetime. A subscription filtered on the old attributes
   * never receives a revision carrying new ones; if a fork is possible, its repair also uses the
   * identity the row was first published under.
   */
  #assertIdentityStable(operation: PendingOperation): void {
    const db = this.#db!
    const id = operation.id!

    const known =
      db.get<{ topic: string; ordering_key: string; attributes: string }>(
        'SELECT topic, ordering_key, attributes FROM materialized_identity WHERE id = ?',
        [id],
      ) ??
      db.get<{ topic: string; ordering_key: string; attributes: string }>(
        'SELECT topic, ordering_key, attributes FROM manifest WHERE id = ? ORDER BY seq DESC LIMIT 1',
        [id],
      ) ??
      db.get<{ topic: string; ordering_key: string; attributes: string }>(
        'SELECT topic, ordering_key, attributes FROM materialized_baseline WHERE id = ? LIMIT 1',
        [id],
      )

    if (!known) return

    const attributes = stableAttributes(operation.attributes)
    if (
      known.topic === operation.topic &&
      known.ordering_key === operation.orderingKey &&
      known.attributes === attributes
    ) {
      return
    }

    const moved =
      known.topic !== operation.topic
        ? `topic "${known.topic}" → "${operation.topic}"`
        : known.ordering_key !== operation.orderingKey
          ? `ordering key "${known.ordering_key}" → "${operation.orderingKey}"`
          : `attributes ${known.attributes} → ${attributes}`

    throw new PubsubTargetError(PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED, [
      `Materialized row "${id}" changed its ${moved} between revisions.`,
      'A subscription filtered on the old attributes never receives a revision carrying the new ones, ' +
        'and a fork repair also uses the identity the row was first published with. Keep a materialized ' +
        'row’s topic, ordering key and attributes stable, or give the new shape a new id.',
    ])
  }

  /**
   * Without forks there is no manifest or payload baseline, but filter safety still needs one
   * durable identity per live materialized id. A delete ends that lifetime and frees the id.
   */
  #updateMaterializedIdentity(operation: PendingOperation): void {
    const db = this.#db!
    const id = operation.id!

    if (operation.op === 'delete') {
      db.exec('DELETE FROM materialized_identity WHERE id = ?', [id])
      return
    }

    db.exec(
      `INSERT OR IGNORE INTO materialized_identity (id, topic, ordering_key, attributes)
       VALUES (?, ?, ?, ?)`,
      [id, operation.topic, operation.orderingKey, stableAttributes(operation.attributes)],
    )
  }

  /**
   * Finality advance: collapse newly-finalized materialized revisions into their baselines,
   * then drop everything a fork can no longer reach. Bounds the rollbackable log by the
   * chain's finality depth times the operation rate.
   */
  #foldFinalized(finalizedNumber: number): void {
    const db = this.#db!

    const latest = db.all<ManifestDbRow>(
      `SELECT m.* FROM manifest m
       WHERE m.mode = 'materialized' AND m.block_number <= ?
         AND m.seq = (
           SELECT MAX(seq) FROM manifest x
           WHERE x.topic = m.topic AND x.ordering_key = m.ordering_key AND x.id = m.id AND x.block_number <= ?
         )`,
      [finalizedNumber, finalizedNumber],
    )

    for (const row of latest) {
      this.#saveBaseline(row)
    }

    db.exec('DELETE FROM manifest WHERE block_number <= ?', [finalizedNumber])
    db.exec('DELETE FROM ledger_blocks WHERE number <= ?', [finalizedNumber])
    db.exec(`
      DELETE FROM rollback_inverse
      WHERE NOT EXISTS (
        SELECT 1 FROM manifest m
        WHERE m.topic = rollback_inverse.topic
          AND m.ordering_key = rollback_inverse.ordering_key
          AND m.id = rollback_inverse.id
      )`)
  }

  /** The last finalized value of a materialized id — what a fork restores when no revision survives. */
  #saveBaseline(row: {
    topic: string
    ordering_key: string
    id: string
    op: string
    attributes: string
    payload: Uint8Array | Buffer | null
    block_number: number
  }): void {
    const db = this.#db!

    if (row.op === 'delete') {
      // A deleted id is not live: dropping its baseline keeps the table bounded, and a later
      // fork that finds neither revision nor baseline compensates with a delete anyway.
      db.exec('DELETE FROM materialized_baseline WHERE topic = ? AND ordering_key = ? AND id = ?', [
        row.topic,
        row.ordering_key,
        row.id,
      ])

      return
    }

    db.exec(
      `INSERT INTO materialized_baseline (topic, ordering_key, id, op, attributes, payload, block_number)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (topic, ordering_key, id) DO UPDATE SET
         op = excluded.op, attributes = excluded.attributes,
         payload = excluded.payload, block_number = excluded.block_number
       WHERE excluded.block_number >= materialized_baseline.block_number`,
      [row.topic, row.ordering_key, row.id, row.op, row.attributes, row.payload, row.block_number],
    )
  }

  // ─── outbox ─────────────────────────────────────────────────────────────────

  async pending(): Promise<OutboxRow[]> {
    const rows = this.#db!.all<OutboxDbRow>('SELECT * FROM outbox ORDER BY row_id ASC')

    return rows.map((row) => ({
      rowId: row.row_id,
      topic: row.topic,
      op: row.op as PubsubOp,
      id: row.id ?? undefined,
      orderingKey: row.ordering_key,
      seq: row.seq,
      attributes: JSON.parse(row.attributes) as Record<string, string>,
      payload: asBytes(row.payload),
    }))
  }

  async confirm(rowIds: number[]): Promise<void> {
    if (!rowIds.length) return

    const db = this.#db!
    db.exec('BEGIN IMMEDIATE')
    try {
      for (const rowId of rowIds) {
        db.exec('DELETE FROM outbox WHERE row_id = ?', [rowId])
      }
      db.exec('COMMIT')
    } catch (e) {
      db.exec('ROLLBACK')
      throw e
    }
  }

  async stats(): Promise<{ outbox: number; manifest: number }> {
    const db = this.#db!

    return {
      outbox: db.get<{ n: number }>('SELECT COUNT(*) AS n FROM outbox')?.n ?? 0,
      manifest: db.get<{ n: number }>('SELECT COUNT(*) AS n FROM manifest')?.n ?? 0,
    }
  }

  // ─── fork (CN-35) ───────────────────────────────────────────────────────────

  async fork(canonicalBlocks: BlockCursor[]): Promise<BlockCursor | null> {
    const db = this.#db!

    const persisted = await this.getCursor()
    const finalized = persisted?.finalized ?? undefined
    const safe = await resolveForkCursor(this.#records(finalized), canonicalBlocks)

    // Dead end: nothing local proves which published operations are canonical, so fold the
    // ENTIRE rollbackable manifest back to the finalized baselines and stop. Fail-closed —
    // if the fork invalidated the finalized floor itself, the consumer needs a rebuild.
    const floor = safe ? safe.number : (finalized?.number ?? -1)

    db.exec('BEGIN IMMEDIATE')
    try {
      const compensations = this.#compensate(floor)
      this.#logger?.debug(
        `fork at block ${floor}: ${compensations} compensating operation(s) enqueued${safe ? '' : ' (dead-end fork)'}`,
      )

      db.exec('DELETE FROM manifest WHERE block_number > ?', [floor])
      db.exec('DELETE FROM ledger_blocks WHERE number > ?', [floor])
      db.exec(`
        DELETE FROM rollback_inverse
        WHERE NOT EXISTS (
          SELECT 1 FROM manifest m
          WHERE m.topic = rollback_inverse.topic
            AND m.ordering_key = rollback_inverse.ordering_key
            AND m.id = rollback_inverse.id
        )`)

      const rewound = safe ?? finalized
      if (rewound) {
        this.#saveCursor(rewound, finalized ?? null)
      }

      db.exec('COMMIT')
    } catch (e) {
      db.exec('ROLLBACK')
      throw e
    }

    return safe
  }

  /**
   * One rule per id: fold away the orphaned revision suffix and publish whatever state
   * remains at the safe cursor — the surviving revision, else the finalized baseline, else
   * the route's stored inverse (a delete by default).
   */
  #compensate(floor: number): number {
    const db = this.#db!

    const orphaned = db.all<ManifestDbRow>(
      `SELECT * FROM manifest WHERE block_number > ? ORDER BY topic, ordering_key, id, seq ASC`,
      [floor],
    )
    if (!orphaned.length) return 0

    const groups = new Map<string, ManifestDbRow[]>()
    for (const row of orphaned) {
      const key = identityKey(row.topic, row.ordering_key, row.id)
      const group = groups.get(key)
      if (group) {
        group.push(row)
      } else {
        groups.set(key, [row])
      }
    }

    let enqueued = 0

    for (const rows of groups.values()) {
      const newest = rows[rows.length - 1]

      const surviving = db.get<ManifestDbRow>(
        `SELECT * FROM manifest
         WHERE topic = ? AND ordering_key = ? AND id = ? AND block_number <= ?
         ORDER BY seq DESC LIMIT 1`,
        [newest.topic, newest.ordering_key, newest.id, floor],
      )

      if (surviving) {
        // A write-once event that also exists below the fork point is still canonical there —
        // consumers hold it and nothing needs repairing.
        if (surviving.mode !== 'materialized') continue

        this.#enqueueCompensation({
          topic: surviving.topic,
          orderingKey: surviving.ordering_key,
          op: surviving.op as PubsubOp,
          id: surviving.id,
          attributes: surviving.attributes,
          payload: asBytes(surviving.payload),
          blockNumber: surviving.block_number,
        })
        enqueued++
        continue
      }

      const baseline = db.get<{
        op: string
        attributes: string
        payload: Uint8Array | Buffer | null
        block_number: number
      }>(
        'SELECT op, attributes, payload, block_number FROM materialized_baseline WHERE topic = ? AND ordering_key = ? AND id = ?',
        [newest.topic, newest.ordering_key, newest.id],
      )

      if (baseline) {
        this.#enqueueCompensation({
          topic: newest.topic,
          orderingKey: newest.ordering_key,
          op: baseline.op as PubsubOp,
          id: newest.id,
          attributes: baseline.attributes,
          payload: asBytes(baseline.payload),
          blockNumber: baseline.block_number,
        })
        enqueued++
        continue
      }

      const inverse = db.get<{ op: string; payload: Uint8Array | Buffer | null }>(
        'SELECT op, payload FROM rollback_inverse WHERE topic = ? AND ordering_key = ? AND id = ?',
        [newest.topic, newest.ordering_key, newest.id],
      )

      // Attributes come from the orphaned operation itself, so the compensation passes the
      // same subscription filters as the operation it repairs (RP-44).
      this.#enqueueCompensation({
        topic: newest.topic,
        orderingKey: newest.ordering_key,
        op: (inverse?.op as PubsubOp) ?? 'delete',
        id: newest.id,
        attributes: newest.attributes,
        payload: inverse ? asBytes(inverse.payload) : new Uint8Array(),
        blockNumber: Math.max(floor, 0),
      })
      enqueued++
    }

    return enqueued
  }

  #enqueueCompensation(compensation: {
    topic: string
    orderingKey: string
    op: PubsubOp
    id: string
    attributes: string
    payload: Uint8Array
    blockNumber: number
  }): void {
    // Compensations take the partition's next number: never a reused or rewound one, so a
    // repair always dominates the operation it repairs.
    const seq = this.#nextSeq(compensation.topic, compensation.orderingKey)

    this.#db!.exec(
      `INSERT INTO outbox (topic, op, id, ordering_key, seq, attributes, payload, block_number)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        compensation.topic,
        compensation.op,
        compensation.id,
        compensation.orderingKey,
        seq,
        compensation.attributes,
        asBlob(compensation.payload),
        compensation.blockNumber,
      ],
    )
  }

  /**
   * The block ledger IS the rollback chain — one record, newest blocks last, plus the
   * persisted finalized floor `resolveForkCursor` refuses to walk past.
   */
  async *#records(finalized?: BlockCursor): AsyncIterable<RollbackRecord> {
    const blocks = this.#db!.all<{ number: number; hash: string | null; timestamp: number | null }>(
      'SELECT number, hash, timestamp FROM ledger_blocks ORDER BY number DESC',
    )

    yield {
      rollbackChain: blocks.map((b) => ({
        number: b.number,
        hash: b.hash ?? undefined,
        timestamp: b.timestamp ?? undefined,
      })) as BlockCursor[],
      finalized,
    }
  }

  async close(): Promise<void> {
    // Releases the exclusive lock — a restart in the same process must be able to reopen it.
    this.#db?.close()
    this.#db = undefined
    this.#logger = undefined
  }
}
