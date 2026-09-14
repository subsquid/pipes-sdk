import {
  BlockCursor,
  CursorKey,
  Logger,
  RollbackRecord,
  TargetState,
  normalizeFinalized,
  resolveForkCursor,
} from '~/core/index.js'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from '../errors.js'
import { MAX_SEQUENCE_VALUE, PubsubOp } from '../protocol.js'
import {
  CommitInput,
  OutboxRow,
  PendingOperation,
  PubsubState,
  STATE_SCHEMA_VERSION,
  stableAttributes,
} from '../pubsub-state-types.js'
import { DriverLockedError, DriverUnavailableError, SqlDriver, toNumber } from './sql-driver.js'

type MetaRow = { value: string }
type CursorRow = { latest: string; finalized: string | null }
type OutboxDbRow = {
  row_id: number | string
  route: string
  topic: string
  op: string
  id: string
  ordering_key: string
  seq: number | string
  attributes: string
  payload: Uint8Array | Buffer | null
}
type ManifestDbRow = {
  route: string
  topic: string
  ordering_key: string
  seq: number | string
  block_number: number | string
  mode: string
  op: string
  id: string
  attributes: string
  payload: Uint8Array | Buffer | null
}

const META_SEQUENCE = 'sequence'
const META_CURSOR_KEY = 'cursor_key'
const META_MATERIALIZED_ID_SOURCE = 'materialized_id_source:'

/**
 * An unambiguous key for a composite identity. A separator-joined string is not one:
 * `("a", "b c")` and `("a b", "c")` collide, and for a fork that means two orphaned rows
 * folding into one group — one compensation published where two are owed.
 */
const identityKey = (...parts: string[]) => JSON.stringify(parts)

function asBytes(value: Uint8Array | Buffer | null): Uint8Array {
  if (!value) return new Uint8Array()

  return value instanceof Uint8Array ? value : new Uint8Array(value)
}

function asBlob(value: Uint8Array): Buffer {
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength)
}

/**
 * What a rolled-back transaction means for whoever reads the error. `confirm` runs after the
 * batch is on the wire, so the one message that fits every site would tell an operator the
 * opposite of what happened and invite a replay.
 */
const ROLLED_BACK = {
  schema: 'The state is unchanged.',
  commit: 'The batch was rolled back and nothing was published.',
  confirm:
    'The batch was already published — only its outbox acknowledgement was rolled back, so those ' +
    'rows stay queued and will be delivered again.',
  fork: 'The fork compensation was rolled back, so the state still describes the pre-fork chain.',
} as const

/**
 * The complete `PubsubState` state machine (CN-17 commit, CN-35 fork resolution, CN-46 cold
 * start, RP-44 finality) held exactly once, driven through the dialect-agnostic `SqlDriver`.
 * `SqlitePubsubState` and `PostgresPubsubState` are thin `PubsubState` wrappers around one of
 * these each — the storage engine differs at the driver seam only; every invariant here is
 * shared and lives in one place.
 */
export class SqlPubsubStateCore implements PubsubState {
  readonly #driver: SqlDriver
  readonly #key: CursorKey
  #logger?: Logger

  constructor(driver: SqlDriver, id?: string) {
    this.#driver = driver
    this.#key = new CursorKey(id)
  }

  get cursorKey(): string {
    return this.#key.value
  }

  async open({
    cursorKey,
    logger,
    allowColdStart = true,
  }: {
    cursorKey: string
    logger: Logger
    allowColdStart?: boolean
  }): Promise<{ coldStart: boolean }> {
    this.#key.bind(cursorKey)
    this.#logger = logger

    try {
      await this.#driver.connect()
    } catch (e) {
      if (e instanceof DriverLockedError) {
        throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_LOCKED, [
          `Another process holds the PubSub state at "${this.#driver.location}".`,
          'Exactly one producer may own a state: it is the authoritative sequencer for every id it ' +
            'publishes, and a second writer would hand consumers sequence numbers they have already seen.',
          e.cause instanceof Error ? e.cause.message : String(e.cause),
        ])
      }

      if (e instanceof DriverUnavailableError) {
        throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_UNAVAILABLE, [
          `Cannot open the PubSub state at "${this.#driver.location}": ` +
            (e.cause instanceof Error ? e.cause.message : String(e.cause)),
          'The state is the producer’s sequencer — it must live on durable, persistent storage, not an ' +
            'ephemeral container filesystem.',
        ])
      }

      throw e
    }

    // Every refusal below happens with the single-writer lock already held, so it releases the
    // connection on the way out: otherwise a caller that retries in the same process meets its
    // own dead connection and reads the mismatch as a second producer.
    try {
      await this.#initializeSchema()

      const version = await this.#getMeta('schema_version')
      if (version && version !== STATE_SCHEMA_VERSION) {
        throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_SCHEMA_VERSION, [
          `The PubSub state at "${this.#driver.location}" was written with schema version ${version}, ` +
            `this build speaks ${STATE_SCHEMA_VERSION}.`,
        ])
      }

      const coldStart = !version

      // Stamping is the bootstrap. A caller that will not permit one gets the report and an
      // untouched state, so its refusal still holds on the next open.
      if (coldStart && !allowColdStart) {
        return { coldStart }
      }

      if (coldStart) {
        await this.#setMeta('schema_version', STATE_SCHEMA_VERSION)
        await this.#setMeta(META_SEQUENCE, '0')
        await this.#setMeta(META_CURSOR_KEY, this.#key.value)
      } else {
        await this.#assertSameProducer()
      }

      return { coldStart }
    } catch (e) {
      await this.close()

      throw e
    }
  }

  /**
   * A state store is one producer's sequencer, not a shared cache. The outbox, the manifest
   * and the counter are producer-wide — only the cursor row is keyed — so opening another
   * pipe's store would report a clean warm start while inheriting its unpublished operations.
   */
  async #assertSameProducer(): Promise<void> {
    const storedKey = await this.#getMeta(META_CURSOR_KEY)
    if (storedKey !== undefined && storedKey !== this.#key.value) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_IDENTITY_MISMATCH, [
        `The PubSub state at "${this.#driver.location}" belongs to producer "${storedKey}", but this pipe ` +
          `binds "${this.#key.value}".`,
        'One state per producer: its outbox, manifest and sequence counters are producer-wide, so ' +
          'adopting this one would publish another producer’s pending operations under this pipe’s identity.',
      ])
    }

    // Written on first open of a state that predates these keys, so the checks bind from now on.
    if (storedKey === undefined) await this.#setMeta(META_CURSOR_KEY, this.#key.value)
  }

  /**
   * The single write path. On failure the transaction is unwound and the caller sees the
   * failure that caused it: the driver has already aborted the transaction itself for a full or
   * failing store, and a bare rollback would throw over the top of the real error.
   */
  async #transaction<T>(immediate: boolean, rolledBack: string, body: () => Promise<T>): Promise<T> {
    try {
      await this.#driver.begin(immediate)
      const result = await body()
      await this.#driver.commit()

      return result
    } catch (e) {
      await this.#driver.rollback()

      throw this.#storageError(e, rolledBack)
    }
  }

  /** A full or unreachable store is the store failing, not the batch — say so by that name. */
  #storageError(e: unknown, rolledBack: string): unknown {
    if (!this.#driver.isStorageFailure(e)) return e

    const error = new PubsubTargetError(PUBSUB_ERROR_CODES.STATE_WRITE_FAILED, [
      `The PubSub state at "${this.#driver.location}" could not be written: ` +
        (e instanceof Error ? e.message : String(e)),
      `${rolledBack} The state is the producer’s sequencer — check the free space and the ` +
        'health of the store that holds it.',
    ])
    error.cause = e

    return error
  }

  async #initializeSchema(): Promise<void> {
    await this.#transaction(false, ROLLED_BACK.schema, async () => {
      for (const statement of this.#driver.schemaDDL()) {
        await this.#driver.exec(statement)
      }
    })
  }

  // ─── meta ───────────────────────────────────────────────────────────────────

  async #getMeta(key: string): Promise<string | undefined> {
    const row = await this.#driver.get<MetaRow>(`SELECT value FROM ${this.#driver.table('meta')} WHERE key = ?`, [key])

    return row?.value
  }

  async #setMeta(key: string, value: string): Promise<void> {
    await this.#driver.exec(
      `INSERT INTO ${this.#driver.table('meta')} (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
      [key, value],
    )
  }

  async getMeta(key: string): Promise<string | undefined> {
    return this.#getMeta(key)
  }

  async setMeta(key: string, value: string): Promise<void> {
    await this.#setMeta(key, value)
  }

  // ─── cursor ─────────────────────────────────────────────────────────────────

  async getCursor(): Promise<TargetState | undefined> {
    const row = await this.#driver.get<CursorRow>(
      `SELECT latest, finalized FROM ${this.#driver.table('cursor')} WHERE id = ?`,
      [this.#key.value],
    )
    if (!row?.latest) return

    // The finalized floor is handed back explicitly — the source seeds its monotonic
    // watermark from it, and `null` has to state the absence rather than omit it (RP-3).
    return {
      latest: JSON.parse(row.latest) as BlockCursor,
      finalized: normalizeFinalized(row.finalized ? (JSON.parse(row.finalized) as BlockCursor) : undefined) ?? null,
    }
  }

  async #saveCursor(cursor: BlockCursor, finalized: BlockCursor | null): Promise<void> {
    await this.#driver.exec(
      `INSERT INTO ${this.#driver.table('cursor')} (id, latest, finalized, updated_at) VALUES (?, ?, ?, ?)
       ON CONFLICT (id) DO UPDATE SET latest = excluded.latest, finalized = excluded.finalized, updated_at = excluded.updated_at`,
      [this.#key.value, JSON.stringify(cursor), finalized ? JSON.stringify(finalized) : null, Date.now()],
    )
  }

  // ─── sequencing ─────────────────────────────────────────────────────────────

  /** One producer-wide version counter, independent of topics and ordering keys. */
  async #nextSeq(context: { route: string; topic: string }): Promise<number> {
    const current = Number((await this.#getMeta(META_SEQUENCE)) ?? '0')
    if (!Number.isSafeInteger(current) || current < 0 || current >= MAX_SEQUENCE_VALUE) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.SEQUENCE_EXHAUSTED, [
        `The PubSub change sequence cannot advance past ${String(current)} for route "${context.route}" ` +
          `(topic "${context.topic}").`,
        'Start a new feed with a fresh namespace and state before publishing more operations.',
      ])
    }

    const next = current + 1
    await this.#setMeta(META_SEQUENCE, String(next))

    return next
  }

  async #enqueue(operation: PendingOperation, seq: number): Promise<void> {
    await this.#driver.exec(
      `INSERT INTO ${this.#driver.table('outbox')} (route, topic, op, id, ordering_key, seq, attributes, payload, block_number)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        operation.route,
        operation.topic,
        operation.op,
        operation.id,
        operation.orderingKey,
        seq,
        stableAttributes(operation.attributes),
        asBlob(operation.payload),
        operation.blockNumber,
      ],
    )
  }

  // ─── per-batch transaction (CN-17) ──────────────────────────────────────────

  async commit({ operations, ledger, cursor, finalized, forkCapable }: CommitInput): Promise<void> {
    await this.#transaction(true, ROLLED_BACK.commit, async () => {
      for (const operation of operations) {
        const seq = await this.#nextSeq(operation)
        await this.#enqueue(operation, seq)

        if (operation.mode === 'materialized') {
          await this.#assertIdentitySourceStable(operation)
          await this.#assertIdentityStable(operation)
          if (!forkCapable) await this.#updateMaterializedIdentity(operation)
        }

        if (!operation.rollbackable) {
          // A materialized row that arrives already finalized never passes through the
          // manifest, but a fork rewinding to it still has to restore its value — so it
          // becomes a baseline directly.
          if (forkCapable && operation.mode === 'materialized') {
            await this.#saveBaseline({
              route: operation.route,
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

        await this.#driver.exec(
          `INSERT INTO ${this.#driver.table('manifest')} (route, topic, ordering_key, seq, block_number, mode, op, id, attributes, payload)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            operation.route,
            operation.topic,
            operation.orderingKey,
            seq,
            operation.blockNumber,
            operation.mode,
            operation.op,
            operation.id,
            stableAttributes(operation.attributes),
            // DELETE compensations must retain the original row's primary-key columns.
            asBlob(operation.payload),
          ],
        )

        if (operation.inverse) {
          // First writer wins: the inverse is pure, so a later revision would only rewrite the
          // same bytes, and the id must keep the identity it was first published under.
          await this.#driver.exec(
            `INSERT INTO ${this.#driver.table('rollback_inverse')} (route, topic, ordering_key, id, op, payload)
             VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (topic, ordering_key, id) DO NOTHING`,
            [
              operation.route,
              operation.topic,
              operation.orderingKey,
              operation.id,
              operation.inverse.op,
              asBlob(operation.inverse.payload),
            ],
          )
        }
      }

      for (const block of ledger) {
        await this.#driver.exec(
          `INSERT INTO ${this.#driver.table('ledger_blocks')} (number, hash, timestamp) VALUES (?, ?, ?)
           ON CONFLICT (number) DO UPDATE SET hash = excluded.hash, timestamp = excluded.timestamp`,
          [block.number, block.hash ?? null, block.timestamp ?? null],
        )
      }

      await this.#saveCursor(cursor, finalized)

      if (finalized) {
        await this.#foldFinalized(finalized.number)
      }
    })
  }

  /**
   * A route emits one homogeneous materialized row family. Switching between `_id`, draft `id`,
   * `deriveId`, and generated ids would make one revision unreachable under the next one's key.
   */
  async #assertIdentitySourceStable(operation: PendingOperation): Promise<void> {
    const key = `${META_MATERIALIZED_ID_SOURCE}${operation.route}`
    const known = await this.#getMeta(key)

    if (known === undefined) {
      await this.#setMeta(key, operation.idSource)
      return
    }

    if (known === operation.idSource) {
      return
    }

    throw new PubsubTargetError(PUBSUB_ERROR_CODES.MATERIALIZED_ID_MOVED, [
      `Materialized route "${operation.route}" changed its id source from "${known}" to "${operation.idSource}".`,
      'Use one identity source consistently for every revision and row on a materialized route.',
    ])
  }

  /**
   * A materialized row outlives the block that last touched it, so its topic, ordering key and
   * filter attributes are fixed for its lifetime. A subscription filtered on the old attributes
   * never receives a revision carrying new ones; if a fork is possible, its repair also uses the
   * identity the row was first published under.
   */
  async #assertIdentityStable(operation: PendingOperation): Promise<void> {
    const id = operation.id

    type IdentityRow = { route: string; topic: string; ordering_key: string; attributes: string }

    const known =
      (await this.#driver.get<IdentityRow>(
        `SELECT route, topic, ordering_key, attributes FROM ${this.#driver.table('materialized_identity')} WHERE id = ?`,
        [id],
      )) ??
      (await this.#driver.get<IdentityRow>(
        `SELECT route, topic, ordering_key, attributes FROM ${this.#driver.table('manifest')} WHERE id = ? ORDER BY seq DESC LIMIT 1`,
        [id],
      )) ??
      (await this.#driver.get<IdentityRow>(
        `SELECT route, topic, ordering_key, attributes FROM ${this.#driver.table('materialized_baseline')} WHERE id = ? LIMIT 1`,
        [id],
      ))

    if (!known) return

    const attributes = stableAttributes(operation.attributes)
    if (
      known.route === operation.route &&
      known.topic === operation.topic &&
      known.ordering_key === operation.orderingKey &&
      known.attributes === attributes
    ) {
      return
    }

    const moved =
      known.route !== operation.route
        ? `route "${known.route}" → "${operation.route}"`
        : known.topic !== operation.topic
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
  async #updateMaterializedIdentity(operation: PendingOperation): Promise<void> {
    const id = operation.id

    if (operation.op === 'delete') {
      await this.#driver.exec(`DELETE FROM ${this.#driver.table('materialized_identity')} WHERE id = ?`, [id])
      return
    }

    await this.#driver.exec(
      `INSERT INTO ${this.#driver.table('materialized_identity')} (id, route, topic, ordering_key, attributes)
       VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`,
      [id, operation.route, operation.topic, operation.orderingKey, stableAttributes(operation.attributes)],
    )
  }

  /**
   * Finality advance: collapse newly-finalized materialized revisions into their baselines,
   * then drop everything a fork can no longer reach. Bounds the rollbackable log by the
   * chain's finality depth times the operation rate.
   */
  async #foldFinalized(finalizedNumber: number): Promise<void> {
    const manifest = this.#driver.table('manifest')

    const latest = await this.#driver.all<ManifestDbRow>(
      `SELECT m.* FROM ${manifest} m
       WHERE m.mode = 'materialized' AND m.block_number <= ?
         AND m.seq = (
           SELECT MAX(seq) FROM ${manifest} x
           WHERE x.topic = m.topic AND x.ordering_key = m.ordering_key AND x.id = m.id AND x.block_number <= ?
         )`,
      [finalizedNumber, finalizedNumber],
    )

    for (const row of latest) {
      await this.#saveBaseline(row)
    }

    await this.#driver.exec(`DELETE FROM ${manifest} WHERE block_number <= ?`, [finalizedNumber])
    await this.#driver.exec(`DELETE FROM ${this.#driver.table('ledger_blocks')} WHERE number <= ?`, [finalizedNumber])
    await this.#driver.exec(`
      DELETE FROM ${this.#driver.table('rollback_inverse')} AS r
      WHERE NOT EXISTS (
        SELECT 1 FROM ${manifest} m
        WHERE m.topic = r.topic
          AND m.ordering_key = r.ordering_key
          AND m.id = r.id
      )`)
  }

  /** The last finalized value of a materialized id — what a fork restores when no revision survives. */
  async #saveBaseline(row: {
    route: string
    topic: string
    ordering_key: string
    id: string
    op: string
    attributes: string
    payload: Uint8Array | Buffer | null
    block_number: number | string
  }): Promise<void> {
    const baseline = this.#driver.table('materialized_baseline')

    if (row.op === 'delete') {
      // A deleted id is not live: dropping its baseline keeps the table bounded, and a later
      // fork that finds neither revision nor baseline compensates with a delete anyway.
      await this.#driver.exec(`DELETE FROM ${baseline} WHERE topic = ? AND ordering_key = ? AND id = ?`, [
        row.topic,
        row.ordering_key,
        row.id,
      ])

      return
    }

    await this.#driver.exec(
      `INSERT INTO ${baseline} (route, topic, ordering_key, id, op, attributes, payload, block_number)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (topic, ordering_key, id) DO UPDATE SET
         route = excluded.route, op = excluded.op, attributes = excluded.attributes,
         payload = excluded.payload, block_number = excluded.block_number
       WHERE excluded.block_number >= ${baseline}.block_number`,
      [row.route, row.topic, row.ordering_key, row.id, row.op, row.attributes, row.payload, row.block_number],
    )
  }

  // ─── outbox ─────────────────────────────────────────────────────────────────

  async pending(): Promise<OutboxRow[]> {
    const rows = await this.#driver.all<OutboxDbRow>(
      `SELECT * FROM ${this.#driver.table('outbox')} ORDER BY row_id ASC`,
    )

    return rows.map((row) => ({
      rowId: toNumber(row.row_id),
      route: row.route,
      topic: row.topic,
      op: row.op as PubsubOp,
      id: row.id,
      orderingKey: row.ordering_key,
      seq: toNumber(row.seq),
      attributes: JSON.parse(row.attributes) as Record<string, string>,
      payload: asBytes(row.payload),
    }))
  }

  async confirm(rowIds: number[]): Promise<void> {
    if (!rowIds.length) return

    await this.#transaction(true, ROLLED_BACK.confirm, async () => {
      for (const rowId of rowIds) {
        await this.#driver.exec(`DELETE FROM ${this.#driver.table('outbox')} WHERE row_id = ?`, [rowId])
      }
    })
  }

  async stats(): Promise<{ outbox: number; manifest: number }> {
    const outbox = await this.#driver.get<{ n: number | string }>(
      `SELECT COUNT(*) AS n FROM ${this.#driver.table('outbox')}`,
    )
    const manifest = await this.#driver.get<{ n: number | string }>(
      `SELECT COUNT(*) AS n FROM ${this.#driver.table('manifest')}`,
    )

    return {
      outbox: outbox ? toNumber(outbox.n) : 0,
      manifest: manifest ? toNumber(manifest.n) : 0,
    }
  }

  // ─── fork (CN-35) ───────────────────────────────────────────────────────────

  async fork(canonicalBlocks: BlockCursor[]): Promise<BlockCursor | null> {
    const persisted = await this.getCursor()
    const finalized = persisted?.finalized ?? undefined
    const safe = await resolveForkCursor(this.#records(finalized), canonicalBlocks)

    // Dead end: nothing local proves which published operations are canonical, so fold the
    // ENTIRE rollbackable manifest back to the finalized baselines and stop. Fail-closed —
    // if the fork invalidated the finalized floor itself, the consumer needs a rebuild.
    const rollbackTo = safe ?? finalized ?? null
    const floor = rollbackTo?.number ?? -1

    await this.#transaction(true, ROLLED_BACK.fork, async () => {
      const compensations = await this.#compensate(floor)
      this.#logger?.debug(
        `fork at block ${floor}: ${compensations} compensating operation(s) enqueued${safe ? '' : ' (dead-end fork)'}`,
      )

      await this.#driver.exec(`DELETE FROM ${this.#driver.table('manifest')} WHERE block_number > ?`, [floor])
      await this.#driver.exec(`DELETE FROM ${this.#driver.table('ledger_blocks')} WHERE number > ?`, [floor])
      await this.#driver.exec(`
        DELETE FROM ${this.#driver.table('rollback_inverse')} AS r
        WHERE NOT EXISTS (
          SELECT 1 FROM ${this.#driver.table('manifest')} m
          WHERE m.topic = r.topic
            AND m.ordering_key = r.ordering_key
            AND m.id = r.id
        )`)

      if (rollbackTo) {
        await this.#saveCursor(rollbackTo, finalized ?? null)
      }
    })

    return safe
  }

  /**
   * One rule per id: fold away the orphaned revision suffix and publish whatever state
   * remains at the safe cursor — the surviving revision, else the finalized baseline, else
   * the route's stored inverse (a delete by default).
   */
  async #compensate(floor: number): Promise<number> {
    const manifest = this.#driver.table('manifest')

    const orphaned = await this.#driver.all<ManifestDbRow>(
      `SELECT * FROM ${manifest} WHERE block_number > ? ORDER BY topic, ordering_key, id, seq ASC`,
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

      const surviving = await this.#driver.get<ManifestDbRow>(
        `SELECT * FROM ${manifest}
         WHERE topic = ? AND ordering_key = ? AND id = ? AND block_number <= ?
         ORDER BY seq DESC LIMIT 1`,
        [newest.topic, newest.ordering_key, newest.id, floor],
      )

      if (surviving) {
        // A write-once event that also exists below the fork point is still canonical there —
        // consumers hold it and nothing needs repairing.
        if (surviving.mode !== 'materialized') continue

        await this.#enqueueCompensation({
          route: surviving.route,
          topic: surviving.topic,
          orderingKey: surviving.ordering_key,
          op: surviving.op as PubsubOp,
          id: surviving.id,
          attributes: surviving.attributes,
          payload: asBytes(surviving.payload),
          blockNumber: toNumber(surviving.block_number),
        })
        enqueued++
        continue
      }

      const baseline = await this.#driver.get<{
        route: string
        op: string
        attributes: string
        payload: Uint8Array | Buffer | null
        block_number: number | string
      }>(
        `SELECT route, op, attributes, payload, block_number FROM ${this.#driver.table('materialized_baseline')} WHERE topic = ? AND ordering_key = ? AND id = ?`,
        [newest.topic, newest.ordering_key, newest.id],
      )

      if (baseline) {
        await this.#enqueueCompensation({
          route: baseline.route,
          topic: newest.topic,
          orderingKey: newest.ordering_key,
          op: baseline.op as PubsubOp,
          id: newest.id,
          attributes: baseline.attributes,
          payload: asBytes(baseline.payload),
          blockNumber: toNumber(baseline.block_number),
        })
        enqueued++
        continue
      }

      const inverse = await this.#driver.get<{ route: string; op: string; payload: Uint8Array | Buffer | null }>(
        `SELECT route, op, payload FROM ${this.#driver.table('rollback_inverse')} WHERE topic = ? AND ordering_key = ? AND id = ?`,
        [newest.topic, newest.ordering_key, newest.id],
      )

      // Attributes come from the orphaned operation itself, so the compensation passes the
      // same subscription filters as the operation it repairs (RP-44).
      await this.#enqueueCompensation({
        route: inverse?.route ?? newest.route,
        topic: newest.topic,
        orderingKey: newest.ordering_key,
        op: (inverse?.op as PubsubOp) ?? 'delete',
        id: newest.id,
        attributes: newest.attributes,
        payload: inverse ? asBytes(inverse.payload) : asBytes(newest.payload),
        blockNumber: Math.max(floor, 0),
      })
      enqueued++
    }

    return enqueued
  }

  async #enqueueCompensation(compensation: {
    route: string
    topic: string
    orderingKey: string
    op: PubsubOp
    id: string
    attributes: string
    payload: Uint8Array
    blockNumber: number
  }): Promise<void> {
    // Compensations take the producer's next number: never a reused or rewound one, so a
    // repair always dominates the operation it repairs.
    const seq = await this.#nextSeq(compensation)

    await this.#driver.exec(
      `INSERT INTO ${this.#driver.table('outbox')} (route, topic, op, id, ordering_key, seq, attributes, payload, block_number)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        compensation.route,
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
    const blocks = await this.#driver.all<{
      number: number | string
      hash: string | null
      timestamp: number | string | null
    }>(`SELECT number, hash, timestamp FROM ${this.#driver.table('ledger_blocks')} ORDER BY number DESC`)

    yield {
      rollbackChain: blocks.map((b) => ({
        number: toNumber(b.number),
        hash: b.hash ?? undefined,
        timestamp: b.timestamp === null ? undefined : toNumber(b.timestamp),
      })) as BlockCursor[],
      finalized,
    }
  }

  async close(): Promise<void> {
    await this.#driver.close()
    this.#logger = undefined
  }
}
