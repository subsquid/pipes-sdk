import { BlockCursor, Logger, TargetState } from '~/core/index.js'

import { PubsubOp } from './protocol.js'

export const STATE_SCHEMA_VERSION = '4'

export type RouteMode = 'event' | 'materialized'
export type RowIdSource = 'row' | 'draft' | 'derived' | 'generated'

/**
 * One wire operation on its way out: everything the state needs to sequence it, publish it,
 * and — when it sits above the finalized watermark — compensate it after a fork.
 */
export type PendingOperation = {
  /** Route name selects the encoder after recovery. */
  route: string
  topic: string
  /** Empty when PubSub message ordering is disabled. */
  orderingKey: string
  mode: RouteMode
  op: PubsubOp
  id: string
  /** How the target resolved `id`; fixed per materialized route. */
  idSource: RowIdSource
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
  route: string
  topic: string
  op: PubsubOp
  id: string
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
}

/**
 * The combined local state: resume cursor, rollback manifest, finalized baselines, publish
 * outbox and sequence counters, all committed in one transaction per batch (CN-17).
 *
 * Kept an interface so a backend can be swapped in wholesale. Two ship with the SDK —
 * `SqlitePubsubState` (default) and `PostgresPubsubState` — both thin wrappers around one
 * shared state machine (`state/state-core.ts`); a fully custom implementation is just as valid,
 * the transactional contract is the core requirement, not the storage engine.
 */
export interface PubsubState {
  /**
   * Opens the backing store, validates its schema, and reports whether it started empty (CN-46).
   *
   * `allowColdStart: false` opens an empty store read-only: the bootstrap markers are what make
   * the next open look warm, so a caller that is about to refuse the run must not leave them
   * behind — otherwise the refusal is a one-time speed bump and the retry restarts the sequence.
   */
  open(ctx: { cursorKey: string; logger: Logger; allowColdStart?: boolean }): Promise<{ coldStart: boolean }>
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
