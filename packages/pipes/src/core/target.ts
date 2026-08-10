import { Logger } from '~/core/logger.js'

import { PortalBatch } from './portal-source.js'
import { BlockCursor } from './types.js'

/**
 * Resume state a target hands back to the source when it starts reading.
 *
 * `latest` is the cursor to resume from (the block after it is the first one
 * fetched). `finalized` is the target's own PERSISTED finalized head; the source
 * seeds its monotonic finalized watermark from it so the watermark survives an
 * unclean restart mid-fork. `finalized` is a required key, explicitly `null` when
 * there is no finalized head (no-finality datasets, the memory target, a
 * cold-started target) — the absence must be stated, never just omitted.
 */
export type TargetState = {
  latest: BlockCursor
  finalized: BlockCursor | null
}

export type ReadOptions = {
  /**
   * Deliver one unfinalized block per batch, and never mix finalized blocks into an unfinalized
   * block's batch. Ask for this only if the target attributes a rollback to the batch's last block
   * (the Postgres target keys its undo snapshots that way) — otherwise a fork to the finalized head
   * would roll back a finalized block that merely shared a batch with the forked one. Targets that
   * carry the block number on the rows themselves (ClickHouse) should leave it off: the default,
   * one batch per response, is cheaper.
   */
  perBlockUnfinalized?: boolean
}

export type Target<In> = {
  /**
   * Set by a target that commits only finalized data. The source then reads `/finalized-stream`
   * whatever the pipe's portal config says, and warns when it had to override it.
   *
   * A hot stream ends once its range has been delivered — finality there is a response header,
   * not a completion condition — so a bounded range whose end sits above the finalized head would
   * leave such a target holding an uncommitted tail and still report success. On
   * `/finalized-stream` every delivered block is final, which is the guarantee these targets
   * actually need, and no fork can arrive.
   */
  requiresFinalizedStream?: boolean
  write: (writer: {
    /**
     * Globally unique, stable identifier for this pipe (the source `id`). Targets use it as
     * the default cursor key, so progress is isolated per pipe even when several pipes share
     * one offset table. A per-target override (e.g. ClickHouse `settings.id`) still wins.
     * Optional only so test harnesses can drive `write()` without a source; `pipeTo` always
     * supplies it.
     */
    id?: string
    /**
     * True when the source reads `/finalized-stream`: no fork can arrive, so a target may
     * skip diagnostics that only matter for reorg handling. Says nothing about the crash
     * window — data written above the cursor survives a restart on either stream.
     */
    finalized?: boolean
    read: (state?: TargetState, options?: ReadOptions) => AsyncIterableIterator<PortalBatch<In>>
    logger: Logger
  }) => Promise<void>
  /**
   * Called when a chain fork is detected. Receives the portal's view of the canonical
   * chain (a.k.a. `previousBlocks` from Portal API `/stream` 409 responses); must find
   * the common ancestor, roll back everything written above it, and return the cursor
   * to resume from (or `null` on a dead-end fork).
   */
  resolveFork?: (canonicalBlocks: BlockCursor[]) => Promise<BlockCursor | null>
}

export function createTarget<In>(options: Target<In>): Target<In> {
  return options
}
