import { maxFinalized, normalizeFinalized } from './finalized-watermark.js'
import { BlockCursor } from './types.js'

export type RollbackRecord = {
  rollbackChain: BlockCursor[]
  finalized?: BlockCursor
}

/**
 * Given an async/sync iterable of historical rollback records (newest first)
 * and the portal's view of the canonical chain from a ForkException
 * (a.k.a. `previousBlocks` from Portal API `/stream` 409 responses),
 * finds the safe cursor to roll back to.
 *
 * The algorithm walks through each record's rollback chain (sorted DESC by block number)
 * and looks for the latest block that both sides agree on (matching hash).
 *
 * Returns the safe cursor, or null if no common ancestor is found at or above the
 * persisted finalized floor: finalized data is never rolled back.
 */
export async function resolveForkCursor(
  records: AsyncIterable<RollbackRecord> | Iterable<RollbackRecord>,
  canonicalBlocks: BlockCursor[],
): Promise<BlockCursor | null> {
  // Every record is read first: records do not always arrive in commit order, so any of them can hold the floor.
  const history: RollbackRecord[] = []
  for await (const record of records) {
    history.push(record)
  }

  const floor = highestFloor(history)

  const { ancestor, window } = searchAboveFloor(history, canonicalBlocks, floor)
  if (ancestor) {
    return ancestor
  }

  return canonicalFloor(window, floor)
}

function highestFloor(records: RollbackRecord[]): BlockCursor | undefined {
  // A store may hold `{}` for a commit made before any finalized head was reported.
  return records.reduce<BlockCursor | undefined>(
    (floor, record) => maxFinalized(floor, normalizeFinalized(record.finalized)),
    undefined,
  )
}

/**
 * Walks the records newest first, each chain descending, and narrows the canonical window past every
 * block visited. A record holds only its own batch's blocks, so older records can still hold blocks
 * above the floor; the walk ends at the first block below it.
 */
function searchAboveFloor(
  records: RollbackRecord[],
  canonicalBlocks: BlockCursor[],
  floor: BlockCursor | undefined,
): { ancestor?: BlockCursor; window: BlockCursor[] } {
  let window = [...canonicalBlocks]
  const blocks = records.flatMap((record) => [...record.rollbackChain].sort((a, b) => b.number - a.number))

  for (const block of blocks) {
    const belowFloor = floor !== undefined && block.number < floor.number
    if (belowFloor) {
      break
    }

    const found = window.find((u) => u.hash === block.hash)
    if (found) {
      return { ancestor: found, window }
    }

    if (!window.length) {
      /*
       * This indicates a deep blockchain fork where we've exhausted all previously known blocks.
       * We'll return the current block as the fork point
       * and let the portal fetch a new valid chain of blocks.
       */
      return { ancestor: block, window }
    }

    // Remove already visited blocks
    window = window.filter((u) => u.number < block.number)
  }

  return { window }
}

/** No retained block above the floor is canonical, but the floor itself may be. */
function canonicalFloor(window: BlockCursor[], floor: BlockCursor | undefined): BlockCursor | null {
  const canonical = floor !== undefined && window.some((u) => u.hash === floor.hash)

  return canonical ? floor : null
}
