/** What the live chain makes, and the messages between the bench and the portal process. */

export type ChainProfile = {
  /** Blocks per second the chain produces (W-BLOCK-RATE). */
  blockRate: number
  /** How many blocks the run produces above the start head. The pipe's range ends at the last one. */
  blocks: number
  /** The head the chain starts at; the pipe starts right above it. */
  startHead: number
  /** latest − finalized (W-FINALITY-DEPTH). */
  finalityDepth: number
  /** A fork every this many produced blocks; 0 for none. */
  forkEvery: number
  /** Blocks one fork replaces below the new head (W-FORK-DEPTH). */
  forkDepth: number
  /** Fraction of blocks that carry matching transfers (W-MATCH-RATE). */
  matchRate: number
  /** Transfers in a matching block: the rows it writes, and a rollback removes. */
  rows: number
  /** Canonical blocks a 409 carries. */
  previousBlocks: number
  /** How long the portal holds a request at the head before it answers 204. The real one: 5 s. */
  holdMs: number
  /** Round-trip time between the pipe and the portal; 0 serves on loopback directly. */
  rttMs: number
}

export type ForkEvent = {
  /** The common ancestor: the last block the fork keeps. */
  at: number
  /** The new head the fork arrived with. */
  head: number
  /** The branch id of the new blocks, carried in their hashes. */
  branch: number
  time: number
}

export type PortalReport = {
  /** When each block version was produced, by hash. */
  produced: [hash: string, time: number][]
  forks: ForkEvent[]
  /** Stream responses by status. */
  statuses: Record<number, number>
  startedAt: number
  finishedAt: number
}

export type ToPortal = { type: 'start' } | { type: 'report' }

export type FromPortal =
  | { type: 'ready'; url: string }
  | { type: 'finished' }
  | { type: 'report'; report: PortalReport }

/** One clock for both processes: wall time with sub-millisecond resolution. */
export function now(): number {
  return performance.timeOrigin + performance.now()
}

export function blockHash(number: number, branch: number): string {
  return `0x${branch.toString(16).padStart(8, '0')}${number.toString(16).padStart(56, '0')}`
}

export function branchOf(hash: string): number {
  return Number.parseInt(hash.slice(2, 10), 16)
}
