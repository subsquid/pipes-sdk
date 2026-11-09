import type { BlockCursor } from '~/core/index.js'

import { type ChainProfile, type PortalReport, branchOf } from './protocol.js'

export type Commit = { number: number; hash: string; blocks: number; time: number; durationMs: number }
/** `branch`: the branch of the canonical parent the 409 carried, which names the fork it reports. */
export type ForkStep = { detected: number; branch: number; rolledBack?: number; ancestor?: BlockCursor | null }

export type Distribution = { p50: number; p90: number; p99: number; max: number; count: number }

export type PointResult = {
  sink: string
  profile: ChainProfile
  /** The pipe's commit head lags the chain's head: commit time − production time of the block. */
  lagMs: Distribution
  /** Linear trend of the lag over the run, ms per second. Above zero the pipe falls behind. */
  lagSlopeMsPerS: number
  /** Commits of produced blocks while the chain ran, after it settled: the lag's samples. */
  liveCommits: number
  /** The longest stretch of the settled run with no such commit, its edges included. */
  maxLiveGapMs: number
  /** Last block produced → last block committed. */
  drainMs: number
  /**
   * `yes`: the lag holds and stays well inside the unfinalized window. `at finality`: it holds only
   * because the pipe fell behind until the blocks it reads are finalized and come in one batch;
   * real-time data is late by about the finality window, and forks above the pipe pass it by.
   * `no`: the lag grows, or a fork never resolved.
   */
  verdict: 'yes' | 'at finality' | 'no'
  keepsUp: boolean
  /** The lag in blocks at the chain's rate: how deep in the unfinalized window the pipe commits. */
  lagBlocks: number
  commitsPerS: number
  blocksPerCommit: number
  /** The target's time on one batch: handed over → it asked for the next. */
  commitMs: Distribution
  /** The slowest commits, with their offset into the run. */
  slowCommits: { at: number; number: number; durationMs: number }[]
  /** Fork produced → a block at or above its new head committed on its branch (SLI-4). */
  forkResolveMs: Distribution
  /** Fork produced → the 409 reached the target. */
  forkDetectMs: Distribution
  /** resolveFork: finding the ancestor and rolling the store back. */
  forkRollbackMs: Distribution
  /** Rollback done → first commit after it. */
  forkResumeMs: Distribution
  forks: number
  /** Forks the pipe handled with a rollback of its own; fewer than `forks` when one 409 covered several. */
  rollbacks: number
  unresolvedForks: number
  statuses: Record<number, number>
  /** Every rollback the target ran, in order. */
  rollbackTimeline: { at: number; depth: number; detectMs: number; rollbackMs: number; resumeMs: number }[]
  error?: string
}

export function distribution(values: number[]): Distribution {
  if (!values.length) return { p50: Number.NaN, p90: Number.NaN, p99: Number.NaN, max: Number.NaN, count: 0 }

  const sorted = [...values].sort((a, b) => a - b)
  const at = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]

  return { p50: at(0.5), p90: at(0.9), p99: at(0.99), max: sorted[sorted.length - 1], count: sorted.length }
}

/** Index of the first commit at or after `time`; commits are in time order. */
function firstAfter(commits: Commit[], time: number): number {
  let low = 0
  let high = commits.length
  while (low < high) {
    const middle = (low + high) >> 1
    if (commits[middle].time < time) low = middle + 1
    else high = middle
  }

  return low
}

function slope(points: { x: number; y: number }[]): number {
  if (points.length < 2) return 0

  const n = points.length
  const mx = points.reduce((s, p) => s + p.x, 0) / n
  const my = points.reduce((s, p) => s + p.y, 0) / n
  const sxy = points.reduce((s, p) => s + (p.x - mx) * (p.y - my), 0)
  const sxx = points.reduce((s, p) => s + (p.x - mx) ** 2, 0)

  return sxx === 0 ? 0 : sxy / sxx
}

/** A pipe falling behind gains this much lag per second of chain time, and more. */
const FALLING_BEHIND_MS_PER_S = 20
/** Fewer lag samples than this say nothing about whether the pipe keeps up. */
const MIN_LIVE_COMMITS = 5
/** A pipe silent this long while the chain runs has stalled, whatever its lag before. */
function stallMs(blockRate: number): number {
  return Math.max(2_000, (20 * 1_000) / blockRate)
}

function longestGap(times: number[], from: number, to: number): number {
  const edges = [from, ...times, to]

  return edges.slice(1).reduce((gap, time, i) => Math.max(gap, time - edges[i]), 0)
}

export function summarize({
  sink,
  profile,
  report,
  commits,
  forkSteps,
}: {
  sink: string
  profile: ChainProfile
  report: PortalReport
  commits: Commit[]
  forkSteps: ForkStep[]
}): PointResult {
  const producedAt = new Map(report.produced)
  const duration = report.finishedAt - report.startedAt
  // The first tenth of the run, or a second, is the pipe settling at the head.
  const steadyFrom = report.startedAt + Math.max(1_000, duration / 10)

  // Only commits made while the chain ran: the drain after it is `drainMs`, and a pipe that commits
  // nothing until the end must not pass on the strength of its last batch.
  const lagged = commits
    .filter((c) => c.time >= steadyFrom && c.time <= report.finishedAt && producedAt.has(c.hash))
    .map((c) => ({ time: c.time, lag: c.time - producedAt.get(c.hash)! }))
  const maxLiveGapMs = longestGap(
    lagged.map((p) => p.time),
    steadyFrom,
    report.finishedAt,
  )

  const lagSlope = slope(lagged.map((p) => ({ x: (p.time - report.startedAt) / 1000, y: p.lag })))
  const lastCommit = commits.at(-1)
  const drainMs = lastCommit ? lastCommit.time - report.finishedAt : Number.NaN

  const during = commits.filter((c) => c.time >= report.startedAt && c.time <= report.finishedAt)
  const commitsPerS = during.length / (duration / 1000)
  const blocksPerCommit = during.reduce((s, c) => s + c.blocks, 0) / Math.max(1, during.length)
  const slowCommits = [...during]
    .sort((a, b) => b.durationMs - a.durationMs)
    .slice(0, 10)
    .map((c) => ({ at: Math.round(c.time - report.startedAt), number: c.number, durationMs: Math.round(c.durationMs) }))

  const resolve: number[] = []
  let unresolved = 0
  for (const fork of report.forks) {
    let done: Commit | undefined
    for (let i = firstAfter(commits, fork.time); i < commits.length && !done; i++) {
      const c = commits[i]
      if (c.number >= fork.head && branchOf(c.hash) >= fork.branch) done = c
    }
    if (done) resolve.push(done.time - fork.time)
    else unresolved++
  }

  const detect: number[] = []
  const rollback: number[] = []
  const resume: number[] = []
  const rollbackTimeline: PointResult['rollbackTimeline'] = []
  // The 409 names its fork by the branch of the canonical parent; the latest fork before the 409
  // arrived may have come while it was in flight.
  const forkOf = new Map(report.forks.map((f) => [f.branch, f]))
  for (const step of forkSteps) {
    const fork = forkOf.get(step.branch)
    if (fork) detect.push(step.detected - fork.time)
    if (step.rolledBack === undefined) continue

    rollback.push(step.rolledBack - step.detected)
    const first = commits[firstAfter(commits, step.rolledBack)]
    if (first) resume.push(first.time - step.rolledBack)

    // Depth as the target saw it: from the highest block it held to the ancestor it returned.
    const held = commits[firstAfter(commits, step.detected) - 1]
    rollbackTimeline.push({
      at: Math.round(step.detected - report.startedAt),
      depth: held && step.ancestor ? held.number - step.ancestor.number : Number.NaN,
      detectMs: fork ? step.detected - fork.time : Number.NaN,
      rollbackMs: step.rolledBack - step.detected,
      resumeMs: first ? first.time - step.rolledBack : Number.NaN,
    })
  }

  const lagMs = distribution(lagged.map((p) => p.lag))
  const lagBlocks = (lagMs.p50 * profile.blockRate) / 1000
  const observed = lagged.length >= MIN_LIVE_COMMITS && maxLiveGapMs <= stallMs(profile.blockRate)
  const holds = observed && lagSlope < FALLING_BEHIND_MS_PER_S && unresolved === 0
  const verdict = !holds ? 'no' : lagBlocks >= profile.finalityDepth / 2 ? 'at finality' : 'yes'

  return {
    sink,
    profile,
    lagMs,
    lagSlopeMsPerS: lagSlope,
    liveCommits: lagged.length,
    maxLiveGapMs,
    drainMs,
    verdict,
    keepsUp: verdict === 'yes',
    lagBlocks,
    commitsPerS,
    blocksPerCommit,
    commitMs: distribution(during.map((c) => c.durationMs)),
    slowCommits,
    forkResolveMs: distribution(resolve),
    forkDetectMs: distribution(detect),
    forkRollbackMs: distribution(rollback),
    forkResumeMs: distribution(resume),
    forks: report.forks.length,
    rollbacks: forkSteps.length,
    unresolvedForks: unresolved,
    statuses: report.statuses,
    rollbackTimeline,
  }
}
