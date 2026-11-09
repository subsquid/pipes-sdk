import { describe, expect, it } from 'vitest'

import { type Commit, type ForkStep, summarize } from './metrics.js'
import { type ChainProfile, type ForkEvent, type PortalReport, blockHash } from './protocol.js'

const PROFILE: ChainProfile = {
  blockRate: 10,
  blocks: 100,
  startHead: 1_000,
  finalityDepth: 32,
  forkEvery: 0,
  forkDepth: 3,
  matchRate: 1,
  rows: 1,
  previousBlocks: 10,
  holdMs: 5_000,
  rttMs: 0,
}

/** A 10 s run at 10 blocks/s: block 1000 + i is produced at i·100 ms, on branch 0. */
function chain(forks: ForkEvent[] = []): PortalReport {
  const produced: [string, number][] = []
  for (let i = 1; i <= 100; i++) produced.push([blockHash(1_000 + i, 0), i * 100])
  for (const fork of forks) produced.push([blockHash(fork.head, fork.branch), fork.time])

  return { produced, forks, statuses: {}, startedAt: 0, finishedAt: 10_000 }
}

function commitAt(number: number, time: number, branch = 0): Commit {
  return { number, hash: blockHash(number, branch), blocks: 1, time, durationMs: 1 }
}

/** Every block committed `lag` ms after it was produced. */
function following(lag: number): Commit[] {
  return Array.from({ length: 100 }, (_, i) => commitAt(1_001 + i, (i + 1) * 100 + lag))
}

function run(report: PortalReport, commits: Commit[], forkSteps: ForkStep[] = []) {
  return summarize({ sink: 'memory', profile: PROFILE, report, commits, forkSteps })
}

describe('fork detection latency', () => {
  it('belongs to the fork whose branch the 409 carried, not to a later one', () => {
    // The 409 of fork 1 is in flight when fork 2 comes; it arrives 150 ms after fork 1.
    const forks = [
      { at: 1_040, head: 1_043, branch: 1, time: 4_300 },
      { at: 1_041, head: 1_044, branch: 2, time: 4_400 },
    ]
    const step: ForkStep = { detected: 4_450, branch: 1, rolledBack: 4_460 }

    const result = run(chain(forks), following(5), [step])

    expect(result.rollbackTimeline[0].detectMs).toBe(150)
    expect(result.forkDetectMs.p50).toBe(150)
  })

  it('has none for a 409 whose branch no fork produced', () => {
    const forks = [{ at: 1_040, head: 1_043, branch: 1, time: 4_300 }]

    const result = run(chain(forks), following(5), [{ detected: 4_350, branch: 7, rolledBack: 4_360 }])

    expect(result.rollbackTimeline[0].detectMs).toBeNaN()
    expect(result.forkDetectMs.count).toBe(0)
  })
})

describe('keeps up', () => {
  it('holds for a pipe that commits every block a few ms behind', () => {
    const result = run(chain(), following(5))

    expect(result.verdict).toBe('yes')
    expect(result.liveCommits).toBeGreaterThan(80)
  })

  it('fails a pipe that commits nothing while the chain runs and one batch at the end', () => {
    const result = run(chain(), [commitAt(1_100, 10_500)])

    expect(result.liveCommits).toBe(0)
    expect(result.commitsPerS).toBe(0)
    expect(result.verdict).toBe('no')
  })

  it('fails a pipe that stalls halfway, though its lag was flat before', () => {
    const halfway = following(5).filter((c) => c.time < 5_000)

    const result = run(chain(), [...halfway, commitAt(1_100, 10_500)])

    expect(result.lagSlopeMsPerS).toBeLessThan(1)
    expect(result.maxLiveGapMs).toBeGreaterThan(5_000)
    expect(result.verdict).toBe('no')
  })
})
