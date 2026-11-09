/**
 * CT-6 head-following and fork benchmark: sweeps the chain's block rate and reports, per rate,
 * whether the pipe keeps up, its head lag and the time a fork takes to resolve (SLI-4).
 *
 *   node --import tsx conformance/perf/run.ts --sink memory --rates 1,10,100 --fork-every 20
 *
 * See README.md for the options and how to read the table.
 */
import { spawn } from 'node:child_process'
import { writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'

import type { Distribution, PointResult } from './metrics.js'
import type { PointOptions, Sink } from './point.js'
import type { ChainProfile } from './protocol.js'

const POINT = fileURLToPath(new URL('./point.ts', import.meta.url))
const PACKAGE_ROOT = fileURLToPath(new URL('../..', import.meta.url))

const { values: args } = parseArgs({
  options: {
    sink: { type: 'string', default: 'memory' },
    rates: { type: 'string', default: '1,4,10,25,50,100,200,500,1000' },
    seconds: { type: 'string', default: '20' },
    'fork-every': { type: 'string', default: '0' },
    'fork-depth': { type: 'string', default: '3' },
    finality: { type: 'string', default: '32' },
    match: { type: 'string', default: '0.3' },
    rows: { type: 'string', default: '1' },
    'previous-blocks': { type: 'string', default: '10' },
    hold: { type: 'string', default: '5000' },
    rtt: { type: 'string', default: '0' },
    'db-rtt': { type: 'string', default: '0' },
    profiler: { type: 'boolean', default: false },
    'stop-on-fail': { type: 'boolean', default: false },
    json: { type: 'string' },
  },
})

const sink = args.sink as Sink
const rates = args.rates.split(',').map(Number)
const seconds = Number(args.seconds)

function profileAt(blockRate: number): ChainProfile {
  const profile: ChainProfile = {
    blockRate,
    blocks: Math.max(10, Math.round(blockRate * seconds)),
    startHead: 1_000,
    finalityDepth: Number(args.finality),
    forkEvery: Number(args['fork-every']),
    forkDepth: Number(args['fork-depth']),
    matchRate: Number(args.match),
    rows: Number(args.rows),
    previousBlocks: Number(args['previous-blocks']),
    holdMs: Number(args.hold),
    rttMs: Number(args.rtt),
  }

  if (profile.forkEvery > 0 && profile.forkDepth >= profile.finalityDepth) {
    throw new Error(`--fork-depth ${profile.forkDepth} must stay below --finality ${profile.finalityDepth}`)
  }

  return profile
}

function runPoint(options: PointOptions, timeoutMs: number): Promise<PointResult> {
  return new Promise((resolve, reject) => {
    const { NODE_OPTIONS, NODE_V8_COVERAGE, ...env } = process.env
    const child = spawn(process.execPath, ['--import', 'tsx', POINT, JSON.stringify(options)], {
      cwd: PACKAGE_ROOT,
      env: { ...env, NODE_ENV: options.profiler ? 'development' : 'production' },
      stdio: ['ignore', 'pipe', 'inherit'],
    })

    let output = ''
    child.stdout.on('data', (chunk) => {
      output += chunk
    })

    const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs)

    child.on('exit', (code, signal) => {
      clearTimeout(timer)
      const line = output.split('\n').find((l) => l.startsWith('RESULT '))
      if (line) {
        try {
          resolve(JSON.parse(line.slice('RESULT '.length)))
        } catch (e) {
          reject(new Error(`point at ${options.profile.blockRate} blocks/s printed a broken result: ${e}`))
        }
        return
      }

      const why = signal === 'SIGKILL' ? `timed out after ${timeoutMs} ms` : `exited with ${code ?? signal}`
      reject(new Error(`point at ${options.profile.blockRate} blocks/s ${why}\n${output.slice(-2_000)}`))
    })
  })
}

const ms = (value: number) => (Number.isFinite(value) ? `${Math.round(value)}` : '-')
const triple = (d: Distribution) => (d.count ? `${ms(d.p50)}/${ms(d.p99)}/${ms(d.max)}` : '-')

function row(r: PointResult): string[] {
  const statuses = Object.entries(r.statuses)
    .map(([status, n]) => `${status}:${n}`)
    .join(' ')

  return [
    `${r.profile.blockRate}`,
    r.error ? 'ERROR' : r.verdict,
    triple(r.lagMs),
    Number.isFinite(r.lagBlocks) ? r.lagBlocks.toFixed(1) : '-',
    r.lagSlopeMsPerS.toFixed(1),
    ms(r.drainMs),
    r.blocksPerCommit.toFixed(1),
    triple(r.commitMs),
    r.forks ? `${r.rollbacks}/${r.forks}` : '-',
    triple(r.forkResolveMs),
    ms(r.forkDetectMs.p50),
    ms(r.forkRollbackMs.p50),
    ms(r.forkResumeMs.p50),
    statuses,
  ]
}

const HEADER = [
  'blocks/s',
  'keeps up',
  'lag ms p50/p99/max',
  'lag blocks',
  'slope ms/s',
  'drain ms',
  'blocks/commit',
  'commit ms p50/p99/max',
  'rollbacks/forks',
  'fork resolve p50/p99/max',
  'detect p50',
  'rollback p50',
  'resume p50',
  'responses',
]

function print(rows: string[][]) {
  const widths = HEADER.map((h, i) => Math.max(h.length, ...rows.map((r) => r[i].length)))
  const line = (cells: string[]) => `| ${cells.map((c, i) => c.padEnd(widths[i])).join(' | ')} |`

  console.log(line(HEADER))
  console.log(`|${widths.map((w) => '-'.repeat(w + 2)).join('|')}|`)
  for (const r of rows) console.log(line(r))
}

const first = profileAt(rates[0])
console.log(
  `sink=${sink} seconds=${seconds} finality=${first.finalityDepth} ` +
    `fork-every=${first.forkEvery} fork-depth=${first.forkDepth} match=${first.matchRate} rows=${first.rows} ` +
    `previous-blocks=${first.previousBlocks} hold=${first.holdMs}ms rtt=${first.rttMs}ms db-rtt=${args['db-rtt']}ms profiler=${args.profiler}\n`,
)

const results: PointResult[] = []
const rows: string[][] = []
for (const rate of rates) {
  const options: PointOptions = {
    sink,
    profile: profileAt(rate),
    profiler: args.profiler,
    dbRttMs: Number(args['db-rtt']),
  }
  const result = await runPoint(options, seconds * 1_000 * 4 + 60_000).catch(
    (e: Error): PointResult => ({ ...emptyResult(options), error: e.message }),
  )

  results.push(result)
  rows.push(row(result))
  console.log(rows.at(-1)!.join('  '))
  if (result.error) console.error(result.error)

  if (args['stop-on-fail'] && (!result.keepsUp || result.error)) break
}

console.log()
print(rows)

if (args.json) await writeFile(args.json, JSON.stringify(results, null, 2))

function emptyResult(options: PointOptions): PointResult {
  const none: Distribution = { p50: Number.NaN, p90: Number.NaN, p99: Number.NaN, max: Number.NaN, count: 0 }

  return {
    sink: options.sink,
    profile: options.profile,
    lagMs: none,
    lagSlopeMsPerS: Number.NaN,
    liveCommits: 0,
    maxLiveGapMs: Number.NaN,
    drainMs: Number.NaN,
    verdict: 'no',
    keepsUp: false,
    lagBlocks: Number.NaN,
    commitsPerS: 0,
    blocksPerCommit: 0,
    commitMs: none,
    slowCommits: [],
    forkResolveMs: none,
    forkDetectMs: none,
    forkRollbackMs: none,
    forkResumeMs: none,
    forks: 0,
    rollbacks: 0,
    unresolvedForks: 0,
    statuses: {},
    rollbackTimeline: [],
  }
}
