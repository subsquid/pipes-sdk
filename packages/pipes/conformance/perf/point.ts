/**
 * One point of a sweep, in a process of its own: a live chain at one block rate, the pipe following
 * it into one sink. Prints `RESULT <json>` on its last line. Options come as JSON in the argument.
 */
import { randomBytes } from 'node:crypto'

import { createClient } from '@clickhouse/client'
import pg from 'pg'

import type { Target } from '~/core/index.js'

import { clickhouseClient } from '../clickhouse/pipe.js'
import { postgresPool } from '../postgres/pipe.js'
import { latencyProxy, portOf } from './latency-proxy.js'
import { LivePortal } from './live-portal.js'
import { type Commit, type ForkStep, summarize } from './metrics.js'
import { type ChainProfile, branchOf } from './protocol.js'
import {
  type Rows,
  clickhouseDdl,
  clickhouseSink,
  memoryTarget,
  postgresDdl,
  postgresTarget,
  runPipe,
} from './sinks.js'

export type Sink = 'memory' | 'postgres' | 'clickhouse'
export type PointOptions = { sink: Sink; profile: ChainProfile; profiler: boolean; dbRttMs: number }

const options: PointOptions = JSON.parse(process.argv[2])
const { sink, profile } = options

const POSTGRES_DSN = process.env['TEST_POSTGRES_DSN'] ?? 'postgresql://postgres:postgres@localhost:5432/postgres'
const CLICKHOUSE = {
  url: process.env['TEST_CLICKHOUSE_URL'] ?? 'http://localhost:10123',
  username: process.env['TEST_CLICKHOUSE_USERNAME'] ?? 'default',
  password: process.env['TEST_CLICKHOUSE_PASSWORD'] ?? 'default',
}

type Store = { target: Target<Rows>; drop: () => Promise<void> }

/** The store's address as the target sees it: behind a latency proxy when `dbRttMs` is set. */
async function distant(url: URL): Promise<URL> {
  if (!options.dbRttMs) return url

  const port = await latencyProxy({ host: url.hostname, port: portOf(url), rttMs: options.dbRttMs })
  const proxied = new URL(url)
  proxied.hostname = '127.0.0.1'
  proxied.port = String(port)

  return proxied
}

async function openStore(name: string): Promise<Store> {
  switch (sink) {
    case 'memory':
      return { target: memoryTarget(), drop: async () => {} }

    case 'postgres': {
      const admin = new pg.Client({ connectionString: POSTGRES_DSN })
      await admin.connect()
      await admin.query(`CREATE SCHEMA "${name}"`)
      await admin.query(postgresDdl(name))
      const dsn = (await distant(new URL(POSTGRES_DSN))).toString()
      const pool = postgresPool({ dsn, schema: name, application: name })

      return {
        target: postgresTarget({ pool, schema: name }),
        drop: async () => {
          await pool.end()
          await admin.query(`DROP SCHEMA "${name}" CASCADE`)
          await admin.end()
        },
      }
    }

    case 'clickhouse': {
      const admin = createClient(CLICKHOUSE)
      await admin.command({ query: `CREATE DATABASE "${name}"` })
      await admin.command({ query: clickhouseDdl(name) })
      const server = { ...CLICKHOUSE, url: (await distant(new URL(CLICKHOUSE.url))).toString() }
      const client = clickhouseClient({ server, database: name, application: name })

      return {
        target: clickhouseSink({ client }),
        drop: async () => {
          await admin.command({ query: `DROP DATABASE "${name}"` })
          await admin.close()
        },
      }
    }
  }
}

const name = `perf_${randomBytes(4).toString('hex')}`
const portal = await LivePortal.open(profile)
const store = await openStore(name)

const commits: Commit[] = []
const forkSteps: ForkStep[] = []
let atHead: () => void = () => {}
const reachedHead = new Promise<void>((resolve) => {
  atHead = resolve
})

const pipe = runPipe({
  id: name,
  url: portal.url,
  // A fork replaces at most `forkDepth` blocks below a head at or above the start one, so its
  // ancestor never falls below the pipe's first block.
  range: { from: profile.startHead - profile.forkDepth, to: profile.startHead + profile.blocks },
  target: store.target,
  profiler: options.profiler,
  witness: {
    commit: ({ cursor, blocks, time, durationMs }) => {
      commits.push({ number: cursor.number, hash: cursor.hash ?? '', blocks, time, durationMs })
      if (cursor.number >= profile.startHead) atHead()
    },
    forkDetected: (time, canonical) => {
      const parent = canonical[canonical.length - 1]
      forkSteps.push({ detected: time, branch: parent?.hash ? branchOf(parent.hash) : 0 })
    },
    rolledBack: (ancestor, time) => Object.assign(forkSteps[forkSteps.length - 1], { rolledBack: time, ancestor }),
  },
})

let error: string | undefined
try {
  await Promise.race([reachedHead, pipe])
  await portal.run()
  await pipe
} catch (e) {
  error = e instanceof Error ? `${e.name}: ${e.message}` : String(e)
}

const report = await portal.report()
portal.close()
await store.drop().catch(() => {})

const result = { ...summarize({ sink, profile, report, commits, forkSteps }), error }
// A pipe is written asynchronously: exit only once the line is out, or the parent reads half of it.
process.stdout.write(`RESULT ${JSON.stringify(result)}\n`, () => process.exit(0))
