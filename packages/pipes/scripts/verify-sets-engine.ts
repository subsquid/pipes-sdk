/**
 * Checks the query engine's sets against topic-only queries on real Ethereum data.
 *
 * For each window it fetches Uniswap v3 Swap logs twice: by topic alone, and filtered by a fuse8 set
 * of every pool created up to the window's end. The set answer must be a subset of the topic answer,
 * hold exactly the topic answer's pool logs, and pass about one non-pool log in 256.
 *
 *   POOLS=pools.txt PORTAL_URL=... PORTAL_TOKEN=... WINDOWS=60 WINDOW=2000 FROM=12400000 TO=21000000 \
 *     npx tsx scripts/verify-sets-engine.ts
 */
import { readFileSync } from 'node:fs'

import { buildFuse8Set } from '~/internal/fuse8/index.js'

const SWAP = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
const OLD_WORKER = "unknown table filter 'sets'"

const env = (name: string, fallback?: string) => process.env[name] ?? fallback!
const url = env('PORTAL_URL', 'https://portal.sqd.dev/datasets/ethereum-mainnet')
const token = env('PORTAL_TOKEN')
const windows = Number(env('WINDOWS', '60'))
const windowBlocks = Number(env('WINDOW', '2000'))
const from = Number(env('FROM', '12400000'))
const to = Number(env('TO', '21000000'))
const attempts = Number(env('ATTEMPTS', '3'))
/** `stream` also reads blocks above the finalized head, which the real-time source serves */
const endpoint = env('ENDPOINT', 'finalized-stream')

const pools = readFileSync(env('POOLS'), 'utf8')
  .trim()
  .split('\n')
  .map((line) => {
    const [block, address] = line.split(' ')
    return { block: Number(block), address: address! }
  })

class OldWorker extends Error {}

/** Every log in `first…last`, following the portal across responses that end early. */
async function fetchLogs(query: object, first: number, last: number, sources: Set<string>): Promise<string[]> {
  const logs: string[] = []
  let next = first

  while (next <= last) {
    const res = await fetch(`${url}/${endpoint}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': token },
      body: JSON.stringify({ ...query, fromBlock: next, toBlock: last }),
    })
    const text = await res.text()
    sources.add(res.headers.get('x-sqd-data-source') ?? 'unknown')

    if (res.status === 400 && text.includes(OLD_WORKER)) throw new OldWorker(`block ${next}`)
    if (res.status !== 200) throw new Error(`HTTP ${res.status} at block ${next}: ${text.slice(0, 300)}`)

    let lastSeen = next - 1
    for (const line of text.trim().split('\n').filter(Boolean)) {
      const block = JSON.parse(line)
      lastSeen = block.header.number
      for (const log of block.logs ?? []) logs.push(`${block.header.number}:${log.logIndex}:${log.address}`)
    }

    if (lastSeen < next) throw new Error(`no progress at block ${next}`)
    next = lastSeen + 1
  }

  return logs
}

const fields = { block: { number: true }, log: { address: true, logIndex: true } }

async function verify(first: number) {
  const last = first + windowBlocks - 1
  const children = pools.filter((p) => p.block <= last).map((p) => p.address)
  const isPool = new Set(children)
  const set = buildFuse8Set(children)

  const sources = new Set<string>()
  const byTopic = await fetchLogs({ type: 'evm', fields, logs: [{ topic0: [SWAP] }] }, first, last, new Set())

  let bySet: string[] | undefined
  let oldWorker = 0
  for (let i = 0; i < attempts && !bySet; i++) {
    try {
      bySet = await fetchLogs(
        { type: 'evm', fields, sets: { pools: set }, logs: [{ address: [{ set: 'pools' }], topic0: [SWAP] }] },
        first,
        last,
        sources,
      )
    } catch (error) {
      if (!(error instanceof OldWorker)) throw error
      oldWorker++
    }
  }
  if (!bySet) return { first, last, status: 'old-worker' as const, sources: [...sources] }

  const pool = (log: string) => isPool.has(log.split(':')[2]!)
  const topicAll = new Set(byTopic)
  const expected = byTopic.filter(pool)
  const actual = bySet.filter(pool)
  const extra = bySet.filter((log) => !pool(log))

  const notInTopic = bySet.filter((log) => !topicAll.has(log)).length
  const same = JSON.stringify(expected) === JSON.stringify(actual)

  return {
    first,
    last,
    status: same && notInTopic === 0 ? ('ok' as const) : ('MISMATCH' as const),
    children: children.length,
    poolSwaps: expected.length,
    missing: expected.filter((log) => !new Set(actual).has(log)).length,
    notInTopic,
    otherSwaps: byTopic.length - expected.length,
    falsePositives: extra.length,
    retries: oldWorker,
    sources: [...sources],
  }
}

async function main() {
  const step = Math.floor((to - from - windowBlocks) / (windows - 1))
  const starts = Array.from({ length: windows }, (_, i) => from + i * step)
  const results: Awaited<ReturnType<typeof verify>>[] = []

  const queue = [...starts]
  await Promise.all(
    Array.from({ length: 4 }, async () => {
      for (let first = queue.shift(); first !== undefined; first = queue.shift()) {
        const result = await verify(first)
        results.push(result)
        console.log(JSON.stringify(result))
      }
    }),
  )

  const done = results.filter((r) => r.status !== 'old-worker') as Extract<
    (typeof results)[number],
    { poolSwaps: number }
  >[]
  const other = done.reduce((s, r) => s + r.otherSwaps, 0)
  const fp = done.reduce((s, r) => s + r.falsePositives, 0)
  console.log(
    JSON.stringify({
      windows: results.length,
      ok: results.filter((r) => r.status === 'ok').length,
      mismatch: results.filter((r) => r.status === 'MISMATCH').length,
      oldWorker: results.filter((r) => r.status === 'old-worker').length,
      poolSwaps: done.reduce((s, r) => s + r.poolSwaps, 0),
      otherSwaps: other,
      falsePositives: fp,
      falsePositiveRate: other ? `1 in ${Math.round(other / fp)}` : 'n/a',
    }),
  )
}

void main().catch((error) => {
  console.error(error)
  process.exit(1)
})
