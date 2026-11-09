/**
 * The portal of a performance run, in a process of its own so that serving blocks takes no CPU from
 * the pipe. It produces a chain at a fixed block rate, forks it on schedule, and holds a stream
 * request at the head until the block it asks for exists, the way hotblocks does.
 *
 * Options come as JSON in the one argument; the parent talks to it over IPC (see protocol.ts).
 */
import { type MockChainPortal, mockPortal } from '~/testing/index.js'

import { erc20Logs } from '../scenarios/erc20.js'
import { latencyProxy } from './latency-proxy.js'
import { type ChainProfile, type ForkEvent, type FromPortal, type ToPortal, blockHash, now } from './protocol.js'

const profile: ChainProfile = JSON.parse(process.argv[2])

const send = (message: FromPortal) => process.send?.(message)

const GENESIS = 1_700_000_000
const [TRANSFER] = erc20Logs({ transfer: 0n })

// One hash per height: the version on the canonical chain.
const hashes = new Map<number, string>()
const produced: [string, number][] = []
const forks: ForkEvent[] = []
const statuses: Record<number, number> = {}

let latest = profile.startHead
let branch = 0

function block(number: number) {
  const hash = blockHash(number, branch)
  hashes.set(number, hash)

  const header = { number, hash, parentHash: hashes.get(number - 1), timestamp: GENESIS + number }

  // Spread matching blocks evenly: block n matches when floor(n·rate) steps up.
  const matches = Math.floor(number * profile.matchRate) !== Math.floor((number - 1) * profile.matchRate)
  if (!matches) {
    return { header }
  }

  const transactionHash = `0x${hash.slice(2, 10)}${'0'.repeat(56)}`
  const logs = Array.from({ length: profile.rows }, (_, logIndex) => ({
    ...TRANSFER,
    data: `0x${(number * 1_000 + logIndex + 1_000_000_000 * branch).toString(16).padStart(64, '0')}`,
    logIndex,
    transactionIndex: 0,
    transactionHash,
  }))

  return {
    header,
    transactions: [{ transactionIndex: 0, hash: transactionHash, from: TRANSFER.topics[1], to: TRANSFER.address }],
    logs,
  }
}

// Requests held at the head, each waiting for its `fromBlock`.
const waiters = new Set<{ from: number; release: () => void }>()

function hold(from: number): Promise<void> | void {
  if (from <= latest) return

  return new Promise((resolve) => {
    const waiter = {
      from,
      release: () => {
        clearTimeout(timer)
        waiters.delete(waiter)
        resolve()
      },
    }
    const timer = setTimeout(waiter.release, profile.holdMs)
    waiters.add(waiter)
  })
}

function releaseReady() {
  for (const waiter of waiters) {
    if (waiter.from <= latest) waiter.release()
  }
}

const history = Array.from({ length: profile.startHead + 1 }, (_, number) => block(number))

const chain: MockChainPortal = await mockPortal({
  blocks: history,
  latest: profile.startHead,
  finalized: Math.max(0, profile.startHead - profile.finalityDepth),
  previousBlocks: profile.previousBlocks,
  delay: ({ query }) => hold(query.fromBlock),
})

function tally() {
  for (const request of chain.requests) {
    if (request.status !== undefined) statuses[request.status] = (statuses[request.status] ?? 0) + 1
  }
  // Answers carry their blocks; the run would otherwise keep every block it served.
  chain.requests.splice(0, chain.requests.length, ...chain.requests.filter((r) => r.status === undefined))
}

function produceNext(index: number) {
  const time = now()
  const forkDue = profile.forkEvery > 0 && index % profile.forkEvery === 0

  if (!forkDue) {
    const next = block(latest + 1)
    chain.append(next)
    latest++
    produced.push([next.header.hash, time])

    return
  }

  // The reorging block arrives as the new head, its ancestor `forkDepth` blocks below the old one.
  const at = latest - profile.forkDepth
  branch++
  const replacement = []
  for (let number = at + 1; number <= latest + 1; number++) {
    const next = block(number)
    replacement.push(next)
    produced.push([next.header.hash, time])
  }
  chain.fork(at, replacement)
  latest++
  forks.push({ at, head: latest, branch, time })
}

let startedAt = 0
let finishedAt = 0

function run() {
  startedAt = now()
  let made = 0

  const tick = () => {
    const due = Math.min(profile.blocks, Math.floor(((now() - startedAt) * profile.blockRate) / 1000))
    while (made < due) {
      made++
      produceNext(made)
    }

    chain.setHead({ finalized: Math.max(0, latest - profile.finalityDepth) })
    releaseReady()
    tally()

    if (made >= profile.blocks) {
      finishedAt = now()
      send({ type: 'finished' })

      return
    }

    const nextDue = startedAt + ((made + 1) * 1000) / profile.blockRate
    setTimeout(tick, Math.max(0, nextDue - now()))
  }

  tick()
}

process.on('message', (message: ToPortal) => {
  switch (message.type) {
    case 'start':
      run()
      break
    case 'report':
      tally()
      send({ type: 'report', report: { produced, forks, statuses, startedAt, finishedAt } })
      break
  }
})

process.on('disconnect', () => {
  chain.close().finally(() => process.exit(0))
})

async function delayed(target: string, rttMs: number): Promise<string> {
  const { hostname, port } = new URL(target)
  const proxy = await latencyProxy({ host: hostname, port: Number(port), rttMs })

  return `http://${hostname}:${proxy}`
}

const url = profile.rttMs > 0 ? await delayed(chain.url, profile.rttMs) : chain.url

send({ type: 'ready', url })
