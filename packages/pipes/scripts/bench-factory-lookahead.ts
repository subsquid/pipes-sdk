/**
 * Indexes Uniswap v3 swaps on Ethereum in one of three modes and prints the time and a digest of
 * every decoded event, so that runs in different modes can be compared for speed and for results.
 *
 *   MODE=topic|list|sets FROM=12369621 TO=13369621 PORTAL_URL=... PORTAL_TOKEN=... \
 *     npx tsx scripts/bench-factory-lookahead.ts
 *
 * topic: child events by topic alone, as without lookahead
 * list:  lookahead with an address list
 * sets:  lookahead with a fuse8 set (needs workers and a portal that serve `sets`)
 * prefill: stores every pool created before FROM, so that the other modes can start at FROM with
 *          the children an earlier run would have stored
 */
import { createHash } from 'node:crypto'
import { copyFileSync, existsSync, rmSync } from 'node:fs'

import { event, indexed } from '@subsquid/evm-abi'
import * as p from '@subsquid/evm-codec'

import { contractFactory, contractFactorySqliteStore, evmEventDecoder, evmPortalStream } from '~/evm/index.js'

const FACTORY = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
const FACTORY_START = 12_369_621

/** No log has this topic, so a prefill run fetches factory events only */
const Never = event(`0x${'0'.repeat(63)}1`, 'Never()', {})

const PoolCreated = event(
  '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118',
  'PoolCreated(address,address,uint24,int24,address)',
  {
    token0: indexed(p.address),
    token1: indexed(p.address),
    fee: indexed(p.uint24),
    tickSpacing: p.int24,
    pool: p.address,
  },
)

const Swap = event(
  '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
  'Swap(address,address,int256,int256,uint160,uint128,int24)',
  {
    sender: indexed(p.address),
    recipient: indexed(p.address),
    amount0: p.int256,
    amount1: p.int256,
    sqrtPriceX96: p.uint160,
    liquidity: p.uint128,
    tick: p.int24,
  },
)

const mode = process.env['MODE'] ?? 'topic'
const from = Number(process.env['FROM'] ?? 12_369_621)
const to = Number(process.env['TO'] ?? 13_369_621)
const url = process.env['PORTAL_URL'] ?? 'https://portal.sqd.dev/datasets/ethereum-mainnet'
const token = process.env['PORTAL_TOKEN']

const lookahead =
  mode === 'topic' || mode === 'prefill'
    ? false
    : mode === 'list'
      ? { filter: 'list' as const }
      : mode === 'sets'
        ? { filter: 'set' as const }
        : undefined
if (lookahead === undefined) throw new Error(`unknown MODE ${mode}`)

async function main() {
  const seedPath = `/tmp/bench-uniswap-v3-seed-${from}.sqlite`
  const dbPath = mode === 'prefill' ? seedPath : `/tmp/bench-uniswap-v3-${mode}.sqlite`
  rmSync(dbPath, { force: true })

  if (mode !== 'prefill' && from > FACTORY_START) {
    if (!existsSync(seedPath)) throw new Error(`run MODE=prefill FROM=${from} first`)

    copyFileSync(seedPath, dbPath)
  }

  const range = mode === 'prefill' ? { from: FACTORY_START, to: from - 1 } : { from, to }

  const stream = evmPortalStream({
    id: `bench-uniswap-v3-${mode}`,
    portal: {
      url,
      http: token ? { headers: { 'x-api-key': token } } : undefined,
    },
    outputs: evmEventDecoder({
      range,
      contracts: contractFactory({
        address: FACTORY,
        event: PoolCreated,
        childAddressField: 'pool',
        database: contractFactorySqliteStore({ path: dbPath }),
        lookahead,
      }),
      events: { swaps: mode === 'prefill' ? Never : Swap },
    }),
  })

  const digest = createHash('sha256')
  let swaps = 0
  let lastBlock = from
  let lastReport = Date.now()
  const started = Date.now()

  for await (const { data } of stream) {
    for (const swap of data.swaps) {
      digest.update(`${swap.block.number}:${swap.rawEvent.logIndex}:${swap.contract}:${swap.rawEvent.data}\n`)
      lastBlock = swap.block.number
    }
    swaps += data.swaps.length

    if (Date.now() - lastReport > 10_000) {
      lastReport = Date.now()
      const seconds = (lastReport - started) / 1000
      console.error(`[${mode}] ${seconds.toFixed(0)}s block ${lastBlock} swaps ${swaps}`)
    }
  }

  const seconds = (Date.now() - started) / 1000
  console.log(
    JSON.stringify({
      mode,
      from,
      to,
      seconds,
      swaps,
      swapsPerSecond: Math.round(swaps / seconds),
      digest: digest.digest('hex'),
    }),
  )
}

void main().catch((error) => {
  console.error(error)
  process.exit(1)
})
