import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { isDeepStrictEqual } from 'node:util'

import { it as base, describe, expect, vi } from 'vitest'

import { commonAbis } from '~/evm/abi/common.js'
import { evmEventDecoder } from '~/evm/evm-decoder.js'
import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { type ParquetTable, parquetTarget } from '~/targets/parquet/index.js'
import { encodeEvent, mockBlock } from '~/testing/evm/index.js'
import { type MockChainPortal, type MockPortalOptions, mockPortal, testLogger } from '~/testing/index.js'

import { type ParquetUnit, readParquetSnapshot, rowsOf } from './parquet-probe.js'
import { type Cursor, type OracleRow, type OracleState, ReferenceModel } from './reference-model.js'
import { type ParquetSnapshot, validateParquetSnapshot, validateRouteMode } from './validators.js'

/** S1: steady backfill over deep history, healthy portal (spec/11). The range ends on blocks with no rows. */
const S1 = { from: 0, to: 59, lastRow: 49, blocksPerResponse: 7 }
const RANGES = [{ from: S1.from, to: S1.to }]
const PIPE_ID = 'ct1-s1'
const TABLE = 'transfers'

const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as const
const ALICE = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' as const
const BOB = '0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f' as const

const TRANSFERS: ParquetTable = {
  table: TABLE,
  schema: {
    blockNumber: { type: 'INT64' },
    value: { type: 'UTF8' },
  },
}

type TransferRow = { blockNumber: number; value: string }

/** A sparse table: one transfer every fourth block up to `lastRow`, its value the block number. */
const hasTransfer = (number: number) => number % 4 === 1 && number <= S1.lastRow

/** The row S1 stores for a block with a transfer, as the store reads it back: INT64 as bigint. */
function rowAt(number: number): OracleRow {
  return { table: TABLE, block: number, value: { blockNumber: BigInt(number), value: String(number) } }
}

/** Every row S1 must leave in the store, from the scenario alone. */
const S1_ROWS: OracleRow[] = numbers(S1.from, S1.to).filter(hasTransfer).map(rowAt)

/** What the simulator holds. Every expectation below derives from it, never from the pipe. */
type Simulator = {
  chain: MockChainPortal
  /**
   * Every finalized head the portal has reported, in order; none on a dataset without finality.
   * The test sets them, so they are ground truth.
   */
  reported: number[]
  /** Moves the finalized head and records the report. */
  finalize(number: number): void
}

/** `finalized: null` serves a dataset without finality. */
type SimulateOptions = Omit<MockPortalOptions, 'blocks' | 'latest' | 'finalized'> & { finalized?: number | null }

const it = base.extend<{
  simulate: (options?: SimulateOptions) => Promise<Simulator>
  dir: string
}>({
  simulate: async ({}, use) => {
    const opened: MockChainPortal[] = []
    await use(async (options = {}) => {
      const finalized = options.finalized === undefined ? S1.to : options.finalized
      const chain = await mockPortal({
        blocksPerResponse: S1.blocksPerResponse,
        ...options,
        blocks: s1Blocks(),
        latest: S1.to,
        finalized,
      })
      opened.push(chain)

      const reported = finalized === null ? [] : [finalized]
      const finalize = (number: number) => {
        reported.push(number)
        chain.setHead({ finalized: number })
      }

      return { chain, reported, finalize }
    })
    await Promise.all(opened.map((chain) => chain.close()))
  },
  dir: async ({}, use) => {
    const dir = await mkdtemp(path.join(tmpdir(), 'sqd-ct1-'))
    await use(dir)
    await rm(dir, { recursive: true, force: true })
  },
})

function numbers(from: number, to: number): number[] {
  return Array.from({ length: to - from + 1 }, (_, i) => from + i)
}

function s1Blocks() {
  return numbers(S1.from, S1.to)
    .filter(hasTransfer)
    .map((number) => {
      const log = encodeEvent({
        abi: commonAbis.erc20.abi,
        eventName: 'Transfer',
        address: WETH,
        args: { from: ALICE, to: BOB, value: BigInt(number) },
      })

      return mockBlock({ number, transactions: [{ logs: [log] }] })
    })
}

/** The chain's cursor for a block, as a batch carries it: given and made-up blocks share `mockBlock`'s header. */
function cursorAt(number: number): Cursor {
  const { hash, timestamp } = mockBlock({ number }).header

  return { number, hash, timestamp }
}

/** A finalized head as the portal reports it: number and hash, no timestamp. */
function reportedAt(number: number): Cursor {
  return { number, hash: cursorAt(number).hash }
}

/**
 * The state S1 must leave in the store, from the simulator alone: the cursor at the range end and
 * the last floor the portal reported. Class K keeps no rollback chain (WP-47).
 */
function s1EndState(simulator: Simulator): OracleState {
  const lastReport = simulator.reported.at(-1)

  return {
    current: cursorAt(S1.to),
    finalized: lastReport === undefined ? undefined : reportedAt(lastReport),
    rollbackChain: [],
  }
}

/** The oracle for S1, as class K: the Parquet sink requires finalized-only delivery. */
function s1Oracle() {
  return new ReferenceModel({
    durability: 'K',
    ranges: RANGES,
    transform: (block) => (hasTransfer(block.number) ? [rowAt(block.number)] : []),
  })
}

function readStore(dir: string): Promise<ParquetSnapshot<ParquetUnit>> {
  return readParquetSnapshot({ dir, id: PIPE_ID, table: TABLE })
}

function requireSame(label: string, actual: unknown, expected: unknown): void {
  if (!isDeepStrictEqual(actual, expected)) {
    throw new Error(`${label} is ${JSON.stringify(actual)}, the simulator has ${JSON.stringify(expected)}`)
  }
}

/**
 * Streams S1 into a Parquet store and returns the store as the run leaves it.
 *
 * The oracle recovers from the store first, as the pipe does, then follows every batch in
 * lockstep, so a transition the spec forbids fails the run at that batch. Each batch's cursor must
 * be the chain's block and the pipe's finalized head one the portal reported (INV-2); the oracle
 * gets the finality the simulator last reported, never the pipe's view.
 *
 * When the run ends, the store must pass every structural validator and hold exactly S1's rows and
 * end state, and so must the oracle. Those come from the simulator, because the lockstep oracle
 * follows the pipe: it agrees with a pipe that stopped early.
 */
async function runS1({
  simulator,
  dir,
  model = s1Oracle(),
}: {
  simulator: Simulator
  dir: string
  /** Pass one to watch it during the run, or to carry it across a restart. */
  model?: ReferenceModel
}): Promise<ParquetSnapshot<ParquetUnit>> {
  const before = await readStore(dir)
  model.recover(before.state)

  let previous = model.readState().current?.number

  const outputs = evmEventDecoder({
    range: { from: S1.from, to: S1.to },
    events: { transfers: commonAbis.erc20.events.Transfer },
  }).pipe((decoded, ctx): TransferRow[] => {
    const current = ctx.stream.state.current
    const first = previous === undefined ? S1.from : previous + 1
    const covered = numbers(first, current.number).map(cursorAt)

    requireSame('the batch cursor', current, covered.at(-1))

    const observed = ctx.stream.head.finalized
    const reported = simulator.reported.some((number) => isDeepStrictEqual(observed, reportedAt(number)))
    const invented = observed !== undefined && !reported
    if (invented) {
      throw new Error(
        `INV-2: the pipe holds the finalized head ${JSON.stringify(observed)}, which the portal never reported`,
      )
    }

    const lastReport = simulator.reported.at(-1)
    const head = lastReport === undefined ? {} : { finalized: reportedAt(lastReport) }
    model.batch(covered, head)
    previous = current.number

    return decoded.transfers.map((transfer) => ({
      blockNumber: transfer.block.number,
      value: transfer.event.value.toString(),
    }))
  })

  await evmPortalStream({
    id: PIPE_ID,
    // Small reads keep batches small, so the run takes many checkpoints, not one.
    portal: { url: simulator.chain.url, maxBytes: 1 },
    outputs,
    logger: testLogger(),
  }).pipeTo(
    parquetTarget({
      dir,
      tables: [TRANSFERS],
      // Every batch that holds a row closes its window. A row count is exact; the engine reports bytes late.
      settings: { rollover: { maxRows: 1 } },
      onData: ({ store, data }) => {
        store.insert(TABLE, data)
      },
    }),
  )

  const after = await readStore(dir)
  const end = s1EndState(simulator)
  const violations = validateParquetSnapshot(after, RANGES)
  const offRoute = validateRouteMode('K', simulator.chain.requests)
  const rows = rowsOf(after)

  expect(violations, 'the structural validators').toEqual([])
  expect(offRoute, 'route-mode coherence').toEqual([])
  expect(after.state, 'the stored state').toStrictEqual(end)
  expect(rows, 'the stored rows').toEqual(S1_ROWS)
  expect(model.readState(), 'the oracle state').toStrictEqual(end)
  expect(model.data, 'the oracle rows').toEqual(S1_ROWS)

  return after
}

/** The raw bytes of every file in the store, by path. */
async function snapshotFiles(dir: string): Promise<Record<string, string>> {
  const entries = await readdir(dir, { recursive: true, withFileTypes: true })
  const files = entries.filter((entry) => entry.isFile())
  const snapshot: Record<string, string> = {}

  for (const file of files) {
    const filePath = path.join(file.parentPath, file.name)
    snapshot[path.relative(dir, filePath)] = (await readFile(filePath)).toString('base64')
  }

  return snapshot
}

/**
 * CT-1 on S1: simulator ↔ oracle lockstep, the structural validators, and a quiescence diff of the
 * sink store and the oracle against S1's fixed end state (spec/13). Every run makes all three
 * checks (`runS1`); each test adds the claim it is named for. The run is bounded, so quiescence is
 * its end.
 */
describe('CT-1 · S1 steady backfill · class K (Parquet)', () => {
  it('leaves exactly S1 in the store: every row, the cursor at the range end, the last floor', async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()

    const store = await runS1({ simulator, dir })
    const rows = rowsOf(store)

    expect(store.state).toStrictEqual(s1EndState(simulator))
    expect(rows).toEqual(S1_ROWS)
  })

  it('leaves exactly S1 on a dataset without finality, with no floor (DEF-15, WP-47)', async ({ simulate, dir }) => {
    const simulator = await simulate({ finalized: null })

    const store = await runS1({ simulator, dir })

    expect(store.state).toStrictEqual({ current: cursorAt(S1.to), finalized: undefined, rollbackChain: [] })
  })

  it('passes every structural validator over many units and many requests', async ({ simulate, dir }) => {
    const simulator = await simulate()

    const store = await runS1({ simulator, dir })
    const violations = validateParquetSnapshot(store, RANGES)

    // Over one unit or one request the validators would prove little.
    expect(store.units.length).toBeGreaterThan(1)
    expect(simulator.chain.requests.length).toBeGreaterThan(1)
    expect(violations).toEqual([])
  })

  it('tiles the whole range, the empty tail as a unit of its own (INV-4, RP-21)', async ({ simulate, dir }) => {
    const model = s1Oracle()

    // The source can merge responses into one batch, which would put the tail in the last row's
    // window. Holding the tail's request until that row is delivered gives the tail its own batch.
    const simulator = await simulate({
      delay: async ({ query }) => {
        if (query.fromBlock > S1.lastRow) {
          await vi.waitFor(() => expect(model.readState().current?.number).toBeGreaterThanOrEqual(S1.lastRow))
        }
      },
    })

    const { units, next } = await runS1({ simulator, dir, model })
    const covered = { from: units[0]?.from, to: units.at(-1)?.to, next }
    const tail = units.at(-1)

    expect(covered).toEqual({ from: S1.from, to: S1.to, next: S1.to + 1 })
    expect(tail?.from).toBeGreaterThan(S1.lastRow)
    expect(tail?.rows).toEqual([])
  })

  it('anchors each request on the hash of the block before it (IB-3)', async ({ simulate, dir }) => {
    const simulator = await simulate()

    await runS1({ simulator, dir })

    const anchors = simulator.chain.requests
      .filter((request) => request.query.fromBlock > S1.from)
      .map((request) => ({
        parent: request.query.parentBlockHash,
        expected: cursorAt(request.query.fromBlock - 1).hash,
      }))
    const unanchored = anchors.filter((anchor) => anchor.parent !== anchor.expected)

    expect(anchors.length).toBeGreaterThan(1)
    expect(unanchored).toEqual([])
  })

  it('asks for nothing outside the configured range (INV-24)', async ({ simulate, dir }) => {
    const simulator = await simulate()

    await runS1({ simulator, dir })

    const requests = simulator.chain.requests
    const outside = requests.filter((request) => request.query.fromBlock < S1.from || request.query.toBlock !== S1.to)

    expect(requests.length).toBeGreaterThan(1)
    expect(outside).toEqual([])
  })

  it('commits the same rows however the portal cuts its responses (INV-22)', async ({ simulate, dir }) => {
    const coarse = await simulate({ blocksPerResponse: 13 })
    const fine = await simulate({ blocksPerResponse: 3 })
    const second = await mkdtemp(path.join(tmpdir(), 'sqd-ct1-'))

    try {
      const a = await runS1({ simulator: coarse, dir })
      const b = await runS1({ simulator: fine, dir: second })
      const unitNames = (store: ParquetSnapshot<ParquetUnit>) => store.units.map((unit) => unit.name)
      const rowsA = rowsOf(a)
      const rowsB = rowsOf(b)

      // Different units prove the two runs were cut differently; otherwise the test proves nothing.
      expect(unitNames(b)).not.toEqual(unitNames(a))
      expect(rowsB).toEqual(rowsA)
      expect(b.state).toStrictEqual(a.state)
    } finally {
      await rm(second, { recursive: true, force: true })
    }
  })

  it('changes nothing on a restart with nothing left to read (INV-10)', async ({ simulate, dir }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    await runS1({ simulator, dir, model })
    const before = await snapshotFiles(dir)
    const units = Object.keys(before).filter((file) => file.endsWith('.parquet'))

    await runS1({ simulator, dir, model })
    const after = await snapshotFiles(dir)

    expect(units.length).toBeGreaterThan(1)
    expect(after).toEqual(before)
  })

  it('delivers no block before it is final, and the rest once it is (INV-25)', async ({ simulate, dir }) => {
    const lag = 30
    const simulator = await simulate({ finalized: lag })
    const model = s1Oracle()

    // The floor stays at the lag until stage 3 moves it, so a run that ends before then stopped early.
    let released = false
    const run = runS1({ simulator, dir, model }).then((store) => {
      if (!released) {
        throw new Error(`the run ended while the floor was ${lag}, before the range end ${S1.to}`)
      }

      return store
    })

    // Stage 1: the pipe delivers every final block, then polls past the floor and gets no data.
    // A run that ends, or a batch the oracle refuses, fails here instead of the poll timing out.
    const polling = vi.waitFor(() => {
      const requests = simulator.chain.requests
      const atHead = requests.some((request) => request.query.fromBlock === lag + 1 && request.status === 204)

      expect(atHead).toBe(true)
      expect(model.readState().current).toEqual(cursorAt(lag))
    })
    await Promise.race([run, polling])

    // Stage 2: nothing above the floor reached the store. Whether it has checkpointed yet is free.
    const early = await readStore(dir)
    const earlyCursor = early.state?.current?.number ?? -1
    const earlyBound = rowsOf(early).at(-1)?.block ?? -1

    expect(earlyCursor).toBeLessThanOrEqual(lag)
    expect(earlyBound).toBeLessThanOrEqual(lag)

    // Stage 3: the floor moves to the range end, and the run ends there with every row.
    released = true
    simulator.finalize(S1.to)
    const store = await run
    const rows = rowsOf(store)

    expect(store.state?.current).toStrictEqual(cursorAt(S1.to))
    expect(rows).toHaveLength(S1_ROWS.length)
  })
})
