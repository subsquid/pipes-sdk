import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { describe, expect, vi } from 'vitest'

import { type ParquetUnit, rowsOf } from './parquet-probe.js'
import { RANGES, S1, S1_ROWS, TABLE_NAMES, cursorAt, s1Oracle } from './s1.js'
import { type S1Store, endStateOf, it, readStore, runS1 } from './s1-harness.js'
import { validateParquetSnapshot } from './validators.js'

/** Every unit the store holds, table by table. */
function unitsOf(store: S1Store): ParquetUnit[] {
  return TABLE_NAMES.flatMap((table) => store.tables[table].units)
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

    expect(store.state).toStrictEqual(endStateOf(simulator))
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
    const fewestUnits = Math.min(...TABLE_NAMES.map((table) => store.tables[table].units.length))

    // Over one unit or one request the validators would prove little.
    expect(fewestUnits).toBeGreaterThan(1)
    expect(simulator.chain.requests.length).toBeGreaterThan(1)
    expect(violations).toEqual([])
  })

  it('tiles the whole range in every table, the empty tail as a unit of its own (INV-4, RP-21)', async ({
    simulate,
    dir,
  }) => {
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

    const store = await runS1({ simulator, dir, model })
    const tiling = TABLE_NAMES.map((table) => {
      const { units, next } = store.tables[table]
      const tail = units.at(-1)
      const tailAfterRows = tail !== undefined && tail.from > S1.lastRow

      return { table, from: units[0]?.from, to: tail?.to, next, tailAfterRows, tailRows: tail?.rows }
    })
    const whole = TABLE_NAMES.map((table) => ({
      table,
      from: S1.from,
      to: S1.to,
      next: S1.to + 1,
      tailAfterRows: true,
      tailRows: [],
    }))

    // Where the tail starts is where a batch ended, which is free.
    expect(tiling).toEqual(whole)
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
      const unitNames = (store: S1Store) => unitsOf(store).map((unit) => unit.name)
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
    const earlyBound = Math.max(-1, ...rowsOf(early).map((row) => row.block))

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
