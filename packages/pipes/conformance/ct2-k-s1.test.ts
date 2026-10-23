import { stat } from 'node:fs/promises'
import path from 'node:path'
import { isDeepStrictEqual } from 'node:util'

import { describe, expect } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { runKilled } from './kill-points.js'
import type { KillPoint } from './kill-switch.js'
import { type ParquetUnit, rowsOf } from './parquet-probe.js'
import type { ReferenceModel } from './reference-model.js'
import { PIPE_ID, RANGES, S1, S1_ROWS, TABLE_NAMES, type TableName, s1Oracle, streamS1 } from './s1.js'
import { type S1Store, type Simulator, it, readStore, runS1 } from './s1-harness.js'
import { type ParquetTableSnapshot, validateParquetSnapshot } from './validators.js'

/** 'PAR1': what a Parquet writer puts in a file before any row. */
const HEADER = 4

const STATE_FILE = String.raw`^_sqd_parquet_state\.${PIPE_ID}\.json$`
const TEMP_STATE = String.raw`^\.tmp-state-`
const ANY_UNIT = String.raw`^[a-z]+/\d{12}-\d{12}\.parquet$`
const ANY_TEMP_UNIT = String.raw`^[a-z]+/\.tmp-\d+\.parquet$`
const unitOf = (table: TableName) => String.raw`^${table}/\d{12}-\d{12}\.parquet$`
const tempUnitOf = (table: TableName) => String.raw`^${table}/\.tmp-\d+\.parquet$`

/**
 * The class-K commit protocol (CN-12) step by step. A checkpoint publishes each table's unit
 * (transfers first), then writes the state to a temp file and renames it over the state record.
 * Block 1 holds a transfer and an approval, so the first checkpoint publishes both tables. Counts
 * are within one run, so a restart counts from 1 again.
 */
const POINTS = {
  midUnitWrite: { name: 'mid-unit-write', op: 'write', path: tempUnitOf('transfers'), at: 2 },
  firstUnitWrite: {
    name: 'mid-unit-write, before the first commit',
    op: 'write',
    path: tempUnitOf('transfers'),
    at: 1,
  },
  beforeStateWrite: {
    name: 'post-publish, before the state write',
    op: 'writeFile',
    path: TEMP_STATE,
    at: 2,
    when: 'before',
  },
  beforeStateRename: {
    name: 'post-publish, before the state rename',
    op: 'rename',
    path: STATE_FILE,
    at: 2,
    when: 'before',
  },
  firstStateRename: {
    name: 'post-publish, before the first state rename',
    op: 'rename',
    path: STATE_FILE,
    at: 1,
    when: 'before',
  },
  midRename: { name: 'mid-rename', op: 'rename', path: unitOf('approvals'), at: 2, when: 'before' },
  stretchedUnit: {
    name: 'post-publish of a stretched unit, before the state write',
    op: 'rename',
    path: unitOf('approvals'),
    at: 1,
    when: 'after',
    straddling: true,
  },
  postState: { name: 'post-state', op: 'rename', path: STATE_FILE, at: 2, when: 'after' },
  recoverySweep: {
    name: 'recovery, before the over-cursor sweep',
    op: 'unlink',
    path: ANY_UNIT,
    at: 1,
    when: 'before',
  },
  recoveryTempSweep: {
    name: 'recovery, before the temp sweep',
    op: 'unlink',
    path: ANY_TEMP_UNIT,
    at: 1,
    when: 'before',
  },
} satisfies Record<string, KillPoint>

/** What a crash left beside its commit point. */
type CrashWindow = {
  /** Whether the store holds a state record. */
  state: boolean
  /** Per table, the units above the cursor (every unit, with no cursor), by name. */
  above: Record<TableName, string[]>
  /** The window ends of the units above the cursor: one checkpoint publishes them all at one end. */
  aboveEnds: number[]
  /** Per table, the size of each temp unit left. */
  temp: Record<TableName, number[]>
  /** How many temp state files are left. */
  tempStates: number
  /** Residue that is neither a temp unit nor a temp state. */
  unknown: string[]
}

function cursorOf(store: S1Store): number {
  return store.state?.current?.number ?? -1
}

async function crashWindowOf(dir: string, store: S1Store): Promise<CrashWindow> {
  const cursor = cursorOf(store)
  const matching = (pattern: string) => store.residue.filter((file) => new RegExp(pattern).test(file))
  const above = {} as Record<TableName, string[]>
  const temp = {} as Record<TableName, number[]>

  for (const table of TABLE_NAMES) {
    const units = store.tables[table].units.filter((unit) => unit.to > cursor)
    const sizes = matching(tempUnitOf(table)).map(async (file) => (await stat(path.join(dir, file))).size)

    above[table] = units.map((unit) => unit.name)
    temp[table] = await Promise.all(sizes)
  }

  const ends = TABLE_NAMES.flatMap((table) => store.tables[table].units.filter((unit) => unit.to > cursor))
  const known = new Set([...matching(ANY_TEMP_UNIT), ...matching(TEMP_STATE)])

  return {
    state: store.state !== undefined,
    above,
    aboveEnds: [...new Set(ends.map((unit) => unit.to))],
    temp,
    tempStates: matching(TEMP_STATE).length,
    unknown: store.residue.filter((file) => !known.has(file)),
  }
}

/** The store as its commit point left it: the units at or below the cursor, and no residue. */
function committedPart(store: S1Store): S1Store {
  const cursor = cursorOf(store)
  const tables = {} as Record<TableName, ParquetTableSnapshot<ParquetUnit>>

  for (const table of TABLE_NAMES) {
    const { units, next } = store.tables[table]

    tables[table] = { units: units.filter((unit) => unit.to <= cursor), next }
  }

  return { state: store.state, tables, residue: [] }
}

/**
 * Kills S1 at `point`, and checks that the crash left its commit point intact (CN-40): with the
 * crash window set apart, the store is a valid one holding exactly S1's rows up to its cursor.
 */
async function crash({
  simulator,
  dir,
  model,
  point,
}: {
  simulator: Simulator
  dir: string
  model: ReferenceModel
  point: KillPoint
}) {
  const store = await runKilled({ simulator, dir, model, point })
  const committed = committedPart(store)
  const cursor = cursorOf(store)
  const violations = validateParquetSnapshot(committed, RANGES)
  const rows = rowsOf(committed)
  const expected = S1_ROWS.filter((row) => row.block <= cursor)

  expect(violations, 'the committed part').toEqual([])
  expect(rows, 'the committed rows').toEqual(expected)

  return { store, window: await crashWindowOf(dir, store) }
}

/**
 * Restarts S1 in-process over what the crash left. The run ends in S1's end state (`runS1`), and
 * its first request starts right after the recovered cursor and anchors on it (WP-1, IB-3).
 */
async function resume({
  simulator,
  dir,
  model,
  store,
}: {
  simulator: Simulator
  dir: string
  model: ReferenceModel
  /** The store as the restart finds it. */
  store: S1Store
}): Promise<void> {
  const first = simulator.chain.requests.length
  const cursor = store.state?.current

  await runS1({ simulator, dir, model })

  const resumed = simulator.chain.requests[first]?.query

  expect(resumed?.fromBlock, 'the first block the restart asks for').toBe(
    cursor === undefined ? S1.from : cursor.number + 1,
  )
  expect(resumed?.parentBlockHash, 'the anchor of the restart').toBe(cursor?.hash)
}

/**
 * CT-2 on S1 for class K: the pipe dies by SIGKILL at each step of the commit protocol and of
 * recovery, and a restart converges to S1's end state (INV-40…INV-42). Every crash must leave its
 * commit point intact and its crash window where the protocol puts it; that window is what proves
 * the kill landed on the step the test is named for.
 */
describe('CT-2 · S1 kill-point matrix · class K (Parquet)', () => {
  it('mid-unit-write: the cut unit is a temp file, and recovery deletes it', async ({ simulate, dir }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const { store, window } = await crash({ simulator, dir, model, point: POINTS.midUnitWrite })

    expect(window).toMatchObject({ state: true, above: { transfers: [], approvals: [] }, tempStates: 0, unknown: [] })
    expect(window.temp.transfers).toEqual([HEADER])
    // Whether the batch opened an approvals unit is free; one it opened is cut the same way.
    expect([[], [HEADER]]).toContainEqual(window.temp.approvals)

    await resume({ simulator, dir, model, store })
  })

  it('mid-unit-write before the first commit: no state and no unit, and the restart starts cold', async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const { store, window } = await crash({ simulator, dir, model, point: POINTS.firstUnitWrite })

    expect(window).toMatchObject({ state: false, above: { transfers: [], approvals: [] }, tempStates: 0, unknown: [] })
    expect(window.temp.transfers).toEqual([HEADER])
    expect([[], [HEADER]]).toContainEqual(window.temp.approvals)

    await resume({ simulator, dir, model, store })
  })

  it('post-publish, before the state write: the units above the cursor are deleted (CN-12)', async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const { store, window } = await crash({ simulator, dir, model, point: POINTS.beforeStateWrite })

    expect(window).toMatchObject({ state: true, temp: { transfers: [], approvals: [] }, tempStates: 0, unknown: [] })
    expect(window.above.transfers).toHaveLength(1)
    // Whether the checkpoint holds an approval is free.
    expect(window.above.approvals.length).toBeLessThanOrEqual(1)
    expect(window.aboveEnds).toHaveLength(1)

    await resume({ simulator, dir, model, store })
  })

  it('post-publish, before the state rename: the temp state and the units above the cursor are deleted', async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const { store, window } = await crash({ simulator, dir, model, point: POINTS.beforeStateRename })

    expect(window).toMatchObject({ state: true, temp: { transfers: [], approvals: [] }, tempStates: 1, unknown: [] })
    expect(window.above.transfers).toHaveLength(1)
    expect(window.above.approvals.length).toBeLessThanOrEqual(1)
    expect(window.aboveEnds).toHaveLength(1)

    await resume({ simulator, dir, model, store })
  })

  it('mid-rename: one table published above the cursor, the other finished but unpublished; both undone', async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const { store, window } = await crash({ simulator, dir, model, point: POINTS.midRename })
    const [finished] = window.temp.approvals

    expect(window).toMatchObject({
      state: true,
      above: { approvals: [] },
      temp: { transfers: [] },
      tempStates: 0,
      unknown: [],
    })
    expect(window.above.transfers).toHaveLength(1)
    expect(window.temp.approvals).toHaveLength(1)
    // Finished: rows and footer are on disk, past the header.
    expect(finished).toBeGreaterThan(HEADER)

    await resume({ simulator, dir, model, store })
  })

  it('post-publish of a stretched unit: the unit straddling the cursor is the checkpoint’s own, and is deleted (CN-12)', async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const { store, window } = await crash({ simulator, dir, model, point: POINTS.stretchedUnit })
    const cursor = cursorOf(store)
    const approvals = store.tables.approvals
    const [stretched] = approvals.units.filter((unit) => unit.to > cursor)

    expect(window).toMatchObject({ state: true, temp: { transfers: [], approvals: [] }, tempStates: 0, unknown: [] })
    expect(window.above.transfers).toHaveLength(1)
    expect(window.above.approvals).toHaveLength(1)
    // It starts where the state says approvals were next due to publish, at or below the cursor.
    expect(stretched.from).toBe(approvals.next)
    expect(stretched.from).toBeLessThanOrEqual(cursor)

    await resume({ simulator, dir, model, store })
  })

  it('post-state: nothing to repair, and the restart resumes right after the new cursor', async ({ simulate, dir }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const { store, window } = await crash({ simulator, dir, model, point: POINTS.postState })
    const newestEnd = Math.max(...TABLE_NAMES.flatMap((table) => store.tables[table].units.map((unit) => unit.to)))

    expect(window).toEqual({
      state: true,
      above: { transfers: [], approvals: [] },
      aboveEnds: [],
      temp: { transfers: [], approvals: [] },
      tempStates: 0,
      unknown: [],
    })
    // The kill came after the rename, so the cursor is the one the checkpoint's units end at.
    expect(cursorOf(store)).toBe(newestEnd)

    await resume({ simulator, dir, model, store })
  })

  it('a crash in recovery, before the over-cursor sweep: recovery runs again and converges (INV-41)', async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const first = await crash({ simulator, dir, model, point: POINTS.beforeStateRename })
    const second = await crash({ simulator, dir, model, point: POINTS.recoverySweep })

    // The temp sweep ran before the kill; the over-cursor sweep did not.
    expect(second.store.state).toStrictEqual(first.store.state)
    expect(second.window).toEqual({ ...first.window, tempStates: 0 })
    expect(first.window.tempStates).toBe(1)

    await resume({ simulator, dir, model, store: second.store })
  })

  it('a crash in recovery, before the temp sweep: recovery runs again and converges (INV-41)', async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const first = await crash({ simulator, dir, model, point: POINTS.midUnitWrite })
    const second = await crash({ simulator, dir, model, point: POINTS.recoveryTempSweep })

    expect(second.store.state).toStrictEqual(first.store.state)
    expect(second.window).toEqual(first.window)
    expect(first.window.temp.transfers).toEqual([HEADER])

    await resume({ simulator, dir, model, store: second.store })
  })

  it("a second crash in the restart's first checkpoint: the residue does not pile up (CN-42)", async ({
    simulate,
    dir,
  }) => {
    const simulator = await simulate()
    const model = s1Oracle()

    const first = await crash({ simulator, dir, model, point: POINTS.beforeStateRename })
    const second = await crash({ simulator, dir, model, point: POINTS.firstStateRename })

    // The restart swept the first crash's window and published one checkpoint of its own above the same cursor.
    expect(second.store.state).toStrictEqual(first.store.state)
    expect(second.window).toMatchObject({ temp: { transfers: [], approvals: [] }, tempStates: 1, unknown: [] })
    expect(second.window.above.transfers).toHaveLength(1)
    expect(second.window.above.approvals.length).toBeLessThanOrEqual(1)
    expect(second.window.aboveEnds).toHaveLength(1)

    await resume({ simulator, dir, model, store: second.store })
  })

  it('GAP-41 (open): recovery keeps the units published before the first state write, and the restart does not converge', async ({
    simulate,
    dir,
  }) => {
    // The restart's first request waits for a read of the store, so the read sees what recovery left.
    let restarted = false
    let recovered: Promise<S1Store> | undefined
    const simulator = await simulate({
      delay: async () => {
        if (restarted && recovered === undefined) {
          recovered = readStore(dir)
          await recovered
        }
      },
    })
    const model = s1Oracle()

    const { store, window } = await crash({ simulator, dir, model, point: POINTS.firstStateRename })
    const published = TABLE_NAMES.flatMap((table) => store.tables[table].units.map((unit) => `${table}/${unit.name}`))

    expect(window).toMatchObject({ state: false, temp: { transfers: [], approvals: [] }, tempStates: 1, unknown: [] })
    expect(window.above.transfers).toHaveLength(1)
    expect(window.above.approvals).toHaveLength(1)

    restarted = true
    const outcome = await streamS1({ url: simulator.chain.url, dir, logger: testLogger(), observe: () => {} }).then(
      () => undefined,
      (error: unknown) => error,
    )

    const afterRecovery = await recovered
    const survivors = TABLE_NAMES.flatMap((table) =>
      (afterRecovery?.tables[table].units ?? []).map((unit) => `${table}/${unit.name}`),
    )
    const end = await readStore(dir)
    const endViolations = validateParquetSnapshot(end, RANGES)
    const endRows = rowsOf(end)
    const converged = outcome === undefined && endViolations.length === 0 && isDeepStrictEqual(endRows, S1_ROWS)

    // CN-12 and INV-42 want no unit left once recovery is done: with no cursor, every unit is above it.
    // GAP-41 skips the sweep when no state exists, so the units survive and the restart collides
    // with them (E2309) or duplicates them, depending on batch partitioning. Flip both lines when it closes.
    expect(survivors).toEqual(published)
    expect(converged).toBe(false)
  })
})
