import { isDeepStrictEqual } from 'node:util'

import { describe, expect } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { validateParquetSnapshot } from '../oracle/validators.js'
import { RANGES, S1, S1_ROWS, s1Oracle } from '../scenarios/backfill.js'
import { TABLE_NAMES } from '../scenarios/erc20.js'
import {
  ANY_TEMP_UNIT,
  ANY_UNIT,
  type ParquetStore,
  STATE_FILE,
  TEMP_STATE,
  crashAt,
  cursorOf,
  it,
  readParquetStore,
  resumeToEnd,
  tempUnitOf,
  unitOf,
} from './harness.js'
import type { KillPoint } from './kill-switch.js'
import { streamParquet } from './pipe.js'
import { rowsOf } from './probe.js'

/** 'PAR1': what a Parquet writer puts in a file before any row. */
const HEADER = 4

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

/**
 * S1 into Parquet, killed by SIGKILL at each step of the commit protocol and of recovery (CT-2).
 * Every test runs the same two checks:
 *
 * - `crashAt` fails unless the crash left a commit point intact (CN-40). It returns the crash
 *   window: what lies beside the commit point. Where the window lies proves the kill landed on the
 *   step the test names;
 * - `resumeToEnd` fails unless the restart starts right after the recovered cursor and ends in S1's
 *   end state (INV-40…INV-42).
 */
describe('Parquet (class K) · S1 backfill · killed at each commit step (CT-2)', () => {
  it('mid-unit-write: the cut unit is a temp file, and recovery deletes it', async ({ simulateS1, dir }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.midUnitWrite })

    expect(window).toMatchObject({ state: true, above: { transfers: [], approvals: [] }, tempStates: 0, unknown: [] })
    expect(window.temp.transfers).toEqual([HEADER])
    // Whether the batch opened an approvals unit is free; one it opened is cut the same way.
    expect([[], [HEADER]]).toContainEqual(window.temp.approvals)

    await resumeToEnd({ simulator, dir, model, store })
  })

  it('mid-unit-write before the first commit: no state and no unit, and the restart starts cold', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.firstUnitWrite })

    expect(window).toMatchObject({ state: false, above: { transfers: [], approvals: [] }, tempStates: 0, unknown: [] })
    expect(window.temp.transfers).toEqual([HEADER])
    expect([[], [HEADER]]).toContainEqual(window.temp.approvals)

    await resumeToEnd({ simulator, dir, model, store })
  })

  it('post-publish, before the state write: the units above the cursor are deleted (CN-12)', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.beforeStateWrite })

    expect(window).toMatchObject({ state: true, temp: { transfers: [], approvals: [] }, tempStates: 0, unknown: [] })
    expect(window.above.transfers).toHaveLength(1)
    // Whether the checkpoint holds an approval is free.
    expect(window.above.approvals.length).toBeLessThanOrEqual(1)
    expect(window.aboveEnds).toHaveLength(1)

    await resumeToEnd({ simulator, dir, model, store })
  })

  it('post-publish, before the state rename: the temp state and the units above the cursor are deleted', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.beforeStateRename })

    expect(window).toMatchObject({ state: true, temp: { transfers: [], approvals: [] }, tempStates: 1, unknown: [] })
    expect(window.above.transfers).toHaveLength(1)
    expect(window.above.approvals.length).toBeLessThanOrEqual(1)
    expect(window.aboveEnds).toHaveLength(1)

    await resumeToEnd({ simulator, dir, model, store })
  })

  it('mid-rename: one table published above the cursor, the other finished but unpublished; both undone', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.midRename })
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

    await resumeToEnd({ simulator, dir, model, store })
  })

  it('post-publish of a stretched unit: the unit straddling the cursor is the checkpoint’s own, and is deleted (CN-12)', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.stretchedUnit })
    const cursor = cursorOf(store)
    const approvals = store.tables.approvals
    const [stretched] = approvals.units.filter((unit) => unit.to > cursor)

    expect(window).toMatchObject({ state: true, temp: { transfers: [], approvals: [] }, tempStates: 0, unknown: [] })
    expect(window.above.transfers).toHaveLength(1)
    expect(window.above.approvals).toHaveLength(1)
    // It starts where the state says approvals were next due to publish, at or below the cursor.
    expect(stretched.from).toBe(approvals.next)
    expect(stretched.from).toBeLessThanOrEqual(cursor)

    await resumeToEnd({ simulator, dir, model, store })
  })

  it('post-state: nothing to repair, and the restart resumes right after the new cursor', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.postState })
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

    await resumeToEnd({ simulator, dir, model, store })
  })

  it('a crash in recovery, before the over-cursor sweep: recovery runs again and converges (INV-41)', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const first = await crashAt({ simulator, dir, model, point: POINTS.beforeStateRename })
    const second = await crashAt({ simulator, dir, model, point: POINTS.recoverySweep })

    // The temp sweep ran before the kill; the over-cursor sweep did not.
    expect(second.store.state).toStrictEqual(first.store.state)
    expect(second.window).toEqual({ ...first.window, tempStates: 0 })
    expect(first.window.tempStates).toBe(1)

    await resumeToEnd({ simulator, dir, model, store: second.store })
  })

  it('a crash in recovery, before the temp sweep: recovery runs again and converges (INV-41)', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const first = await crashAt({ simulator, dir, model, point: POINTS.midUnitWrite })
    const second = await crashAt({ simulator, dir, model, point: POINTS.recoveryTempSweep })

    expect(second.store.state).toStrictEqual(first.store.state)
    expect(second.window).toEqual(first.window)
    expect(first.window.temp.transfers).toEqual([HEADER])

    await resumeToEnd({ simulator, dir, model, store: second.store })
  })

  it("a second crash in the restart's first checkpoint: the residue does not pile up (CN-42)", async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const first = await crashAt({ simulator, dir, model, point: POINTS.beforeStateRename })
    const second = await crashAt({ simulator, dir, model, point: POINTS.firstStateRename })

    // The restart swept the first crash's window and published one checkpoint of its own above the same cursor.
    expect(second.store.state).toStrictEqual(first.store.state)
    expect(second.window).toMatchObject({ temp: { transfers: [], approvals: [] }, tempStates: 1, unknown: [] })
    expect(second.window.above.transfers).toHaveLength(1)
    expect(second.window.above.approvals.length).toBeLessThanOrEqual(1)
    expect(second.window.aboveEnds).toHaveLength(1)

    await resumeToEnd({ simulator, dir, model, store: second.store })
  })

  it('GAP-41 (open): recovery keeps the units published before the first state write, and the restart does not converge', async ({
    simulateS1,
    dir,
  }) => {
    // The restart's first request waits for a read of the store, so the read sees what recovery left.
    let restarted = false
    let recovered: Promise<ParquetStore> | undefined
    const simulator = await simulateS1({
      delay: async () => {
        if (restarted && recovered === undefined) {
          recovered = readParquetStore(dir)
          await recovered
        }
      },
    })
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.firstStateRename })
    const published = TABLE_NAMES.flatMap((table) => store.tables[table].units.map((unit) => `${table}/${unit.name}`))

    expect(window).toMatchObject({ state: false, temp: { transfers: [], approvals: [] }, tempStates: 1, unknown: [] })
    expect(window.above.transfers).toHaveLength(1)
    expect(window.above.approvals).toHaveLength(1)

    restarted = true
    const outcome = await streamParquet({
      range: { from: S1.from, to: S1.to },
      url: simulator.chain.url,
      dir,
      logger: testLogger(),
      observe: () => {},
    }).then(
      () => undefined,
      (error: unknown) => error,
    )

    const afterRecovery = await recovered
    const survivors = TABLE_NAMES.flatMap((table) =>
      (afterRecovery?.tables[table].units ?? []).map((unit) => `${table}/${unit.name}`),
    )
    const end = await readParquetStore(dir)
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
