import { describe, expect } from 'vitest'

import { S1, s1Oracle } from '../scenarios/backfill.js'
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

/** 'PAR1': what a Parquet writer puts in a file before any row. */
const HEADER = 4

/**
 * The class-K commit protocol (CN-12) step by step. A run with no commit first writes a start state
 * (IB-22). A checkpoint publishes each table's unit (transfers first), then writes the state to a
 * temp file and renames it over the state record. Block 1 holds a transfer and an approval, so the
 * first checkpoint publishes both tables. Counts are within one run, so a restart counts from 1
 * again, and a restart that resumes from a commit writes no start state.
 */
const POINTS = {
  midUnitWrite: { name: 'mid-unit-write', op: 'write', path: tempUnitOf('transfers'), at: 2 },
  firstUnitWrite: {
    name: 'mid-unit-write, before the first commit',
    op: 'write',
    path: tempUnitOf('transfers'),
    at: 1,
  },
  startStateRename: {
    name: 'before the start state rename',
    op: 'rename',
    path: STATE_FILE,
    at: 1,
    when: 'before',
  },
  beforeStateWrite: {
    name: 'post-publish, before the state write',
    op: 'writeFile',
    path: TEMP_STATE,
    at: 3,
    when: 'before',
  },
  beforeStateRename: {
    name: 'post-publish, before the state rename',
    op: 'rename',
    path: STATE_FILE,
    at: 3,
    when: 'before',
  },
  firstCommitRename: {
    name: 'post-publish, before the first commit’s state rename',
    op: 'rename',
    path: STATE_FILE,
    at: 2,
    when: 'before',
  },
  restartStateRename: {
    name: 'the first checkpoint of a restart from a commit, before its state rename',
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
  postState: { name: 'post-state', op: 'rename', path: STATE_FILE, at: 3, when: 'after' },
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

  it('before the start state rename: no state, the temp start state, and the restart starts cold', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.startStateRename })

    expect(window).toEqual({
      state: false,
      start: undefined,
      above: { transfers: [], approvals: [] },
      aboveEnds: [],
      temp: { transfers: [], approvals: [] },
      tempStates: 1,
      unknown: [],
    })

    await resumeToEnd({ simulator, dir, model, store })
  })

  it('mid-unit-write before the first commit: the start state and no unit, and the restart starts cold', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.firstUnitWrite })

    expect(window).toMatchObject({
      state: false,
      start: S1.from,
      above: { transfers: [], approvals: [] },
      tempStates: 0,
      unknown: [],
    })
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
      start: undefined,
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
    const second = await crashAt({ simulator, dir, model, point: POINTS.restartStateRename })

    // The restart swept the first crash's window and published one checkpoint of its own above the same cursor.
    expect(second.store.state).toStrictEqual(first.store.state)
    expect(second.window).toMatchObject({ temp: { transfers: [], approvals: [] }, tempStates: 1, unknown: [] })
    expect(second.window.above.transfers).toHaveLength(1)
    expect(second.window.above.approvals.length).toBeLessThanOrEqual(1)
    expect(second.window.aboveEnds).toHaveLength(1)

    await resumeToEnd({ simulator, dir, model, store: second.store })
  })

  it('post-publish, before the first commit’s state rename: recovery deletes the units, and the restart converges (CN-12)', async ({
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

    const { store, window } = await crashAt({ simulator, dir, model, point: POINTS.firstCommitRename })

    expect(window).toMatchObject({
      state: false,
      start: S1.from,
      temp: { transfers: [], approvals: [] },
      tempStates: 1,
      unknown: [],
    })
    expect(window.above.transfers).toHaveLength(1)
    expect(window.above.approvals).toHaveLength(1)

    restarted = true
    await resumeToEnd({ simulator, dir, model, store })

    // No commit, so every unit is above the cursor and recovery leaves none (INV-42).
    const afterRecovery = await recovered
    const survivors = TABLE_NAMES.flatMap((table) => afterRecovery?.tables[table].units ?? [])

    expect(afterRecovery).toBeDefined()
    expect(survivors).toEqual([])
  })

  it('a second crash in the first checkpoint after a start state: the residue does not pile up (CN-42)', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const first = await crashAt({ simulator, dir, model, point: POINTS.firstCommitRename })
    // The restart starts cold, so it writes a start state of its own before its first commit.
    const second = await crashAt({ simulator, dir, model, point: POINTS.firstCommitRename })

    expect(second.window).toEqual(first.window)

    await resumeToEnd({ simulator, dir, model, store: second.store })
  })

  it('a crash in recovery from a start state, before its sweep: recovery runs again and converges (INV-41)', async ({
    simulateS1,
    dir,
  }) => {
    const simulator = await simulateS1()
    const model = s1Oracle()

    const first = await crashAt({ simulator, dir, model, point: POINTS.firstCommitRename })
    const second = await crashAt({ simulator, dir, model, point: POINTS.recoverySweep })

    // The temp sweep ran before the kill; the sweep of the units did not.
    expect(second.window).toEqual({ ...first.window, tempStates: 0 })
    expect(first.window.tempStates).toBe(1)

    await resumeToEnd({ simulator, dir, model, store: second.store })
  })
})
