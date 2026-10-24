import { isDeepStrictEqual } from 'node:util'

import { describe, expect } from 'vitest'

import type { OracleRow } from '../oracle/reference-model.js'
import { validateClickhouseSnapshot } from '../oracle/validators.js'
import { TABLE_NAMES } from '../scenarios/erc20.js'
import { S3, S3_RANGES, S3_ROWS, s3EndState } from '../scenarios/fork.js'
import {
  type ClickhouseStore,
  S3_CLICKHOUSE,
  crashAt,
  cursorOf,
  it,
  readClickhouseStore,
  resumeToEnd,
  rowsBetween,
  runKilled,
  runUnwatched,
} from './harness.js'
import type { RequestKillPoint } from './kill-switch.js'
import { rowsOfClickhouse } from './probe.js'

/**
 * The steps of S3 that S1 never reaches: a batch above the floor, and the fork. A batch is a portal
 * answer: the one from 21 to 27 lies above the floor at 19. The fork comes once the pipe has reached
 * the head: the target reads its sync rows for the ancestor, then the repair hook cancels the rows
 * above it, both tables at once, each after a netting read. Nothing is written to the sync table
 * (GAP-21).
 */
const POINTS = {
  aboveFloor: {
    name: 'inside a batch above the floor, after its data, before its cursor write',
    request: '^insert sync$',
    inBatch: 27,
    at: 1,
    when: 'before',
  },
  forkBeforeCancel: {
    name: 'inside the fork, before any cancel row',
    request: '^cancel ',
    inFork: true,
    at: 1,
    when: 'before',
  },
  forkMidRollback: {
    name: 'inside the fork, after the cancel rows of one table',
    request: '^cancel ',
    inFork: true,
    at: 1,
    when: 'after',
  },
  forkAfterRollback: {
    name: 'after the fork rolled both tables back, before the next commit',
    request: '^cancel ',
    inFork: true,
    at: 2,
    when: 'after',
  },
  // The first answer after the fork runs from the ancestor's child.
  afterForkBatch: {
    name: 'the first batch after the fork, after its data, before its cursor write',
    request: '^insert sync$',
    inBatch: S3.ancestor + S3.blocksPerResponse,
    at: 1,
    when: 'before',
  },
} satisfies Record<string, RequestKillPoint>

const scenario = S3_CLICKHOUSE

const ofTable = (rows: OracleRow[], table: string) => rows.filter((row) => row.table === table)

/**
 * Where the store's rows of each table stand against the forked branch its cursor is on: all of
 * the branch, rolled back to the ancestor, or neither.
 */
function standingOf(store: ClickhouseStore): string[] {
  const branch = scenario.rowsUpTo(cursorOf(store))
  const rows = rowsOfClickhouse(store)

  return TABLE_NAMES.map((table) => {
    const held = ofTable(rows, table)
    const forked = ofTable(branch, table)
    const rolledBack = forked.filter((row) => row.block <= S3.ancestor)

    if (isDeepStrictEqual(held, rolledBack)) {
      return 'rolled back'
    }

    if (isDeepStrictEqual(held, forked)) {
      return 'forked'
    }

    return 'neither'
  })
}

/**
 * S3 into ClickHouse, killed by SIGKILL in a batch above the floor and in the fork (CT-2). Every
 * test runs the same two checks as on S1: `crashAt` fails unless the crash left a commit point
 * intact (CN-40), and `resumeToEnd` fails unless the restart ends in S3's end state.
 */
describe('ClickHouse (class A) · S3 one fork · killed at each commit step (CT-2)', () => {
  it('inside a batch above the floor: its rows are above the cursor, and recovery removes them', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.aboveFloor,
    })
    const [before, batch] = batches.slice(-2)

    expect(killed.request).toBe('insert sync')
    expect(batch.current.number).toBe(27)
    expect(cursorOf(store)).toStrictEqual(before.current)
    expect(window).toEqual(rowsBetween(scenario, before.current, batch.current))

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('inside the fork, before any cancel row: the store keeps the forked branch, and the restart forks again', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks, killed, window } = await crashAt({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.forkBeforeCancel,
    })

    // The ancestor search reads the sync rows; each table's cancel follows its own netting read.
    expect(killed.sent[0]).toBe('select sync')
    expect(killed.sent).toContain(killed.request.replace('cancel', 'net'))
    expect(forks).toHaveLength(1)
    expect(cursorOf(store)).toStrictEqual(batches.at(-1)?.current)
    expect(window).toEqual([])

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('a second crash inside the fork: the restart forks again, and converges (WP-46)', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const first = await crashAt({ scenario, simulator, database, model, point: POINTS.forkBeforeCancel })
    const second = await crashAt({ scenario, simulator, database, model, point: POINTS.forkBeforeCancel })

    expect(second.forks).toHaveLength(1)
    expect(second.batches).toEqual([])
    expect(second.store).toStrictEqual(first.store)

    await resumeToEnd({ scenario, simulator, database, model, store: second.store })
  })

  it('GAP-21 (open): inside the fork rollback, the cursor stays on the forked branch above a table rolled back', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    // CN-34 wants the ancestor persisted. The binding persists nothing, so the store reads as the
    // forked branch's head over rows partly rolled back: a state no commit wrote.
    const crash = crashAt({ scenario, simulator, database, model, point: POINTS.forkMidRollback })
    await expect(crash).rejects.toThrow('the committed rows')

    const store = await readClickhouseStore(database)
    const standing = standingOf(store)

    // Whether the other table's cancel went out before the kill is free.
    expect(cursorOf(store)?.number).toBe(S3.latest)
    expect(standing).toContain('rolled back')
    expect(standing).not.toContain('neither')
    // A fix that persists the ancestor once the rollback ends leaves this point as it is: the
    // ancestor must be persisted before the hook removes rows, so a crash leaves only the class's
    // own window above the cursor (CN-13).

    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('GAP-21 (open): after the fork, the cursor stays on the forked branch above rows rolled back to the ancestor', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const crash = crashAt({ scenario, simulator, database, model, point: POINTS.forkAfterRollback })
    await expect(crash).rejects.toThrow('the committed rows')

    const store = await readClickhouseStore(database)

    // Flip to a passing `crashAt` when GAP-21 closes: the cursor at the ancestor, over its rows.
    expect(cursorOf(store)?.number).toBe(S3.latest)
    expect(standingOf(store)).toEqual(['rolled back', 'rolled back'])

    // A restart still ends in S3's end state: its first request meets the fork again.
    await resumeToEnd({ scenario, simulator, database, model, store })
  })

  it('GAP-21 (open): in the first batch after the fork, rows of the new branch sit under the forked cursor', async ({
    simulateS3,
    database,
  }) => {
    const simulator = await simulateS3()
    const model = scenario.oracle()

    const { store, batches, forks } = await runKilled({
      scenario,
      simulator,
      database,
      model,
      point: POINTS.afterForkBatch,
    })
    const [batch] = batches.slice(-1)

    expect(forks).toHaveLength(1)
    expect(batch.current.number).toBe(POINTS.afterForkBatch.inBatch)
    expect(cursorOf(store)?.number).toBe(S3.latest)
    expect(rowsOfClickhouse(store)).toEqual(rowsBetween(scenario, undefined, batch.current))
    // The oracle's last two commits are the fork and the batch; the store recovers to neither.
    await expect(resumeToEnd({ scenario, simulator, database, model, store })).rejects.toThrow('CN-40')

    // A restart still ends in S3's end state: recovery removes the rows above the stale cursor, and
    // the first request meets the fork again.
    const end = await runUnwatched({ scenario, simulator, database })

    expect(validateClickhouseSnapshot(end, S3_RANGES)).toEqual([])
    expect(end.state).toStrictEqual(s3EndState())
    expect(rowsOfClickhouse(end)).toEqual(S3_ROWS)
  })
})
