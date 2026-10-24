import { setTimeout as sleep } from 'node:timers/promises'

import { NodeBaseConnection } from '@clickhouse/client/dist/connection/node_base_connection.js'
import { describe, expect } from 'vitest'

import { type Database, it, quiet } from './harness.js'
import {
  type KillEvidence,
  type RequestKillPoint,
  RequestKillSwitch,
  armRequestKillSwitch,
  labelOf,
} from './kill-switch.js'
import { clickhouseClient } from './pipe.js'

/** The answers of `killSwitch.request` to `labels`, in order. */
function answers(killSwitch: RequestKillSwitch, labels: string[]) {
  return labels.map((label) => killSwitch.request(label))
}

describe('labelOf', () => {
  it.for([
    ['INSERT INTO transfers FORMAT JSONEachRow', 'insert transfers'],
    ['INSERT INTO "ct_1"."sync" FORMAT JSONEachRow', 'insert sync'],
    ['INSERT INTO `ct_1`.`approvals` (a, b) FORMAT JSONEachRow', 'insert approvals'],
    ['\nCREATE TABLE IF NOT EXISTS "ct_1"."sync"\n(id String)', 'create sync'],
    ['ALTER TABLE transfers ADD INDEX i x TYPE minmax', 'alter transfers'],
    ['SELECT * FROM "ct_1"."sync" WHERE id = {id:String} ORDER BY timestamp DESC LIMIT 1', 'select sync'],
    ['\n  SELECT database, name FROM system.tables WHERE name = {name:String}', 'select tables'],
    ['SELECT `blockNumber`, `value`, sum(sign) AS _sqd_net\n FROM transfers WHERE x GROUP BY 1', 'net transfers'],
    ['SYSTEM STOP MERGES t', 'system'],
  ])('names %j as %j', ([query, label]) => {
    expect(labelOf(query)).toBe(label)
  })

  it('names an insert with deduplication off a cancel: a rollback sends its cancel rows so (IB-20)', () => {
    const query = 'INSERT INTO transfers FORMAT JSONEachRow'

    expect(labelOf(query, { insert_deduplicate: 0 })).toBe('cancel transfers')
    expect(labelOf(query, { insert_deduplicate: 1 })).toBe('insert transfers')
  })
})

describe('RequestKillSwitch', () => {
  it('lands on the n-th matching request, and on no other', () => {
    const point: RequestKillPoint = { name: 'second cursor write', request: '^insert sync$', at: 2, when: 'before' }

    const landed = answers(new RequestKillSwitch(point), [
      'insert sync',
      'insert transfers',
      'insert syncs',
      'insert sync',
      'insert sync',
    ])

    expect(landed).toEqual([undefined, undefined, undefined, 'before', undefined])
  })

  it.for(['before', 'sent', 'after'] as const)('lands %s the request when the point says so', (when) => {
    const point: RequestKillPoint = { name: 'cursor write', request: '^insert sync$', at: 1, when }

    const landed = new RequestKillSwitch(point).request('insert sync')

    expect(landed).toBe(when)
  })

  it('counts only requests between the batch the point names and the next batch or fork', () => {
    const point: RequestKillPoint = {
      name: 'batch at 13',
      request: '^insert sync$',
      inBatch: 13,
      at: 1,
      when: 'before',
    }
    const killSwitch = new RequestKillSwitch(point)

    const before = answers(killSwitch, ['insert sync'])
    killSwitch.batch(6)
    const atSix = answers(killSwitch, ['insert sync'])
    killSwitch.batch(13)
    killSwitch.fork()
    const inFork = answers(killSwitch, ['insert sync'])
    killSwitch.batch(13)
    const atThirteen = answers(killSwitch, ['insert transfers', 'insert sync'])

    expect([before, atSix, inFork, atThirteen]).toEqual([[undefined], [undefined], [undefined], [undefined, 'before']])
  })

  it('counts only requests between a fork and the next batch when the point names the fork', () => {
    const point: RequestKillPoint = { name: 'fork', request: '^cancel ', inFork: true, at: 1, when: 'before' }
    const killSwitch = new RequestKillSwitch(point)

    const atStart = answers(killSwitch, ['cancel transfers'])
    killSwitch.fork()
    killSwitch.batch(27)
    const afterFork = answers(killSwitch, ['cancel transfers'])
    killSwitch.fork()
    const inFork = answers(killSwitch, ['select sync', 'cancel approvals'])

    expect([atStart, afterFork, inFork]).toEqual([[undefined], [undefined], [undefined, 'before']])
  })

  it('shows what the run sent since its last batch or fork: without the request when the kill comes first', () => {
    const kill = (when: RequestKillPoint['when']) => {
      const killSwitch = new RequestKillSwitch({ name: 'cursor', request: '^insert sync$', at: 1, when })
      answers(killSwitch, ['select sync'])
      killSwitch.batch(6)
      answers(killSwitch, ['insert transfers', 'insert sync'])

      return killSwitch.evidence
    }
    const killInFork = () => {
      const killSwitch = new RequestKillSwitch({ name: 'cancel', request: '^cancel ', at: 1, when: 'before' })
      killSwitch.batch(29)
      answers(killSwitch, ['insert sync'])
      killSwitch.fork()
      answers(killSwitch, ['select sync', 'net transfers', 'cancel transfers'])

      return killSwitch.evidence
    }

    expect(kill('before')).toEqual({ request: 'insert sync', sent: ['insert transfers'] })
    expect(kill('sent')).toEqual({ request: 'insert sync', sent: ['insert transfers', 'insert sync'] })
    expect(kill('after')).toEqual({ request: 'insert sync', sent: ['insert transfers', 'insert sync'] })
    expect(killInFork()).toEqual({ request: 'cancel transfers', sent: ['select sync', 'net transfers'] })
  })
})

/** What a test's `kill` throws instead of dying. */
class Killed extends Error {
  constructor(readonly evidence: KillEvidence) {
    super('killed')
  }
}

describe('armRequestKillSwitch', () => {
  async function rowsIn({ admin, name }: Database): Promise<number> {
    const result = await admin.query({
      query: `SELECT count() AS rows FROM "${name}"."transfers"`,
      format: 'JSONEachRow',
    })
    const [{ rows }] = await result.json<{ rows: string }>()

    return Number(rows)
  }

  /**
   * Sends one insert with the switch armed at `when`. Reports how the insert failed, and how many
   * rows the table holds once the server runs none of the client's queries.
   */
  async function killAt(database: Database, when: RequestKillPoint['when']) {
    const application = `ct-arm-${when}`
    const client = clickhouseClient({ server: database.server, database: database.name, application })
    const { disarm } = armRequestKillSwitch({
      point: { name: 'insert', request: '^insert transfers$', at: 1, when },
      kill: (evidence) => {
        throw new Killed(evidence)
      },
    })

    let rejected: unknown
    try {
      await client.insert({
        table: 'transfers',
        values: [{ blockNumber: 1, value: 'a', sign: 1 }],
        format: 'JSONEachRow',
      })
    } catch (error) {
      rejected = error
    } finally {
      disarm()
      await client.close()
    }

    await quiet(database.admin, application)

    return { rejected, rows: await rowsIn(database) }
  }

  it('before: kills before the request goes out', async ({ database }) => {
    const { rejected, rows } = await killAt(database, 'before')

    expect((rejected as Killed).evidence).toEqual({ request: 'insert transfers', sent: [] })
    expect(rows).toBe(0)
  })

  it('sent: kills once the request is on the wire, so the server runs it with no one to answer', async ({
    database,
  }) => {
    const { rejected, rows } = await killAt(database, 'sent')

    expect((rejected as Killed).evidence).toEqual({ request: 'insert transfers', sent: ['insert transfers'] })
    expect(rows).toBe(1)
  })

  it('after: kills once the server has answered', async ({ database }) => {
    const { rejected, rows } = await killAt(database, 'after')

    expect((rejected as Killed).evidence).toEqual({ request: 'insert transfers', sent: ['insert transfers'] })
    expect(rows).toBe(1)
  })

  it('after: waits for the whole call, not only for the headers of its answer', async () => {
    // A stand-in for the connection's insert, which reads the rest of the answer after its headers.
    const base = NodeBaseConnection.prototype as unknown as { insert: (params: object) => Promise<unknown> }
    const real = base.insert
    let ended = false
    let endedAtKill: boolean | undefined
    base.insert = async () => {
      await sleep(20)
      ended = true

      return {}
    }

    const { disarm } = armRequestKillSwitch({
      point: { name: 'insert', request: '^insert transfers$', at: 1, when: 'after' },
      kill: (evidence) => {
        endedAtKill = ended
        throw new Killed(evidence)
      },
    })

    try {
      const call = base.insert.call({}, { query: 'INSERT INTO transfers FORMAT JSONEachRow' })

      await expect(call).rejects.toBeInstanceOf(Killed)
    } finally {
      disarm()
      base.insert = real
    }

    expect(endedAtKill).toBe(true)
  })

  it('after: refuses a read, whose rows its caller reads later', async ({ database }) => {
    const client = clickhouseClient({ server: database.server, database: database.name, application: 'ct-arm-read' })
    const { disarm } = armRequestKillSwitch({
      point: { name: 'read', request: '^select$', at: 1, when: 'after' },
      kill: (evidence) => {
        throw new Killed(evidence)
      },
    })

    try {
      const read = client.query({ query: 'SELECT 1' })

      await expect(read).rejects.toThrow("the kill point 'read' waits for a read, whose rows its caller reads later")
    } finally {
      disarm()
      await client.close()
    }
  })

  it('lets every request through once disarmed', async ({ database }) => {
    const { disarm } = armRequestKillSwitch({
      point: { name: 'insert', request: '^insert transfers$', at: 1, when: 'before' },
      kill: (evidence) => {
        throw new Killed(evidence)
      },
    })
    disarm()

    const client = clickhouseClient({ server: database.server, database: database.name, application: 'ct-disarmed' })
    await client.insert({
      table: 'transfers',
      values: [{ blockNumber: 1, value: 'a', sign: 1 }],
      format: 'JSONEachRow',
    })
    await client.close()

    expect(await rowsIn(database)).toBe(1)
  })
})
