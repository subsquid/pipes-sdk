import { PubSub } from '@google-cloud/pubsub'
import Database from 'better-sqlite3'
import { describe, expect } from 'vitest'

import { it as harness } from './harness.js'
import {
  type KillEvidence,
  PUBLISH,
  type StepKillPoint,
  StepKillSwitch,
  armStepKillSwitch,
  labelOf,
} from './kill-switch.js'
import { TOPIC } from './pipe.js'

/** The answers of `killSwitch.step` to `labels`, in order. */
function answers(killSwitch: StepKillSwitch, labels: string[]) {
  return labels.map((label) => killSwitch.step(label))
}

/** Arms the switch with a kill that throws instead of dying, and disarms it when the test ends. */
const it = harness.extend<{ arm: (point: StepKillPoint) => { kills: KillEvidence[] } }>({
  arm: async ({}, use) => {
    const disarms: (() => void)[] = []

    await use((point) => {
      const kills: KillEvidence[] = []
      const { disarm } = armStepKillSwitch({
        point,
        kill: (evidence) => {
          kills.push(evidence)
          throw new Error(`killed at '${point.name}'`)
        },
      })
      disarms.push(disarm)

      return { kills }
    })

    for (const disarm of disarms.reverse()) {
      disarm()
    }
  },
})

describe('labelOf', () => {
  it.for([
    ['BEGIN IMMEDIATE', 'begin'],
    ['COMMIT', 'commit'],
    [
      'INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value',
      'insert meta',
    ],
    ['\n      INSERT INTO outbox (route, topic) VALUES (?, ?)', 'insert outbox'],
    ['INSERT OR IGNORE INTO materialized_identity (id) VALUES (?)', 'insert materialized_identity'],
    ['DELETE FROM outbox WHERE row_id = ?', 'delete outbox'],
    ['PRAGMA synchronous = FULL;', 'pragma'],
    ['CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)', 'create'],
  ])('names %j as %j', ([sql, label]) => {
    expect(labelOf(sql)).toBe(label)
  })
})

describe('StepKillSwitch', () => {
  it('lands on the n-th matching step and on no other', () => {
    const point: StepKillPoint = { name: 'second publish', step: PUBLISH, at: 2, when: 'after' }

    const landed = answers(new StepKillSwitch(point), ['begin', PUBLISH, 'commit', PUBLISH, PUBLISH])

    expect(landed).toEqual([undefined, undefined, undefined, 'after', undefined])
  })

  it('counts only the steps of the batch it names, until the next batch or fork', () => {
    const killSwitch = new StepKillSwitch({ name: 'in 13', step: PUBLISH, inBatch: 13, at: 2, when: 'before' })

    killSwitch.batch(6)
    const inSix = answers(killSwitch, [PUBLISH, PUBLISH])
    killSwitch.batch(13)
    const inThirteen = answers(killSwitch, [PUBLISH])
    killSwitch.fork()
    const inFork = answers(killSwitch, [PUBLISH])
    killSwitch.batch(13)
    const again = answers(killSwitch, [PUBLISH])

    expect([inSix, inThirteen, inFork, again]).toEqual([[undefined, undefined], [undefined], [undefined], ['before']])
  })

  it('counts only the steps of a fork, when it names one', () => {
    const killSwitch = new StepKillSwitch({ name: 'fork', step: '^commit$', inFork: true, at: 1, when: 'after' })

    killSwitch.batch(6)
    const inBatch = answers(killSwitch, ['begin', 'commit'])
    killSwitch.fork()
    const inFork = answers(killSwitch, ['begin', 'commit'])

    expect([inBatch, inFork]).toEqual([
      [undefined, undefined],
      [undefined, 'after'],
    ])
  })

  it('names a transaction by what it ran before the step, and forgets it at its end', () => {
    const point: StepKillPoint = {
      name: 'confirmation',
      step: '^commit$',
      inTransaction: '^delete outbox$',
      at: 1,
      when: 'after',
    }

    const landed = answers(new StepKillSwitch(point), [
      'begin',
      'insert cursor',
      'commit',
      'delete outbox',
      'commit',
      'begin',
      'delete outbox',
      'commit',
    ])

    expect(landed).toEqual([undefined, undefined, undefined, undefined, undefined, undefined, undefined, 'after'])
  })

  it('shows what the run did since its last batch: without the step when the kill comes first', () => {
    const kill = (when: StepKillPoint['when']) => {
      const killSwitch = new StepKillSwitch({ name: 'publish', step: PUBLISH, at: 1, when })
      killSwitch.batch(6)
      answers(killSwitch, ['begin', 'commit', PUBLISH])

      return killSwitch.evidence
    }

    expect(kill('before')).toEqual({ step: PUBLISH, sent: ['begin', 'commit'] })
    expect(kill('after')).toEqual({ step: PUBLISH, sent: ['begin', 'commit', PUBLISH] })
  })
})

describe('armStepKillSwitch', () => {
  it.for(['before', 'after'] as const)('lands %s a statement: the statement runs only after', (when, { arm }) => {
    const { kills } = arm({ name: 'insert', step: '^insert t$', at: 1, when })
    const db = new Database(':memory:')
    db.prepare('CREATE TABLE t (n INTEGER)').run()

    expect(() => db.prepare('INSERT INTO t VALUES (1)').run()).toThrow("killed at 'insert'")
    const stored = db.prepare('SELECT COUNT(*) AS n FROM t').get()
    db.close()
    const sent = when === 'before' ? ['create'] : ['create', 'insert t']

    expect(kills).toEqual([{ step: 'insert t', sent }])
    expect(stored).toEqual({ n: when === 'before' ? 0 : 1 })
  })

  it.for([
    'before',
    'after',
  ] as const)('lands %s a publish: the bus takes the message only after, and the producer never reads its answer', async (when, {
    arm,
    pubsub,
  }) => {
    const { kills } = arm({ name: 'publish', step: PUBLISH, at: 1, when })
    const client = new PubSub({ projectId: pubsub.bus.projectId, apiEndpoint: pubsub.bus.endpoint })

    const publish = client.topic(TOPIC).publishMessage({ data: Buffer.from('m') })
    await expect(publish).rejects.toThrow("killed at 'publish'")
    await client.close()
    const taken = pubsub.bus.messages.map((message) => message.data.toString())

    expect(kills).toEqual([{ step: PUBLISH, sent: when === 'before' ? [] : [PUBLISH] }])
    expect(taken).toEqual(when === 'before' ? [] : ['m'])
  })
})
