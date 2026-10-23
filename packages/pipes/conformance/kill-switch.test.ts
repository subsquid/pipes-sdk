import { describe, expect, it } from 'vitest'

import { type KillPoint, KillSwitch } from './kill-switch.js'

const STATE = String.raw`^_state\.json$`
const TRANSFERS_TEMP = String.raw`^transfers/\.tmp-\d+\.parquet$`
const APPROVALS_UNIT = String.raw`^approvals/\d{12}-\d{12}\.parquet$`

const noCommit = () => undefined

/** The answers of `killSwitch.call` to `calls`, in order. */
function answers(killSwitch: KillSwitch, calls: ['rename' | 'writeFile' | 'unlink', string][]) {
  return calls.map(([op, target]) => killSwitch.call(op, target))
}

describe('KillSwitch', () => {
  it('lands on the n-th matching call, and on no other', () => {
    const point: KillPoint = { name: 'second state rename', op: 'rename', path: STATE, at: 2, when: 'before' }
    const killSwitch = new KillSwitch(point, noCommit)

    const landed = answers(killSwitch, [
      ['rename', '_state.json'],
      ['rename', 'transfers/000000000000-000000000006.parquet'],
      ['unlink', '_state.json'],
      ['writeFile', '_state.json'],
      ['rename', '_state.json'],
      ['rename', '_state.json'],
    ])

    expect(landed).toEqual([undefined, undefined, undefined, undefined, 'before', undefined])
  })

  it('lands after the call when the point says so', () => {
    const point: KillPoint = { name: 'post-state', op: 'rename', path: STATE, at: 1, when: 'after' }

    const landed = new KillSwitch(point, noCommit).call('rename', '_state.json')

    expect(landed).toBe('after')
  })

  it('counts only units that start at or below the committed cursor when it wants a straddle', () => {
    const point: KillPoint = {
      name: 'stretched unit',
      op: 'rename',
      path: APPROVALS_UNIT,
      at: 2,
      when: 'after',
      straddling: true,
    }
    const killSwitch = new KillSwitch(point, () => 13)

    const landed = answers(killSwitch, [
      ['rename', 'approvals/000000000014-000000000020.parquet'],
      ['rename', 'approvals/000000000007-000000000020.parquet'],
      ['rename', 'approvals/000000000014-000000000027.parquet'],
      ['rename', 'approvals/000000000013-000000000027.parquet'],
    ])

    expect(landed).toEqual([undefined, undefined, undefined, 'after'])
  })

  it('finds no straddle while the store holds no cursor', () => {
    const point: KillPoint = {
      name: 'stretched',
      op: 'rename',
      path: APPROVALS_UNIT,
      at: 1,
      when: 'after',
      straddling: true,
    }

    const landed = new KillSwitch(point, noCommit).call('rename', 'approvals/000000000000-000000000006.parquet')

    expect(landed).toBeUndefined()
  })

  it('lands before the second write to the n-th matching file, after its header', () => {
    const point: KillPoint = { name: 'mid-unit-write', op: 'write', path: TRANSFERS_TEMP, at: 2 }
    const killSwitch = new KillSwitch(point, noCommit)

    killSwitch.opened(3, 'transfers/.tmp-000000.parquet')
    killSwitch.opened(4, 'approvals/.tmp-000001.parquet')
    killSwitch.opened(5, 'transfers/.tmp-000002.parquet')
    const writes = [3, 3, 4, 4, 5, 5, 5].map((fd) => killSwitch.wrote(fd))

    expect(writes).toEqual([false, false, false, false, false, true, false])
  })

  it('forgets a closed file, so a descriptor the system reuses starts over', () => {
    const point: KillPoint = { name: 'mid-unit-write', op: 'write', path: TRANSFERS_TEMP, at: 1 }
    const killSwitch = new KillSwitch(point, noCommit)

    killSwitch.opened(3, 'transfers/.tmp-000000.parquet')
    killSwitch.wrote(3)
    killSwitch.closed(3)
    killSwitch.opened(3, '_state.json')

    expect(killSwitch.wrote(3)).toBe(false)
  })

  it('watches no file and no call for an operation it is not set on', () => {
    const rename: KillPoint = { name: 'rename', op: 'rename', path: TRANSFERS_TEMP, at: 1, when: 'before' }
    const write: KillPoint = { name: 'write', op: 'write', path: TRANSFERS_TEMP, at: 1 }
    const renameSwitch = new KillSwitch(rename, noCommit)
    const writeSwitch = new KillSwitch(write, noCommit)

    renameSwitch.opened(3, 'transfers/.tmp-000000.parquet')
    const wrote = [renameSwitch.wrote(3), renameSwitch.wrote(3)]
    const called = writeSwitch.call('rename', 'transfers/.tmp-000000.parquet')

    expect(wrote).toEqual([false, false])
    expect(called).toBeUndefined()
  })
})
