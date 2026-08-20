import { describe, expect, it } from 'vitest'

import { FieldSelection } from '~/portal-client/query/evm.js'

import { augmentFields, dropEmptyBlocks, selectionGrew } from './project.js'

/**
 * `augmentFields` adds the fields a request's where-clauses must read to be *evaluated* client-side
 * (the RPC source filters full blocks locally), even when those fields aren't selected for output;
 * `selectionGrew` reports whether that augmentation added anything (so the caller can project back
 * down to the user's original selection). Mirrors the Squid evm-rpc-stream augment tests.
 */

describe('augmentFields', () => {
  it('adds a where-only field (log address) not in the original selection', () => {
    const fields: FieldSelection = { log: { topics: true } }
    const augmented = augmentFields(fields, { logs: [{ address: ['0xaaa'] }] })
    expect((augmented.log as any).address).toBe(true)
    expect((augmented.log as any).topics).toBe(true) // original selection preserved
  })

  it('adds log.topics when a topic filter is present', () => {
    const augmented = augmentFields({}, { logs: [{ topic0: ['0xt0'] }] })
    expect((augmented.log as any).topics).toBe(true)
  })

  it('adds transaction where fields (to / from / sighash / type)', () => {
    const augmented = augmentFields({}, { transactions: [{ to: ['0xa'], from: ['0xb'], sighash: ['0xc'], type: [2] }] })
    expect(augmented.transaction).toMatchObject({ to: true, from: true, sighash: true, type: true })
  })

  it('maps trace where-keys to trace selection keys', () => {
    const augmented = augmentFields({}, { traces: [{ callTo: ['0xa'], rewardAuthor: ['0xb'] }] })
    expect(augmented.trace).toMatchObject({ callTo: true, rewardAuthor: true })
  })

  it('does not add a field that is already selected', () => {
    const fields: FieldSelection = { log: { address: true } }
    const augmented = augmentFields(fields, { logs: [{ address: ['0xaaa'] }] })
    expect(selectionGrew(augmented, fields)).toBe(false)
  })

  it('does not grow when no where-clause references an unselected field', () => {
    const fields: FieldSelection = { log: { data: true } }
    const augmented = augmentFields(fields, { logs: [{}] }) // empty where — nothing to add
    expect(selectionGrew(augmented, fields)).toBe(false)
  })
})

describe('selectionGrew', () => {
  it('is true when augmentation added a field', () => {
    const fields: FieldSelection = { log: { topics: true } }
    const augmented = augmentFields(fields, { logs: [{ address: ['0xaaa'] }] })
    expect(selectionGrew(augmented, fields)).toBe(true)
  })
})

describe('dropEmptyBlocks', () => {
  const blk = (
    number: number,
    parts: { logs?: unknown[]; transactions?: unknown[]; traces?: unknown[]; stateDiffs?: unknown[] } = {},
  ) => ({
    header: { number },
    logs: parts.logs ?? [],
    transactions: parts.transactions ?? [],
    traces: parts.traces ?? [],
    stateDiffs: parts.stateDiffs ?? [],
  })

  it('drops empty interior blocks but keeps the boundaries', () => {
    const blocks = [blk(1), blk(2, { logs: [{}] }), blk(3), blk(4)]
    // 1 kept (first), 2 kept (has a log), 3 dropped (empty interior), 4 kept (last)
    expect(dropEmptyBlocks(blocks).map((b) => b.header.number)).toEqual([1, 2, 4])
  })

  it('keeps every block when includeAllBlocks is set', () => {
    const blocks = [blk(1), blk(2), blk(3)]
    expect(dropEmptyBlocks(blocks, true).map((b) => b.header.number)).toEqual([1, 2, 3])
  })

  it('keeps an interior block that has any data (tx / trace / stateDiff)', () => {
    const blocks = [
      blk(1),
      blk(2, { transactions: [{}] }),
      blk(3, { traces: [{}] }),
      blk(4, { stateDiffs: [{}] }),
      blk(5),
    ]
    expect(dropEmptyBlocks(blocks).map((b) => b.header.number)).toEqual([1, 2, 3, 4, 5])
  })

  it('keeps a lone empty block (it is both first and last — the progress cursor)', () => {
    expect(dropEmptyBlocks([blk(7)]).map((b) => b.header.number)).toEqual([7])
  })
})
