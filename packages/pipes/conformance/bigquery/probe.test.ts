import { describe, expect, it } from 'vitest'

import { readBigquerySnapshot } from './probe.js'

const PIPE_ID = 'probe'

const block = (number: number, hash = `0x${number}`) => ({ number, hash })

/** A run of consecutive blocks, the shape every batch's fragment above the floor takes. */
const blocks = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, i) => block(from + i))

/**
 * A WAL row as `SELECT *` reads it back; `current`, `finalized` and `rollback_chain` are JSON. A
 * row's chain names only its own batch's blocks above the floor — the store keeps fragments, and
 * the fork search unions them when it runs.
 */
function walRow(
  row: Partial<{
    op: 'commit' | 'rollback'
    current: unknown
    finalized: unknown
    chain: unknown[]
    low: number
    high: number
    committed: boolean
  }>,
) {
  const json = (value: unknown) => (value === undefined ? null : JSON.stringify(value))

  return {
    id: PIPE_ID,
    op: row.op ?? 'commit',
    current: json(row.current),
    finalized: json(row.finalized),
    rollback_chain: JSON.stringify(row.chain ?? []),
    range_low: row.low ?? null,
    range_high: row.high ?? null,
    committed: row.committed ?? true,
    timestamp: 1,
  }
}

/**
 * The BigQuery client as the probe uses it: every query names one table in its FROM, and each
 * table answers from the rows it holds.
 */
function clientWith(
  sync: ReturnType<typeof walRow>[],
  tables: Record<string, { blockNumber: number | string; value: string }[]>,
) {
  return {
    query: async ({ query }: { query: string }) => {
      const from = /from `\w+\.(\w+)`/i.exec(query)?.[1]
      if (from === 'sync') return [sync]
      if (from && tables[from]) return [tables[from]]

      throw new Error(`Not found: Table ${from}`)
    },
  } as unknown as Parameters<typeof readBigquerySnapshot>[0]['client']
}

describe('readBigquerySnapshot — the recovery state', () => {
  it('a quiescent state after a fork holds the union of the committed rows’ fragments, trimmed to its floor', async () => {
    const client = clientWith(
      [
        walRow({ current: block(20), finalized: block(19), chain: [block(20)] }),
        walRow({ current: block(27), finalized: block(19), chain: blocks(21, 27) }),
        walRow({ current: block(29), finalized: block(23), chain: [block(28), block(29)] }),
        walRow({ op: 'rollback', current: block(26), finalized: block(23), chain: [] }),
        walRow({ current: block(33), finalized: block(23), chain: blocks(27, 33) }),
        walRow({ current: block(39), finalized: block(23), chain: blocks(34, 39) }),
      ],
      {},
    )

    const snapshot = await readBigquerySnapshot({ client, dataset: 'd', id: PIPE_ID, tables: [] })

    expect(snapshot.inFlight).toBeUndefined()
    expect(snapshot.sync).toHaveLength(6)
    expect(snapshot.state).toEqual({
      current: block(39),
      finalized: block(23),
      rollbackChain: blocks(24, 39),
    })
  })

  it('a write in flight recovers the pre-batch state: its own fragment — the blocks it was about to write — is not in the chain', async () => {
    const client = clientWith(
      [
        walRow({ current: block(20), finalized: block(19), chain: [block(20)] }),
        walRow({
          current: block(20),
          finalized: block(19),
          chain: blocks(21, 27),
          low: 21,
          high: 27,
          committed: false,
        }),
      ],
      {},
    )

    const snapshot = await readBigquerySnapshot({ client, dataset: 'd', id: PIPE_ID, tables: [] })

    expect(snapshot.inFlight).toEqual({ op: 'commit', current: block(20), range: { low: 21, high: 27 } })
    expect(snapshot.state).toEqual({ current: block(20), finalized: block(19), rollbackChain: [block(20)] })
  })

  it('a rollback in flight recovers the ancestor state: the pre-fork fragments trimmed to the ancestor', async () => {
    const client = clientWith(
      [
        walRow({ current: block(27), finalized: block(19), chain: blocks(21, 27) }),
        walRow({ current: block(29), finalized: block(23), chain: [block(28), block(29)] }),
        // The pre-rollback row names the ancestor; its own chain is empty by design.
        walRow({
          op: 'rollback',
          current: block(26),
          finalized: block(23),
          chain: [],
          low: 27,
          high: 29,
          committed: false,
        }),
      ],
      {},
    )

    const snapshot = await readBigquerySnapshot({ client, dataset: 'd', id: PIPE_ID, tables: [] })

    expect(snapshot.inFlight).toEqual({ op: 'rollback', current: block(26), range: { low: 27, high: 29 } })
    expect(snapshot.state).toEqual({
      current: block(26),
      finalized: block(23),
      rollbackChain: blocks(24, 26),
    })
  })

  it('a recovery’s committed marker row clears the in-flight row it answers', async () => {
    const client = clientWith(
      [
        walRow({ current: block(27), finalized: block(19), chain: blocks(21, 27) }),
        walRow({ current: block(29), finalized: block(23), chain: [block(28), block(29)] }),
        walRow({
          op: 'rollback',
          current: block(26),
          finalized: block(23),
          chain: [],
          low: 27,
          high: 29,
          committed: false,
        }),
        walRow({ op: 'rollback', current: block(26), finalized: block(23), chain: [] }),
      ],
      {},
    )

    const snapshot = await readBigquerySnapshot({ client, dataset: 'd', id: PIPE_ID, tables: [] })

    expect(snapshot.inFlight).toBeUndefined()
    expect(snapshot.state).toEqual({
      current: block(26),
      finalized: block(23),
      rollbackChain: blocks(24, 26),
    })
  })

  it('a completed rollback, then a write in flight: the chain stands at the ancestor until the new batch commits', async () => {
    const client = clientWith(
      [
        walRow({ current: block(27), finalized: block(19), chain: blocks(21, 27) }),
        walRow({ current: block(29), finalized: block(23), chain: [block(28), block(29)] }),
        walRow({ op: 'rollback', current: block(26), finalized: block(23), chain: [] }),
        walRow({
          current: block(26),
          finalized: block(23),
          chain: blocks(27, 33),
          low: 27,
          high: 33,
          committed: false,
        }),
      ],
      {},
    )

    const snapshot = await readBigquerySnapshot({ client, dataset: 'd', id: PIPE_ID, tables: [] })

    expect(snapshot.inFlight).toEqual({ op: 'commit', current: block(26), range: { low: 27, high: 33 } })
    expect(snapshot.state).toEqual({
      current: block(26),
      finalized: block(23),
      rollbackChain: blocks(24, 26),
    })
  })

  it('a first write in flight recovers no state at all', async () => {
    const client = clientWith(
      [
        walRow({
          current: undefined,
          finalized: block(19),
          chain: blocks(0, 6),
          low: 0,
          high: 6,
          committed: false,
        }),
      ],
      {},
    )

    const snapshot = await readBigquerySnapshot({ client, dataset: 'd', id: PIPE_ID, tables: [] })

    expect(snapshot.inFlight).toEqual({ op: 'commit', current: undefined, range: { low: 0, high: 6 } })
    expect(snapshot.state).toBeUndefined()
  })

  it('a floor above every fragment leaves an empty chain, as the run itself holds', async () => {
    const client = clientWith(
      [
        walRow({ current: block(6), finalized: block(59), chain: [] }),
        walRow({ current: block(13), finalized: block(59), chain: [block(13)] }),
      ],
      {},
    )

    const snapshot = await readBigquerySnapshot({ client, dataset: 'd', id: PIPE_ID, tables: [] })

    expect(snapshot.state).toEqual({ current: block(13), finalized: block(59), rollbackChain: [] })
  })

  it('a table that is not there reads as empty; a value it cannot parse fails the read', async () => {
    const client = clientWith([walRow({ current: block(6), finalized: block(3), chain: [] })], {
      transfers: [
        { blockNumber: 4, value: 'a number' },
        { blockNumber: '9', value: 'a safe height as a string' },
      ],
    })

    const snapshot = await readBigquerySnapshot({
      client,
      dataset: 'd',
      id: PIPE_ID,
      tables: ['transfers', 'approvals'],
    })

    expect(snapshot.tables.approvals).toEqual({ rows: [] })
    expect(snapshot.tables.transfers.rows.map((row) => row.block)).toEqual([4, 9])

    const pastSafe = clientWith([walRow({ current: block(6), finalized: block(3), chain: [] })], {
      transfers: [{ blockNumber: '9007199254740993', value: 'past every safe integer' }],
    })

    await expect(
      readBigquerySnapshot({ client: pastSafe, dataset: 'd', id: PIPE_ID, tables: ['transfers'] }),
    ).rejects.toThrow(/blockNumber/)
  })
})
