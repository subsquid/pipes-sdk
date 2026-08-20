import { describe, expect, it } from 'vitest'

import { BlockStreamClient, ForkException, StreamData } from '~/portal-client/index.js'

import { makeCapabilityProbe } from './fallback-capability.js'
import { BlockCursor } from './types.js'

function cursor(n: number): BlockCursor {
  return { number: n, hash: `0x${n}` }
}

function batch(n: number): StreamData<{ header: { number: number; hash: string } }> {
  return {
    blocks: [{ header: { number: n, hash: `0x${n}` } }],
    head: {},
    meta: { bytes: 0, requestedFromBlock: n, lastBlockReceivedAt: new Date(), requests: {} },
  }
}

type StreamFn = (query: any) => AsyncGenerator<StreamData<any>>

/** A mock client; `reads` records every `getStream` query it received. */
function client(stream: StreamFn): BlockStreamClient & { reads: any[] } {
  const reads: any[] = []
  return {
    finalized: false,
    reads,
    getUrl: () => 'mock://probe',
    getMetadata: async () => ({ dataset: 'mock', aliases: [], real_time: true, start_block: 0 }),
    getHead: async () => undefined,
    resolveTimestamp: async () => {
      throw new Error('unsupported')
    },
    getStream: (query: any) => {
      reads.push(query)
      return { [Symbol.asyncIterator]: () => stream(query)[Symbol.asyncIterator]() }
    },
  }
}

const QUERY = { type: 'evm', fields: { block: { number: true } }, fromBlock: 0, parentBlockHash: '0xdead' }

describe('makeCapabilityProbe', () => {
  it('reads a one-block slice just past the cursor and reports capable when it serves', async () => {
    const c = client(async function* () {
      yield batch(100)
    })
    expect(await makeCapabilityProbe(c, QUERY)(cursor(99))).toEqual({ ok: true })

    // The slice is bounded to one block at the frontier, keeps the full query (fields + request),
    // and drops the resume anchor — a probe must never fault a 409 out of a reorg.
    expect(c.reads).toHaveLength(1)
    expect(c.reads[0]).toMatchObject({ fromBlock: 100, toBlock: 100, fields: QUERY.fields })
    expect(c.reads[0].parentBlockHash).toBeUndefined()
  })

  it('anchors at the query start when no cursor is given', async () => {
    const c = client(async function* () {
      yield batch(0)
    })
    expect(await makeCapabilityProbe(c, QUERY)()).toEqual({ ok: true })
    expect(c.reads[0]).toMatchObject({ fromBlock: 0, toBlock: 0 })
  })

  it('reports capable when the slice is empty (served the query, nothing matched)', async () => {
    const c = client(async function* () {})
    expect(await makeCapabilityProbe(c, QUERY)(cursor(99))).toEqual({ ok: true })
  })

  it('reports not-capable, with the classified cause, when the source cannot serve the slice', async () => {
    const c = client(async function* () {
      throw new Error('the method trace_block does not exist')
    })
    const r = await makeCapabilityProbe(c, QUERY)(cursor(99))
    expect(r.ok).toBe(false)
    expect(r.cause?.check).toBe('capability')
    expect(r.cause?.detail).toContain('trace_block')
  })

  it('classifies a Portal HTTP 400 as an http failure carrying its status code', async () => {
    const c = client(async function* () {
      throw Object.assign(new Error('Got 400 from https://portal.example/q'), {
        name: 'HttpError',
        response: { status: 400, url: 'https://portal.example/q', body: 'not a hypothetical' },
      })
    })
    const r = await makeCapabilityProbe(c, QUERY)(cursor(99))
    expect(r.ok).toBe(false)
    expect(r.cause?.reason).toBe('http')
    expect(r.cause?.code).toBe(400)
  })

  it('treats a ForkException as capable (served + reorg, not an inability to serve)', async () => {
    const c = client(async function* () {
      throw new ForkException([cursor(99)], { fromBlock: 100, parentBlockHash: '0x99' })
    })
    expect(await makeCapabilityProbe(c, QUERY)(cursor(99))).toEqual({ ok: true })
  })

  it('reports not-capable when the slice exceeds the probe timeout', async () => {
    const c = client(async function* () {
      await new Promise<void>(() => {}) // hang — never yields
    })
    const r = await makeCapabilityProbe(c, QUERY, { timeoutMs: 20 })(cursor(99))
    expect(r.ok).toBe(false)
    expect(r.cause?.reason).toBe('timeout')
    expect(r.cause?.detail).toContain('timed out')
  })
})
