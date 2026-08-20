import { describe, expect, it } from 'vitest'

import { ForkException } from '~/portal-client/index.js'

import { makeCapabilityProbe } from './fallback-capability.js'
import { FallbackUnderlyingSource } from './fallback-source.js'
import { PortalBatch } from './portal-source.js'
import { BlockCursor } from './types.js'

function cursor(n: number): BlockCursor {
  return { number: n, hash: `0x${n}` }
}

function pbatch(n: number): PortalBatch<number[]> {
  return {
    data: [n],
    ctx: { stream: { state: { current: cursor(n) }, head: {} } },
  } as unknown as PortalBatch<number[]>
}

type ReadFn = (cursor?: BlockCursor) => AsyncGenerator<PortalBatch<number[]>>

function source(read: ReadFn): FallbackUnderlyingSource<number[]> & { reads: (BlockCursor | undefined)[] } {
  const reads: (BlockCursor | undefined)[] = []
  return {
    name: 'mock',
    reads,
    read: (c) => {
      reads.push(c)
      return read(c)
    },
  }
}

describe('makeCapabilityProbe', () => {
  it('reads a slice at the asked-for cursor and reports capable when it serves', async () => {
    const s = source(async function* () {
      yield pbatch(100)
    })
    expect(await makeCapabilityProbe(s)(cursor(99))).toEqual({ ok: true })
    expect(s.reads).toEqual([cursor(99)])
  })

  it('reports capable when the slice is empty (served the query, nothing matched)', async () => {
    const s = source(async function* () {})
    expect(await makeCapabilityProbe(s)(cursor(99))).toEqual({ ok: true })
  })

  it('reports not-capable, with the classified cause, when the source cannot serve the slice', async () => {
    const s = source(async function* () {
      throw new Error('the method trace_block does not exist')
    })
    const r = await makeCapabilityProbe(s)(cursor(99))
    expect(r.ok).toBe(false)
    expect(r.cause?.check).toBe('capability')
    expect(r.cause?.detail).toContain('trace_block')
  })

  it('classifies a Portal HTTP 400 as an http failure carrying its status code', async () => {
    const s = source(async function* () {
      throw Object.assign(new Error('Got 400 from https://portal.example/q'), {
        name: 'HttpError',
        response: { status: 400, url: 'https://portal.example/q', body: 'not a hypothetical' },
      })
    })
    const r = await makeCapabilityProbe(s)(cursor(99))
    expect(r.ok).toBe(false)
    expect(r.cause?.reason).toBe('http')
    expect(r.cause?.code).toBe(400)
  })

  it('treats a ForkException as capable (served + reorg, not an inability to serve)', async () => {
    const s = source(async function* () {
      throw new ForkException([cursor(99)], { fromBlock: 100, parentBlockHash: '0x99' })
    })
    expect(await makeCapabilityProbe(s)(cursor(99))).toEqual({ ok: true })
  })

  it('reports not-capable when the slice exceeds the probe timeout', async () => {
    const s = source(async function* () {
      await new Promise<void>(() => {}) // hang — never yields
    })
    const r = await makeCapabilityProbe(s, { timeoutMs: 20 })(cursor(99))
    expect(r.ok).toBe(false)
    expect(r.cause?.reason).toBe('timeout')
    expect(r.cause?.detail).toContain('timed out')
  })
})
