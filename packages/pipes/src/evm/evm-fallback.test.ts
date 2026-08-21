import { describe, expect, it } from 'vitest'

import { FallbackClient } from '~/core/index.js'
import { BlockStreamClient } from '~/portal-client/index.js'

import * as evmBarrel from './browser.js'
import { createEvmFallbackClient, translateMissingRpcPeer } from './evm-fallback.js'

describe('public ./evm exports', () => {
  it('surfaces the fallback + facade through the evm barrel (reachable by consumers)', () => {
    // Guards the whole feature being unreachable — the barrel must re-export it, and doing so must
    // not eagerly pull the optional evm-rpc peers (the import chain references them as types only).
    expect(typeof evmBarrel.createEvmFallbackClient).toBe('function')
    expect(typeof evmBarrel.evmStream).toBe('function')
    expect(evmBarrel.evmPortalStream).toBe(evmBarrel.evmStream) // deprecated alias preserved
  })
})

/**
 * The lazy RPC loader must translate a *missing optional peer* into an actionable message, while
 * letting every other load failure surface unchanged — mirrors the Squid evm-rpc-stream
 * load-rpc-stream tests. A blanket `catch` that always blamed the peers would misdiagnose a broken
 * transitive dependency or an init error inside the RPC stack.
 */

function moduleNotFound(message: string, code = 'ERR_MODULE_NOT_FOUND'): NodeJS.ErrnoException {
  const e = new Error(message) as NodeJS.ErrnoException
  e.code = code
  return e
}

describe('translateMissingRpcPeer', () => {
  it('maps a missing @subsquid/evm-rpc (ESM loader) to an actionable, named error', () => {
    const out = translateMissingRpcPeer(
      moduleNotFound("Cannot find package '@subsquid/evm-rpc' imported from /app/evm-rpc-source.js"),
      'rpc-1',
    )
    expect(out).toBeInstanceOf(Error)
    expect((out as Error).message).toContain('rpc-1')
    expect((out as Error).message).toContain('@subsquid/evm-rpc')
    expect((out as Error).message).toContain('@subsquid/evm-normalization')
  })

  it('maps a missing @subsquid/evm-normalization (CJS loader code) too', () => {
    const out = translateMissingRpcPeer(
      moduleNotFound("Cannot find module '@subsquid/evm-normalization'", 'MODULE_NOT_FOUND'),
    )
    expect((out as Error).message).toContain('optional peer dependencies')
  })

  it("maps evm-rpc's transitive peers (http-client / rpc-client) and names the whole stack", () => {
    // A consumer who installed only evm-rpc + evm-normalization can still miss these; the message
    // must name them so the guidance is complete.
    const out = translateMissingRpcPeer(
      moduleNotFound("Cannot find package '@subsquid/http-client' imported from /app/rpc.js"),
    )
    const msg = (out as Error).message
    expect(msg).toContain('@subsquid/http-client')
    expect(msg).toContain('@subsquid/rpc-client')
    expect(msg).toContain('@subsquid/evm-rpc')
    expect(msg).toContain('@subsquid/evm-normalization')
  })

  it('passes through a module-not-found for an UNRELATED module unchanged', () => {
    const original = moduleNotFound("Cannot find package 'some-other-dep' imported from /app/x.js")
    expect(translateMissingRpcPeer(original)).toBe(original) // not masked as a missing peer
  })

  it('passes through a non-module-not-found fault (e.g. an init/syntax error) unchanged', () => {
    const original = new SyntaxError('Unexpected token in the RPC stack')
    expect(translateMissingRpcPeer(original)).toBe(original)
  })
})

describe('createEvmFallbackClient — source specs', () => {
  it('builds portal sources from strings and option objects, naming them by position or by `name`', () => {
    const fb = createEvmFallbackClient([
      'http://localhost:1/datasets/eth',
      { url: 'http://localhost:2/datasets/eth', name: 'private' },
    ])

    expect(fb).toBeInstanceOf(FallbackClient)
    expect(fb.metrics().sources.map((s) => s.name)).toEqual(['portal-0', 'private'])
    expect(fb.getUrl()).toBe('http://localhost:1/datasets/eth')
  })

  it("builds an RPC source from a plain {type: 'rpc'} spec without touching the peer at build time", () => {
    // Construction must not import @subsquid/evm-rpc: the lazy client resolves its URL and
    // metadata surface up front, and only loads the stack when the endpoint is actually needed.
    const fb = createEvmFallbackClient([
      'http://localhost:1/datasets/eth',
      { type: 'rpc', url: 'https://rpc.example/eth/verysecretapikeyvalue1234567890', rateLimit: 10 },
    ])

    const names = fb.metrics().sources.map((s) => s.name)
    expect(names).toEqual(['portal-0', 'rpc-1'])
  })

  it('redacts credentials in the lazy RPC source URL', async () => {
    const fb = createEvmFallbackClient([
      { type: 'rpc', name: 'rpc', url: 'https://rpc.example/eth/verysecretapikeyvalue1234567890' },
    ])

    expect(fb.getUrl()).not.toContain('verysecretapikeyvalue1234567890')
    expect(fb.getUrl()).toContain('rpc.example')
  })

  it('loads the RPC stack lazily on first use — getMetadata answers without any network', async () => {
    const fb = createEvmFallbackClient([{ type: 'rpc', name: 'standby', url: 'http://localhost:1' }])

    expect(await fb.getMetadata()).toMatchObject({ dataset: 'standby', real_time: true })
  })

  it('accepts a custom BlockStreamClient and validates its shape', () => {
    const custom = {
      finalized: false,
      getUrl: () => 'custom://x',
      getMetadata: async () => ({ dataset: 'x', aliases: [], real_time: true, start_block: 0 }),
      getHead: async () => undefined,
      resolveTimestamp: async () => 0,
      getStream: () => ({ [Symbol.asyncIterator]: async function* () {} }),
    } as unknown as BlockStreamClient

    const fb = createEvmFallbackClient([{ type: 'custom', name: 'mine', client: custom }])
    expect(fb.metrics().sources[0].name).toBe('mine')

    expect(() => createEvmFallbackClient([{ type: 'custom', client: {} as BlockStreamClient }])).toThrowError(
      /BlockStreamClient/,
    )
  })

  it('applies a uniform `finalized` to every source', () => {
    const fb = createEvmFallbackClient(
      ['http://localhost:1/datasets/eth', { type: 'rpc', url: 'http://localhost:2' }],
      { finalized: true },
    )

    expect(fb.finalized).toBe(true)
  })

  it('allows a finalized-only primary with a hot standby, and reports the set as hot', () => {
    // The "cheap bulk, then follow the tip" topology: backfill from a finalized portal, hand off to
    // an RPC at the finality frontier. The pipe must report hot so the target keeps fork handling.
    const fb = createEvmFallbackClient([
      { url: 'http://localhost:1/datasets/eth', name: 'bulk', finalized: true },
      { type: 'rpc', url: 'http://localhost:2', name: 'tip' }, // defaults to finalized: false
    ])

    expect(fb.finalized).toBe(false)
    expect(fb.metrics().sources.map((s) => s.name)).toEqual(['bulk', 'tip'])
  })
})
