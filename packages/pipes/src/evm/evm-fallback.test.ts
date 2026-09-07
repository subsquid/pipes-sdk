import http from 'node:http'
import { AddressInfo } from 'node:net'

import { afterEach, describe, expect, it } from 'vitest'

import { FallbackClient } from '~/core/index.js'
import { BlockStreamClient } from '~/portal-client/index.js'
import { MockPortal, mockPortal } from '~/testing/index.js'

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

  it('rides out a transient transport failure instead of burning a switch', async () => {
    // A lone portal retries a retryable status indefinitely (there is nothing else to read); the
    // transport default is zero retries, which would hand over on every blip. Inside a list the
    // budget is short but non-zero.
    const flaky = await mockPortal([
      { statusCode: 503 },
      { statusCode: 503 },
      { statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] },
    ])
    const standby = await mockPortal([
      { statusCode: 200, data: [{ header: { number: 1, hash: '0xbad', timestamp: 1000 } }] },
    ])

    try {
      const fb = createEvmFallbackClient([flaky.url, standby.url], { detection: { capabilityProbe: false } })
      const seen: string[] = []
      for await (const batch of fb.getStream({ type: 'evm', fromBlock: 0 } as any)) {
        seen.push(...batch.blocks.map((b: any) => b.header.hash))
        break
      }

      expect(seen).toEqual(['0x1']) // served by the flaky primary, after its retries
      expect(fb.switchCount).toBe(0)
    } finally {
      await flaky.close()
      await standby.close()
    }
  }, 20_000)

  it('lets the retry budget be tuned for the whole list', async () => {
    // The budget is a parameter of the fallback, so a caller using bare URL specs can raise or
    // lower it without rewriting every source into an options object.
    const flaky = await mockPortal([
      { statusCode: 503 },
      { statusCode: 503 },
      { statusCode: 503 },
      { statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] },
    ])
    const standby = await mockPortal([
      { statusCode: 200, data: [{ header: { number: 1, hash: '0xstandby', timestamp: 1000 } }] },
    ])

    try {
      const fb = createEvmFallbackClient([flaky.url, standby.url], {
        sourceRetries: 0, // opt out entirely: hand over on the first failure
        detection: { capabilityProbe: false },
      })
      const seen: string[] = []
      for await (const batch of fb.getStream({ type: 'evm', fromBlock: 0 } as any)) {
        seen.push(...batch.blocks.map((b: any) => b.header.hash))
        break
      }

      expect(seen).toEqual(['0xstandby'])
      expect(fb.switchCount).toBe(1)
    } finally {
      await flaky.close()
      await standby.close()
    }
  }, 20_000)

  it("lets a caller's own transport settings win", async () => {
    // Opting out of the budget must actually opt out: the first failure hands over.
    const flaky = await mockPortal([{ statusCode: 503 }, { statusCode: 503 }, { statusCode: 503 }])
    const standby = await mockPortal([
      { statusCode: 200, data: [{ header: { number: 1, hash: '0xstandby', timestamp: 1000 } }] },
    ])

    try {
      const fb = createEvmFallbackClient([{ url: flaky.url, http: { retryAttempts: 0 } }, standby.url], {
        detection: { capabilityProbe: false },
      })
      const seen: string[] = []
      for await (const batch of fb.getStream({ type: 'evm', fromBlock: 0 } as any)) {
        seen.push(...batch.blocks.map((b: any) => b.header.hash))
        break
      }

      expect(seen).toEqual(['0xstandby'])
      expect(fb.switchCount).toBe(1)
    } finally {
      await flaky.close()
      await standby.close()
    }
  }, 20_000)

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

describe('lazy RPC source — head-poll wire cost', () => {
  let portal: MockPortal | undefined
  let rpcServer: http.Server | undefined

  afterEach(async () => {
    await portal?.close()
    portal = undefined
    await new Promise<void>((resolve) => (rpcServer ? rpcServer.close(() => resolve()) : resolve()))
    rpcServer = undefined
  })

  it('polls a standby RPC source with eth_blockNumber, never a block lookup', async () => {
    // Regression: the lazy wrapper enumerates the client contract method by method, so a newly
    // added optional method (`getHeight`) that is not forwarded is silently invisible — the
    // fallback then quietly downgrades every poll to the full `eth_getBlockByNumber` lookup.
    // Only an end-to-end path through `createEvmFallbackClient` catches that; the internal
    // framework has no RPC mock, so a minimal method-counting JSON-RPC stub stands in (it never
    // has to serve blocks — the standby is polled, not streamed).
    const methods: string[] = []
    rpcServer = http.createServer(async (req, res) => {
      const chunks: Buffer[] = []
      for await (const c of req) chunks.push(c as Buffer)
      const body = JSON.parse(Buffer.concat(chunks).toString())
      const calls = Array.isArray(body) ? body : [body]
      for (const c of calls) methods.push(c.method)

      const answer = (c: any) => {
        if (c.method === 'eth_blockNumber') return { jsonrpc: '2.0', id: c.id, result: '0x64' }
        return { jsonrpc: '2.0', id: c.id, error: { code: -32601, message: `unexpected ${c.method}` } }
      }
      res.setHeader('content-type', 'application/json')
      res.end(JSON.stringify(Array.isArray(body) ? calls.map(answer) : answer(calls[0])))
    })
    await new Promise<void>((resolve) => rpcServer?.listen(0, resolve))
    const rpcUrl = `http://127.0.0.1:${(rpcServer!.address() as AddressInfo).port}`

    portal = await mockPortal([
      { statusCode: 200, data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }] },
      { statusCode: 200, data: [{ header: { number: 2, hash: '0x2', timestamp: 2000 } }] },
    ])

    const fb = createEvmFallbackClient([portal.url, { type: 'rpc', url: rpcUrl, name: 'standby' }], {
      // Lag detection stays on (the default) so the boundary polls the standby; probes off so the
      // standby is never asked to stream; headTtlMs 0 so every boundary re-polls.
      detection: { capabilityProbe: false, headTtlMs: 0, headPollTimeoutMs: 5000 },
    })

    const numbers: number[] = []
    for await (const b of fb.getStream({ type: 'evm', fromBlock: 1, toBlock: 2 } as never)) {
      numbers.push(...(b.blocks as { header: { number: number } }[]).map((x) => x.header.number))
    }

    expect(numbers).toEqual([1, 2])
    expect(methods).toContain('eth_blockNumber')
    expect(methods).not.toContain('eth_getBlockByNumber')
  })
})
