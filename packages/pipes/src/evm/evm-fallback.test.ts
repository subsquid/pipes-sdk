import { describe, expect, it } from 'vitest'

import * as evmBarrel from './browser.js'
import { translateMissingRpcPeer } from './evm-fallback.js'

describe('public ./evm exports', () => {
  it('surfaces the RPC-fallback source through the evm barrel (reachable by consumers)', () => {
    // Guards the whole feature being unreachable — the barrel must re-export it, and doing so must
    // not eagerly pull the optional evm-rpc peers (the import chain references them as types only).
    expect(typeof evmBarrel.createEvmFallback).toBe('function')
    expect(typeof evmBarrel.evmPortalReadSource).toBe('function')
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
