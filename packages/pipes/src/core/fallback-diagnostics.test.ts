import { describe, expect, it } from 'vitest'

import { capabilityFailure, classifyError, redactText, redactUrl } from './fallback-diagnostics.js'

describe('redactUrl', () => {
  it('strips the fragment, which can also carry a token', () => {
    expect(redactUrl('https://rpc.example.com/v1#token=secret')).toBe('https://rpc.example.com/v1')
  })

  it('redacts credentials in a ws:// endpoint too', () => {
    expect(redactUrl('wss://user:pass@rpc.example.com/ws?apikey=secret123456')).toBe('wss://rpc.example.com/ws')
  })

  it('strips userinfo and query string', () => {
    expect(redactUrl('https://user:pass@portal.example.com/v1?apikey=secret123456')).toBe(
      'https://portal.example.com/v1',
    )
  })

  it('masks SQD key tokens and long opaque path segments', () => {
    expect(redactUrl('https://rpc.example.com/sqd_abc123DEF456')).toBe('https://rpc.example.com/sqd_***')
    expect(redactUrl('https://portal.example.com/datasets/0123456789abcdef0123456789abcdef')).toBe(
      'https://portal.example.com/datasets/***',
    )
  })

  it('returns undefined for an unparseable value rather than leak it', () => {
    expect(redactUrl('not a url')).toBeUndefined()
    expect(redactUrl(undefined)).toBeUndefined()
  })
})

describe('redactText', () => {
  it('redacts an embedded https URL, fragment and all', () => {
    expect(redactText('request to https://user:pass@rpc.example.com/v1?apikey=secret#t=x failed')).toBe(
      'request to https://rpc.example.com/v1 failed',
    )
  })

  it('redacts an embedded ws:// / wss:// endpoint (JSON-RPC providers quote these)', () => {
    expect(redactText('socket wss://k3y@node.example.com/rpc dropped')).toBe('socket wss://node.example.com/rpc dropped')
  })
})

describe('classifyError', () => {
  it('classifies a Portal HttpError by status + redacted url, keeping credentials out of detail', () => {
    const err = Object.assign(new Error('Got 400 from https://portal.example/q?key=secret123456789'), {
      name: 'HttpError',
      response: { status: 400, url: 'https://portal.example/q?key=secret123456789', body: 'not a hypothetical' },
    })
    const info = classifyError('capability', err)
    expect(info).toMatchObject({ check: 'capability', reason: 'http', code: 400 })
    expect(info.endpoint).toBe('https://portal.example/q')
    expect(info.detail).toContain('http 400')
    expect(info.detail).not.toContain('key=secret')
  })

  it('classifies a JSON-RPC error (HTTP 200 with error body) with its code, and includes the request', () => {
    const err = Object.assign(new Error('the method debug_traceBlockByNumber does not exist'), {
      name: 'RpcError',
      code: -32601,
      rpcUrl: 'https://rpc.example/sqd_keyTOKEN1234567',
      rpcMethod: 'debug_traceBlockByNumber',
      rpcParams: ['0x1', { tracer: 'callTracer' }],
    })
    const info = classifyError('stream', err)
    expect(info).toMatchObject({ check: 'stream', reason: 'rpc', code: -32601 })
    expect(info.endpoint).toBe('https://rpc.example/sqd_***')
    expect(info.detail).toContain('debug_traceBlockByNumber')
    expect(info.detail).toContain('callTracer')
  })

  it('classifies transport timeouts and connection errors', () => {
    expect(classifyError('liveness', Object.assign(new Error('x'), { name: 'HttpTimeoutError' })).reason).toBe(
      'timeout',
    )
    expect(classifyError('liveness', Object.assign(new Error('x'), { name: 'RpcConnectionError' })).reason).toBe(
      'connection',
    )
  })

  it('falls back to unknown for an unrecognised error, keeping the message in detail', () => {
    const info = classifyError('stream', new Error('something odd'))
    expect(info.reason).toBe('unknown')
    expect(info.code).toBeUndefined()
    expect(info.detail).toContain('something odd')
  })
})

describe('capabilityFailure', () => {
  it('defaults to check=capability, reason=unknown (a not-capable probe is not `stale`)', () => {
    const info = capabilityFailure('probe reported not-capable')
    expect(info.check).toBe('capability')
    expect(info.reason).toBe('unknown')
    expect(info.reason).not.toBe('stale')
    expect(info.detail).toContain('probe reported not-capable')
  })

  it('carries an explicit accurate reason, e.g. timeout', () => {
    expect(capabilityFailure('probe timed out after 20ms', 'timeout').reason).toBe('timeout')
  })
})
