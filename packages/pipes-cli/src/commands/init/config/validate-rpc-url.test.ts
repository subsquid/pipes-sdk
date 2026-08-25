import { describe, expect, it } from 'vitest'

import { validateRpcUrl } from './validate-rpc-url.js'

describe('validateRpcUrl', () => {
  it('accepts blank input — the URL can be filled into .env later', () => {
    expect(validateRpcUrl('')).toBe(true)
    expect(validateRpcUrl('   ')).toBe(true)
  })

  it('accepts http(s) URLs', () => {
    expect(validateRpcUrl('https://eth-mainnet.example.com/v2/KEY')).toBe(true)
    expect(validateRpcUrl('http://localhost:8545')).toBe(true)
    expect(validateRpcUrl('  https://rpc.example.com  ')).toBe(true)
  })

  it('rejects non-http(s) protocols with a message', () => {
    expect(validateRpcUrl('ftp://rpc.example.com')).toMatch(/http/)
    expect(validateRpcUrl('wss://rpc.example.com')).toMatch(/http/)
  })

  it('rejects text that is not a URL with a message', () => {
    expect(validateRpcUrl('not a url')).toMatch(/valid URL/)
    expect(validateRpcUrl('rpc.example.com')).toMatch(/valid URL/)
  })
})
