import { describe, expect, it } from 'vitest'

import { portOf } from './latency-proxy.js'

describe('portOf', () => {
  it('keeps an explicit port', () => {
    expect(portOf(new URL('postgresql://u:p@db:6543/x'))).toBe(6543)
    expect(portOf(new URL('http://ch:10123'))).toBe(10123)
  })

  it('fills in the default port a URL leaves out', () => {
    expect(portOf(new URL('postgresql://u:p@db/x'))).toBe(5432)
    expect(portOf(new URL('postgres://db/x'))).toBe(5432)
    expect(portOf(new URL('https://ch.example'))).toBe(443)
  })

  it('reads an explicit default port that the URL parser dropped', () => {
    const url = new URL('http://ch:80')

    expect(url.port).toBe('')
    expect(portOf(url)).toBe(80)
  })

  it('refuses a scheme it has no default for', () => {
    expect(() => portOf(new URL('tcp://db'))).toThrow(/No default port/)
  })
})
