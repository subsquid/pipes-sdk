import { describe, expect, it } from 'vitest'

import { CursorKey, LEGACY_DEFAULT_CURSOR_ID } from './cursor-key.js'

describe('CursorKey', () => {
  it('adopts the source id when no explicit id is given', () => {
    const key = new CursorKey(undefined)
    key.bind('pipe-x')

    expect(key.value).toBe('pipe-x')
    expect(key.isExplicit).toBe(false)
  })

  it('keeps an explicit id over the source id', () => {
    const key = new CursorKey('pinned')
    key.bind('pipe-x')

    expect(key.value).toBe('pinned')
    expect(key.isExplicit).toBe(true)
  })

  it('exposes the legacy default only until the key is bound', () => {
    const key = new CursorKey(undefined)

    expect(key.value).toBe(LEGACY_DEFAULT_CURSOR_ID)
  })

  it('refuses to bind without a source id', () => {
    const key = new CursorKey(undefined)

    expect(() => key.bind(undefined)).toThrow(expect.objectContaining({ code: 'E0001' }))
    expect(() => key.bind('  ')).toThrow(expect.objectContaining({ code: 'E0001' }))
  })

  it('honours a custom default', () => {
    const key = new CursorKey(undefined, 'custom-default')

    expect(key.value).toBe('custom-default')
  })
})
