import { describe, expect, expectTypeOf, it } from 'vitest'

import { api, fillsApi } from './index.js'

describe('@subsquid/pipes/hyperliquid', () => {
  it('exports the fills wire types as fillsApi and as the deprecated api', () => {
    expectTypeOf<api.FieldSelection>().toEqualTypeOf<fillsApi.FieldSelection>()
    expect(api).toBe(fillsApi)
  })
})
