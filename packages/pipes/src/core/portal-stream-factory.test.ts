import { describe, expect, expectTypeOf, it } from 'vitest'

import { bitcoinQuery } from '~/bitcoin/bitcoin-query-builder.js'
import { BlockValidationError } from '~/core/errors.js'
import type { PortalStream } from '~/core/portal-source.js'
import { HyperliquidFillsQueryBuilder, hyperliquidFillsQuery } from '~/hyperliquid/hyperliquid-fills-query-builder.js'
import { readAll } from '~/testing/index.js'
import { withPortal } from '~/testing/portal-fixture.js'

import { portalStreamOf } from './portal-stream-factory.js'

const fillsStream = portalStreamOf<HyperliquidFillsQueryBuilder<any>>(() =>
  hyperliquidFillsQuery().addFields({ block: { hash: true, number: true } }),
)

type DataOf<S> = S extends PortalStream<any, infer T> ? T : never

// biome-ignore lint/complexity/noBannedTypes: a header with no selected fields is the empty object type
type NoHeaderFields = {}

const header = { number: 1, hash: '0x01', parentHash: '0x00' }

describe('portalStreamOf', () => {
  it('yields the blocks of a single builder', () => {
    const outputs = hyperliquidFillsQuery().addFields({ fill: { px: true } })

    expectTypeOf<DataOf<ReturnType<typeof fillsStream<typeof outputs>>>>().toEqualTypeOf<
      { header: NoHeaderFields; fills: { px: number }[] }[]
    >()
  })

  it('yields one entry per key of a record', () => {
    const outputs = {
      a: hyperliquidFillsQuery().addFields({ fill: { px: true } }),
      b: hyperliquidFillsQuery()
        .addFields({ fill: { sz: true } })
        .build(),
    }

    expectTypeOf<DataOf<ReturnType<typeof fillsStream<typeof outputs>>>>().toEqualTypeOf<{
      a: { header: NoHeaderFields; fills: { px: number }[] }[]
      b: { header: NoHeaderFields; fills: { sz: number }[] }[]
    }>()
  })

  it('rejects a builder for another dataset', () => {
    function neverCalled() {
      // @ts-expect-error a bitcoin builder is not an output of the fills stream
      fillsStream({ id: 't', portal: 'u', outputs: bitcoinQuery() })
    }

    expect(neverCalled).toBeTypeOf('function')
  })

  withPortal('adds the block hash and number to the query', async ({ portal: open }) => {
    const portal = await open({ blocks: [{ header, fills: [{ px: 1.5 }] }] })

    const stream = fillsStream({
      id: 'test',
      logger: 'silent',
      portal: portal.url,
      outputs: hyperliquidFillsQuery()
        .addFields({ fill: { px: true } })
        .addRange({ from: 1, to: 1 }),
    })

    await readAll(stream)

    const [sent] = portal.requests

    expect(sent.query['fields']).toEqual({ block: { hash: true, number: true }, fill: { px: true } })
  })

  withPortal('fails on a block that does not match the selected fields', async ({ portal: open }) => {
    const portal = await open({ blocks: [{ header, fills: [{ px: 'high' }] }] })

    const stream = fillsStream({
      id: 'test',
      logger: 'silent',
      portal: portal.url,
      outputs: hyperliquidFillsQuery()
        .addFields({ fill: { px: true } })
        .addRange({ from: 1, to: 1 }),
    })

    await expect(readAll(stream)).rejects.toThrow(BlockValidationError)
  })
})
