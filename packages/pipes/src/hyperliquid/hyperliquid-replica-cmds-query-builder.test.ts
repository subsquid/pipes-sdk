import { describe, expect, it } from 'vitest'

import { stubPortal } from '~/testing/stub-portal.js'

import type * as api from '../portal-client/query/hyperliquid-replica-cmds.js'
import {
  HyperliquidReplicaCmdsQueryBuilder,
  hyperliquidReplicaCmdsQuery,
} from './hyperliquid-replica-cmds-query-builder.js'

const user = ['0x6978e7917dffd1f1815c858ca1528e51a3af1cb5']
const cloid = ['0x00000000000000000000000000000001']
const range = { from: 10, to: 20 }

const portal = stubPortal({ getHead: async () => ({ number: 100, hash: '0x' }) })

async function bounded(builder: HyperliquidReplicaCmdsQueryBuilder) {
  return (await builder.calculateRanges({ portal })).bounded
}

type RequestMethod =
  | 'addActionRequest'
  | 'addOrderActionRequest'
  | 'addCancelActionRequest'
  | 'addCancelByCloidActionRequest'
  | 'addBatchModifyActionRequest'

describe('HyperliquidReplicaCmdsQueryBuilder', () => {
  it('reports its network type', () => {
    expect(new HyperliquidReplicaCmdsQueryBuilder().getType()).toBe('hyperliquidReplicaCmds')
  })

  it.each<[RequestMethod, keyof api.DataRequest, object]>([
    ['addActionRequest', 'actions', { actionType: ['order'], status: 'err' }],
    ['addOrderActionRequest', 'orderActions', { user, containsAsset: [223], containsCloid: cloid }],
    ['addCancelActionRequest', 'cancelActions', { user, containsAsset: [223], status: 'ok' }],
    ['addCancelByCloidActionRequest', 'cancelByCloidActions', { user, containsCloid: cloid }],
    ['addBatchModifyActionRequest', 'batchModifyActions', { user, containsAsset: [223] }],
  ])('%s fills the %s family', async (method, family, request) => {
    const builder = new HyperliquidReplicaCmdsQueryBuilder()[method]({ range, request })

    expect(await bounded(builder)).toEqual([{ range, request: { [family]: [request] } }])
  })

  it('merges its families in the order the Portal query lists them', () => {
    const families = ['actions', 'orderActions', 'cancelActions', 'cancelByCloidActions', 'batchModifyActions']
    const requests = [{ includeAllBlocks: true }, ...[...families].reverse().map((family) => ({ [family]: [{}] }))]
    const merged = new HyperliquidReplicaCmdsQueryBuilder().mergeDataRequests(...requests)

    expect(Object.keys(merged)).toEqual([...families, 'includeAllBlocks'])
  })

  it('hyperliquidReplicaCmdsQuery returns a builder', () => {
    expect(hyperliquidReplicaCmdsQuery()).toBeInstanceOf(HyperliquidReplicaCmdsQueryBuilder)
  })
})
