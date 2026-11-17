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

describe('HyperliquidReplicaCmdsQueryBuilder', () => {
  it('reports its network type', () => {
    expect(new HyperliquidReplicaCmdsQueryBuilder().getType()).toBe('hyperliquidReplicaCmds')
  })

  describe('request methods', () => {
    it('addActionRequest fills the actions family', async () => {
      const request: api.ActionRequest = { actionType: ['order'], status: 'err' }
      const builder = new HyperliquidReplicaCmdsQueryBuilder().addActionRequest({ range, request })

      expect(await bounded(builder)).toEqual([{ range, request: { actions: [request] } }])
    })

    it('addOrderActionRequest fills the orderActions family', async () => {
      const request: api.OrderActionRequest = { user, containsAsset: [223], containsCloid: cloid }
      const builder = new HyperliquidReplicaCmdsQueryBuilder().addOrderActionRequest({ range, request })

      expect(await bounded(builder)).toEqual([{ range, request: { orderActions: [request] } }])
    })

    it('addCancelActionRequest fills the cancelActions family', async () => {
      const request: api.CancelActionRequest = { user, containsAsset: [223], status: 'ok' }
      const builder = new HyperliquidReplicaCmdsQueryBuilder().addCancelActionRequest({ range, request })

      expect(await bounded(builder)).toEqual([{ range, request: { cancelActions: [request] } }])
    })

    it('addCancelByCloidActionRequest fills the cancelByCloidActions family', async () => {
      const request: api.CancelByCloidActionRequest = { user, containsCloid: cloid }
      const builder = new HyperliquidReplicaCmdsQueryBuilder().addCancelByCloidActionRequest({ range, request })

      expect(await bounded(builder)).toEqual([{ range, request: { cancelByCloidActions: [request] } }])
    })

    it('addBatchModifyActionRequest fills the batchModifyActions family', async () => {
      const request: api.BatchModifyActionRequest = { user, containsAsset: [223] }
      const builder = new HyperliquidReplicaCmdsQueryBuilder().addBatchModifyActionRequest({ range, request })

      expect(await bounded(builder)).toEqual([{ range, request: { batchModifyActions: [request] } }])
    })
  })

  describe('mergeDataRequests', () => {
    const first: api.DataRequest = {
      actions: [{ actionType: ['order'] }],
      orderActions: [{ user }],
      cancelActions: [{ containsAsset: [1] }],
      cancelByCloidActions: [{ containsCloid: cloid }],
      batchModifyActions: [{ status: 'ok' }],
    }
    const second: api.DataRequest = {
      actions: [{ status: 'err' }],
      orderActions: [{ containsAsset: [223] }],
      cancelActions: [{ user }],
      cancelByCloidActions: [{ user }],
      batchModifyActions: [{ containsCloid: cloid }],
    }

    it('concatenates each family in order', () => {
      const merged = new HyperliquidReplicaCmdsQueryBuilder().mergeDataRequests(first, second)

      expect(merged).toEqual({
        actions: [{ actionType: ['order'] }, { status: 'err' }],
        orderActions: [{ user }, { containsAsset: [223] }],
        cancelActions: [{ containsAsset: [1] }, { user }],
        cancelByCloidActions: [{ containsCloid: cloid }, { user }],
        batchModifyActions: [{ status: 'ok' }, { containsCloid: cloid }],
      })
    })

    it('sets includeAllBlocks when either request has it', () => {
      const builder = new HyperliquidReplicaCmdsQueryBuilder()

      expect(builder.mergeDataRequests(first, { includeAllBlocks: true }).includeAllBlocks).toBe(true)
      expect(builder.mergeDataRequests({ includeAllBlocks: true }, first).includeAllBlocks).toBe(true)
      expect(builder.mergeDataRequests(first, second).includeAllBlocks).toBeUndefined()
    })
  })

  it('hyperliquidReplicaCmdsQuery returns a builder', () => {
    expect(hyperliquidReplicaCmdsQuery()).toBeInstanceOf(HyperliquidReplicaCmdsQueryBuilder)
  })
})
