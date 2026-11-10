import { mapRpcBlock } from '@subsquid/evm-normalization'
import { Block as RpcBlock } from '@subsquid/evm-rpc'
import { toJSON } from '@subsquid/util-internal-json'

import { DataRequest, FieldSelection } from '~/portal-client/query/evm.js'

import { createBlockFilter } from './filter.js'
import { RequiredData, toRequiredData } from './request.js'

/** A portal-wire-shaped block with its filterable item arrays materialized. */
export type FilterableWireBlock = {
  header: { number: number; hash: string; timestamp?: number }
  logs: any[]
  transactions: any[]
  traces: any[]
  stateDiffs: any[]
}

export interface WireBlockMapper {
  /** The coarse per-kind fetch toggles derived from the request — feeds the RPC data request. */
  requiredData: RequiredData
  map(raw: RpcBlock): FilterableWireBlock
}

/**
 * Build the raw-RPC-block → portal-wire-block mapper for one query: normalize with the same
 * `mapRpcBlock` the portal's dataset producers use, serialize to the wire shape, and keep the items
 * the request matches. The wire output keeps every field; the downstream normalize step decodes the
 * surviving items at the user's selection, exactly as it does portal output.
 */
export function createWireBlockMapper(fields: FieldSelection, request: DataRequest): WireBlockMapper {
  const requiredData = toRequiredData(request, fields)
  const filter = createBlockFilter(request)

  return {
    requiredData,
    map(raw: RpcBlock): FilterableWireBlock {
      const normalized = mapRpcBlock(raw, {
        withTraces: requiredData.traces,
        withStateDiffs: requiredData.stateDiffs,
      })

      const wire = shimWireBlock(toJSON(normalized)) as FilterableWireBlock
      wire.logs ??= []
      wire.transactions ??= []
      wire.traces ??= []
      wire.stateDiffs ??= []

      filter(wire)

      return wire
    },
  }
}

/**
 * Reshape `toJSON(normalized)` to the Portal wire schema. The two differ only on traces (the Squid
 * RPC source applies the same shim):
 *  - the suicide trace tag: normalized `'selfdestruct'` → schema `'suicide'`;
 *  - the reward action field: normalized `action.rewardType` → schema `action.type`.
 * Mutates in place.
 */
export function shimWireBlock(block: any): any {
  const traces = block?.traces
  if (!Array.isArray(traces)) {
    return block
  }

  for (const trace of traces) {
    if (trace.type === 'selfdestruct') {
      trace.type = 'suicide'
      continue
    }

    const rewardAction = trace.type === 'reward' && typeof trace.action === 'object' && trace.action !== null
    if (rewardAction && 'rewardType' in trace.action) {
      trace.action.type = trace.action.rewardType
      delete trace.action.rewardType
    }
  }

  return block
}

/**
 * Match the Portal's empty-block handling: RPC fetches every block of a range, so drop a block left
 * empty by filtering, except the batch's first and last blocks (the last is the progress cursor),
 * and except every block when `includeAllBlocks` is set.
 */
export function dropEmptyBlocks<B extends Pick<FilterableWireBlock, 'logs' | 'transactions' | 'traces' | 'stateDiffs'>>(
  blocks: B[],
  includeAllBlocks = false,
): B[] {
  if (includeAllBlocks) {
    return blocks
  }

  return blocks.filter((b, i) => {
    const boundary = i === 0 || i === blocks.length - 1
    const hasItems = b.logs.length > 0 || b.transactions.length > 0 || b.traces.length > 0 || b.stateDiffs.length > 0

    return boundary || hasItems
  })
}
