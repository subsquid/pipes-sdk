import { mapRpcBlock } from '@subsquid/evm-normalization'
import { Block as RpcBlock } from '@subsquid/evm-rpc'
import { toJSON } from '@subsquid/util-internal-json'
import { cast } from '@subsquid/util-internal-validation'

import { DataRequest, FieldSelection, getBlockSchema } from '~/portal-client/query/evm.js'

import { withRequiredFields } from './decode.js'
import { Relations, filterBlock, setUpRelations } from './filter.js'
import { augmentFields, keptByPosition } from './project.js'
import { RequiredData, toRequiredData } from './request.js'
import { shimWireBlock } from './shim.js'

/** A portal-wire-shaped block with its filterable item arrays materialized. */
export type FilterableWireBlock = {
  header: { number: number; hash: string; timestamp?: number }
  logs: unknown[]
  transactions: unknown[]
  traces: unknown[]
  stateDiffs: unknown[]
}

export interface WireBlockMapper {
  /** The coarse per-kind fetch toggles derived from the request — feeds the RPC data request. */
  requiredData: RequiredData
  map(raw: RpcBlock): FilterableWireBlock
}

/**
 * Build the raw-RPC-block → portal-wire-block mapper for one query (fields + request): normalize
 * with the same `mapRpcBlock` the portal's dataset producers use, serialize to the wire shape, and
 * filter the item arrays down to what the request matches.
 *
 * The filter predicates run on a throwaway *decoded* copy (`cast` at the augmented fields — the
 * where-clause fields must be decoded even when not selected for output); the surviving positions
 * are then mapped back onto the wire arrays. Decoded and wire arrays align 1:1 by construction,
 * and position + object identity (never a synthesized key) keeps structurally identical items
 * apart. The wire output keeps every field — the downstream normalize/cast prunes it to the user's
 * selection, exactly as it prunes portal output.
 */
export function createWireBlockMapper(fields: FieldSelection, request: DataRequest): WireBlockMapper {
  // Decode only what *filtering* reads: the structural fields relations are built from, plus the
  // fields the where-clauses evaluate. Deliberately NOT the user's output selection — a receipt-
  // backed output field (`gasUsed`, `status`) is only fetched when transactions are requested, so
  // casting the pre-filter block at the output selection would throw on a perfectly valid query
  // that selects transaction fields while requesting only logs. The wire block keeps every field
  // regardless; the downstream normalize/cast decodes the surviving items at the user's selection.
  const filterFields = augmentFields(withRequiredFields({}), request)
  const schema = getBlockSchema(filterFields)
  const requiredData = toRequiredData(request, fields)

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

      const decoded: any = cast(schema, wire)
      const preLogs = decoded.logs ?? []
      const preTransactions = decoded.transactions ?? []
      const preTraces = decoded.traces ?? []
      const preStateDiffs = decoded.stateDiffs ?? []

      const relations: Relations = setUpRelations(decoded)
      filterBlock(decoded, request, relations)

      wire.logs = keptByPosition(wire.logs, preLogs, decoded.logs ?? [])
      wire.transactions = keptByPosition(wire.transactions, preTransactions, decoded.transactions ?? [])
      wire.traces = keptByPosition(wire.traces, preTraces, decoded.traces ?? [])
      wire.stateDiffs = keptByPosition(wire.stateDiffs, preStateDiffs, decoded.stateDiffs ?? [])

      return wire
    },
  }
}
