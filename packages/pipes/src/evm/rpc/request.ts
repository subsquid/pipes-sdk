import { DataRequest, FieldSelection } from '~/portal-client/query/evm.js'

/**
 * The coarse data types an RPC source must fetch to satisfy a request — drives both the evm-rpc
 * fetch toggles and the capability a health probe must verify. Ported from
 * `@subsquid/evm-processor` `ds-rpc/request.ts`; Pipes' `DataRequest` is already flat, so it is
 * consumed directly.
 */
export interface RequiredData {
  logs: boolean
  receipts: boolean
  traces: boolean
  stateDiffs: boolean
}

export function toRequiredData(req: DataRequest, fields: FieldSelection): RequiredData {
  const txs = transactionsRequested(req)
  const logs = logsRequested(req)
  const receipts = txs && isRequested(TX_RECEIPT_FIELDS, fields.transaction)

  // No `transactions` toggle: the RPC source always fetches full transactions (mapRpcBlock needs
  // them to normalize a block — see `EvmRpcSource`), so deriving whether they're required would be
  // dead — the fetch can't be turned off. `txs` is still needed to decide whether requested
  // *receipt* fields upgrade the fetch to receipts.
  return {
    logs: logs && !receipts,
    receipts,
    traces: tracesRequested(req),
    stateDiffs: stateDiffsRequested(req),
  }
}

function transactionsRequested(req: DataRequest): boolean {
  if (req.transactions?.length) return true
  for (const items of [req.logs, req.traces, req.stateDiffs]) {
    if (items) {
      for (const it of items) {
        if (it.transaction) return true
      }
    }
  }

  return false
}

function logsRequested(req: DataRequest): boolean {
  if (req.logs?.length) return true
  if (req.transactions) {
    for (const tx of req.transactions) if (tx.logs) return true
  }
  if (req.traces) {
    for (const trace of req.traces) if (trace.transactionLogs) return true
  }

  return false
}

function tracesRequested(req: DataRequest): boolean {
  if (req.traces?.length) return true
  if (req.transactions) {
    for (const tx of req.transactions) if (tx.traces) return true
  }
  if (req.logs) {
    for (const log of req.logs) if (log.transactionTraces) return true
  }

  return false
}

function stateDiffsRequested(req: DataRequest): boolean {
  if (req.stateDiffs?.length) return true
  if (req.transactions) {
    for (const tx of req.transactions) if (tx.stateDiffs) return true
  }
  if (req.logs) {
    for (const log of req.logs) if (log.transactionStateDiffs) return true
  }

  return false
}

const TX_RECEIPT_FIELDS: Record<string, true> = {
  gasUsed: true,
  cumulativeGasUsed: true,
  effectiveGasPrice: true,
  contractAddress: true,
  type: true,
  status: true,
  l1Fee: true,
  l1FeeScalar: true,
  l1GasPrice: true,
  l1GasUsed: true,
  l1BaseFeeScalar: true,
  l1BlobBaseFee: true,
  l1BlobBaseFeeScalar: true,
}

function isRequested(set: Record<string, boolean>, selection?: Record<string, boolean>): boolean {
  if (selection == null) return false
  for (const key in selection) {
    if (set[key] && selection[key]) return true
  }

  return false
}
