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

export function toRequiredData(
  { transactions = [], logs = [], traces = [], stateDiffs = [] }: DataRequest,
  fields: FieldSelection,
): RequiredData {
  const hasTransactions =
    transactions.length > 0 ||
    logs.some((log) => log.transaction) ||
    traces.some((trace) => trace.transaction) ||
    stateDiffs.some((diff) => diff.transaction)
  const receipts = hasTransactions && TX_RECEIPT_FIELDS.some((field) => fields.transaction?.[field])

  // Full transactions are always fetched for normalization; receipts also supply logs.
  return {
    logs:
      !receipts &&
      (logs.length > 0 || transactions.some((tx) => tx.logs) || traces.some((trace) => trace.transactionLogs)),
    receipts,
    traces:
      traces.length > 0 || transactions.some((tx) => tx.traces) || logs.some((log) => log.transactionTraces),
    stateDiffs:
      stateDiffs.length > 0 || transactions.some((tx) => tx.stateDiffs) || logs.some((log) => log.transactionStateDiffs),
  }
}

const TX_RECEIPT_FIELDS = [
  'gasUsed',
  'cumulativeGasUsed',
  'effectiveGasPrice',
  'contractAddress',
  'type',
  'status',
  'logsBloom',
  'blobGasUsed',
  'blobGasPrice',
  'l1Fee',
  'l1FeeScalar',
  'l1GasPrice',
  'l1GasUsed',
  'l1BaseFeeScalar',
  'l1BlobBaseFee',
  'l1BlobBaseFeeScalar',
] as const satisfies readonly (keyof NonNullable<FieldSelection['transaction']>)[]
