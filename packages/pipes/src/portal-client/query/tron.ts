import {
  ANY,
  BOOLEAN,
  NAT,
  STRING,
  ValidationFailure,
  Validator,
  array,
  object,
  option,
  withDefault,
} from '@subsquid/util-internal-validation'

import {
  type ObjectValidatorShape,
  type PortalQuery,
  type Select,
  type Selected,
  type Selector,
  type Simplify,
  type Trues,
  project,
} from './common.js'

// int64 amounts, sent as decimal strings that can be negative (e.g. `feeLimit: "-18395898"`).
// Unsigned `BIG_NAT` would reject the negatives and halt the stream.
const BIG_INT: Validator<bigint, string> = {
  cast(value) {
    return typeof value === 'string' && /^-?\d+$/.test(value)
      ? BigInt(value)
      : new ValidationFailure(value, '{value} is not a string representing an integer')
  },
  validate(value) {
    return typeof value === 'string' && /^-?\d+$/.test(value)
      ? undefined
      : new ValidationFailure(value, '{value} is not a string representing an integer')
  },
  phantom() {
    return '0'
  },
}

// int64 ms timestamps served as JSON numbers; the portal emits garbage above 2^53 that
// `JSON.parse` has already rounded to a float, so tolerate any integer instead of `NAT`.
const TIMESTAMP_MS: Validator<number, number> = {
  cast(value) {
    return typeof value === 'number' && Number.isInteger(value)
      ? value
      : new ValidationFailure(value, '{value} is not a millisecond timestamp')
  },
  validate(value) {
    return typeof value === 'number' && Number.isInteger(value)
      ? undefined
      : new ValidationFailure(value, '{value} is not a millisecond timestamp')
  },
  phantom() {
    return 0
  },
}

// TRON portal hex strings come WITHOUT the `0x` prefix EVM uses — addresses are
// 21-byte hex starting with `41` (e.g. `41a614f803b6...`), block/tx hashes and
// log topics are bare hex too. We validate them as plain strings rather than via
// the shared `BYTES` validator (which requires `0x`).
type Hex = string

// https://github.com/subsquid/data/blob/master/crates/query/src/query/tron.rs

/** Per-contract execution result, e.g. `{ contractRet: 'SUCCESS' }`. Raw JSON passthrough. */
export type TransactionResult = {
  contractRet?: string
}

/** Value transferred by an internal transaction. Raw JSON passthrough — amounts stay decimal strings. */
export type CallValueInfo = {
  callValue?: string | null
  tokenId?: string | null
}

/** Contract call payload; `value` shape depends on the contract `type`. Raw JSON passthrough. */
export type TransactionParameter = {
  value: Record<string, any>
  type_url: string
}

export type BlockHeaderFields = {
  number: number
  hash: Hex
  parentHash: Hex
  txTrieRoot: Hex
  version?: number
  /** Unix timestamp in milliseconds. */
  timestamp: number
  witnessAddress: Hex
  witnessSignature?: Hex
}

export type TransactionFields = {
  transactionIndex: number
  hash: Hex
  ret?: TransactionResult[]
  signature?: Hex[]
  type: string
  /** Contract call parameter; shape depends on `type`. Raw JSON passthrough. */
  parameter: TransactionParameter
  permissionId?: number
  refBlockBytes?: Hex
  refBlockHash?: Hex
  feeLimit?: bigint
  /** Unix timestamp in milliseconds. */
  expiration?: number
  /** Unix timestamp in milliseconds. */
  timestamp?: number
  rawDataHex: Hex
  fee?: bigint
  contractResult?: Hex
  contractAddress?: Hex
  resMessage?: string
  withdrawAmount?: bigint
  unfreezeAmount?: bigint
  withdrawExpireAmount?: bigint
  /** Map of unfreeze timestamp to amount. Raw JSON passthrough. */
  cancelUnfreezeV2Amount?: Record<string, string>
  result?: string
  energyFee?: bigint
  energyUsage?: bigint
  energyUsageTotal?: bigint
  netUsage?: bigint
  netFee?: bigint
  originEnergyUsage?: bigint
  energyPenaltyTotal?: bigint
}

export type LogFields = {
  transactionIndex: number
  logIndex: number
  address: Hex
  data?: Hex
  topics?: Hex[]
}

export type InternalTransactionFields = {
  transactionIndex: number
  internalTransactionIndex: number
  hash: Hex
  callerAddress: Hex
  transferToAddress?: Hex
  callValueInfo: CallValueInfo[]
  note: Hex
  rejected?: boolean
  extra?: Hex
}

export type BlockHeaderFieldSelection = Selector<keyof BlockHeaderFields>
export type BlockHeader<F extends BlockHeaderFieldSelection = Trues<BlockHeaderFieldSelection>> = Select<
  BlockHeaderFields,
  F
>

export type TransactionFieldSelection = Selector<keyof TransactionFields>
export type Transaction<F extends TransactionFieldSelection = Trues<TransactionFieldSelection>> = Select<
  TransactionFields,
  F
>

export type LogFieldSelection = Selector<keyof LogFields>
export type Log<F extends LogFieldSelection = Trues<LogFieldSelection>> = Select<LogFields, F>

export type InternalTransactionFieldSelection = Selector<keyof InternalTransactionFields>
export type InternalTransaction<
  F extends InternalTransactionFieldSelection = Trues<InternalTransactionFieldSelection>,
> = Select<InternalTransactionFields, F>

export type FieldSelection = {
  block?: BlockHeaderFieldSelection
  transaction?: TransactionFieldSelection
  log?: LogFieldSelection
  internalTransaction?: InternalTransactionFieldSelection
}

export type TransactionRequest = {
  type?: string[]
  logs?: boolean
  internalTransactions?: boolean
}

export type TransferTransactionRequest = {
  owner?: Hex[]
  to?: Hex[]
  logs?: boolean
  internalTransactions?: boolean
}

export type TransferAssetTransactionRequest = {
  owner?: Hex[]
  to?: Hex[]
  asset?: string[]
  logs?: boolean
  internalTransactions?: boolean
}

export type TriggerSmartContractTransactionRequest = {
  owner?: Hex[]
  contract?: Hex[]
  sighash?: Hex[]
  logs?: boolean
  internalTransactions?: boolean
}

export type LogRequest = {
  address?: Hex[]
  topic0?: Hex[]
  topic1?: Hex[]
  topic2?: Hex[]
  topic3?: Hex[]
  transaction?: boolean
}

export type InternalTransactionRequest = {
  caller?: Hex[]
  transferTo?: Hex[]
  transaction?: boolean
}

export type DataRequest = {
  includeAllBlocks?: boolean
  transactions?: TransactionRequest[]
  transferTransactions?: TransferTransactionRequest[]
  transferAssetTransactions?: TransferAssetTransactionRequest[]
  triggerSmartContractTransactions?: TriggerSmartContractTransactionRequest[]
  logs?: LogRequest[]
  internalTransactions?: InternalTransactionRequest[]
}

export type Query<F extends FieldSelection = FieldSelection> = Simplify<
  PortalQuery & {
    type: 'tron'
    fields: F
  } & DataRequest
>

export type Block<F extends FieldSelection> = Simplify<{
  header: BlockHeader<Selected<F, 'block'>>
  transactions: Transaction<Selected<F, 'transaction'>>[]
  logs: Log<Selected<F, 'log'>>[]
  internalTransactions: InternalTransaction<Selected<F, 'internalTransaction'>>[]
}>

export function getBlockSchema<F extends FieldSelection>(fields: F): Validator<Block<F>, unknown> {
  const header = object(project(BlockHeaderShape, fields.block))
  const transaction = object(project(TransactionShape, fields.transaction))
  const log = object(project(LogShape, fields.log))
  const internalTransaction = object(project(InternalTransactionShape, fields.internalTransaction))

  return object({
    header,
    transactions: withDefault([], array(transaction)),
    logs: withDefault([], array(log)),
    internalTransactions: withDefault([], array(internalTransaction)),
  }) as Validator<Block<F>, unknown>
}

const BlockHeaderShape: ObjectValidatorShape<BlockHeaderFields> = {
  number: NAT,
  hash: STRING,
  parentHash: STRING,
  txTrieRoot: STRING,
  version: option(NAT),
  timestamp: TIMESTAMP_MS,
  witnessAddress: STRING,
  witnessSignature: option(STRING),
}

const TransactionShape: ObjectValidatorShape<TransactionFields> = {
  transactionIndex: NAT,
  hash: STRING,
  ret: option(ANY),
  signature: option(array(STRING)),
  type: STRING,
  parameter: ANY,
  permissionId: option(NAT),
  refBlockBytes: option(STRING),
  refBlockHash: option(STRING),
  // int64 amounts -> signed BIG_INT (decimal strings, not hex QTY); int64 ms -> TIMESTAMP_MS.
  feeLimit: option(BIG_INT),
  expiration: option(TIMESTAMP_MS),
  timestamp: option(TIMESTAMP_MS),
  rawDataHex: STRING,
  fee: option(BIG_INT),
  contractResult: option(STRING),
  contractAddress: option(STRING),
  resMessage: option(STRING),
  withdrawAmount: option(BIG_INT),
  unfreezeAmount: option(BIG_INT),
  withdrawExpireAmount: option(BIG_INT),
  cancelUnfreezeV2Amount: option(ANY),
  result: option(STRING),
  energyFee: option(BIG_INT),
  energyUsage: option(BIG_INT),
  energyUsageTotal: option(BIG_INT),
  netUsage: option(BIG_INT),
  netFee: option(BIG_INT),
  originEnergyUsage: option(BIG_INT),
  energyPenaltyTotal: option(BIG_INT),
}

const LogShape: ObjectValidatorShape<LogFields> = {
  transactionIndex: NAT,
  logIndex: NAT,
  address: STRING,
  data: option(STRING),
  topics: option(array(STRING)),
}

const InternalTransactionShape: ObjectValidatorShape<InternalTransactionFields> = {
  transactionIndex: NAT,
  internalTransactionIndex: NAT,
  hash: STRING,
  callerAddress: STRING,
  transferToAddress: option(STRING),
  callValueInfo: ANY,
  note: STRING,
  rejected: option(BOOLEAN),
  extra: option(STRING),
}
