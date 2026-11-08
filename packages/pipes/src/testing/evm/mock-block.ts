import {
  type Abi,
  type AbiEvent,
  type ContractEventName,
  type GetEventArgs,
  encodeAbiParameters,
  encodeEventTopics,
  keccak256,
  toHex,
} from 'viem'

import type { Hex } from '~/portal-client/query/common.js'
import type { BlockHeaderFields, LogFields, TransactionFields } from '~/portal-client/query/evm.js'

type PortalLog = Pick<LogFields, 'logIndex' | 'transactionIndex' | 'transactionHash' | 'address' | 'data' | 'topics'>

type PortalTransaction = Pick<TransactionFields, 'transactionIndex' | 'hash' | 'from' | 'to' | 'sighash'>

export type PortalBlock = {
  header: Pick<BlockHeaderFields, 'number' | 'hash' | 'parentHash' | 'timestamp'>
  transactions: PortalTransaction[]
  logs: PortalLog[]
}

/**
 * Encodes event arguments into the portal log format.
 *
 * @example
 * ```ts
 * const log = encodeEvent({
 *   abi: erc20Abi,
 *   eventName: 'Transfer',
 *   address: '0xA0b8...3606eB48',
 *   args: { from: '0x...', to: '0x...', value: 100n },
 * })
 * ```
 */
export function encodeEvent<
  const TAbi extends Abi | readonly unknown[],
  TEventName extends ContractEventName<TAbi> = ContractEventName<TAbi>,
>({
  abi,
  eventName,
  address,
  args,
}: {
  abi: TAbi
  eventName: TEventName
  address: Hex
  args?: GetEventArgs<TAbi, TEventName extends string ? TEventName : string, { IndexedOnly: false; Required: true }>
}): { address: Hex; topics: Hex[]; data: Hex } {
  const topics = encodeEventTopics({ abi: abi as Abi, eventName: eventName as string, args } as any) as Hex[]

  const abiEvent = (abi as readonly AbiEvent[]).find((item) => item.type === 'event' && item.name === eventName) as
    | AbiEvent
    | undefined

  if (!abiEvent) {
    throw new Error(`Event "${eventName}" not found in ABI`)
  }

  const nonIndexedInputs = abiEvent.inputs.filter((input) => !input.indexed)

  let data: Hex = '0x'
  if (nonIndexedInputs.length > 0 && args) {
    const values = nonIndexedInputs.map((input) => {
      const value = (args as Record<string, unknown>)[input.name!]
      if (value === undefined) {
        throw new Error(`Missing value for non-indexed parameter "${input.name}"`)
      }
      return value
    })
    data = encodeAbiParameters(nonIndexedInputs as any, values as any) as Hex
  }

  return { address, topics, data }
}

type MockTransaction = {
  from?: Hex
  to?: Hex
  logs: { address: Hex; topics: Hex[]; data: Hex }[]
}

type MockBlockOptions = {
  /** Override the auto-generated block number */
  number?: number
  /** Override the auto-generated timestamp */
  timestamp?: number
  /** Override the auto-generated hash */
  hash?: Hex
  /** Override the auto-generated parentHash */
  parentHash?: Hex
  /** Transactions with their logs */
  transactions?: MockTransaction[]
}

let blockCounter = 0

/**
 * Plausible epoch seconds (2023-11-14) that mock blocks count up from. A block timestamp
 * outside the plausibility window is dropped as untrustworthy by `toEpochSeconds` — a cursor
 * round-trip through a class-T sync column would lose it, so fixtures must stay inside.
 */
export const MOCK_GENESIS_TIMESTAMP = 1_700_000_000

/**
 * Creates a block in portal format with auto-generated metadata.
 * Block number, timestamp, hash, and parentHash are generated sequentially
 * unless explicitly overridden.
 *
 * @example
 * ```ts
 * const block = mockBlock({
 *   transactions: [
 *     { logs: [event1, event2] },
 *     { logs: [event3] },
 *   ],
 * })
 * ```
 */
export function mockBlock(options: MockBlockOptions = {}): PortalBlock {
  blockCounter++
  const number = options.number ?? blockCounter
  const timestamp = options.timestamp ?? MOCK_GENESIS_TIMESTAMP + number
  const hash = options.hash ?? (`0x${number.toString(16).padStart(64, '0')}` as Hex)
  const parentHash = options.parentHash ?? (`0x${(number - 1).toString(16).padStart(64, '0')}` as Hex)

  const transactions: PortalTransaction[] = []
  const logs: PortalLog[] = []

  const txs = options.transactions ?? []
  for (let txIndex = 0; txIndex < txs.length; txIndex++) {
    const tx = txs[txIndex]
    const txHash = keccak256(toHex(`${hash}:tx:${txIndex}`)) as Hex

    transactions.push({
      transactionIndex: txIndex,
      hash: txHash,
      from: tx.from ?? '0x0000000000000000000000000000000000000001',
      to: tx.to,
      sighash: '0x00000000',
    })

    for (let logIdx = 0; logIdx < tx.logs.length; logIdx++) {
      const log = tx.logs[logIdx]
      logs.push({
        logIndex: logs.length,
        transactionIndex: txIndex,
        transactionHash: txHash,
        address: log.address,
        topics: log.topics,
        data: log.data,
      })
    }
  }

  return {
    header: { number, hash, parentHash, timestamp },
    transactions,
    logs,
  }
}

/**
 * Resets the internal block counter used by mockBlock.
 * Call this in beforeEach to get deterministic block numbers across tests.
 */
export function resetMockBlockCounter() {
  blockCounter = 0
}
