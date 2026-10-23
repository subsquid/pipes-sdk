import { type BlockCursor, type Logger } from '~/core/index.js'
import { commonAbis } from '~/evm/abi/common.js'
import { evmEventDecoder } from '~/evm/evm-decoder.js'
import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { encodeEvent } from '~/testing/evm/index.js'

import type { OracleRow } from '../oracle/reference-model.js'

/**
 * What every scenario streams: WETH transfers and approvals, decoded into two tables, whatever
 * sink they go to. This module holds nothing of vitest, so a child process can run the pipe.
 */
export const PIPE_ID = 'ct-s1'

const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as const
const ALICE = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' as const
const BOB = '0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f' as const

/** Two tables, so one checkpoint publishes two units and a crash can fall between them. */
export const TABLE_NAMES = ['transfers', 'approvals'] as const
export type TableName = (typeof TABLE_NAMES)[number]

export type Erc20Row = { blockNumber: number; value: string }

export function numbers(from: number, to: number): number[] {
  return Array.from({ length: to - from + 1 }, (_, i) => from + i)
}

/** Rows in the order the store reads them: table by table in declaration order, blocks ascending. */
export function inStoreOrder(rows: readonly OracleRow[]): OracleRow[] {
  const rank = (row: OracleRow) => TABLE_NAMES.indexOf(row.table as TableName)

  return [...rows].sort((a, b) => rank(a) - rank(b) || a.block - b.block)
}

/** The logs of a block that holds a WETH transfer of `transfer` and, when given, an approval of `approval`. */
export function erc20Logs({ transfer, approval }: { transfer: bigint; approval?: bigint }) {
  const erc20 = commonAbis.erc20.abi
  const logs = [
    encodeEvent({ abi: erc20, eventName: 'Transfer', address: WETH, args: { from: ALICE, to: BOB, value: transfer } }),
  ]

  if (approval !== undefined) {
    logs.push(
      encodeEvent({
        abi: erc20,
        eventName: 'Approval',
        address: WETH,
        args: { owner: ALICE, spender: BOB, value: approval },
      }),
    )
  }

  return logs
}

/** What the pipe holds when a batch reaches the transformer. */
export type Observation = {
  /** The batch's cursor: its last block. */
  current: BlockCursor
  /** The finalized head the pipe holds; absent on a dataset without finality. */
  finalized: BlockCursor | undefined
}

/**
 * The source and transform of the ERC-20 scenarios over `range`, whatever sink they stream into.
 * `observe` sees every batch before the sink does, and fails the run by throwing.
 */
export function erc20Stream({
  range,
  url,
  logger,
  observe,
}: {
  range: { from: number; to: number }
  url: string
  logger: Logger
  observe: (observation: Observation) => void
}) {
  const outputs = evmEventDecoder({
    range,
    events: { transfers: commonAbis.erc20.events.Transfer, approvals: commonAbis.erc20.events.Approval },
  }).pipe((decoded, ctx): Record<TableName, Erc20Row[]> => {
    observe({ current: ctx.stream.state.current, finalized: ctx.stream.head.finalized })

    return {
      transfers: decoded.transfers.map((transfer) => ({
        blockNumber: transfer.block.number,
        value: transfer.event.value.toString(),
      })),
      approvals: decoded.approvals.map((approval) => ({
        blockNumber: approval.block.number,
        value: approval.event.value.toString(),
      })),
    }
  })

  return evmPortalStream({
    id: PIPE_ID,
    // Small reads keep batches small, so the run takes many commits, not one.
    portal: { url, maxBytes: 1 },
    outputs,
    logger,
  })
}
