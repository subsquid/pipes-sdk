import { type BlockCursor, type Logger } from '~/core/index.js'
import { commonAbis } from '~/evm/abi/common.js'
import { evmEventDecoder } from '~/evm/evm-decoder.js'
import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { type ParquetTable, parquetTarget } from '~/targets/parquet/index.js'
import { encodeEvent, mockBlock } from '~/testing/evm/index.js'

import { type Cursor, type OracleRow, type OracleState, ReferenceModel } from './reference-model.js'

/**
 * S1: steady backfill over deep history, healthy portal (spec/11). The range ends on blocks with
 * no rows. This module holds the scenario and the pipe and nothing of vitest, so a child process
 * can run the same pipe (`kill-run.ts`).
 */
export const S1 = { from: 0, to: 59, lastRow: 49, blocksPerResponse: 7 }
export const RANGES = [{ from: S1.from, to: S1.to }]
export const PIPE_ID = 'ct-s1'

const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as const
const ALICE = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' as const
const BOB = '0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f' as const

/** Approval values sit apart from transfer values, so a row written to the wrong table shows. */
const APPROVAL_OFFSET = 1000

/** Two tables, so one checkpoint publishes two units and a crash can fall between them. */
export const TABLE_NAMES = ['transfers', 'approvals'] as const
export type TableName = (typeof TABLE_NAMES)[number]

const COLUMNS: ParquetTable['schema'] = {
  blockNumber: { type: 'INT64' },
  value: { type: 'UTF8' },
}
const TABLES: ParquetTable[] = TABLE_NAMES.map((table) => ({ table, schema: COLUMNS }))

type Row = { blockNumber: number; value: string }

/** A sparse table: one transfer every fourth block up to `lastRow`, its value the block number. */
const hasTransfer = (number: number) => number % 4 === 1 && number <= S1.lastRow

/** A sparser one: an approval on every other transfer block, in the same block as the transfer. */
const hasApproval = (number: number) => number % 8 === 1 && number <= S1.lastRow

export function numbers(from: number, to: number): number[] {
  return Array.from({ length: to - from + 1 }, (_, i) => from + i)
}

/** The rows S1 stores for a block, as the store reads them back: INT64 as bigint. */
export function rowsAt(number: number): OracleRow[] {
  const rows: OracleRow[] = []

  if (hasTransfer(number)) {
    rows.push({ table: 'transfers', block: number, value: { blockNumber: BigInt(number), value: String(number) } })
  }

  if (hasApproval(number)) {
    const value = String(number + APPROVAL_OFFSET)
    rows.push({ table: 'approvals', block: number, value: { blockNumber: BigInt(number), value } })
  }

  return rows
}

/** Rows in the order the store reads them: table by table in declaration order, blocks ascending. */
export function inStoreOrder(rows: readonly OracleRow[]): OracleRow[] {
  const rank = (row: OracleRow) => TABLE_NAMES.indexOf(row.table as TableName)

  return [...rows].sort((a, b) => rank(a) - rank(b) || a.block - b.block)
}

/** Every row S1 must leave in the store, from the scenario alone. */
export const S1_ROWS: OracleRow[] = inStoreOrder(numbers(S1.from, S1.to).flatMap(rowsAt))

export function s1Blocks() {
  const erc20 = commonAbis.erc20.abi

  return numbers(S1.from, S1.to)
    .filter(hasTransfer)
    .map((number) => {
      const transfer = encodeEvent({
        abi: erc20,
        eventName: 'Transfer',
        address: WETH,
        args: { from: ALICE, to: BOB, value: BigInt(number) },
      })
      const approval = encodeEvent({
        abi: erc20,
        eventName: 'Approval',
        address: WETH,
        args: { owner: ALICE, spender: BOB, value: BigInt(number + APPROVAL_OFFSET) },
      })
      const logs = hasApproval(number) ? [transfer, approval] : [transfer]

      return mockBlock({ number, transactions: [{ logs }] })
    })
}

/** The chain's cursor for a block, as a batch carries it: given and made-up blocks share `mockBlock`'s header. */
export function cursorAt(number: number): Cursor {
  const { hash, timestamp } = mockBlock({ number }).header

  return { number, hash, timestamp }
}

/** A finalized head as the portal reports it: number and hash, no timestamp. */
export function reportedAt(number: number): Cursor {
  return { number, hash: cursorAt(number).hash }
}

/**
 * The state S1 must leave in the store: the cursor at the range end and the last floor the portal
 * reported. Class K keeps no rollback chain (WP-47).
 */
export function s1EndState(lastReport: number | undefined): OracleState {
  return {
    current: cursorAt(S1.to),
    finalized: lastReport === undefined ? undefined : reportedAt(lastReport),
    rollbackChain: [],
  }
}

/** The oracle for S1, as class K: the Parquet sink requires finalized-only delivery. */
export function s1Oracle(): ReferenceModel {
  return new ReferenceModel({
    durability: 'K',
    ranges: RANGES,
    transform: (block) => rowsAt(block.number),
  })
}

/** What the pipe holds when a batch reaches the transformer. */
export type Observation = {
  /** The batch's cursor: its last block. */
  current: BlockCursor
  /** The finalized head the pipe holds; absent on a dataset without finality. */
  finalized: BlockCursor | undefined
}

/**
 * Streams S1 from the portal at `url` into a Parquet store in `dir`. `observe` sees every batch
 * before the store does, and fails the run by throwing.
 */
export function streamS1({
  url,
  dir,
  logger,
  observe,
}: {
  url: string
  dir: string
  logger: Logger
  observe: (observation: Observation) => void
}): Promise<void> {
  const outputs = evmEventDecoder({
    range: { from: S1.from, to: S1.to },
    events: { transfers: commonAbis.erc20.events.Transfer, approvals: commonAbis.erc20.events.Approval },
  }).pipe((decoded, ctx): Record<TableName, Row[]> => {
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
    // Small reads keep batches small, so the run takes many checkpoints, not one.
    portal: { url, maxBytes: 1 },
    outputs,
    logger,
  }).pipeTo(
    parquetTarget({
      dir,
      tables: TABLES,
      // Every batch that holds a row closes its window. A row count is exact; the engine reports bytes late.
      settings: { rollover: { maxRows: 1 } },
      onData: ({ store, data }) => {
        for (const table of TABLE_NAMES) {
          store.insert(table, data[table])
        }
      },
    }),
  )
}
