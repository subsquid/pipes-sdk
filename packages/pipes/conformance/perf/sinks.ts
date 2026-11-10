import type { ClickHouseClient } from '@clickhouse/client'
import { drizzle } from 'drizzle-orm/node-postgres'
import { bigint, integer, pgTable, primaryKey, text } from 'drizzle-orm/pg-core'
import type pg from 'pg'

import { type RollbackRecord, resolveForkCursor } from '~/core/fork.js'
import { type BlockCursor, type Target, createTarget } from '~/core/index.js'
import type { PortalBatch } from '~/core/portal-source.js'
import { commonAbis } from '~/evm/abi/common.js'
import { evmEventDecoder } from '~/evm/evm-decoder.js'
import { evmPortalStream } from '~/evm/evm-portal-source.js'
import { clickhouseTarget } from '~/targets/clickhouse/index.js'
import { chunkForInsert, drizzleTarget } from '~/targets/drizzle/node-postgres/index.js'

import { now } from './protocol.js'

/** One table, many rows per block: the rollback has to remove every row a block wrote. */
export type Transfer = { block_number: number; log_index: number; value: string }
export type Rows = Transfer[]

const transfers = pgTable(
  'transfers',
  {
    block_number: bigint('block_number', { mode: 'number' }).notNull(),
    log_index: integer('log_index').notNull(),
    value: text('value').notNull(),
  },
  (t) => [primaryKey({ columns: [t.block_number, t.log_index] })],
)

export function postgresDdl(schema: string): string {
  return `CREATE TABLE "${schema}"."transfers" (
    block_number int8 NOT NULL, log_index int4 NOT NULL, value text NOT NULL,
    PRIMARY KEY (block_number, log_index)
  )`
}

/** Collapsing, as the ClickHouse target documents for rollbacks; `block_number` gets the rollback skip index. */
export function clickhouseDdl(database: string): string {
  return `CREATE TABLE "${database}"."transfers" (block_number UInt64, log_index UInt32, value String, sign Int8)
    ENGINE = CollapsingMergeTree(sign) ORDER BY (block_number, log_index)`
}

/** What the bench sees of a run: a commit per batch, and the three steps of every fork. */
export type Witness = {
  /** The target finished `cursor`'s batch: it came back for the next one. */
  commit(commit: { cursor: BlockCursor; blocks: number; time: number; durationMs: number }): void
  /** A 409 reached the target: the start of its rollback. `canonical` is its `previousBlocks`. */
  forkDetected(time: number, canonical: BlockCursor[]): void
  /** The target returned the common ancestor. */
  rolledBack(ancestor: BlockCursor | null, time: number): void
}

/**
 * Times the target's commits without knowing its store: every target here commits a batch before
 * it reads the next one, so asking for the next batch marks the previous one committed.
 */
function witnessed<T>(target: Target<T>, witness: Witness): Target<T> {
  async function* timed(source: AsyncIterable<PortalBatch<T>>) {
    for await (const batch of source) {
      const handedAt = now()
      yield batch
      const committedAt = now()
      witness.commit({
        cursor: batch.ctx.stream.state.current,
        blocks: batch.ctx.batch.blocksCount,
        time: committedAt,
        durationMs: committedAt - handedAt,
      })
    }
  }

  return {
    ...target,
    write: (writer) => target.write({ ...writer, read: (state, options) => timed(writer.read(state, options)) }),
    resolveFork: async (canonical) => {
      witness.forkDetected(now(), canonical)
      const ancestor = await target.resolveFork!(canonical)
      witness.rolledBack(ancestor, now())

      return ancestor
    },
  }
}

/**
 * A fork-aware target that keeps rows in memory: the pipe's own cost with no store behind it. It
 * keeps the rollback chain of the last batches and resolves forks the way the stores do.
 */
export function memoryTarget(): Target<Rows> {
  const rows: Transfer[] = []
  let history: RollbackRecord[] = []

  return createTarget<Rows>({
    write: async ({ read }) => {
      for await (const { data, ctx } of read()) {
        for (const row of data) rows.push(row)

        const finalized = ctx.stream.head.finalized
        history.push({ rollbackChain: ctx.stream.state.rollbackChain, finalized })
        // Records whose blocks are all finalized can never be rolled back to.
        history = history.filter((record) => record.rollbackChain.some((b) => b.number > (finalized?.number ?? -1)))
      }
    },
    resolveFork: async (canonical) => {
      const ancestor = await resolveForkCursor([...history].reverse(), canonical)
      if (!ancestor) return null

      while (rows.length && rows[rows.length - 1].block_number > ancestor.number) rows.pop()
      history = history
        .map((record) => ({
          ...record,
          rollbackChain: record.rollbackChain.filter((b) => b.number <= ancestor.number),
        }))
        .filter((record) => record.rollbackChain.length > 0)

      return ancestor
    },
  })
}

export function postgresTarget({ pool, schema }: { pool: pg.Pool; schema: string }): Target<Rows> {
  return drizzleTarget<Rows>({
    db: drizzle(pool),
    tables: [transfers],
    settings: { state: { schema } },
    onData: async ({ tx, data }) => {
      for (const values of chunkForInsert(data)) {
        await tx.insert(transfers).values(values)
      }
    },
  })
}

export function clickhouseSink({ client }: { client: ClickHouseClient }): Target<Rows> {
  return clickhouseTarget<Rows>({
    client,
    onData: async ({ store, data }) => {
      if (!data.length) return

      await store.insert({
        table: 'transfers',
        values: data.map((row) => ({ ...row, sign: 1 })),
        format: 'JSONEachRow',
      })
    },
    onRollback: async ({ store, safeCursor }) => {
      await store.removeRowsAfter(safeCursor, { tables: ['transfers'] })
    },
  })
}

/** Streams WETH transfers over `range` from `url` into `target`, timed by `witness`. */
export function runPipe({
  id,
  url,
  range,
  target,
  witness,
  profiler,
}: {
  id: string
  url: string
  range: { from: number; to: number }
  target: Target<Rows>
  witness: Witness
  profiler: boolean
}): Promise<void> {
  const outputs = evmEventDecoder({
    range,
    events: { transfers: commonAbis.erc20.events.Transfer },
  }).pipe(
    (decoded): Rows =>
      decoded.transfers.map((t) => ({
        block_number: t.block.number,
        log_index: t.rawEvent.logIndex,
        value: t.event.value.toString(),
      })),
  )

  return evmPortalStream({ id, portal: { url }, outputs, logger: 'error', profiler }).pipeTo(
    witnessed(target, witness),
    { shutdown: false },
  )
}
