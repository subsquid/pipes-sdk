import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { test } from 'vitest'

import type { BatchContext, BlockCursor } from '~/core/index.js'
import { loadSqlite } from '~/drivers/sqlite/sqlite.js'
import { evmQuery } from '~/evm/evm-query-builder.js'
import { mockMetricsServer, testLogger } from '~/testing/index.js'

import type { DrainResult, PublishMessage, Publisher } from './publisher.js'

/**
 * Shared scaffolding for the PubSub target suites: a publisher that records what would go on
 * the wire, a keyed block decoder, and a synthetic batch context for the crash-matrix tests
 * that drive `target.write()` without a portal.
 *
 * @internal
 */

export type PublishedMessage = {
  topic: string
  orderingKey: string
  attributes: Record<string, string>
  payload: string
}

export class FakePublisher implements Publisher {
  readonly published: PublishedMessage[] = []
  readonly setupCalls: string[][] = []
  /** Return an error to fail that publish; the row (and everything behind it) stays queued. */
  failOn?: (row: PublishMessage, index: number) => Error | undefined
  /** Runs inside the measured publish window, so a fake-timer test can give it a real duration. */
  onDrain?: (rows: PublishMessage[]) => void
  closed = false

  async setup(topics: string[]): Promise<void> {
    this.setupCalls.push(topics)
  }

  async drain(rows: PublishMessage[]): Promise<DrainResult> {
    const confirmed: number[] = []
    let bytes = 0

    this.onDrain?.(rows)

    for (let i = 0; i < rows.length; i++) {
      const failure = this.failOn?.(rows[i], i)
      if (failure) {
        return { confirmed, bytes, error: failure }
      }

      this.published.push({
        topic: rows[i].topic,
        orderingKey: rows[i].orderingKey,
        attributes: rows[i].attributes,
        payload: new TextDecoder().decode(rows[i].payload),
      })
      confirmed.push(rows[i].rowId)
      bytes += rows[i].payload.byteLength
    }

    return { confirmed, bytes }
  }

  async close(): Promise<void> {
    this.closed = true
  }

  operations(topic?: string): { op: string; id?: string; payload: string; seq: number }[] {
    return this.published
      .filter((message) => !topic || message.topic === topic)
      .map((message) => {
        const payload = JSON.parse(message.payload)

        return {
          op: payload._CHANGE_TYPE === 'UPSERT' ? 'upsert' : 'delete',
          id: payload._id,
          payload: message.payload,
          seq: Number.parseInt(payload._CHANGE_SEQUENCE_NUMBER, 16),
        }
      })
  }
}

const tempDirs: string[] = []

export function tempStatePath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'pubsub-target-'))
  tempDirs.push(dir)

  return join(dir, 'state.sqlite')
}

export function cleanupTempState(): void {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true })
  }
}

/** A state path in a fresh directory, removed when the test ends. */
export const withStatePath = test.extend<{ statePath: string }>({
  statePath: async ({}, use) => {
    const dir = mkdtempSync(join(tmpdir(), 'pubsub-state-'))
    await use(join(dir, 'state.sqlite'))
    rmSync(dir, { recursive: true, force: true })
  },
})

/**
 * Rewrites a schema-4 state file the way a schema-3 build (`1.0.0-beta.4`) left it. Nothing else
 * differs: the outbox's `kind` column, the `fork_epoch` counter, and a wire config keyed by route
 * kind with no producer-wide attributes.
 */
export async function downgradeToSchemaV3(
  path: string,
  options: { signalRowIds?: number[]; signalRoutes?: string[] } = {},
): Promise<void> {
  const db = await loadSqlite({ path })
  const setMeta = (key: string, value: string) =>
    db.exec('INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value', [
      key,
      value,
    ])

  try {
    db.exec("ALTER TABLE outbox ADD COLUMN kind TEXT NOT NULL DEFAULT 'cdc'")
    for (const rowId of options.signalRowIds ?? []) {
      db.exec("UPDATE outbox SET kind = 'signal', op = NULL, id = NULL WHERE row_id = ?", [rowId])
    }

    setMeta('fork_epoch', '0')

    const wireConfig = db.get<{ value: string }>("SELECT value FROM meta WHERE key = 'wire_config'")?.value
    if (wireConfig !== undefined) {
      const [namespace, uidAttribute, messageOrdering, , encoders] = JSON.parse(wireConfig) as [
        string,
        boolean,
        boolean,
        string,
        [string, string][],
      ]
      const v3Encoders = [
        ...encoders.map(([stream, kind]) => [`cdc:${stream}`, kind]),
        ...(options.signalRoutes ?? []).map((stream) => [`signal:${stream}`, 'signal']),
      ]

      setMeta('wire_config', JSON.stringify([namespace, uidAttribute, messageOrdering, v3Encoders]))
    }

    setMeta('schema_version', '3')
  } finally {
    db.close()
  }
}

/** What a closed state file holds on disk. Fails while a producer still holds its lock. */
export async function inspectStateFile(path: string) {
  const db = await loadSqlite({ path })

  try {
    const meta = (key: string) => db.get<{ value: string }>('SELECT value FROM meta WHERE key = ?', [key])?.value
    const outboxColumns = db.all<{ name: string }>('PRAGMA table_info(outbox)').map((column) => column.name)

    return { schemaVersion: meta('schema_version'), forkEpoch: meta('fork_epoch'), outboxColumns }
  } finally {
    db.close()
  }
}

/** Block headers under a named stream key, so `topics` has something to route. */
export function keyedBlockDecoder(range: { from: number; to?: number }) {
  return evmQuery()
    .addRange(range)
    .addFields({ block: { number: true, hash: true, timestamp: true } })
    .build()
    .pipe((data) => ({ blocks: data.flatMap((b) => b.header) }))
}

export type TestBlock = { number: number; hash: string; timestamp: number }

export function portalBlocks(numbers: number[], suffix = ''): { header: TestBlock }[] {
  return numbers.map((number) => ({
    header: { number, hash: `0x${number}${suffix}`, timestamp: 1_700_000_000 + number },
  }))
}

export function makeBatchContext({
  current,
  finalized,
  rollbackChain,
  latest,
  lastBlockReceivedAt,
  metrics = mockMetricsServer().server.metrics,
}: {
  current: BlockCursor
  finalized?: BlockCursor
  rollbackChain?: BlockCursor[]
  latest?: BlockCursor
  lastBlockReceivedAt?: Date
  metrics?: BatchContext['metrics']
}): BatchContext {
  const profilerStub: Record<string, unknown> = {
    start: () => profilerStub,
    measure: async (_: unknown, fn: (span: unknown) => unknown) => fn(profilerStub),
    end: () => {},
  }

  return {
    id: 'test-pipe',
    logger: testLogger(),
    profiler: profilerStub,
    metrics,
    stream: {
      dataset: { dataset: 'mock-dataset' },
      state: { current, rollbackChain: rollbackChain ?? [current], initial: 0, last: current.number, ranges: [] },
      head: { finalized, latest: latest ?? current },
    },
    batch: { blocksCount: 1, bytesSize: 0, requests: {}, lastBlockReceivedAt: lastBlockReceivedAt ?? new Date() },
  } as unknown as BatchContext
}
