import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import type { BatchContext, BlockCursor } from '~/core/index.js'
import { evmQuery } from '~/evm/evm-query-builder.js'
import { mockMetricsServer, testLogger } from '~/testing/index.js'

import type { DrainResult, Publisher } from './publisher.js'
import type { OutboxRow } from './pubsub-state.js'

/**
 * Shared scaffolding for the Pub/Sub target suites: a publisher that records what would go on
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
  failOn?: (row: OutboxRow, index: number) => Error | undefined
  closed = false

  async setup(topics: string[]): Promise<void> {
    this.setupCalls.push(topics)
  }

  async drain(rows: (OutboxRow & { attributes: Record<string, string> })[]): Promise<DrainResult> {
    const confirmed: number[] = []
    let bytes = 0

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
      .map((message) => ({
        op: message.attributes['_op'],
        id: message.attributes['_id'],
        payload: message.payload,
        seq: Number(message.attributes['_seq']),
      }))
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
  metrics = mockMetricsServer().server.metrics,
}: {
  current: BlockCursor
  finalized?: BlockCursor
  rollbackChain?: BlockCursor[]
  latest?: BlockCursor
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
    batch: { blocksCount: 1, bytesSize: 0, requests: {}, lastBlockReceivedAt: new Date(0) },
  } as unknown as BatchContext
}
