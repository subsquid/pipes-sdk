import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { ParquetReader, ParquetSchema, ParquetWriter } from '@dsnp/parquetjs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { evmPortalStream } from '~/evm/index.js'
import { type MockPortal, type MockResponse, blockDecoder, mockPortal } from '~/testing/index.js'

import type { ParquetEngine } from './engine.js'
import { parquetTarget } from './parquet-target.js'

/**
 * A minimal third-party engine built ONLY from the SDK's public extension surface plus a
 * Parquet library of its own choosing (`@dsnp/parquetjs` used directly here, the way an
 * external engine would use its native writer). It buffers rows in memory and writes a real
 * Parquet file at the target-assigned temp path on `finish()` — so the published output is
 * readable by actual Parquet readers and passes the target's envelope verification honestly.
 * Flat leaf schemas only; that is all this test needs.
 */
function tinyParquetEngine(calls: string[]): ParquetEngine {
  return {
    name: 'tiny-test',
    table(table) {
      calls.push(`table:${table.table}`)
      const schema = new ParquetSchema(
        // Flat leaf columns only (see the engine doc) — the SDK leaf names used here (INT64,
        // UTF8) are also valid parquetjs type names.
        Object.fromEntries(
          Object.entries(table.schema).map(([name, column]) => [name, { type: column.type as 'INT64' | 'UTF8' }]),
        ),
      )

      return {
        createSegment(tmpPath) {
          const rows: Record<string, unknown>[] = []

          return {
            async append(batch) {
              rows.push(...batch)
            },
            async size() {
              // An estimate is fine for rotation feedback — this engine stages in memory.
              return rows.length * 64
            },
            async finish() {
              const writer = await ParquetWriter.openFile(schema, tmpPath)
              for (const row of rows) await writer.appendRow(row)
              await writer.close()
            },
            async abort() {
              rows.length = 0
            },
          }
        },
      }
    },
  }
}

/**
 * An engine that ignores the output contract: its finished file is arbitrary JSON wrapped in
 * the Parquet magic bytes — the exact spoof the envelope check's footer-length validation
 * exists to refuse.
 */
function magicWrappedJsonEngine(): ParquetEngine {
  return {
    name: 'broken-test',
    table() {
      return {
        createSegment(tmpPath) {
          const rows: Record<string, unknown>[] = []

          return {
            async append(batch) {
              rows.push(...batch)
            },
            async size() {
              return JSON.stringify(rows).length
            },
            async finish() {
              await writeFile(tmpPath, `PAR1${JSON.stringify(rows)}PAR1`)
            },
            async abort() {},
          }
        },
      }
    },
  }
}

async function readAllRows(file: string): Promise<Record<string, unknown>[]> {
  const reader = await ParquetReader.openFile(file)
  try {
    const cursor = reader.getCursor()
    const rows: Record<string, unknown>[] = []
    let row: Record<string, unknown> | null
    while ((row = (await cursor.next()) as Record<string, unknown> | null)) rows.push(row)

    return rows
  } finally {
    // Close even when iteration throws, so a failing test doesn't leak the fd.
    await reader.close()
  }
}

function blocksResponse(numbers: number[], finalized?: number): MockResponse {
  return {
    statusCode: 200,
    data: numbers.map((n) => ({ header: { number: n, hash: `0x${n}`, timestamp: n * 1000 } })),
    head: finalized === undefined ? undefined : { finalized: { number: finalized, hash: `0x${finalized}` } },
  }
}

describe('parquetTarget with a custom engine', () => {
  let portal: MockPortal | undefined
  let dir: string

  beforeEach(async () => {
    dir = await mkdtemp(path.join(tmpdir(), 'sqd-parquet-custom-engine-test-'))
  })

  afterEach(async () => {
    await portal?.close()
    portal = undefined
    await rm(dir, { recursive: true, force: true })
  })

  it('drives a from-scratch engine through finalization, publish and cursor persistence', async () => {
    portal = await mockPortal([blocksResponse([1, 2, 3, 4, 5], 3)])
    const calls: string[] = []

    await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 5 }) }).pipeTo(
      parquetTarget({
        dir,
        tables: [
          {
            table: 'blocks',
            schema: { blockNumber: { type: 'INT64' }, hash: { type: 'UTF8' } },
          },
        ],
        settings: { engine: tinyParquetEngine(calls) },
        onData: ({ store, data }) => {
          store.insert(
            'blocks',
            data.map((b) => ({ blockNumber: b.number, hash: b.hash })),
          )
        },
      }),
    )

    // table() received the declared table, once, at construction. Encoding options are the
    // engine's own business (its factory), so no context travels across the seam.
    expect(calls).toEqual(['table:blocks'])

    // All rows published: the bounded stream waits for finality to reach the range end (the mock
    // finalizes everything it served), so the tail is released before the stream ends. The file
    // is named by the target for the coverage window — from the configured start (0) to the
    // final boundary (5). The engine never saw the window or chose the name.
    const files = (await readdir(path.join(dir, 'blocks'))).sort()
    expect(files).toEqual(['000000000000-000000000005.parquet'])

    // The published file is real Parquet, readable by an actual reader, with the rows the
    // engine received — in order, in the engine-neutral plain shape.
    const rows = await readAllRows(path.join(dir, 'blocks', files[0]!))
    expect(rows.map((r) => [r['blockNumber'], r['hash']])).toEqual([
      [1n, '0x1'],
      [2n, '0x2'],
      [3n, '0x3'],
      [4n, '0x4'],
      [5n, '0x5'],
    ])

    // The checkpoint that published the segment also persisted the cursor.
    const stateFiles = (await readdir(dir)).filter((f) => f.startsWith('_sqd_parquet_state.'))
    expect(stateFiles).toHaveLength(1)
  })

  it('refuses an engine output that merely wraps non-Parquet bytes in the magic', async () => {
    portal = await mockPortal([blocksResponse([1, 2, 3], 3)])

    await expect(
      evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 3 }) }).pipeTo(
        parquetTarget({
          dir,
          tables: [{ table: 'blocks', schema: { blockNumber: { type: 'INT64' } } }],
          settings: { engine: magicWrappedJsonEngine() },
          onData: ({ store, data }) => {
            store.insert(
              'blocks',
              data.map((b) => ({ blockNumber: b.number })),
            )
          },
        }),
      ),
    ).rejects.toThrowError(/complete Parquet files/)

    // Nothing was published, the temp file was cleaned up, and no cursor was persisted —
    // the run is fully recoverable.
    expect(await readdir(path.join(dir, 'blocks'))).toEqual([])
    expect((await readdir(dir)).filter((f) => f.startsWith('_sqd_parquet_state.'))).toEqual([])
  })
})
