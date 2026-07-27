import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { ParquetReader, ParquetSchema, ParquetWriter } from '@dsnp/parquetjs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { type PortalRange, type SpanHooks, createTarget } from '~/core/index.js'
import { evmPortalStream, evmQuery } from '~/evm/index.js'
import { type MockPortal, type MockResponse, blockDecoder, mockPortal, testLogger } from '~/testing/index.js'

import { PARQUET_ERROR_CODES, ParquetTargetError } from './errors.js'
import { ParquetState } from './parquet-state.js'
import { ParquetStore } from './parquet-store.js'
import { parquetTarget } from './parquet-target.js'
import { parquetjsEngine } from './parquetjs/parquetjs-engine.js'
import { ParquetSegmentWriter } from './parquetjs/parquetjs-writer.js'
import type { ParquetTable } from './schema.js'
import { finalizeSegmentFile } from './segment.js'

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

type BlockRow = { blockNumber: number; hash: string; timestamp: number }
type LogRow = { blockNumber: number; logIndex: number; address: string }

const BLOCKS_TABLE: ParquetTable = {
  table: 'blocks',
  schema: {
    blockNumber: { type: 'INT64' },
    hash: { type: 'UTF8' },
    timestamp: { type: 'INT64' },
  },
}

/** Decoded EVM headers expose `number`/`hash`/`timestamp`; map them onto the `blocks` schema. */
const insertBlocks = ({
  store,
  data,
}: {
  store: ParquetStore
  data: { number: number; hash: string; timestamp: number }[]
}) => {
  store.insert(
    'blocks',
    data.map((b) => ({ blockNumber: b.number, hash: b.hash, timestamp: b.timestamp })),
  )
}

/**
 * A decoder over two disjoint ranges — `blockDecoder` only takes one. The blocks between them are
 * never queried, which is what the coverage naming has to notice.
 */
function twoRangeDecoder(a: PortalRange, b: PortalRange) {
  return evmQuery()
    .addRange(a)
    .addRange(b)
    .addFields({ block: { number: true, hash: true, timestamp: true } })
    .build()
    .pipe((d) => d.flatMap((block) => block.header))
}

/** Builds a 200 response carrying the given block numbers, optionally with a finalized head. */
function blocksResponse(numbers: number[], finalized?: number): MockResponse {
  return {
    statusCode: 200,
    data: numbers.map((n) => ({ header: { number: n, hash: `0x${n}`, timestamp: n * 1000 } })),
    head: finalized === undefined ? undefined : { finalized: { number: finalized, hash: `0x${finalized}` } },
  }
}

/** Published data file names (excludes temp + state files), sorted lexically. */
async function listDataFiles(dir: string, table: string): Promise<string[]> {
  const entries = await readdir(path.join(dir, table)).catch(() => [] as string[])

  return entries.filter((f) => /^\d+-\d+\.parquet$/.test(f)).sort()
}

/** All entries in a table dir (to assert temp-file cleanup). */
async function listAll(dir: string, table: string): Promise<string[]> {
  return (await readdir(path.join(dir, table)).catch(() => [] as string[])).sort()
}

async function readRows<T>(dir: string, table: string, decode: (raw: Record<string, unknown>) => T): Promise<T[]> {
  const files = await listDataFiles(dir, table)
  const rows: T[] = []
  for (const file of files) {
    const reader = await ParquetReader.openFile(path.join(dir, table, file))
    const cursor = reader.getCursor()
    let raw: Record<string, unknown> | null
    while ((raw = (await cursor.next()) as Record<string, unknown> | null)) {
      rows.push(decode(raw))
    }
    await reader.close()
  }

  return rows
}

/** Reads the `blocks` table, normalized + sorted by block number. */
async function readBlocks(dir: string): Promise<BlockRow[]> {
  const rows = await readRows(dir, 'blocks', (raw) => ({
    blockNumber: Number(raw['blockNumber']),
    hash: String(raw['hash']),
    timestamp: Number(raw['timestamp']),
  }))

  return rows.sort((a, b) => a.blockNumber - b.blockNumber)
}

/** Reads the `logs` table, normalized + sorted. */
async function readLogs(dir: string): Promise<LogRow[]> {
  const rows = await readRows(dir, 'logs', (raw) => ({
    blockNumber: Number(raw['blockNumber']),
    logIndex: Number(raw['logIndex']),
    address: String(raw['address']),
  }))

  return rows.sort((a, b) => a.blockNumber - b.blockNumber || a.logIndex - b.logIndex)
}

/**
 * Records every span the source opens as a `>`-joined path ("batch > parquet > flush batch"), so a
 * test can assert nesting and not just that some span with a given name existed.
 */
function spanTracker() {
  const started: string[] = []
  const ended: string[] = []

  const track = (path: string): SpanHooks => ({
    onStart: (name) => {
      const child = path ? `${path} > ${name}` : name
      started.push(child)

      return track(child)
    },
    onEnd: () => {
      ended.push(path)
    },
  })

  return {
    hooks: track(''),
    /** Span paths under the target's own span, in the order they opened. */
    targetSpans: () => started.filter((p) => p.startsWith('batch > parquet')),
    unended: () => started.filter((p) => p.startsWith('batch > parquet') && !ended.includes(p)),
  }
}

/** Writes a Parquet file directly (used to seed stale/over-cursor files for recovery tests). */
async function seedParquetFile(filePath: string, rows: BlockRow[]): Promise<void> {
  const schema = new ParquetSchema({
    blockNumber: { type: 'INT64' },
    hash: { type: 'UTF8' },
    timestamp: { type: 'INT64' },
  })
  const writer = await ParquetWriter.openFile(schema, filePath, { rowGroupSize: 1 })
  for (const row of rows) await writer.appendRow(row)
  await writer.close()
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

describe('parquetTarget', () => {
  let portal: MockPortal | undefined
  let dir: string

  beforeEach(async () => {
    dir = await mkdtemp(path.join(tmpdir(), 'sqd-parquet-test-'))
  })

  afterEach(async () => {
    await portal?.close()
    portal = undefined
    await rm(dir, { recursive: true, force: true })
  })

  describe('block-number value guard', () => {
    it('fails loudly instead of coercing a null block number to 0, even in production', async () => {
      // Disable the dev-mode value check so the always-on block guard is what trips.
      const prev = process.env.NODE_ENV
      process.env.NODE_ENV = 'production'
      portal = await mockPortal([blocksResponse([1], 1)])

      try {
        await expect(
          evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 1 }) }).pipeTo(
            parquetTarget({
              dir,
              tables: [BLOCKS_TABLE],
              onData: ({ store }) => {
                store.insert('blocks', [{ blockNumber: null as unknown as number, hash: '0x1', timestamp: 1000 }])
              },
            }),
          ),
        ).rejects.toThrowError(/finite integer/)
      } finally {
        if (prev === undefined) delete process.env.NODE_ENV
        else process.env.NODE_ENV = prev
      }

      // No bogus block-0 file was published.
      expect(await listDataFiles(dir, 'blocks')).toEqual([])
    })
  })

  describe('finalized-only', () => {
    it('does not write blocks above the finalized head', async () => {
      portal = await mockPortal([blocksResponse([1, 2, 3, 4, 5], 2)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 5 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      // Only blocks 1–2 are finalized (head = 2); 3–5 stay buffered and are never written.
      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 2])
    })

    it('publishes previously-unfinalized blocks once a later batch finalizes them', async () => {
      portal = await mockPortal([blocksResponse([1, 2, 3, 4, 5], 2), blocksResponse([6, 7], 7)])

      // maxBytes:1 keeps the two responses as distinct batches so the second batch's higher
      // finalized head genuinely releases what the first batch left buffered.
      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 7 }),
      }).pipeTo(parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }))

      // The later finalized head (7) releases the 3–5 held from the first batch.
      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 2, 3, 4, 5, 6, 7])
    })
  })

  describe('read-back correctness', () => {
    it('writes exactly the finalized rows with correct types and a <from>-<to> coverage filename', async () => {
      portal = await mockPortal([blocksResponse([1, 2, 3], 3)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 3 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      // The stream starts at block 0, so the window covered is 0–3 even though no row sits at 0.
      expect(await listDataFiles(dir, 'blocks')).toEqual(['000000000000-000000000003.parquet'])
      expect(await readBlocks(dir)).toEqual([
        { blockNumber: 1, hash: '0x1', timestamp: 1000 },
        { blockNumber: 2, hash: '0x2', timestamp: 2000 },
        { blockNumber: 3, hash: '0x3', timestamp: 3000 },
      ])
    })

    it('reads INT64 columns back as bigint (input contract)', async () => {
      portal = await mockPortal([blocksResponse([1], 1)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 1 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      const reader = await ParquetReader.openFile(path.join(dir, 'blocks', '000000000000-000000000001.parquet'))
      const raw = (await reader.getCursor().next()) as Record<string, unknown>
      await reader.close()
      expect(typeof raw['blockNumber']).toBe('bigint')
      expect(raw['blockNumber']).toBe(1n)
    })

    it('round-trips TIMESTAMP, DATE, JSON, STRUCT and LIST columns with the expected footer annotations', async () => {
      const kitchenSink: ParquetTable = {
        table: 'sink',
        schema: {
          blockNumber: { type: 'INT64' },
          at: { type: 'TIMESTAMP' },
          day: { type: 'DATE' },
          dayNum: { type: 'DATE' },
          meta: { type: 'JSON', optional: true },
          user: { type: 'STRUCT', fields: { name: { type: 'UTF8' }, age: { type: 'INT32', optional: true } } },
          tags: { type: 'LIST', element: { type: 'UTF8', optional: true }, optional: true },
          xfers: {
            type: 'LIST',
            element: { type: 'STRUCT', fields: { to: { type: 'UTF8' }, amt: { type: 'INT64' } } },
          },
          matrix: { type: 'LIST', element: { type: 'LIST', element: { type: 'INT32' } } },
        },
      }
      const at = new Date('2024-01-01T15:30:45.123Z')

      portal = await mockPortal([blocksResponse([1, 2], 2)])
      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 2 }) }).pipeTo(
        parquetTarget({
          dir,
          tables: [kitchenSink],
          onData: ({ store, data }) => {
            store.insert(
              'sink',
              data.map((b) => ({
                blockNumber: b.number,
                at,
                day: at, // non-midnight Date — must land on its UTC calendar day
                dayNum: 19724, // whole days since epoch = 2024-01-02
                meta: b.number === 1 ? { fee: 12, keys: ['k1', 'k2'] } : null,
                user: { name: `user-${b.number}`, age: b.number === 1 ? 30 : null },
                tags: b.number === 1 ? ['a', null, 'c'] : [],
                xfers: [{ to: '0xa', amt: 5n }],
                matrix: [[1, 2], [3]],
              })),
            )
          },
        }),
      )

      const [file] = await listDataFiles(dir, 'sink')
      const reader = await ParquetReader.openFile(path.join(dir, 'sink', file))
      const cursor = reader.getCursor()
      const rows: Record<string, unknown>[] = []
      let raw: Record<string, unknown> | null
      while ((raw = (await cursor.next()) as Record<string, unknown> | null)) rows.push(raw)

      // Legacy footer annotations (the pinned library never writes the modern LogicalType field;
      // per the format spec readers map these to the corresponding logical types):
      // TIMESTAMP_MILLIS=9 (the library's legacy spelling of TIMESTAMP), DATE=6, JSON=19, LIST=3 on the outer
      // list groups; STRUCT groups carry children but no converted_type.
      const annotations = new Map(reader.metadata!.schema.map((s) => [s.name, s.converted_type] as const))
      const children = new Map(reader.metadata!.schema.map((s) => [s.name, s.num_children] as const))
      await reader.close()
      expect(annotations.get('at')).toBe(9)
      expect(annotations.get('day')).toBe(6)
      expect(annotations.get('dayNum')).toBe(6)
      expect(annotations.get('meta')).toBe(19)
      expect(annotations.get('tags')).toBe(3)
      expect(annotations.get('xfers')).toBe(3)
      expect(annotations.get('matrix')).toBe(3)
      expect(annotations.get('user')).toBeNull()
      expect(Number(children.get('user'))).toBe(2)

      rows.sort((a, b) => Number(a['blockNumber']) - Number(b['blockNumber']))
      const [r1, r2] = rows
      expect((r1['at'] as Date).toISOString()).toBe('2024-01-01T15:30:45.123Z')
      expect((r1['day'] as Date).toISOString()).toBe('2024-01-01T00:00:00.000Z')
      expect((r1['dayNum'] as Date).toISOString()).toBe('2024-01-02T00:00:00.000Z')
      expect(r1['meta']).toEqual({ fee: 12, keys: ['k1', 'k2'] })
      expect(r1['user']).toEqual({ name: 'user-1', age: 30 })
      // This library's reader materializes the canonical layout verbatim: wrapped elements, an
      // empty list as { list: null } (still distinct from a null column at the definition level).
      expect(r1['tags']).toEqual({ list: [{ element: 'a' }, { element: null }, { element: 'c' }] })
      expect(r1['xfers']).toEqual({ list: [{ element: { to: '0xa', amt: 5n } }] })
      // Nested lists round-trip with each level in the canonical wrapped shape.
      expect(r1['matrix']).toEqual({
        list: [{ element: { list: [{ element: 1 }, { element: 2 }] } }, { element: { list: [{ element: 3 }] } }],
      })

      expect(r2['meta']).toBeNull()
      expect(r2['user']).toEqual({ name: 'user-2', age: null })
      expect(r2['tags']).toEqual({ list: null })
    })
  })

  describe('rotation', () => {
    it('rotates into multiple files with disjoint, contiguous ranges under a small maxBytes', async () => {
      portal = await mockPortal([blocksResponse([1], 1), blocksResponse([2], 2), blocksResponse([3], 3)])

      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE],
          settings: { rollover: { maxBytes: 1 }, engine: parquetjsEngine({ rowGroupSize: 1 }) },
          onData: insertBlocks,
        }),
      )

      // Every block from the stream's start (0) through the last checkpoint is claimed exactly
      // once, with no gap between one file's `to` and the next file's `from`.
      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000002.parquet',
        '000000000003-000000000003.parquet',
      ])
      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 2, 3])
    })
  })

  describe('recovery', () => {
    it('removes over-cursor and temp files, resumes from the cursor, stays duplicate-free', async () => {
      // Seed a committed cursor at block 2, a stale data file above it, and an orphan temp file.
      // The state file is namespaced by the pipe id ('test') the runs below use.
      await writeFile(
        path.join(dir, '_sqd_parquet_state.test.json'),
        JSON.stringify({ cursor: { number: 2, hash: '0x2' } }),
      )
      const blocksDir = path.join(dir, 'blocks')
      await mkdir(blocksDir, { recursive: true })
      await seedParquetFile(path.join(blocksDir, '000000000003-000000000005.parquet'), [
        { blockNumber: 3, hash: 'STALE', timestamp: 0 },
      ])
      await writeFile(path.join(blocksDir, '.tmp-orphan.parquet'), 'garbage')

      portal = await mockPortal([
        {
          ...blocksResponse([3, 4, 5], 5),
          validateRequest: (req) => {
            expect(req).toMatchObject({ fromBlock: 3, parentBlockHash: '0x2' })
          },
        },
      ])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 5 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      // The stale STALE-hash row was deleted and replaced by correct, duplicate-free data.
      expect(await readBlocks(dir)).toEqual([
        { blockNumber: 3, hash: '0x3', timestamp: 3000 },
        { blockNumber: 4, hash: '0x4', timestamp: 4000 },
        { blockNumber: 5, hash: '0x5', timestamp: 5000 },
      ])
      // No orphan temp file survives recovery.
      expect((await listAll(dir, 'blocks')).some((f) => f.startsWith('.tmp-'))).toBe(false)
    })
  })

  describe('finalized watermark (persist + restart)', () => {
    it('persists the source-clamped finalized head through the loop at checkpoint', async () => {
      portal = await mockPortal([blocksResponse([1, 2, 3], 3)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 3 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      // The loop threads the finalized head into saveCursor, so the state file (namespaced by the
      // pipe id) carries it for the source to re-seed its watermark on restart.
      const persisted = JSON.parse(await readFile(path.join(dir, '_sqd_parquet_state.test.json'), 'utf8'))
      expect(persisted.cursor).toEqual({ number: 3, hash: '0x3' })
      expect(persisted.finalized).toEqual({ number: 3, hash: '0x3' })
    })

    it('re-seeds the watermark from a real persisted finalized head and clamps a regression below it', async () => {
      // A real ParquetState persists a finalized head of 5, exactly as the target loop would.
      const persistState = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })
      await persistState.getCursor()
      await persistState.saveCursor({ number: 5, hash: '0x5' }, { number: 5, hash: '0x5f' })

      // Restart: a real getCursor hands that finalized head back as resume state.
      const resume = await new ParquetState({ dir, tables: ['blocks'], logger: testLogger() }).getCursor()
      expect(resume).toEqual({ latest: { number: 5, hash: '0x5' }, finalized: { number: 5, hash: '0x5f' } })

      // The source seeds its floor from the persisted finalized and clamps a lower reported head.
      portal = await mockPortal([blocksResponse([6], 3)])
      const seen: unknown[] = []
      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 6 }) }).pipeTo(
        createTarget({
          write: async ({ read }) => {
            for await (const { ctx } of read(resume)) {
              seen.push(ctx.stream.head.finalized)
            }
          },
        }) as any,
      )

      // The persisted floor (5) survives the restart and clamps the lower reported head (3).
      expect(seen).toEqual([{ number: 5, hash: '0x5f' }])
    })
  })

  describe('fork', () => {
    it('drops buffered forked rows and leaves the pre-fork published file intact', async () => {
      portal = await mockPortal([
        blocksResponse([1, 2, 3, 4, 5], 1),
        {
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 2, hash: '0x2' },
              { number: 3, hash: '0x3' },
              { number: 4, hash: '0x4-1' },
              { number: 5, hash: '0x5-1' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
            { header: { number: 6, hash: '0x6a', timestamp: 6000 } },
            { header: { number: 7, hash: '0x7a', timestamp: 7000 } },
          ],
          head: { finalized: { number: 7, hash: '0x7a' } },
          validateRequest: (req) => {
            expect(req).toMatchObject({ fromBlock: 4, parentBlockHash: '0x3' })
          },
        },
      ])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 7 }) }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE],
          // Checkpoint every batch so block 1 is published before the fork fires.
          settings: { rollover: { intervalBlocks: 1 } },
          onData: insertBlocks,
        }),
      )

      // Forked blocks (0x4-1 / 0x5-1) were dropped from the buffer; the new chain replaces them.
      expect(await readBlocks(dir)).toEqual([
        { blockNumber: 1, hash: '0x1', timestamp: 1000 },
        { blockNumber: 2, hash: '0x2', timestamp: 2000 },
        { blockNumber: 3, hash: '0x3', timestamp: 3000 },
        { blockNumber: 4, hash: '0x4a', timestamp: 4000 },
        { blockNumber: 5, hash: '0x5a', timestamp: 5000 },
        { blockNumber: 6, hash: '0x6a', timestamp: 6000 },
        { blockNumber: 7, hash: '0x7a', timestamp: 7000 },
      ])
      // The block-1 file was published before the reorg and must survive it untouched.
      expect(await listDataFiles(dir, 'blocks')).toContain('000000000000-000000000001.parquet')
    })

    it('drops forked rows in every table buffer, not just the first (multi-table)', async () => {
      const logsTable: ParquetTable = {
        table: 'logs',
        schema: {
          blockNumber: { type: 'INT64' },
          logIndex: { type: 'INT32' },
          address: { type: 'UTF8' },
        },
      }
      portal = await mockPortal([
        blocksResponse([1, 2, 3, 4, 5], 1),
        {
          statusCode: 409,
          data: {
            previousBlocks: [
              { number: 2, hash: '0x2' },
              { number: 3, hash: '0x3' },
              { number: 4, hash: '0x4-1' },
              { number: 5, hash: '0x5-1' },
            ],
          },
        },
        {
          statusCode: 200,
          data: [
            { header: { number: 4, hash: '0x4a', timestamp: 4000 } },
            { header: { number: 5, hash: '0x5a', timestamp: 5000 } },
            { header: { number: 6, hash: '0x6a', timestamp: 6000 } },
            { header: { number: 7, hash: '0x7a', timestamp: 7000 } },
          ],
          head: { finalized: { number: 7, hash: '0x7a' } },
        },
      ])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 7 }) }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE, logsTable],
          onData: ({ store, data }) => {
            store.insert(
              'blocks',
              data.map((b) => ({ blockNumber: b.number, hash: b.hash, timestamp: b.timestamp })),
            )
            // One log row per block, tagged with the block hash so the forked chain (0x4-1 / 0x5-1)
            // is distinguishable from the replacement chain (0x4a / 0x5a).
            store.insert(
              'logs',
              data.map((b) => ({ blockNumber: b.number, logIndex: 0, address: b.hash })),
            )
          },
        }),
      )

      // Both tables dropped the forked rows: no duplicate block 4/5 in either. If fork() had
      // dropped rows in only the first buffer, the logs table would still carry the stale forked
      // rows (blockNumbers [1, 2, 3, 4, 4, 5, 5, 6, 7]).
      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 2, 3, 4, 5, 6, 7])
      const logs = await readLogs(dir)
      expect(logs.map((r) => r.blockNumber)).toEqual([1, 2, 3, 4, 5, 6, 7])
      // Blocks 4 & 5 in the logs table carry the replacement-chain hash, proving the drop happened.
      expect(logs.find((r) => r.blockNumber === 4)?.address).toBe('0x4a')
      expect(logs.find((r) => r.blockNumber === 5)?.address).toBe('0x5a')
    })
  })

  describe('coverage naming', () => {
    it("stretches a sparse table's next file back over windows that produced no rows", async () => {
      // `sparse` has rows only in blocks 1 and 6, and each response is its own batch + checkpoint
      // (maxBytes:1), so blocks 2–5 are windows it was present for but produced nothing in.
      const sparseTable: ParquetTable = { table: 'sparse', schema: { blockNumber: { type: 'INT64' } } }
      portal = await mockPortal([
        blocksResponse([1], 1),
        blocksResponse([2, 3], 3),
        blocksResponse([4, 5], 5),
        blocksResponse([6], 6),
      ])

      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 6 }),
      }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE, sparseTable],
          settings: { rollover: { maxBytes: 1 }, engine: parquetjsEngine({ rowGroupSize: 1 }) },
          onData: ({ store, data }) => {
            insertBlocks({ store, data })
            const rows = data.filter((b) => b.number === 1 || b.number === 6)
            if (rows.length > 0) {
              store.insert(
                'sparse',
                rows.map((b) => ({ blockNumber: b.number })),
              )
            }
          },
        }),
      )

      // Contiguous: 0–1 then 2–6. The empty windows are named by the file that comes after them,
      // so "no file covers block 4" never happens just because block 4 had no rows.
      expect(await listDataFiles(dir, 'sparse')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000006.parquet',
      ])
      // ...and no empty file was written for the windows sparse sat out.
      expect((await readRows(dir, 'sparse', (raw) => Number(raw['blockNumber']))).sort((a, b) => a - b)).toEqual([1, 6])
      // The dense table checkpoints on the same windows and stays contiguous too.
      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000003.parquet',
        '000000000004-000000000005.parquet',
        '000000000006-000000000006.parquet',
      ])
    })

    it('resumes coverage where the previous run left off, across a restart', async () => {
      // Run 1: blocks 1–3, sparse writes only at block 1.
      const sparseTable: ParquetTable = { table: 'sparse', schema: { blockNumber: { type: 'INT64' } } }
      const tables = [BLOCKS_TABLE, sparseTable]
      const onData = ({
        store,
        data,
      }: {
        store: ParquetStore
        data: { number: number; hash: string; timestamp: number }[]
      }) => {
        insertBlocks({ store, data })
        const rows = data.filter((b) => b.number === 1 || b.number === 5)
        if (rows.length > 0) {
          store.insert(
            'sparse',
            rows.map((b) => ({ blockNumber: b.number })),
          )
        }
      }

      portal = await mockPortal([blocksResponse([1], 1), blocksResponse([2, 3], 3)])
      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(parquetTarget({ dir, tables, settings: { rollover: { maxBytes: 1 } }, onData }))
      await portal.close()

      // sparse published 0–1, sat out 2–3, then closed that tail at stream end — so run 1 leaves
      // no unclaimed blocks behind and the next run starts cleanly at 4.
      expect(await listDataFiles(dir, 'sparse')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000003.parquet',
      ])
      const persisted = JSON.parse(await readFile(path.join(dir, '_sqd_parquet_state.test.json'), 'utf8'))
      expect(persisted.coverage).toEqual({ blocks: 4, sparse: 4 })

      // Run 2 resumes at block 4 and sparse finally writes again at block 5.
      portal = await mockPortal([blocksResponse([4, 5], 5)])
      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 5 }),
      }).pipeTo(parquetTarget({ dir, tables, settings: { rollover: { maxBytes: 1 } }, onData }))

      // Both tables cover 0–5 end to end, with no gap and no overlap across the restart boundary.
      expect(await listDataFiles(dir, 'sparse')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000003.parquet',
        '000000000004-000000000005.parquet',
      ])
      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000003.parquet',
        '000000000004-000000000005.parquet',
      ])
      // The 2–3 file is sparse's tail marker: it claims the window without inventing rows.
      expect((await readRows(dir, 'sparse', (raw) => Number(raw['blockNumber']))).sort((a, b) => a - b)).toEqual([1, 5])
    })

    it('leaves the gap between two disjoint ranges unclaimed', async () => {
      // Blocks 2-4 are never queried, so no file may name them: "absent" has to keep meaning
      // "not indexed". Stretching the second file back to block 2 would claim the pipe covered
      // blocks it never asked the portal for.
      portal = await mockPortal([blocksResponse([0, 1], 1), blocksResponse([5, 6], 6)])

      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: twoRangeDecoder({ from: 0, to: 1 }, { from: 5, to: 6 }),
      }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], settings: { rollover: { maxBytes: 1 } }, onData: insertBlocks }),
      )

      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000005-000000000006.parquet',
      ])
    })

    it('resumes a disjoint-range backfill interrupted between the ranges', async () => {
      // The exact durable state a run leaves after finishing range [0,1] and crossing toward [5,6]:
      // cursor at block 1, coverage at 5 (the next queried block past the 2-4 gap). The store
      // unit tests prove a real run produces this pair; here we drive the resume path off it.
      await mkdir(path.join(dir, 'blocks'), { recursive: true })
      await writeFile(
        path.join(dir, '_sqd_parquet_state.test.json'),
        JSON.stringify({ id: 'test', cursor: { number: 1, hash: '0x1' }, coverage: { blocks: 5 } }),
      )
      await seedParquetFile(path.join(dir, 'blocks', '000000000000-000000000001.parquet'), [
        { blockNumber: 1, hash: '0x1', timestamp: 1000 },
      ])

      // Resume must accept coverage 5 at cursor 1 (the gap justifies it) instead of rejecting the
      // state, then finish the second range — leaving both windows tiled end to end.
      portal = await mockPortal([blocksResponse([5, 6], 6)])
      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: twoRangeDecoder({ from: 0, to: 1 }, { from: 5, to: 6 }),
      }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], settings: { rollover: { maxBytes: 1 } }, onData: insertBlocks }),
      )

      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000005-000000000006.parquet',
      ])
    })

    it('resumes a run that crashed between publishing a stretched file and committing', async () => {
      // The durable state left by a crash inside a checkpoint: cursor 5 committed, and the
      // interrupted checkpoint at cursor 6 already renamed `sparse`'s stretched 2-6 file into
      // place. `sparse` sat out the cursor-5 checkpoint, so its coverage start (2) is below the
      // cursor and the remnant straddles it — the ordinary case, not a corrupt state file.
      const sparseTable: ParquetTable = { table: 'sparse', schema: BLOCKS_TABLE.schema }
      for (const table of ['blocks', 'sparse']) {
        await mkdir(path.join(dir, table), { recursive: true })
      }
      await writeFile(
        path.join(dir, '_sqd_parquet_state.test.json'),
        JSON.stringify({ id: 'test', cursor: { number: 5, hash: '0x5' }, coverage: { blocks: 6, sparse: 2 } }),
      )
      await seedParquetFile(path.join(dir, 'blocks', '000000000000-000000000005.parquet'), [
        { blockNumber: 5, hash: '0x5', timestamp: 5000 },
      ])
      await seedParquetFile(path.join(dir, 'sparse', '000000000000-000000000001.parquet'), [
        { blockNumber: 1, hash: '0x1', timestamp: 1000 },
      ])
      await seedParquetFile(path.join(dir, 'sparse', '000000000002-000000000006.parquet'), [
        { blockNumber: 6, hash: '0x6', timestamp: 6000 },
      ])

      portal = await mockPortal([blocksResponse([6, 7], 7)])
      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 7 }),
      }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE, sparseTable],
          settings: { rollover: { maxBytes: 1 } },
          onData: ({ store, data }) => {
            insertBlocks({ store, data })
            const rows = data.filter((b) => b.number === 7)
            if (rows.length > 0) {
              store.insert(
                'sparse',
                rows.map((b) => ({ blockNumber: b.number, hash: b.hash, timestamp: b.timestamp })),
              )
            }
          },
        }),
      )

      // The remnant was re-derived from the re-fetched blocks, and both tables tile 0-7 end to end.
      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000005.parquet',
        '000000000006-000000000007.parquet',
      ])
      expect(await listDataFiles(dir, 'sparse')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000007.parquet',
      ])
      expect((await readRows(dir, 'sparse', (raw) => Number(raw['blockNumber']))).sort((a, b) => a - b)).toEqual([1, 7])
    })

    it('does not fail a row keyed below the window that emitted it', async () => {
      // An OHLC-style aggregate keyed at its window's FIRST block but emitted once the window
      // closes. The row lands in a later file than its own block number — legitimate, because the
      // filename states which blocks were processed, not where the rows point.
      portal = await mockPortal([blocksResponse([1], 1), blocksResponse([2, 3], 3)])

      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE],
          settings: { rollover: { intervalBlocks: 1 } },
          onData: ({ store, data }) => {
            store.insert(
              'blocks',
              data.map((b) => ({
                blockNumber: b.number === 2 ? 1 : b.number,
                hash: b.hash,
                timestamp: b.timestamp,
              })),
            )
          },
        }),
      )

      // The back-keyed row is in the 2–3 file; nothing threw and no row was dropped.
      expect((await readBlocks(dir)).map((r) => r.blockNumber).sort((a, b) => a - b)).toEqual([1, 1, 3])
      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000003.parquet',
      ])
    })

    it('starts coverage at the stream start, not block 0, for a backfill from a non-zero block', async () => {
      portal = await mockPortal([blocksResponse([100, 101], 101)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 100, to: 101 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      // Naming this 0-101 would claim 100 blocks the pipe never looked at.
      expect(await listDataFiles(dir, 'blocks')).toEqual(['000000000100-000000000101.parquet'])
    })

    it('does not reach back past the cursor when resuming state that predates coverage tracking', async () => {
      // Legacy state: a cursor, no `coverage` field. Blocks 0–2 belong to whatever the old run
      // wrote under row-based names, so the first new file must start at 3 — not at the stream's
      // configured start of 0, which would re-claim them.
      await writeFile(
        path.join(dir, '_sqd_parquet_state.test.json'),
        JSON.stringify({ cursor: { number: 2, hash: '0x2' } }),
      )
      portal = await mockPortal([blocksResponse([3, 4], 4)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 4 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      expect(await listDataFiles(dir, 'blocks')).toEqual(['000000000003-000000000004.parquet'])
    })

    it('closes a tail owed by the persisted state even when the resume yields no batches', async () => {
      // The state a run leaves when it crashes between its final regular checkpoint and the
      // stream-end checkpoint: cursor already at the end of the range, `blocks` still owing 2-3.
      // The resumed run gets zero batches (the backfill is complete), so no batch ever arrives to
      // seed coverage — the tail must close from the persisted map alone, or it stays unclaimed
      // on every subsequent run.
      await mkdir(path.join(dir, 'blocks'), { recursive: true })
      await writeFile(
        path.join(dir, '_sqd_parquet_state.test.json'),
        JSON.stringify({ id: 'test', cursor: { number: 3, hash: '0x3' }, coverage: { blocks: 2 } }),
      )
      await seedParquetFile(path.join(dir, 'blocks', '000000000000-000000000001.parquet'), [
        { blockNumber: 1, hash: '0x1', timestamp: 1000 },
      ])

      portal = await mockPortal([])
      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 3 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000003.parquet',
      ])
      // The tail file claims the window without inventing rows.
      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1])
      const persisted = JSON.parse(await readFile(path.join(dir, '_sqd_parquet_state.test.json'), 'utf8'))
      expect(persisted.coverage).toEqual({ blocks: 4 })
    })

    it('holds rows for an unnameable window across no-op rotations until the boundary moves', async () => {
      // The finalized head sticks at 1 while later batches release back-keyed rows into a writer
      // whose window cannot be named yet (coverage 2 > boundary 1). Size-rotation keeps firing;
      // those checkpoints publish nothing and skip the durable-state rewrite, and the held rows
      // must survive to the file that finally names their window.
      portal = await mockPortal([blocksResponse([1], 1), blocksResponse([2], 1), blocksResponse([3, 4], 4)])

      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 4 }),
      }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE],
          settings: { rollover: { maxBytes: 1 }, engine: parquetjsEngine({ rowGroupSize: 1 }) },
          onData: ({ store, data }) => {
            // Every row keyed at block 1 (an aggregate re-stamped to an already-final block), so
            // rows keep releasing while the boundary cursor stays pinned at 1.
            store.insert(
              'blocks',
              data.map((b) => ({ blockNumber: 1, hash: b.hash, timestamp: b.timestamp })),
            )
          },
        }),
      )

      expect(await listDataFiles(dir, 'blocks')).toEqual([
        '000000000000-000000000001.parquet',
        '000000000002-000000000004.parquet',
      ])
      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 1, 1, 1])
    })
  })

  describe('coverage naming across gaps (store-level)', () => {
    const newStore = async () => {
      await mkdir(path.join(dir, 'blocks'), { recursive: true })

      return new ParquetStore({
        dir,
        tables: [BLOCKS_TABLE],
        engine: parquetjsEngine({ rowGroupSize: 100 }),
      })
    }

    it('clamps a resume fallback that lands in a gap up to the next queried block', async () => {
      // Legacy resume (no persisted coverage) at the end of range [0,1]: fallbackStart is cursor+1=2,
      // which sits in the never-queried gap 2-4. The next file must start at 5, not 2 — a `2-6` name
      // would claim blocks 2-4 the pipe never fetched.
      const store = await newStore()
      store.seedCoverage(undefined, 2, [
        { from: 0, to: 1 },
        { from: 5, to: 6 },
      ])

      const published = await store.publishAll(6, { closeTails: true })
      expect(published.map((p) => `${p.from}-${p.to}`)).toEqual(['5-6'])
    })

    it('does not fold a skipped empty middle range into the next file', async () => {
      // Ranges [0,1], [5,6] (produces no batch, so it is skipped), [10,11]. After [0,1] closes,
      // coverage advances to 5; entering [10,11] must lift it to 10 so the next file is `10-11`, not
      // `5-11` — the latter would claim the never-queried gap 7-9.
      const store = await newStore()
      store.seedCoverage(undefined, 0, [
        { from: 0, to: 1 },
        { from: 5, to: 6 },
        { from: 10, to: 11 },
      ])

      await store.publishAll(1, { closeTails: true })
      store.advanceCoverageInto(10)
      const published = await store.publishAll(11, { closeTails: true })
      expect(published.map((p) => `${p.from}-${p.to}`)).toEqual(['10-11'])
    })

    it('leaves a table owing a window within the current range untouched', async () => {
      // advanceCoverageInto must only lift a coverage start that lags BELOW the range it is entering;
      // a table mid-range (owing a sat-out window) keeps its earlier start so it still stretches.
      const store = await newStore()
      store.seedCoverage(undefined, 0, [{ from: 0, to: 10 }])

      store.advanceCoverageInto(0)
      const published = await store.publishAll(4, { closeTails: true })
      expect(published.map((p) => `${p.from}-${p.to}`)).toEqual(['0-4'])
    })

    it('accepts a persisted coverage start that a query gap justifies', async () => {
      // Ranges [0,1] and [5,6], resume at cursor+1 = 2 (in the gap). The furthest a file could start
      // is nextQueriedBlock(2) = 5, so a persisted 5 is exactly consistent — not ahead.
      const store = await newStore()
      store.seedCoverage({ blocks: 5 }, 2, [
        { from: 0, to: 1 },
        { from: 5, to: 6 },
      ])

      const published = await store.publishAll(6, { closeTails: true })
      expect(published.map((p) => `${p.from}-${p.to}`)).toEqual(['5-6'])
    })

    it('clamps a persisted coverage start ahead of what the cursor allows, and reports it', async () => {
      // Single range 0-100, resume at cursor+1 = 4. A persisted start of 50 cannot be consistent
      // with that cursor (no gap justifies it). Honouring it would leave blocks 4-49 named by no
      // file at all, so it is clamped back to 4 — and the caller is told, since the usual cause is
      // an edit to the configured ranges.
      const store = await newStore()

      const clamped = store.seedCoverage({ blocks: 50 }, 4, [{ from: 0, to: 100 }])

      expect(clamped).toEqual([{ table: 'blocks', persisted: 50, seeded: 4 }])
      const published = await store.publishAll(60, { closeTails: true })
      expect(published.map((p) => `${p.from}-${p.to}`)).toEqual(['4-60'])
    })

    it('re-covers the blocks a removed gap exposed when the query ranges change', async () => {
      // Run 1 queried [0,1] + [5,6] and stopped at cursor 1 with coverage 5. Run 2 widens that to a
      // single [0,10]: blocks 2-4 are now fetched, so the next file must claim them rather than
      // honour a start that referred to a gap which no longer exists.
      const store = await newStore()

      store.seedCoverage({ blocks: 5 }, 2, [{ from: 0, to: 10 }])

      const published = await store.publishAll(10, { closeTails: true })
      expect(published.map((p) => `${p.from}-${p.to}`)).toEqual(['2-10'])
    })
  })

  describe('empty table', () => {
    it('claims its window with a zero-row file when a declared table receives no rows', async () => {
      const emptyTable: ParquetTable = { table: 'empty', schema: { blockNumber: { type: 'INT64' } } }
      portal = await mockPortal([blocksResponse([1, 2, 3], 3)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 3 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE, emptyTable], onData: insertBlocks }),
      )

      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 2, 3])
      // Blocks 0-3 WERE indexed, they just held nothing for this table — so one file says exactly
      // that. Publishing nothing would be indistinguishable from never having indexed the range.
      expect(await listDataFiles(dir, 'empty')).toEqual(['000000000000-000000000003.parquet'])

      const reader = await ParquetReader.openFile(path.join(dir, 'empty', '000000000000-000000000003.parquet'))
      expect(Number(reader.getRowCount())).toBe(0)
      await reader.close()
    })

    it('claims one window per run, not one per checkpoint', async () => {
      // Four checkpoints, but `empty` sits out all of them: the stretch means it owes a single
      // file at stream end rather than a degenerate one per window.
      const emptyTable: ParquetTable = { table: 'empty', schema: { blockNumber: { type: 'INT64' } } }
      portal = await mockPortal([blocksResponse([1], 1), blocksResponse([2], 2), blocksResponse([3], 3)])

      await evmPortalStream({
        id: 'test',
        portal: { url: portal.url, maxBytes: 1 },
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE, emptyTable],
          settings: { rollover: { maxBytes: 1 }, engine: parquetjsEngine({ rowGroupSize: 1 }) },
          onData: insertBlocks,
        }),
      )

      expect(await listDataFiles(dir, 'empty')).toEqual(['000000000000-000000000003.parquet'])
    })
  })

  describe('no-finality passthrough', () => {
    it('writes every row immediately when there is no finalized head', async () => {
      // No `head` → no finalized head → threshold Infinity → nothing is buffered.
      portal = await mockPortal([blocksResponse([1, 2, 3])])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 3 }) }).pipeTo(
        parquetTarget({ dir, tables: [BLOCKS_TABLE], onData: insertBlocks }),
      )

      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 2, 3])
    })
  })

  describe('profiling', () => {
    it('opens a target span per batch, with a child span per phase', async () => {
      portal = await mockPortal([blocksResponse([1, 2, 3], 3)])
      const spans = spanTracker()

      // The checkpoint has to fire inside the batch (that is where a span exists to nest under);
      // `intervalBlocks` triggers off the cursor, so unlike `maxBytes` it doesn't depend on the
      // write stream having reached disk by the time `size()` stats it.
      await evmPortalStream({
        id: 'test',
        portal: portal.url,
        profiler: spans.hooks,
        outputs: blockDecoder({ from: 0, to: 3 }),
      }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE],
          settings: { rollover: { intervalBlocks: 1 } },
          onData: insertBlocks,
        }),
      )

      expect(spans.targetSpans()).toEqual([
        'batch > parquet',
        'batch > parquet > data handler',
        'batch > parquet > flush batch',
        'batch > parquet > checkpoint (interval-blocks)',
        'batch > parquet > checkpoint (interval-blocks) > publish files',
        'batch > parquet > checkpoint (interval-blocks) > save cursor',
      ])
      expect(spans.unended()).toEqual([])
    })

    it('closes the target span when the batch throws', async () => {
      portal = await mockPortal([blocksResponse([1, 2, 3], 3)])
      const spans = spanTracker()

      await expect(
        evmPortalStream({
          id: 'test',
          portal: portal.url,
          profiler: spans.hooks,
          outputs: blockDecoder({ from: 0, to: 3 }),
        }).pipeTo(
          parquetTarget({
            dir,
            tables: [BLOCKS_TABLE],
            onData: () => {
              throw new Error('onData failed')
            },
          }),
        ),
      ).rejects.toThrowError('onData failed')

      expect(spans.targetSpans()).toEqual(['batch > parquet', 'batch > parquet > data handler'])
      expect(spans.unended()).toEqual([])
    })
  })

  describe('multi-table', () => {
    it('writes independent files per table from the same batch', async () => {
      const logsTable: ParquetTable = {
        table: 'logs',
        schema: {
          blockNumber: { type: 'INT64' },
          logIndex: { type: 'INT32' },
          address: { type: 'UTF8' },
        },
      }
      portal = await mockPortal([blocksResponse([1, 2, 3], 3)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 3 }) }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE, logsTable],
          onData: ({ store, data }) => {
            store.insert(
              'blocks',
              data.map((b) => ({ blockNumber: b.number, hash: b.hash, timestamp: b.timestamp })),
            )
            // Two log rows per block, giving the logs table a different shape + cardinality.
            store.insert(
              'logs',
              data.flatMap((b) => [
                { blockNumber: b.number, logIndex: 0, address: `0xa${b.number}` },
                { blockNumber: b.number, logIndex: 1, address: `0xb${b.number}` },
              ]),
            )
          },
        }),
      )

      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 2, 3])
      expect(await readLogs(dir)).toEqual([
        { blockNumber: 1, logIndex: 0, address: '0xa1' },
        { blockNumber: 1, logIndex: 1, address: '0xb1' },
        { blockNumber: 2, logIndex: 0, address: '0xa2' },
        { blockNumber: 2, logIndex: 1, address: '0xb2' },
        { blockNumber: 3, logIndex: 0, address: '0xa3' },
        { blockNumber: 3, logIndex: 1, address: '0xb3' },
      ])
      // Each table published its own single file covering the same window, blocks 0–3.
      expect(await listDataFiles(dir, 'blocks')).toEqual(['000000000000-000000000003.parquet'])
      expect(await listDataFiles(dir, 'logs')).toEqual(['000000000000-000000000003.parquet'])
    })
  })

  describe('store.insert guard', () => {
    it('throws synchronously when inserting into an undeclared table', async () => {
      portal = await mockPortal([blocksResponse([1], 1)])

      await expect(
        evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 1 }) }).pipeTo(
          parquetTarget({
            dir,
            tables: [BLOCKS_TABLE],
            onData: ({ store }) => {
              store.insert('not_declared', [{ blockNumber: 1 }])
            },
          }),
        ),
      ).rejects.toThrowError(/not declared/)
    })

    it('accumulates multiple inserts into the same table within a batch', async () => {
      portal = await mockPortal([blocksResponse([1, 2], 2)])

      await evmPortalStream({ id: 'test', portal: portal.url, outputs: blockDecoder({ from: 0, to: 2 }) }).pipeTo(
        parquetTarget({
          dir,
          tables: [BLOCKS_TABLE],
          onData: ({ store, data }) => {
            // Two separate inserts for the same table must both be staged + flushed.
            for (const b of data) {
              store.insert('blocks', [{ blockNumber: b.number, hash: b.hash, timestamp: b.timestamp }])
            }
          },
        }),
      )

      expect((await readBlocks(dir)).map((r) => r.blockNumber)).toEqual([1, 2])
    })
  })

  describe('crash safety', () => {
    it('discards open temp files and does not advance the cursor when a batch throws', async () => {
      // Batch 1 opens a writer with a finalized row (no checkpoint — huge default maxBytes);
      // batch 2 throws before any checkpoint, so the open temp file must be discarded and the
      // cursor must never be persisted. maxBytes:1 forces one batch per response.
      portal = await mockPortal([blocksResponse([1], 1), blocksResponse([2], 2)])

      let batches = 0
      await expect(
        evmPortalStream({
          id: 'test',
          portal: { url: portal.url, maxBytes: 1 },
          outputs: blockDecoder({ from: 0, to: 2 }),
        }).pipeTo(
          parquetTarget({
            dir,
            tables: [BLOCKS_TABLE],
            onData: ({ store, data }) => {
              insertBlocks({ store, data })
              batches++
              if (batches === 2) throw new Error('boom')
            },
          }),
        ),
      ).rejects.toThrowError('boom')

      // No file was published (the open writer was discarded, not finalized)...
      expect(await listDataFiles(dir, 'blocks')).toEqual([])
      // ...no temp file leaked...
      expect((await listAll(dir, 'blocks')).filter((f) => f.startsWith('.tmp-'))).toEqual([])
      // ...and the cursor was never persisted (no checkpoint happened). The pipe runs with
      // id 'test', so the namespaced state file is the one that must be absent.
      expect((await listAll(dir, '')).includes('_sqd_parquet_state.test.json')).toBe(false)
    })
  })

  describe('ParquetSegmentWriter', () => {
    it('lazy-opens, grows the temp file, and finish() completes a readable Parquet file', async () => {
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })
      const schema = new ParquetSchema({ blockNumber: { type: 'INT64' } })

      const tmpPath = path.join(segDir, '.tmp-writer.parquet')
      const writer = new ParquetSegmentWriter({ tmpPath, schema, rowGroupSize: 1 })
      expect(await writer.size()).toBe(0)

      await writer.append([{ blockNumber: 5 }, { blockNumber: 8 }])
      expect(await writer.size()).toBeGreaterThan(0)

      await writer.finish()
      const reader = await ParquetReader.openFile(tmpPath)
      expect(Number(reader.getRowCount())).toBe(2)
      await reader.close()
    })

    it('finish() with no appended rows writes a real, schema-only file (tail closing)', async () => {
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })
      const schema = new ParquetSchema({ blockNumber: { type: 'INT64' } })

      const tmpPath = path.join(segDir, '.tmp-empty.parquet')
      const writer = new ParquetSegmentWriter({ tmpPath, schema, rowGroupSize: 1 })
      await writer.finish()

      const reader = await ParquetReader.openFile(tmpPath)
      expect(Number(reader.getRowCount())).toBe(0)
      expect(reader.getSchema().fieldList.length).toBe(1)
      await reader.close()
    })

    it('abort() releases the stream and is safe to call twice', async () => {
      // Exercises the error-path fd cleanup: destroy the owned stream, idempotently, with no throw.
      // Deleting the temp file is the target's job, not the writer's.
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })
      const schema = new ParquetSchema({ blockNumber: { type: 'INT64' } })

      const writer = new ParquetSegmentWriter({
        tmpPath: path.join(segDir, '.tmp-abort.parquet'),
        schema,
        rowGroupSize: 1,
      })
      await writer.append([{ blockNumber: 1 }])
      await writer.abort()
      await writer.abort()
    })
  })

  describe('finalizeSegmentFile', () => {
    /** Writes a finished single-column segment at `tmpPath` via the parquetjs writer. */
    async function writeSegment(tmpPath: string, blockNumbers: number[]): Promise<void> {
      const schema = new ParquetSchema({ blockNumber: { type: 'INT64' } })
      const writer = new ParquetSegmentWriter({ tmpPath, schema, rowGroupSize: 1 })
      if (blockNumbers.length > 0) await writer.append(blockNumbers.map((blockNumber) => ({ blockNumber })))
      await writer.finish()
    }

    it('names the file for the coverage range, not the rows inside it', async () => {
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })

      // Rows span 5–8, but the window covered is 2–10: the name follows the window.
      const tmpPath = path.join(segDir, '.tmp-pub.parquet')
      await writeSegment(tmpPath, [5, 8])

      const published = await finalizeSegmentFile({
        dir: segDir,
        tmpPath,
        rows: 2,
        range: { from: 2, to: 10 },
        engine: 'test',
      })
      expect(published.path.endsWith('000000000002-000000000010.parquet')).toBe(true)
      expect(published.rows).toBe(2)
      expect(published.bytes).toBeGreaterThan(0)
      expect((await readdir(segDir)).sort()).toEqual(['000000000002-000000000010.parquet'])
    })

    it('publishes a zero-row segment so a table can claim a window it produced nothing in', async () => {
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })

      const tmpPath = path.join(segDir, '.tmp-zero.parquet')
      await writeSegment(tmpPath, [])

      const published = await finalizeSegmentFile({
        dir: segDir,
        tmpPath,
        rows: 0,
        range: { from: 2, to: 4 },
        engine: 'test',
      })
      expect(published.path.endsWith('000000000002-000000000004.parquet')).toBe(true)
      expect(published.rows).toBe(0)

      // A real, readable Parquet file — schema and footer present, just no rows.
      const reader = await ParquetReader.openFile(published.path)
      expect(Number(reader.getRowCount())).toBe(0)
      await reader.close()
    })

    it('refuses an inverted coverage range', async () => {
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })

      const tmpPath = path.join(segDir, '.tmp-inv.parquet')
      await writeSegment(tmpPath, [5])

      await expect(
        finalizeSegmentFile({ dir: segDir, tmpPath, rows: 1, range: { from: 10, to: 4 }, engine: 'test' }),
      ).rejects.toThrowError(/inverted coverage range/)
    })

    it('refuses to overwrite an existing file (collision)', async () => {
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })

      const firstTmp = path.join(segDir, '.tmp-first.parquet')
      await writeSegment(firstTmp, [1])
      await finalizeSegmentFile({ dir: segDir, tmpPath: firstTmp, rows: 1, range: { from: 0, to: 1 }, engine: 'test' })

      const secondTmp = path.join(segDir, '.tmp-second.parquet')
      await writeSegment(secondTmp, [1])
      await expect(
        finalizeSegmentFile({ dir: segDir, tmpPath: secondTmp, rows: 1, range: { from: 0, to: 1 }, engine: 'test' }),
      ).rejects.toThrowError(/Refusing to overwrite/)
    })

    it('refuses a finished file that is not Parquet (magic-bytes check)', async () => {
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })

      const tmpPath = path.join(segDir, '.tmp-junk.parquet')
      await writeFile(tmpPath, 'this is definitely not a parquet file')

      await expect(
        finalizeSegmentFile({ dir: segDir, tmpPath, rows: 1, range: { from: 0, to: 1 }, engine: 'test' }),
      ).rejects.toThrowError(/Parquet magic bytes/)
      // The junk file was not renamed into a published name.
      expect((await readdir(segDir)).sort()).toEqual(['.tmp-junk.parquet'])
    })

    it('refuses arbitrary bytes merely wrapped in the magic (footer length check)', async () => {
      const segDir = path.join(dir, 'seg')
      await mkdir(segDir, { recursive: true })

      // Correct magic at both ends, but the 4 bytes before the trailing magic — the footer
      // length field — are payload, not a length that fits inside the file.
      const tmpPath = path.join(segDir, '.tmp-spoof.parquet')
      await writeFile(tmpPath, 'PAR1this is not a parquet footer PAR1')

      await expect(
        finalizeSegmentFile({ dir: segDir, tmpPath, rows: 1, range: { from: 0, to: 1 }, engine: 'test' }),
      ).rejects.toThrowError(/footer length field/)
      expect((await readdir(segDir)).sort()).toEqual(['.tmp-spoof.parquet'])
    })
  })

  describe('ParquetState', () => {
    it('throws STATE_CORRUPT on an unparseable state file', async () => {
      await writeFile(path.join(dir, '_sqd_parquet_state.json'), 'not-json')
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      await expect(state.getCursor()).rejects.toThrowError(/could not be parsed/)
    })

    it('returns undefined on a cold start and creates table directories', async () => {
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      expect(await state.getCursor()).toBeUndefined()
      expect(await listAll(dir, 'blocks')).toEqual([])
    })

    it('returns the persisted cursor and finalized head as resume state', async () => {
      await writeFile(
        path.join(dir, '_sqd_parquet_state.json'),
        JSON.stringify({ cursor: { number: 5, hash: '0x5' }, finalized: { number: 5, hash: '0x5f' } }),
      )
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      expect(await state.getCursor()).toEqual({
        latest: { number: 5, hash: '0x5' },
        finalized: { number: 5, hash: '0x5f' },
      })
    })

    it('returns finalized: null for state written before the finalized field existed', async () => {
      await writeFile(path.join(dir, '_sqd_parquet_state.json'), JSON.stringify({ cursor: { number: 5, hash: '0x5' } }))
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      expect(await state.getCursor()).toEqual({ latest: { number: 5, hash: '0x5' }, finalized: null })
    })

    it('persists the finalized head alongside the cursor so the source can re-seed on restart', async () => {
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })
      await state.getCursor()

      await state.saveCursor({ number: 7, hash: '0x7' }, { number: 7, hash: '0x7f' })

      const persisted = JSON.parse(await readFile(path.join(dir, '_sqd_parquet_state.json'), 'utf8'))
      expect(persisted).toMatchObject({
        cursor: { number: 7, hash: '0x7' },
        finalized: { number: 7, hash: '0x7f' },
      })
    })

    it('refuses a data file that straddles the cursor without destroying it first', async () => {
      // A restored older state file, or a hand-rewound cursor: the committed cursor is block 1, but
      // a data file covers 0-3 — it straddles the cursor, so it holds committed data (blocks <= 1) a
      // resume from block 2 would never re-fetch. Deleting it would lose that data.
      await writeFile(
        path.join(dir, '_sqd_parquet_state.json'),
        JSON.stringify({ cursor: { number: 1, hash: '0x1' }, coverage: { blocks: 4 } }),
      )
      const blocksDir = path.join(dir, 'blocks')
      await mkdir(blocksDir, { recursive: true })
      await seedParquetFile(path.join(blocksDir, '000000000000-000000000003.parquet'), [
        { blockNumber: 3, hash: '0x3', timestamp: 0 },
      ])
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      await expect(state.getCursor()).rejects.toThrowError(/straddles the committed cursor/)
      // The point of refusing first: the file is still there to be recovered from by hand.
      expect(await listDataFiles(dir, 'blocks')).toEqual(['000000000000-000000000003.parquet'])
    })

    it("deletes a sparse table's stretched remnant, which straddles the cursor by construction", async () => {
      // The ordinary crash window, not a corrupt state file: the checkpoint at cursor 6 published
      // `sparse`'s stretched 2-6 file and died before saveCursor, so the state still says cursor 5.
      // `sparse` sat out the cursor-5 checkpoint, which is exactly why its coverage start (2) is
      // below the cursor and its next file straddles it. Refusing here would make an ordinary crash
      // unrecoverable; the file holds only rows from blocks above the cursor, which a resume
      // re-fetches and regenerates.
      await writeFile(
        path.join(dir, '_sqd_parquet_state.json'),
        JSON.stringify({ cursor: { number: 5, hash: '0x5' }, coverage: { blocks: 6, sparse: 2 } }),
      )
      for (const table of ['blocks', 'sparse']) {
        await mkdir(path.join(dir, table), { recursive: true })
      }
      await seedParquetFile(path.join(dir, 'sparse', '000000000002-000000000006.parquet'), [
        { blockNumber: 6, hash: '0x6', timestamp: 0 },
      ])
      await seedParquetFile(path.join(dir, 'blocks', '000000000000-000000000005.parquet'), [
        { blockNumber: 5, hash: '0x5', timestamp: 0 },
      ])
      const state = new ParquetState({ dir, tables: ['blocks', 'sparse'], logger: testLogger() })

      expect(await state.getCursor()).toEqual({ latest: { number: 5, hash: '0x5' }, finalized: null })
      expect(await listDataFiles(dir, 'sparse')).toEqual([])
      // The committed file, which ends exactly at the cursor, is untouched.
      expect(await listDataFiles(dir, 'blocks')).toEqual(['000000000000-000000000005.parquet'])
    })

    it('still refuses a straddling file that the persisted coverage does not explain', async () => {
      // Same straddle shape as the remnant above, but the coverage map says `sparse` was next due
      // to publish from block 4 — so the 2-6 file was written by a different run, and its blocks
      // 2-3 are committed data no resume would re-fetch.
      await writeFile(
        path.join(dir, '_sqd_parquet_state.json'),
        JSON.stringify({ cursor: { number: 5, hash: '0x5' }, coverage: { sparse: 4 } }),
      )
      await mkdir(path.join(dir, 'sparse'), { recursive: true })
      await seedParquetFile(path.join(dir, 'sparse', '000000000002-000000000006.parquet'), [
        { blockNumber: 6, hash: '0x6', timestamp: 0 },
      ])
      const state = new ParquetState({ dir, tables: ['sparse'], logger: testLogger() })

      await expect(state.getCursor()).rejects.toThrowError(/straddles the committed cursor/)
      expect(await listDataFiles(dir, 'sparse')).toEqual(['000000000002-000000000006.parquet'])
    })

    it('accepts coverage ahead of the cursor when a query gap justifies it (no straddling file)', async () => {
      // Regression: a disjoint-range backfill stopped at a range boundary persists cursor=1 with
      // coverage=5 (the next queried block after the 2-4 gap between ranges [0,1] and [5,6]). The
      // [0,1] file ends exactly at the cursor, so nothing straddles it — resume must accept this.
      await writeFile(
        path.join(dir, '_sqd_parquet_state.json'),
        JSON.stringify({ cursor: { number: 1, hash: '0x1' }, coverage: { blocks: 5 } }),
      )
      const blocksDir = path.join(dir, 'blocks')
      await mkdir(blocksDir, { recursive: true })
      await seedParquetFile(path.join(blocksDir, '000000000000-000000000001.parquet'), [
        { blockNumber: 1, hash: '0x1', timestamp: 0 },
      ])
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      expect(await state.getCursor()).toEqual({ latest: { number: 1, hash: '0x1' }, finalized: null })
      expect(state.coverage).toEqual({ blocks: 5 })
      // The committed file is untouched.
      expect(await listDataFiles(dir, 'blocks')).toEqual(['000000000000-000000000001.parquet'])
    })

    it('accepts coverage exactly one block past the cursor (the normal case)', async () => {
      await writeFile(
        path.join(dir, '_sqd_parquet_state.json'),
        JSON.stringify({ cursor: { number: 3, hash: '0x3' }, coverage: { blocks: 4 } }),
      )
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      expect(await state.getCursor()).toEqual({ latest: { number: 3, hash: '0x3' }, finalized: null })
      expect(state.coverage).toEqual({ blocks: 4 })
    })

    it('points a legacy (no-coverage) straddle at the one-file remedy', async () => {
      // Pre-upgrade state: a cursor, no coverage map. The old version's crash remnant can itself
      // straddle the cursor (a back-keyed row puts a row-min/max name's `from` at or below it),
      // and with nothing to explain the straddle the refusal fires on the first post-upgrade
      // start. The message must offer the single-file remedy for that case, not only the
      // delete-the-table one.
      await writeFile(path.join(dir, '_sqd_parquet_state.json'), JSON.stringify({ cursor: { number: 2, hash: '0x2' } }))
      await mkdir(path.join(dir, 'blocks'), { recursive: true })
      await seedParquetFile(path.join(dir, 'blocks', '000000000002-000000000004.parquet'), [
        { blockNumber: 2, hash: '0x2', timestamp: 0 },
      ])
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      await expect(state.getCursor()).rejects.toThrowError(/deleting just this file and restarting is enough/)
      expect(await listDataFiles(dir, 'blocks')).toEqual(['000000000002-000000000004.parquet'])
    })

    it('throws (does not silently leave overlap) when an over-cursor file cannot be deleted', async () => {
      await writeFile(path.join(dir, '_sqd_parquet_state.json'), JSON.stringify({ cursor: { number: 2, hash: '0x2' } }))
      // A *directory* named like a data file cannot be unlink()-ed (EISDIR/EPERM, even as root),
      // forcing the recovery delete to fail; it must surface rather than leave a duplicate-causing file.
      await mkdir(path.join(dir, 'blocks', '000000000003-000000000005.parquet'), { recursive: true })
      const state = new ParquetState({ dir, tables: ['blocks'], logger: testLogger() })

      expect.assertions(1)
      try {
        await state.getCursor()
      } catch (e) {
        expect((e as ParquetTargetError).code).toBe(PARQUET_ERROR_CODES.RECOVERY_DELETE_FAILED)
      }
    })
  })
})

describe('parquet index barrel', () => {
  it('re-exports the public API', async () => {
    const mod = await import('./index.js')
    expect(typeof mod.parquetTarget).toBe('function')
    expect(typeof mod.ParquetStore).toBe('function')
    expect(typeof mod.ParquetTargetError).toBe('function')
    expect(mod.PARQUET_ERROR_CODES.NO_TABLES).toBe('E2301')
  })
})
