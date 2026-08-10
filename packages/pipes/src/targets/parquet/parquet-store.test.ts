import { mkdir, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { PARQUET_ERROR_CODES, ParquetTargetError } from './errors.js'
import { ParquetStore } from './parquet-store.js'
import { parquetjsEngine } from './parquetjs/parquetjs-engine.js'
import type { ParquetTable } from './schema.js'

const BLOCKS_TABLE: ParquetTable = {
  table: 'blocks',
  schema: {
    blockNumber: { type: 'INT64' },
    hash: { type: 'UTF8' },
    timestamp: { type: 'INT64' },
  },
}

describe('ParquetStore', () => {
  let dir: string

  beforeEach(async () => {
    dir = await mkdtemp(path.join(tmpdir(), 'sqd-parquet-store-test-'))
  })

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true })
  })

  describe('dev-mode value validation', () => {
    const bytesTable: ParquetTable = {
      table: 't',
      schema: { blockNumber: { type: 'INT64' }, raw: { type: 'BYTE_ARRAY' } },
    }

    it('rejects a hex string handed to a BYTE_ARRAY column', () => {
      const store = new ParquetStore({
        dir,
        tables: [bytesTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      expect.assertions(1)
      try {
        store.insert('t', [{ blockNumber: 1, raw: '0xdeadbeef' }])
      } catch (e) {
        expect((e as ParquetTargetError).code).toBe(PARQUET_ERROR_CODES.VALUE_INVALID)
      }
    })

    it('rejects an INT64 number above 2^53 (precision already lost)', () => {
      const store = new ParquetStore({
        dir,
        tables: [BLOCKS_TABLE],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      expect.assertions(1)
      try {
        store.insert('blocks', [{ blockNumber: 2 ** 53, hash: '0x1', timestamp: 1 }])
      } catch (e) {
        expect((e as ParquetTargetError).code).toBe(PARQUET_ERROR_CODES.VALUE_INVALID)
      }
    })

    it('accepts a Buffer for BYTE_ARRAY and a bigint for INT64', () => {
      const store = new ParquetStore({
        dir,
        tables: [bytesTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      expect(() => store.insert('t', [{ blockNumber: 1n, raw: Buffer.from('deadbeef', 'hex') }])).not.toThrow()
    })

    const stampsTable: ParquetTable = {
      table: 'stamps',
      schema: {
        blockNumber: { type: 'INT64' },
        at: { type: 'TIMESTAMP' },
        day: { type: 'DATE', optional: true },
        meta: { type: 'JSON', optional: true },
      },
    }

    it('accepts Date/epoch-millis TIMESTAMP, Date/integer-days/null DATE and JSON-serializable values', () => {
      const store = new ParquetStore({
        dir,
        tables: [stampsTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      expect(() =>
        store.insert('stamps', [
          {
            blockNumber: 1,
            at: new Date('2024-01-01T15:30:00Z'),
            day: new Date('2024-01-01T15:30:00Z'),
            meta: { a: [1, 'x'] },
          },
          { blockNumber: 2, at: 1704122400000, day: 19723, meta: 'plain string is valid JSON' },
          { blockNumber: 3, at: new Date('2024-01-01T15:30:00Z'), day: null, meta: null },
        ]),
      ).not.toThrow()
    })

    it('rejects fractional, negative, epoch-millis-magnitude and pre-1970 DATE values', () => {
      const store = new ParquetStore({
        dir,
        tables: [stampsTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      // 19723.5: not whole days; -1: the library rejects negatives; 1704067200000: epoch millis
      // passed by mistake (would crash the int32 encoder at flush time); pre-1970 Date: the
      // library truncates toward zero, landing on the wrong day.
      const bad = [19723.5, -1, 1704067200000, new Date('1969-12-31T12:00:00Z')]
      expect.assertions(4)
      for (const day of bad) {
        try {
          store.insert('stamps', [{ blockNumber: 1, at: new Date(0), day }])
        } catch (e) {
          expect((e as ParquetTargetError).code).toBe(PARQUET_ERROR_CODES.VALUE_INVALID)
        }
      }
    })

    it('rejects JSON values the writer cannot serialize (bigint, circular)', () => {
      const store = new ParquetStore({
        dir,
        tables: [stampsTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      const circular: Record<string, unknown> = {}
      circular['self'] = circular
      // The library would crash mid-flush with a context-free "Do not know how to serialize a
      // BigInt" — the dev check surfaces the table/column instead.
      const bad = [{ amount: 5n }, circular]
      expect.assertions(2)
      for (const meta of bad) {
        try {
          store.insert('stamps', [{ blockNumber: 1, at: new Date(0), meta }])
        } catch (e) {
          expect((e as ParquetTargetError).code).toBe(PARQUET_ERROR_CODES.VALUE_INVALID)
        }
      }
    })

    const nestedTable: ParquetTable = {
      table: 'n',
      schema: {
        blockNumber: { type: 'INT64' },
        user: { type: 'STRUCT', fields: { name: { type: 'UTF8' }, age: { type: 'INT32', optional: true } } },
        tags: { type: 'LIST', element: { type: 'UTF8' }, optional: true },
      },
    }

    it('accepts plain nested objects for STRUCT and plain arrays for LIST', () => {
      const store = new ParquetStore({
        dir,
        tables: [nestedTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      expect(() =>
        store.insert('n', [
          { blockNumber: 1, user: { name: 'alice', age: 30 }, tags: ['a', 'b'] },
          { blockNumber: 2, user: { name: 'bob', age: null }, tags: [] },
          { blockNumber: 3, user: { name: 'eve' }, tags: null },
        ]),
      ).not.toThrow()
    })

    it('rejects bad nested values with the offending path in the message', () => {
      const store = new ParquetStore({
        dir,
        tables: [nestedTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      // [row, expected message fragment]: scalar where a struct object is expected; missing
      // required nested field; non-array LIST; null element under a required element decl;
      // wrong element type.
      const cases: [Record<string, unknown>, RegExp][] = [
        [{ blockNumber: 1, user: 'oops', tags: null }, /user.*expects a plain object/],
        [{ blockNumber: 1, user: { age: 1 }, tags: null }, /user\.name.*required/],
        [{ blockNumber: 1, user: { name: 'a' }, tags: 'x,y' }, /tags.*expects an array/],
        [{ blockNumber: 1, user: { name: 'a' }, tags: ['ok', null] }, /tags\[1\].*required/],
        [{ blockNumber: 1, user: { name: 'a' }, tags: [42] }, /tags\[0\].*expects a string/],
      ]
      expect.assertions(2 * cases.length)
      for (const [row, fragment] of cases) {
        try {
          store.insert('n', [row])
        } catch (e) {
          expect((e as ParquetTargetError).code).toBe(PARQUET_ERROR_CODES.VALUE_INVALID)
          expect((e as ParquetTargetError).message).toMatch(fragment)
        }
      }
    })

    it('rejects Map and Set STRUCT values (entries are invisible to property access)', () => {
      const store = new ParquetStore({
        dir,
        tables: [nestedTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      // The writer shreds structs via `value[field]`, so Map/Set entries read as undefined:
      // required fields fail with a misleading "required but undefined" and optional fields
      // silently write null — reject the container itself instead.
      const cases: unknown[] = [new Map([['name', 'alice']]), new Set(['alice'])]
      expect.assertions(2 * cases.length)
      for (const user of cases) {
        try {
          store.insert('n', [{ blockNumber: 1, user, tags: null }])
        } catch (e) {
          expect((e as ParquetTargetError).code).toBe(PARQUET_ERROR_CODES.VALUE_INVALID)
          expect((e as ParquetTargetError).message).toMatch(/user.*expects a plain object/)
        }
      }
    })

    it('accepts class instances and null-prototype objects for STRUCT', () => {
      const store = new ParquetStore({
        dir,
        tables: [nestedTable],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      // Both expose their fields to property access, so they write correctly — the STRUCT
      // check must not tighten to a prototype test that would reject them.
      class User {
        name = 'alice'
      }
      const nullProto = Object.assign(Object.create(null), { name: 'bob' })

      expect(() =>
        store.insert('n', [
          { blockNumber: 1, user: new User(), tags: null },
          { blockNumber: 2, user: nullProto, tags: null },
        ]),
      ).not.toThrow()
    })
  })

  describe('coverage', () => {
    it('seeds from persisted starts and falls back for tables the state does not mention', () => {
      const other: ParquetTable = { table: 'other', schema: { blockNumber: { type: 'INT64' } } }
      const store = new ParquetStore({
        dir,
        tables: [BLOCKS_TABLE, other],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })

      // `blocks` owes an earlier window (start 5) while this run resumes at 7; `other` was declared
      // after the state was written, so it falls back to where this run does.
      store.seedCoverage({ blocks: 5 }, 7)

      expect(store.coverage()).toEqual({ blocks: 5, other: 7 })
    })

    it.each([
      ['non-numeric', 'garbage' as unknown as number],
      ['NaN', Number.NaN],
      // pad(-5) renders as "0000000000-5", so the filename would gain an extra dash and stop
      // matching the data-file pattern — crash recovery would never see that file again.
      ['negative', -5],
      ['fractional', 2.5],
    ])('ignores a %s persisted start rather than steering the filename with it', (_label, value) => {
      const store = new ParquetStore({
        dir,
        tables: [BLOCKS_TABLE],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })

      store.seedCoverage({ blocks: value }, 7)

      expect(store.coverage()).toEqual({ blocks: 7 })
    })

    it('skips the gap between disjoint ranges when advancing past a publish', async () => {
      await mkdir(path.join(dir, 'blocks'), { recursive: true })
      const store = new ParquetStore({
        dir,
        tables: [BLOCKS_TABLE],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      store.seedCoverage(undefined, 0, [
        { from: 0, to: 1 },
        { from: 5, to: 6 },
      ])
      store.insert('blocks', [{ blockNumber: 1, hash: '0x1', timestamp: 1 }])
      await store.flushBatch()

      await store.publishAll(1)

      // Blocks 2-4 are never queried, so the next file must start at 5 — not 2.
      expect(store.coverage()).toEqual({ blocks: 5 })
    })

    it('keeps a writer it could not name a window for, and publishes it once the boundary moves', async () => {
      // The boundary cursor does not advance while the finalized head is stuck, so a second
      // checkpoint at the same block has no window to name (coverage start 2 > to 1). The rows
      // released in between — an aggregate keyed back at block 1 — must stay in the open writer:
      // dropping it here would lose them and orphan its temp file.
      await mkdir(path.join(dir, 'blocks'), { recursive: true })
      const store = new ParquetStore({
        dir,
        tables: [BLOCKS_TABLE],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      store.seedCoverage(undefined, 0)
      store.insert('blocks', [{ blockNumber: 1, hash: '0x1', timestamp: 1 }])
      await store.flushBatch()
      await store.publishAll(1)

      store.insert('blocks', [{ blockNumber: 1, hash: '0x1', timestamp: 1 }])
      await store.flushBatch()
      expect(await store.publishAll(1)).toEqual([])
      expect(store.hasOpenWriters).toBe(true)

      // Once the head moves the window exists, and the held row is in the file that names it.
      const published = await store.publishAll(3)
      expect(published.map((p) => `${p.from}-${p.to}`)).toEqual(['2-3'])
      expect(published[0].rows).toBe(1)
    })

    it('does not drop the writers of tables left out of an explicit publish set', async () => {
      // The range-end checkpoint passes only the tables that owe coverage. Any other open writer
      // belongs to a later window, not to the bin.
      const other: ParquetTable = { table: 'other', schema: { blockNumber: { type: 'INT64' } } }
      for (const table of ['blocks', 'other']) {
        await mkdir(path.join(dir, table), { recursive: true })
      }
      const store = new ParquetStore({
        dir,
        tables: [BLOCKS_TABLE, other],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      store.seedCoverage(undefined, 0)

      store.insert('blocks', [{ blockNumber: 1, hash: '0x1', timestamp: 1 }])
      store.insert('other', [{ blockNumber: 1 }])
      await store.flushBatch()

      const published = await store.publishAll(1, { closeTails: true, tables: ['blocks'] })

      expect(published.map((p) => p.table)).toEqual(['blocks'])
      expect(store.hasOpenWriters).toBe(true)
      expect((await store.publishAll(2)).map((p) => `${p.table} ${p.from}-${p.to}`)).toEqual(['other 0-2'])
    })

    it('refuses to publish before coverage is seeded rather than inventing a range', async () => {
      // The target gets its table dirs from ParquetState.getCursor(); this test drives the store
      // directly, so it has to make them itself.
      await mkdir(path.join(dir, 'blocks'), { recursive: true })
      const store = new ParquetStore({
        dir,
        tables: [BLOCKS_TABLE],
        engine: parquetjsEngine({ rowGroupSize: 1 }),
      })
      store.insert('blocks', [{ blockNumber: 1, hash: '0x1', timestamp: 1 }])
      await store.flushBatch()

      expect.assertions(1)
      try {
        await store.publishAll(1)
      } catch (e) {
        expect((e as ParquetTargetError).code).toBe(PARQUET_ERROR_CODES.COVERAGE_RANGE_INVALID)
      } finally {
        await store.close()
      }
    })
  })
})
