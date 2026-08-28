import { describe, expect, it } from 'vitest'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import {
  buildAttributes,
  buildCdcMessage,
  canonicalCdcMessageBytes,
  canonicalJson,
  changeSequenceNumber,
  encodeCdcMessage,
  encodeRow,
  readRowId,
  uidValue,
  validateUserAttributes,
} from './protocol.js'

function codeOf(fn: () => unknown): string {
  try {
    fn()
  } catch (e) {
    if (e instanceof PubsubTargetError) return e.code

    throw e
  }

  throw new Error('expected the codec to reject this value')
}

describe('canonicalJson', () => {
  const accepted: [name: string, input: unknown, output: string][] = [
    ['bigint', 10n ** 18n, '"1000000000000000000"'],
    ['negative bigint', -1n, '"-1"'],
    ['safe integer', 42, '42'],
    ['finite non-integer', 0.5, '0.5'],
    ['exponential non-integer', 1e-7, '1e-7'],
    ['negative zero', -0, '0'],
    ['string', 'a"b\\c\n', '"a\\"b\\\\c\\n"'],
    ['non-ascii string stays raw', 'привет', '"привет"'],
    ['boolean', true, 'true'],
    ['null', null, 'null'],
    ['undefined object property is omitted', { a: 1, b: undefined }, '{"a":1}'],
    ['Uint8Array', new Uint8Array([0xde, 0xad, 0xbe, 0xef]), '"0xdeadbeef"'],
    ['empty Uint8Array', new Uint8Array([]), '"0x"'],
    ['other ArrayBufferView', new Uint16Array([0x0102]), '"0x0201"'],
    ['ArrayBuffer', new Uint8Array([1, 255]).buffer, '"0x01ff"'],
    ['Date is RFC 3339 and keeps milliseconds', new Date(1_700_000_000_999), '"2023-11-14T22:13:20.999Z"'],
    ['Date before the epoch', new Date(-1), '"1969-12-31T23:59:59.999Z"'],
    ['array preserves order', [3, 1, 2], '[3,1,2]'],
    ['nested object sorts keys', { b: 1, a: { d: 2, c: 3 } }, '{"a":{"c":3,"d":2},"b":1}'],
    ['null-prototype object', Object.assign(Object.create(null), { z: 1, a: 2 }), '{"a":2,"z":1}'],
    ['empty object', {}, '{}'],
  ]

  it.each(accepted)('encodes %s', (_name, input, output) => {
    expect(canonicalJson(input)).toBe(output)
  })

  const rejected: [name: string, input: unknown, code: string][] = [
    ['unsafe integer', 2 ** 53, PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['NaN', Number.NaN, PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['Infinity', Number.POSITIVE_INFINITY, PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['-Infinity', Number.NEGATIVE_INFINITY, PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['undefined at the top level', undefined, PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['undefined in an array', [1, undefined], PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['function in an array', [() => 1], PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['symbol in an array', [Symbol('x')], PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['function property', { fn: () => 1 }, PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['Map', new Map([['a', 1]]), PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['Set', new Set([1]), PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['RegExp', /x/, PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['class instance', new (class Row {})(), PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
    ['Invalid Date', new Date(Number.NaN), PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE],
  ]

  it.each(rejected)('rejects %s', (_name, input, code) => {
    expect(codeOf(() => canonicalJson(input))).toBe(code)
  })

  it('rejects cycles', () => {
    const row: Record<string, unknown> = { a: 1 }
    row['self'] = row

    expect(codeOf(() => canonicalJson(row))).toBe(PUBSUB_ERROR_CODES.CODEC_CYCLE)
  })

  it('names the offending path', () => {
    expect(() => canonicalJson({ swap: { amounts: [1, Number.NaN] } })).toThrow('$.swap.amounts[1]')
  })

  it('encodes the same value identically regardless of key insertion order', () => {
    const a = { pool: '0xab', amount: 1n, block: { number: 10, hash: '0xcd' } }
    const b = { block: { hash: '0xcd', number: 10 }, amount: 1n, pool: '0xab' }

    expect(canonicalJson(a)).toBe(canonicalJson(b))
  })

  it('reuses a repeated (non-cyclic) reference instead of calling it a cycle', () => {
    const shared = { a: 1 }

    expect(canonicalJson({ x: shared, y: shared })).toBe('{"x":{"a":1},"y":{"a":1}}')
  })

  it('matches the golden bytes of a decoded transfer', () => {
    const row = {
      to: '0x0000000000000000000000000000000000000002',
      from: '0x0000000000000000000000000000000000000001',
      value: 1_000_000_000_000_000_000n,
      block: { number: 31842007, hash: '0xab34', timestamp: new Date(1_700_000_000_000) },
      raw: new Uint8Array([0x00, 0x0f]),
    }

    expect(canonicalJson(row)).toBe(
      '{"block":{"hash":"0xab34","number":31842007,"timestamp":"2023-11-14T22:13:20.000Z"},' +
        '"from":"0x0000000000000000000000000000000000000001",' +
        '"raw":"0x000f",' +
        '"to":"0x0000000000000000000000000000000000000002",' +
        '"value":"1000000000000000000"}',
    )
  })
})

describe('BigQuery CDC encoding', () => {
  it('stores a plain row in canonical form', () => {
    expect(encodeRow({ b: 1, a: 2n })).toEqual(new TextEncoder().encode('{"a":"2","b":1}'))
  })

  it('stores a user-supplied id separately from the row payload', () => {
    expect(encodeRow({ _id: 'row-1', value: 2 })).toEqual(new TextEncoder().encode('{"value":2}'))
  })

  it('rejects invalid ids, non-object rows, and target-owned CDC fields', () => {
    expect(codeOf(() => encodeRow({ _id: 42 }))).toBe(PUBSUB_ERROR_CODES.INVALID_CDC_ROW)
    expect(codeOf(() => encodeRow({ _id: '' }))).toBe(PUBSUB_ERROR_CODES.INVALID_CDC_ROW)
    expect(codeOf(() => encodeRow(null))).toBe(PUBSUB_ERROR_CODES.INVALID_CDC_ROW)
    expect(codeOf(() => encodeRow(undefined))).toBe(PUBSUB_ERROR_CODES.INVALID_CDC_ROW)
    expect(codeOf(() => readRowId(null))).toBe(PUBSUB_ERROR_CODES.INVALID_CDC_ROW)
    expect(codeOf(() => readRowId(undefined))).toBe(PUBSUB_ERROR_CODES.INVALID_CDC_ROW)
    expect(codeOf(() => encodeRow([]))).toBe(PUBSUB_ERROR_CODES.INVALID_CDC_ROW)
    expect(codeOf(() => encodeRow({ _CHANGE_TYPE: 'UPSERT' }))).toBe(PUBSUB_ERROR_CODES.INVALID_CDC_ROW)
  })

  it('flattens the row and adds the BigQuery CDC fields', () => {
    expect(buildCdcMessage({ op: 'upsert', id: 'row-1', seq: 43981, payload: encodeRow({ value: 2n }) })).toEqual({
      value: '2',
      _id: 'row-1',
      _CHANGE_TYPE: 'UPSERT',
      _CHANGE_SEQUENCE_NUMBER: 'ABCD',
    })
  })

  it('publishes a delete with the row columns needed to identify composite primary keys', () => {
    expect(
      buildCdcMessage({
        op: 'delete',
        id: 'row-1',
        seq: 2,
        payload: encodeRow({ account: '0x01', asset: 'USDC' }),
      }),
    ).toEqual({
      account: '0x01',
      asset: 'USDC',
      _id: 'row-1',
      _CHANGE_TYPE: 'DELETE',
      _CHANGE_SEQUENCE_NUMBER: '2',
    })
  })

  it('passes the complete CDC message to a custom encoder', () => {
    let received: object | undefined
    const encoded = encodeCdcMessage(
      { op: 'upsert', id: 'row-1', seq: 1, payload: encodeRow({ value: 2 }) },
      (message) => {
        received = message
        return new Uint8Array([7])
      },
    )

    expect(encoded).toEqual(new Uint8Array([7]))
    expect(received).toMatchObject({ value: 2, _id: 'row-1', _CHANGE_TYPE: 'UPSERT' })
  })

  it('formats the sequence as uppercase hexadecimal', () => {
    expect(changeSequenceNumber(43981)).toBe('ABCD')
  })

  it('uses a dedicated error for sequence exhaustion and names the route', () => {
    expect(
      codeOf(() => changeSequenceNumber('9007199254740992', { route: 'transfers', topic: 'evm.base.transfers' })),
    ).toBe(PUBSUB_ERROR_CODES.SEQUENCE_EXHAUSTED)
    expect(() => changeSequenceNumber('9007199254740992', { route: 'transfers', topic: 'evm.base.transfers' })).toThrow(
      'route "transfers" (topic "evm.base.transfers")',
    )
  })

  it('computes the canonical wire size without decoding the stored row', () => {
    const operation = { op: 'delete' as const, id: 'строка-1', seq: 2, payload: encodeRow({ key: 'значение' }) }

    expect(canonicalCdcMessageBytes(operation)).toBe(encodeCdcMessage(operation).byteLength)
  })
})

describe('buildAttributes', () => {
  const operation = {
    kind: 'cdc' as const,
    topic: 'evm.base.transfers',
    op: 'upsert' as const,
    id: 'pipe:transfers:1:0xab:0',
    seq: 1041,
    orderingKey: '',
    attributes: { token: '0x42' },
    payload: new Uint8Array(),
  }

  it('keeps only business filter attributes beside the message kind', () => {
    expect(buildAttributes(operation, { namespace: 'pipe' })).toEqual({ _type: 'cdc', token: '0x42' })
  })

  it('declares a control record as such, so a filtered subscription can exclude it', () => {
    const attributes = buildAttributes({ ...operation, kind: 'control' }, { namespace: 'pipe' })

    expect(attributes['_type']).toBe('control')
  })

  it('mirrors the producer-wide constant attributes a route declares once', () => {
    const attributes = buildAttributes(operation, { namespace: 'pipe', constant: { chain: 'base', table: 'logs' } })

    expect(attributes).toEqual({ _type: 'cdc', chain: 'base', table: 'logs', token: '0x42' })
  })

  it('fully qualifies _uid when it is enabled', () => {
    const attributes = buildAttributes(
      { ...operation, orderingKey: 'shard-1' },
      { namespace: 'pipe', uidAttribute: true },
    )

    expect(attributes['_uid']).toBe('["pipe","evm.base.transfers","shard-1","1041"]')
  })

  it('keeps _uid distinct when namespace and ordering key contain separators', () => {
    const first = uidValue({ topic: 'events', orderingKey: 'events:shard', seq: 1 }, 'pipe')
    const second = uidValue({ topic: 'events', orderingKey: 'shard', seq: 1 }, 'pipe:events')

    expect(first).not.toBe(second)
  })
})

describe('validateUserAttributes', () => {
  const context = { topic: 'evm.base.transfers', route: 'transfers', protocolAttributes: 1 }

  it('accepts unprefixed business names, including id and op', () => {
    expect(() => validateUserAttributes({ id: 'x', op: 'y', token: 'z' }, context)).not.toThrow()
  })

  it('rejects the reserved underscore namespace', () => {
    expect(codeOf(() => validateUserAttributes({ _uid: '1' }, context))).toBe(PUBSUB_ERROR_CODES.RESERVED_ATTRIBUTE)
  })

  it('rejects GCP-reserved names', () => {
    expect(codeOf(() => validateUserAttributes({ googclient_x: '1' }, context))).toBe(
      PUBSUB_ERROR_CODES.RESERVED_ATTRIBUTE,
    )
  })

  it('rejects more user attributes than fit beside protocol attributes', () => {
    const many = Object.fromEntries(Array.from({ length: 100 }, (_, i) => [`a${i}`, '1']))

    expect(codeOf(() => validateUserAttributes(many, context))).toBe(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET)
    expect(() => validateUserAttributes(many, { ...context, protocolAttributes: 0 })).not.toThrow()
  })

  it('rejects oversized keys and values', () => {
    expect(codeOf(() => validateUserAttributes({ ['k'.repeat(257)]: '1' }, context))).toBe(
      PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET,
    )
    expect(codeOf(() => validateUserAttributes({ k: 'v'.repeat(1025) }, context))).toBe(
      PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET,
    )
  })

  it('rejects non-string values instead of letting the client coerce them', () => {
    expect(codeOf(() => validateUserAttributes({ block: 1 as unknown as string }, context))).toBe(
      PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET,
    )
  })
})
