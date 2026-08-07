import { describe, expect, it } from 'vitest'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import { buildEnvelope, canonicalJson, encodePayload, validateUserAttributes } from './protocol.js'

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
    ['Date truncates to unix seconds', new Date(1_700_000_000_999), '1700000000'],
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
      '{"block":{"hash":"0xab34","number":31842007,"timestamp":1700000000},' +
        '"from":"0x0000000000000000000000000000000000000001",' +
        '"raw":"0x000f",' +
        '"to":"0x0000000000000000000000000000000000000002",' +
        '"value":"1000000000000000000"}',
    )
  })
})

describe('encodePayload', () => {
  it('passes strings and bytes through untouched', () => {
    expect(encodePayload('raw')).toEqual(new TextEncoder().encode('raw'))
    expect(encodePayload(new Uint8Array([1, 2]))).toEqual(new Uint8Array([1, 2]))
  })

  it('routes objects through the canonical codec by default', () => {
    expect(encodePayload({ b: 1, a: 2n })).toEqual(new TextEncoder().encode('{"a":"2","b":1}'))
  })

  it('lets a route override the wire format', () => {
    expect(encodePayload({ a: 1 }, () => new Uint8Array([7]))).toEqual(new Uint8Array([7]))
  })
})

describe('buildEnvelope', () => {
  const operation = {
    topic: 'evm.base.transfers',
    op: 'upsert' as const,
    id: 'pipe:transfers:1:0xab:0',
    seq: 1041,
    orderingKey: '',
    attributes: { token: '0x42' },
    payload: new Uint8Array(),
  }

  it('carries four attributes plus the user’s', () => {
    expect(buildEnvelope(operation, { namespace: 'pipe' })).toEqual({
      token: '0x42',
      _op: 'upsert',
      _seq: '1041',
      _id: 'pipe:transfers:1:0xab:0',
      _v: '1',
    })
  })

  it('omits _id on a heartbeat — a heartbeat is not a row', () => {
    const attributes = buildEnvelope({ ...operation, op: 'heartbeat', id: undefined }, { namespace: 'pipe' })

    expect(attributes['_id']).toBeUndefined()
    expect(attributes['_op']).toBe('heartbeat')
  })

  it('fully qualifies _uid when it is enabled', () => {
    const attributes = buildEnvelope(
      { ...operation, orderingKey: 'shard-1' },
      { namespace: 'pipe', uidAttribute: true },
    )

    expect(attributes['_uid']).toBe('pipe:evm.base.transfers:shard-1:1041')
  })
})

describe('validateUserAttributes', () => {
  const context = { topic: 'evm.base.transfers', route: 'transfers', envelopeSize: 4 }

  it('accepts unprefixed business names, including id and op', () => {
    expect(() => validateUserAttributes({ id: 'x', op: 'y', token: 'z' }, context)).not.toThrow()
  })

  it('rejects the reserved underscore namespace', () => {
    expect(codeOf(() => validateUserAttributes({ _seq: '1' }, context))).toBe(PUBSUB_ERROR_CODES.RESERVED_ATTRIBUTE)
  })

  it('rejects GCP-reserved names', () => {
    expect(codeOf(() => validateUserAttributes({ googclient_x: '1' }, context))).toBe(
      PUBSUB_ERROR_CODES.RESERVED_ATTRIBUTE,
    )
  })

  it('rejects more user attributes than fit beside the envelope', () => {
    const many = Object.fromEntries(Array.from({ length: 97 }, (_, i) => [`a${i}`, '1']))

    expect(codeOf(() => validateUserAttributes(many, context))).toBe(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET)
    expect(() => validateUserAttributes(many, { ...context, envelopeSize: 3 })).not.toThrow()
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
