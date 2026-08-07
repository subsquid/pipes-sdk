import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'

/**
 * Wire-protocol version: the envelope (5.2) plus the canonical codec (5.4). Bumped only on
 * breaking changes, so a consumer can branch on it before decoding anything.
 */
export const WIRE_VERSION = '1'

/** Pub/Sub hard limits the target validates against before a publish (section 10). */
export const PUBSUB_LIMITS = {
  maxMessageBytes: 10 * 1024 * 1024,
  maxAttributes: 100,
  maxAttributeKeyBytes: 256,
  maxAttributeValueBytes: 1024,
} as const

export type PubsubOp = 'upsert' | 'delete' | 'heartbeat'

/** Attribute names the envelope owns. Everything unprefixed belongs to the user. */
export const ENVELOPE_ATTRIBUTES = ['_seq', '_id', '_op', '_v', '_uid'] as const

// ─── Canonical payload codec (5.4) ────────────────────────────────────────────

const HEX = '0123456789abcdef'

function hex(bytes: Uint8Array): string {
  let out = '0x'
  for (const byte of bytes) {
    out += HEX[byte >> 4] + HEX[byte & 0x0f]
  }

  return out
}

function reject(code: string, path: string, message: string): never {
  throw new PubsubTargetError(code, [
    `Canonical codec cannot encode the value at ${path}: ${message}`,
    'Pass a supported value, or set a custom `encode` on the route to take over the wire format.',
  ])
}

function isPlainObject(value: object): boolean {
  const proto = Object.getPrototypeOf(value)

  return proto === Object.prototype || proto === null
}

function encodeValue(value: unknown, path: string, seen: Set<object>, out: string[]): void {
  switch (typeof value) {
    case 'bigint':
      // Decimal string: no precision loss, no float rounding, and JSON-portable.
      out.push(`"${value.toString(10)}"`)
      return

    case 'number':
      if (!Number.isFinite(value)) {
        reject(
          PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE,
          path,
          `${String(value)} has no JSON spelling; encoding it as null would be indistinguishable from a real null`,
        )
      }
      if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
        reject(
          PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE,
          path,
          `${String(value)} is an integer beyond Number.MAX_SAFE_INTEGER and is already imprecise — pass a bigint or a string`,
        )
      }
      // `String` gives the shortest round-trip decimal, and turns -0 into "0".
      out.push(String(value === 0 ? 0 : value))
      return

    case 'string':
      // Minimal escaping of `"`, `\` and U+0000–U+001F, matching the table in 5.4.
      out.push(JSON.stringify(value))
      return

    case 'boolean':
      out.push(value ? 'true' : 'false')
      return

    case 'undefined':
      reject(PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE, path, 'undefined is not a JSON value')

    case 'function':
      reject(PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE, path, 'functions are not part of the protocol')

    case 'symbol':
      reject(PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE, path, 'symbols are not part of the protocol')
  }

  if (value === null) {
    out.push('null')
    return
  }

  const object = value as object

  if (seen.has(object)) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.CODEC_CYCLE, [
      `Canonical codec met a cycle at ${path}.`,
      'Cyclic values cannot be encoded — break the cycle or set a custom `encode` on the route.',
    ])
  }

  if (object instanceof Date) {
    const ms = object.getTime()
    if (!Number.isFinite(ms)) {
      reject(PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE, path, 'Invalid Date has no representation')
    }
    // Unix seconds: sub-second precision is dropped by design — pass a number for anything finer.
    out.push(String(Math.floor(ms / 1000)))
    return
  }

  if (object instanceof ArrayBuffer) {
    out.push(`"${hex(new Uint8Array(object))}"`)
    return
  }

  if (ArrayBuffer.isView(object)) {
    const view = object as ArrayBufferView
    out.push(`"${hex(new Uint8Array(view.buffer, view.byteOffset, view.byteLength))}"`)
    return
  }

  seen.add(object)
  try {
    if (Array.isArray(object)) {
      out.push('[')
      for (let i = 0; i < object.length; i++) {
        if (i > 0) out.push(',')

        const item = object[i]
        // JSON.stringify turns these into `null` inside arrays, which is indistinguishable
        // from a real null — and an ambiguous payload breaks "same operation ⇒ same meaning".
        if (item === undefined || typeof item === 'function' || typeof item === 'symbol') {
          reject(
            PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE,
            `${path}[${i}]`,
            `${item === undefined ? 'undefined' : typeof item} in an array would silently become null`,
          )
        }

        encodeValue(item, `${path}[${i}]`, seen, out)
      }
      out.push(']')

      return
    }

    if (!isPlainObject(object)) {
      reject(
        PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE,
        path,
        `${object.constructor?.name ?? 'this value'} is not a plain object; only plain objects, arrays, byte views and Date are part of the protocol`,
      )
    }

    // Ascending UTF-16 code-unit order — JavaScript's default string comparison, pinned so a
    // non-JS implementation reproduces the same bytes rather than nearly the same bytes.
    const keys = Object.keys(object).sort()

    out.push('{')
    let written = 0
    for (const key of keys) {
      const item = (object as Record<string, unknown>)[key]
      if (item === undefined) continue

      if (written > 0) out.push(',')
      out.push(JSON.stringify(key), ':')
      encodeValue(item, `${path}.${key}`, seen, out)
      written++
    }
    out.push('}')
  } finally {
    seen.delete(object)
  }
}

/**
 * The normative payload encoding for object drafts (PUBSUB.md 5.4).
 *
 * Decoded chain data is not plain-JSON-safe — ABI integers decode to `bigint`, byte fields to
 * `Uint8Array` — and "same operation ⇒ same bytes" (9.1) is a protocol guarantee, not an
 * implementation detail. So the mapping is total: every input either encodes to one specific
 * minified JSON document or raises a coded error naming its path.
 */
export function canonicalJson(value: unknown): string {
  const out: string[] = []
  encodeValue(value, '$', new Set(), out)

  return out.join('')
}

const encoder = new TextEncoder()

export function toBytes(value: Uint8Array | string): Uint8Array {
  return typeof value === 'string' ? encoder.encode(value) : value
}

export function encodePayload(data: Uint8Array | string | object, encode?: (data: object) => Uint8Array | string) {
  if (typeof data === 'string' || data instanceof Uint8Array) {
    return toBytes(data)
  }

  return toBytes(encode ? encode(data as object) : canonicalJson(data))
}

// ─── Envelope (5.2) ───────────────────────────────────────────────────────────

export type WireOperation = {
  topic: string
  op: PubsubOp
  /** Absent on `heartbeat` — a heartbeat is not a row. */
  id?: string
  seq: number
  /** Empty string under `lww`, where Pub/Sub ordering keys are not used at all. */
  orderingKey: string
  attributes: Record<string, string>
  payload: Uint8Array
}

/**
 * Build the published attribute map: the four-attribute envelope plus the user's own.
 * `_uid` is added only when `publish.uidAttribute` is on, since it costs an attribute slot.
 */
export function buildEnvelope(
  operation: WireOperation,
  options: { namespace: string; uidAttribute?: boolean },
): Record<string, string> {
  const attributes: Record<string, string> = {
    ...operation.attributes,
    _op: operation.op,
    _seq: String(operation.seq),
    _v: WIRE_VERSION,
  }

  if (operation.id !== undefined) {
    attributes['_id'] = operation.id
  }

  if (options.uidAttribute) {
    // Fully qualified because neither the producer nor the partition is implied by `_seq`
    // alone: a fan-in topic runs one counter per producer, a sharded topic one per key.
    attributes['_uid'] = `${options.namespace}:${operation.topic}:${operation.orderingKey}:${operation.seq}`
  }

  return attributes
}

const utf8Length = (value: string) => encoder.encode(value).length

/**
 * Reject user attributes that collide with the protocol's namespace or blow Pub/Sub's
 * per-message budget, with a coded error naming the attribute — the client's own reject is
 * a generic INVALID_ARGUMENT far from the route that caused it.
 */
export function validateUserAttributes(
  attributes: Record<string, string> | undefined,
  context: { topic: string; route: string; envelopeSize: number },
): void {
  if (!attributes) return

  const budget = PUBSUB_LIMITS.maxAttributes - context.envelopeSize
  const names = Object.keys(attributes)

  if (names.length > budget) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
      `Route "${context.route}" produced ${names.length} user attributes for topic "${context.topic}", ` +
        `but only ${budget} fit beside the ${context.envelopeSize}-attribute envelope ` +
        `(Pub/Sub allows ${PUBSUB_LIMITS.maxAttributes} per message).`,
    ])
  }

  for (const name of names) {
    if (name.startsWith('_')) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.RESERVED_ATTRIBUTE, [
        `Attribute "${name}" on route "${context.route}" starts with "_", which is the protocol's ` +
          `reserved namespace (${ENVELOPE_ATTRIBUTES.join(', ')}).`,
        'Business names without the underscore — including `id` and `op` — are free to use.',
      ])
    }

    if (name.toLowerCase().startsWith('goog')) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.RESERVED_ATTRIBUTE, [
        `Attribute "${name}" on route "${context.route}" starts with "goog", which Google Cloud reserves.`,
      ])
    }

    const keyBytes = utf8Length(name)
    if (keyBytes > PUBSUB_LIMITS.maxAttributeKeyBytes) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
        `Attribute key "${name.slice(0, 32)}…" on route "${context.route}" is ${keyBytes} bytes; ` +
          `Pub/Sub allows ${PUBSUB_LIMITS.maxAttributeKeyBytes}.`,
      ])
    }

    const value = attributes[name]
    if (typeof value !== 'string') {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
        `Attribute "${name}" on route "${context.route}" is ${typeof value}; Pub/Sub attributes are strings. ` +
          'Format the value in `map` — filters compare strings, not numbers.',
      ])
    }

    const valueBytes = utf8Length(value)
    if (valueBytes > PUBSUB_LIMITS.maxAttributeValueBytes) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
        `Attribute "${name}" on route "${context.route}" carries a ${valueBytes}-byte value; ` +
          `Pub/Sub allows ${PUBSUB_LIMITS.maxAttributeValueBytes}.`,
      ])
    }
  }
}

export function assertPayloadSize(payload: Uint8Array, context: { topic: string; route: string }): void {
  if (payload.byteLength <= PUBSUB_LIMITS.maxMessageBytes) return

  throw new PubsubTargetError(PUBSUB_ERROR_CODES.MESSAGE_TOO_LARGE, [
    `Route "${context.route}" produced a ${payload.byteLength}-byte payload for topic "${context.topic}"; ` +
      `Pub/Sub allows ${PUBSUB_LIMITS.maxMessageBytes} bytes per message.`,
    'Split the row, or compress it with a custom `encode`.',
  ])
}
