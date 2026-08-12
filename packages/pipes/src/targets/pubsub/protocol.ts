import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'

/**
 * Wire-protocol version: the envelope plus the canonical codec (RP-24). Bumped only on
 * breaking changes, so a consumer can branch on it before decoding anything.
 */
export const WIRE_VERSION = '1'

/**
 * PubSub hard limits, checked before an operation is committed rather than at publish time
 * (IB-28): a message the service will reject must never reach the durable outbox, where it
 * would block that partition on every restart.
 */
export const PUBSUB_LIMITS = {
  maxMessageBytes: 10 * 1024 * 1024,
  maxPublishRequestBytes: 10 * 1024 * 1024,
  maxAttributes: 100,
  maxAttributeKeyBytes: 256,
  maxAttributeValueBytes: 1024,
  maxOrderingKeyBytes: 1024,
} as const

/** Upper bound on the decimal `_seq` a `_uid` can carry, for the pre-commit length check. */
const MAX_SEQ_VALUE = '9'.repeat(20)

/**
 * A short topic id is expanded by the client to `projects/{project}/topics/{topic}`. Use the
 * longest valid resource name when checking the request before the client and project id exist.
 */
const MAX_TOPIC_RESOURCE_BYTES = 'projects/'.length + 30 + '/topics/'.length + 255

export type PubsubOp = 'upsert' | 'delete' | 'heartbeat'

/** Attribute names the envelope owns. Everything unprefixed belongs to the user. */
export const ENVELOPE_ATTRIBUTES = ['_seq', '_id', '_op', '_v', '_uid'] as const

// ─── Canonical payload codec (RP-24) ──────────────────────────────────────────

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
      // Minimal escaping of `"`, `\` and U+0000–U+001F.
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
 * The normative payload encoding for object drafts (RP-24).
 *
 * Decoded chain data is not plain-JSON-safe — ABI integers decode to `bigint`, byte fields to
 * `Uint8Array` — and "same operation ⇒ same bytes" is a protocol guarantee, not an
 * implementation detail: it is what lets a consumer recognise a republished operation. So the
 * mapping is total — every input either encodes to one specific minified JSON document or
 * raises a coded error naming its path.
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

// ─── Envelope (RP-24) ─────────────────────────────────────────────────────────

export type WireOperation = {
  topic: string
  op: PubsubOp
  /** Absent on `heartbeat` — a heartbeat is not a row. */
  id?: string
  seq: number
  /** Empty string under `lww`, where PubSub ordering keys are not used at all. */
  orderingKey: string
  attributes: Record<string, string>
  payload: Uint8Array
}

/**
 * Build the published attribute map: the four-attribute envelope plus the user's own.
 * `_uid` is added only when `publish.uidAttribute` is on, since it costs an attribute slot.
 */
export function buildEnvelope(
  operation: Omit<WireOperation, 'seq'> & { seq: number | string },
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
    attributes['_uid'] = uidValue(operation, options.namespace)
  }

  return attributes
}

const utf8Length = (value: string) => encoder.encode(value).length

/** The `_uid` value for an operation: a canonical tuple that cannot collide at field boundaries. */
export function uidValue(
  operation: { topic: string; orderingKey: string; seq: number | string },
  namespace: string,
): string {
  return JSON.stringify([namespace, operation.topic, operation.orderingKey, String(operation.seq)])
}

/**
 * Everything the service checks that the user-attribute pass does not: the envelope's own
 * values and the ordering key. Run BEFORE the batch commits — PubSub rejects an oversized
 * `_id` or ordering key at publish time, and by then the operation is durable, so its
 * partition would fail identically on every restart with no way to make progress.
 */
export function assertWireLimits(
  message: { id?: string; orderingKey: string; uidNamespace?: string },
  context: { topic: string; route: string },
): void {
  const where = `route "${context.route}" (topic "${context.topic}")`

  if (message.id !== undefined) {
    const bytes = utf8Length(message.id)
    if (bytes > PUBSUB_LIMITS.maxAttributeValueBytes) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
        `The row id produced by ${where} is ${bytes} bytes; it is published as the \`_id\` attribute, ` +
          `and PubSub allows ${PUBSUB_LIMITS.maxAttributeValueBytes}.`,
        'Shorten the id — it is an identity, not a payload.',
      ])
    }
  }

  if (message.orderingKey) {
    const bytes = utf8Length(message.orderingKey)
    if (bytes > PUBSUB_LIMITS.maxOrderingKeyBytes) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
        `The ordering key produced by ${where} is ${bytes} bytes; PubSub allows ${PUBSUB_LIMITS.maxOrderingKeyBytes}.`,
      ])
    }
  }

  if (message.uidNamespace !== undefined) {
    // The sequence is assigned in the commit transaction, so bound it by its widest decimal form.
    const bytes = utf8Length(
      uidValue({ topic: context.topic, orderingKey: message.orderingKey, seq: MAX_SEQ_VALUE }, message.uidNamespace),
    )
    if (bytes > PUBSUB_LIMITS.maxAttributeValueBytes) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
        `The \`_uid\` attribute for ${where} would be up to ${bytes} bytes; PubSub allows ` +
          `${PUBSUB_LIMITS.maxAttributeValueBytes}.`,
        'Shorten the namespace, the topic name, or the ordering key — `_uid` identifies all three.',
      ])
    }
  }
}

/**
 * Reject user attributes that collide with the protocol's namespace or blow PubSub's
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
        `(PubSub allows ${PUBSUB_LIMITS.maxAttributes} per message).`,
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
          `PubSub allows ${PUBSUB_LIMITS.maxAttributeKeyBytes}.`,
      ])
    }

    const value = attributes[name]
    if (typeof value !== 'string') {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
        `Attribute "${name}" on route "${context.route}" is ${typeof value}; PubSub attributes are strings. ` +
          'Format the value in `map` — filters compare strings, not numbers.',
      ])
    }

    const valueBytes = utf8Length(value)
    if (valueBytes > PUBSUB_LIMITS.maxAttributeValueBytes) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
        `Attribute "${name}" on route "${context.route}" carries a ${valueBytes}-byte value; ` +
          `PubSub allows ${PUBSUB_LIMITS.maxAttributeValueBytes}.`,
      ])
    }
  }
}

function varintBytes(value: number): number {
  let bytes = 1
  let remaining = value

  while (remaining >= 128) {
    bytes++
    remaining = Math.floor(remaining / 128)
  }

  return bytes
}

/** One protobuf length-delimited field; every PublishRequest field used here has a one-byte tag. */
const lengthDelimitedFieldBytes = (valueBytes: number) => 1 + varintBytes(valueBytes) + valueBytes

function serializedMessageBytes(message: {
  payload: Uint8Array
  attributes: Record<string, string>
  orderingKey: string
}): number {
  let bytes = lengthDelimitedFieldBytes(message.payload.byteLength)

  for (const [key, value] of Object.entries(message.attributes)) {
    const entryBytes = lengthDelimitedFieldBytes(utf8Length(key)) + lengthDelimitedFieldBytes(utf8Length(value))
    bytes += lengthDelimitedFieldBytes(entryBytes)
  }

  if (message.orderingKey) {
    bytes += lengthDelimitedFieldBytes(utf8Length(message.orderingKey))
  }

  return bytes
}

/**
 * Bound both PubSub size limits before commit: the data field and the complete single-message
 * PublishRequest, including the wire envelope and protobuf framing. The Node client keeps
 * multi-message batches below the service request limit separately.
 */
export function assertPublishRequestSize(
  operation: Omit<WireOperation, 'seq'>,
  options: { namespace: string; uidAttribute?: boolean },
  context: { route: string },
): void {
  if (operation.payload.byteLength > PUBSUB_LIMITS.maxMessageBytes) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.MESSAGE_TOO_LARGE, [
      `Route "${context.route}" produced a ${operation.payload.byteLength}-byte payload for topic ` +
        `"${operation.topic}"; PubSub allows ${PUBSUB_LIMITS.maxMessageBytes} bytes in the data field.`,
      'Split the row, or compress it with a custom `encode`.',
    ])
  }

  const attributes = buildEnvelope({ ...operation, seq: MAX_SEQ_VALUE }, options)
  const messageBytes = serializedMessageBytes({ ...operation, attributes })
  const topicBytes = Math.max(utf8Length(operation.topic), MAX_TOPIC_RESOURCE_BYTES)
  const requestBytes = lengthDelimitedFieldBytes(topicBytes) + lengthDelimitedFieldBytes(messageBytes)

  if (requestBytes <= PUBSUB_LIMITS.maxPublishRequestBytes) return

  throw new PubsubTargetError(PUBSUB_ERROR_CODES.MESSAGE_TOO_LARGE, [
    `Route "${context.route}" could produce a ${requestBytes}-byte single-message publish request for topic ` +
      `"${operation.topic}"; PubSub allows ${PUBSUB_LIMITS.maxPublishRequestBytes} bytes in the complete request.`,
    'Split the row, shorten its attributes, or compress it with a custom `encode`.',
  ])
}
