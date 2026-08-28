import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'

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

/** Largest sequence this implementation can increment without precision loss. */
export const MAX_SEQUENCE_VALUE = Number.MAX_SAFE_INTEGER

/**
 * A short topic id is expanded by the client to `projects/{project}/topics/{topic}`. Use the
 * longest valid resource name when checking the request before the client and project id exist.
 */
const MAX_TOPIC_RESOURCE_BYTES = 'projects/'.length + 30 + '/topics/'.length + 255

export type PubsubOp = 'upsert' | 'delete'

/**
 * What a message says. `cdc` is a row change; `control` is a statement the producer makes about
 * the feed itself. Both are complete CDC rows — the marker is deliberately coarse, so a filter
 * written today stays correct when a new kind of control record appears.
 */
export type MessageKind = 'cdc' | 'control'

/** Protocol-owned PubSub attributes; row metadata lives in `data`. */
export const PROTOCOL_ATTRIBUTES = ['_type', '_uid'] as const

export const CDC_FIELDS = {
  id: '_id',
  changeType: '_CHANGE_TYPE',
  changeSequenceNumber: '_CHANGE_SEQUENCE_NUMBER',
} as const

type BigQueryCdcMetadata = {
  [CDC_FIELDS.id]: string
  [CDC_FIELDS.changeType]: 'UPSERT' | 'DELETE'
  /**
   * Producer ordering key, encoded as uppercase hexadecimal because BigQuery compares matching
   * primary-key changes as unsigned hexadecimal numbers.
   * @see https://cloud.google.com/bigquery/docs/change-data-capture#manage_custom_ordering
   */
  [CDC_FIELDS.changeSequenceNumber]: string
}

export type BigQueryCdcMessage = Record<string, unknown> & BigQueryCdcMetadata

export type CdcEncoder = (message: BigQueryCdcMessage) => Uint8Array | string

type CdcOperation = {
  route?: string
  topic?: string
  op: PubsubOp
  id: string
  seq: number | string
  payload: Uint8Array
}

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
    'Convert it to a supported value before returning the draft.',
  ])
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') {
    return false
  }

  const proto = Object.getPrototypeOf(value)

  return proto === Object.prototype || proto === null
}

function assertPlainRow(data: unknown): asserts data is Record<string, unknown> {
  if (!isPlainObject(data)) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.INVALID_CDC_ROW, [
      'A PubSub row must be a plain object so the target can add the BigQuery CDC fields.',
      'Wrap primitive, array, or binary values in an object field before returning the draft.',
    ])
  }
}

function readPlainRowId(data: Record<string, unknown>): string | undefined {
  if (!Object.hasOwn(data, CDC_FIELDS.id)) {
    return undefined
  }

  const value = data[CDC_FIELDS.id]
  if (value === undefined || value === null) {
    return undefined
  }

  if (typeof value !== 'string' || value.length === 0) {
    const description = typeof value === 'string' ? 'an empty string' : typeof value
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.INVALID_CDC_ROW, [
      `A PubSub row set "${CDC_FIELDS.id}" to ${description}; a row id must be a non-empty string.`,
      `Use a non-empty string "${CDC_FIELDS.id}", or omit it to let the target derive one.`,
    ])
  }

  return value
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
      'Cyclic values cannot be encoded — break the cycle before returning the draft.',
    ])
  }

  if (object instanceof Date) {
    if (!Number.isFinite(object.getTime())) {
      reject(PUBSUB_ERROR_CODES.CODEC_UNSUPPORTED_VALUE, path, 'Invalid Date has no representation')
    }
    // RFC 3339, not unix seconds: BigQuery reads a JSON number in a TIMESTAMP column as
    // microseconds, so the former seconds encoding silently landed timestamps near 1970.
    out.push(JSON.stringify(object.toISOString()))
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

/** Read an optional user-supplied row id before the row is stored without CDC metadata. */
export function readRowId(data: unknown): string | undefined {
  assertPlainRow(data)

  return readPlainRowId(data)
}

/**
 * Store a route row in a transport-neutral canonical form. The durable state keeps these
 * bytes without CDC metadata so a fork can rebuild an operation with a fresh sequence number.
 */
export function encodeRow(data: unknown): Uint8Array {
  assertPlainRow(data)
  readPlainRowId(data)

  for (const field of [CDC_FIELDS.changeType, CDC_FIELDS.changeSequenceNumber]) {
    if (Object.hasOwn(data, field)) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.INVALID_CDC_ROW, [
        `A PubSub row set reserved field "${field}".`,
        'The target owns the change type and change sequence number.',
      ])
    }
  }

  const row = { ...data }
  delete row[CDC_FIELDS.id]

  return toBytes(canonicalJson(row))
}

const decoder = new TextDecoder()

function decodeRow(data: Uint8Array): Record<string, unknown> {
  return JSON.parse(decoder.decode(data)) as Record<string, unknown>
}

export function changeSequenceNumber(seq: number | string, context: { route?: string; topic?: string } = {}): string {
  const value = BigInt(seq)
  if (value < 0n || value > BigInt(MAX_SEQUENCE_VALUE)) {
    const where = context.route && context.topic ? ` for route "${context.route}" (topic "${context.topic}")` : ''
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.SEQUENCE_EXHAUSTED, [
      `Sequence ${String(seq)}${where} is outside the supported range 0…${MAX_SEQUENCE_VALUE}.`,
      'Start a new feed with a fresh namespace and state before publishing more operations.',
    ])
  }

  // BigQuery compares this CDC ordering key as an unsigned hexadecimal number:
  // https://cloud.google.com/bigquery/docs/change-data-capture#manage_custom_ordering
  return value.toString(16).toUpperCase()
}

function buildCdcMetadata(operation: CdcOperation): BigQueryCdcMetadata {
  return {
    [CDC_FIELDS.id]: operation.id,
    [CDC_FIELDS.changeType]: operation.op === 'upsert' ? 'UPSERT' : 'DELETE',
    [CDC_FIELDS.changeSequenceNumber]: changeSequenceNumber(operation.seq, operation),
  }
}

export function buildCdcMessage(operation: CdcOperation): BigQueryCdcMessage {
  const row = decodeRow(operation.payload)

  return {
    ...row,
    ...buildCdcMetadata(operation),
  }
}

/** Exact canonical JSON byte length without decoding and re-encoding the stored row. */
export function canonicalCdcMessageBytes(operation: CdcOperation): number {
  const metadataBytes = utf8Length(canonicalJson(buildCdcMetadata(operation)))
  const separatorBytes = operation.payload.byteLength > 2 ? 1 : 0

  return operation.payload.byteLength + metadataBytes - 2 + separatorBytes
}

export function encodeCdcMessage(operation: CdcOperation, encode?: CdcEncoder): Uint8Array {
  const message = buildCdcMessage(operation)

  return toBytes(encode ? encode(message) : canonicalJson(message))
}

// ─── Wire attributes ──────────────────────────────────────────────────────────

export type WireOperation = {
  kind: MessageKind
  topic: string
  op: PubsubOp
  id: string
  seq: number
  /** Empty when PubSub message ordering is disabled. */
  orderingKey: string
  attributes: Record<string, string>
  payload: Uint8Array
}

/**
 * Build business filter attributes plus `_type` and the optional Dataflow deduplication id. Row
 * identity, operation, and version are fields in the BigQuery CDC payload.
 *
 * `constant` is the producer's topic-wide attribute set. It rides control records too, so a
 * subscription filtered on it still receives the statements the producer makes about its feed.
 */
export function buildAttributes(
  operation: Omit<WireOperation, 'seq'> & { seq: number | string },
  options: { namespace: string; uidAttribute?: boolean; constant?: Record<string, string> },
): Record<string, string> {
  const attributes: Record<string, string> = { ...options.constant, ...operation.attributes }

  // Carried by every message, so a subscription selects what it wants rather than excluding
  // what it happens to know about today.
  attributes['_type'] = operation.kind

  if (options.uidAttribute) {
    // Fully qualified so independently configured producers cannot collide even if they reuse
    // a sequence number under different topics or keys.
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
 * Everything the service checks that the user-attribute pass does not: `_uid` and the ordering
 * key. Run BEFORE the batch commits — PubSub rejects an oversized value at publish time, and
 * by then the operation is durable, so its
 * partition would fail identically on every restart with no way to make progress.
 */
export function assertWireLimits(
  message: { orderingKey: string; uidNamespace?: string },
  context: { topic: string; route: string },
): void {
  const where = `route "${context.route}" (topic "${context.topic}")`

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
      uidValue(
        { topic: context.topic, orderingKey: message.orderingKey, seq: MAX_SEQUENCE_VALUE },
        message.uidNamespace,
      ),
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
  context: { topic: string; route: string; protocolAttributes: number },
): void {
  if (!attributes) return

  const budget = PUBSUB_LIMITS.maxAttributes - context.protocolAttributes
  const names = Object.keys(attributes)

  if (names.length > budget) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.ATTRIBUTE_BUDGET, [
      `Route "${context.route}" produced ${names.length} user attributes for topic "${context.topic}", ` +
        `but only ${budget} fit beside ${context.protocolAttributes} protocol attribute(s) ` +
        `(PubSub allows ${PUBSUB_LIMITS.maxAttributes} per message).`,
    ])
  }

  for (const name of names) {
    if (name.startsWith('_')) {
      throw new PubsubTargetError(PUBSUB_ERROR_CODES.RESERVED_ATTRIBUTE, [
        `Attribute "${name}" on route "${context.route}" starts with "_", which is the protocol's ` +
          `reserved namespace (${PROTOCOL_ATTRIBUTES.join(', ')}).`,
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
  payloadBytes: number
  attributes: Record<string, string>
  orderingKey: string
}): number {
  let bytes = lengthDelimitedFieldBytes(message.payloadBytes)

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
 * PublishRequest, including filter attributes and protobuf framing. The Node client keeps
 * multi-message batches below the service request limit separately.
 */
export function assertPublishRequestSize(
  message: { topic: string; orderingKey: string; attributes: Record<string, string>; payloadBytes: number },
  context: { route: string },
): void {
  if (message.payloadBytes > PUBSUB_LIMITS.maxMessageBytes) {
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.MESSAGE_TOO_LARGE, [
      `Route "${context.route}" produced a ${message.payloadBytes}-byte payload for topic ` +
        `"${message.topic}"; PubSub allows ${PUBSUB_LIMITS.maxMessageBytes} bytes in the data field.`,
      'Split the row, or compress it with a custom `encode`.',
    ])
  }

  const messageBytes = serializedMessageBytes(message)
  const topicBytes = Math.max(utf8Length(message.topic), MAX_TOPIC_RESOURCE_BYTES)
  const requestBytes = lengthDelimitedFieldBytes(topicBytes) + lengthDelimitedFieldBytes(messageBytes)

  if (requestBytes <= PUBSUB_LIMITS.maxPublishRequestBytes) return

  throw new PubsubTargetError(PUBSUB_ERROR_CODES.MESSAGE_TOO_LARGE, [
    `Route "${context.route}" could produce a ${requestBytes}-byte single-message publish request for topic ` +
      `"${message.topic}"; PubSub allows ${PUBSUB_LIMITS.maxPublishRequestBytes} bytes in the complete request.`,
    'Split the row, shorten its attributes, or compress it with a custom `encode`.',
  ])
}
