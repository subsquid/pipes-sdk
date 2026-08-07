import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import { MessageDraft, TopicRoute } from './pubsub-target.js'

/**
 * One emission of an aggregated window. Produced by the `createAggregator` transformer and
 * mapped onto the changelog by `windowTopic` — the same row shape either side of the seam.
 */
export type WindowRow<Out = Record<string, unknown>> = {
  group: string
  /** Unix seconds, UTC-aligned. */
  windowStart: number
  windowEnd: number
  timeframe: string
  /** False on previews and fork re-opens. */
  complete: boolean
  /** True when a fork left the window with no contributions. */
  empty: boolean
  values: Out
  block: { number: number; hash?: string; timestamp?: number }
}

export type WindowBounds = Pick<WindowRow, 'group' | 'windowStart' | 'windowEnd' | 'timeframe'>

export type WindowTopicOptions<Out> = {
  topic: string
  /**
   * Filter attributes for the window's rows. `timeframe` is added automatically (a window's
   * defining property) unless this returns its own. Must be stable across revisions — every
   * revision and every fork compensation of a window id carries the same attributes.
   */
  attributes?: (row: WindowRow<Out>) => Record<string, string>
  /** Payload encoder. Defaults to the canonical codec (RP-24). */
  encode?: (data: object) => Uint8Array | string
  /**
   * What an emptied window publishes: `delete` (default) removes the row, `upsert` keeps the
   * topic delete-free by publishing a neutral value.
   */
  emptyWindows?: 'delete' | 'upsert'
  /**
   * The neutral value for an emptied window. Required with `emptyWindows: 'upsert'` — it is
   * also the route's fork inverse, and only the route knows what an empty candle looks like.
   */
  emptyValues?: (window: WindowBounds) => Out
  /** Ordered profile only: shard the topic per series. A window id must never move keys. */
  orderingKey?: (row: WindowRow<Out>) => string
}

/**
 * Builds a `TopicRoute` for an aggregator window stream: every re-emission of a window is an
 * `upsert` on the same id, so "last write wins by `_seq`" replaces any consumer-side revision
 * arithmetic.
 */
export function windowTopic<Out>(options: WindowTopicOptions<Out>): TopicRoute<WindowRow<Out>[]> {
  const emptyWindows = options.emptyWindows ?? 'delete'

  if (emptyWindows === 'upsert' && !options.emptyValues) {
    // A route that is delete-free in normal operation but emits a delete on a fork is the worst
    // of both: its consumers skipped tombstone retention on the strength of the guarantee.
    throw new PubsubTargetError(PUBSUB_ERROR_CODES.MISSING_EMPTY_VALUES, [
      `windowTopic("${options.topic}") sets \`emptyWindows: "upsert"\` without \`emptyValues\`.`,
      'The two go together: a fork can orphan every revision of a window id, and the compensation for ' +
        'that has no row behind it — the target must synthesize one, and only this route knows what an ' +
        'empty window looks like.',
    ])
  }

  const attributesOf = (row: WindowRow<Out>): Record<string, string> => ({
    timeframe: row.timeframe,
    ...options.attributes?.(row),
  })

  const emptyRow = (window: WindowBounds): WindowRow<Out> => ({
    ...window,
    complete: false,
    empty: true,
    values: options.emptyValues!(window),
    block: { number: 0 },
  })

  return {
    topic: options.topic,
    // Window ids are updated in place, so a fork restores the surviving revision instead of
    // orphaning the row.
    mode: 'materialized',
    encode: options.encode,

    deriveId: (draft, { namespace, stream }) => {
      const row = draft.data as WindowRow<Out>

      return `${namespace}:${stream}:${row.group}:${row.windowStart}`
    },

    map: ({ data }) =>
      data.map((row): MessageDraft => {
        const draft: MessageDraft = {
          data: row.empty && emptyWindows === 'upsert' ? { ...row, values: options.emptyValues!(row) } : row,
          block: row.block,
          attributes: attributesOf(row),
          orderingKey: options.orderingKey?.(row),
        }

        if (row.empty && emptyWindows === 'delete') {
          draft.op = 'delete'
        }

        return draft
      }),

    rollbackWhenMissing:
      emptyWindows === 'upsert'
        ? (draft) => {
            const row = draft.data as WindowRow<Out>

            return { op: 'upsert', data: emptyRow(row) }
          }
        : undefined,
  }
}
