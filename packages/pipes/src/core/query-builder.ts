import { PortalClient, Query } from '~/portal-client/index.js'

import { Heap } from '../internal/heap.js'
import { BlockRangeConfigurationError } from './errors.js'
import { Logger } from './logger.js'
import { PortalRange, parsePortalRange } from './portal-range.js'
import { QueryAwareTransformer, SetupQueryFn, TransformerArgs } from './transformer.js'

/**
 * A range of blocks with inclusive boundaries
 */
export type Range = {
  from: number
  to?: number
}

/**
 * A range whose boundaries can be block numbers, `Date` timestamps, or the special `'latest'` token.
 * Dates are resolved to block numbers via the portal before use.
 */
export type NaturalRange = { from: number | Date; to?: number | Date } | { from: 'latest'; to?: number }

/** A data request scoped to a specific block range. */
export interface RangeRequest<Req, R = Range> {
  range: R
  request: Req
}

export type RequestOptions<R> = {
  range: PortalRange
  request: R
}

export type QueryTransformerOpts<In, Out, Query extends QueryBuilder<any>> = TransformerArgs<In, Out> & {
  setupQuery?: SetupQueryFn<Query>
}

export type PlanRequestArgs = {
  /** The first block the stream fetches next. */
  from: number
  /** The last block of the range the stream is in. Absent for an open range. */
  to?: number
  portal: PortalClient
  logger: Logger
}

export type PlannedRequest<R> = {
  request: R
  /** The last block the request holds for. Absent: it holds to the end of the range. */
  to?: number
}

/**
 * Picks a request while the stream runs, for data whose best filter is only learned on the way.
 *
 * The stream calls the planner before it fetches a block range, uses the answer up to the block it
 * holds for, then calls the planner again for the blocks after it. Every answer must select at least
 * the items the transformers keep: a narrower request silently drops data.
 */
export type RequestPlanner<R> = (args: PlanRequestArgs) => Promise<PlannedRequest<R>>

/** A resolved range with its static request and the planners that add to it while the stream runs. */
export type PlannedRangeRequest<R> = RangeRequest<R> & {
  planners?: RequestPlanner<R>[]
}

export abstract class QueryBuilder<F extends {}, R = any> {
  protected fields: F = {} as F
  protected requests: RangeRequest<R, NaturalRange>[] = []
  protected planners: RangeRequest<RequestPlanner<R>, NaturalRange>[] = []

  abstract getType(): string
  abstract mergeDataRequests(...requests: R[]): R
  abstract addFields(fields: F): QueryBuilder<F, R>
  abstract build(opts?: { setupQuery?: SetupQueryFn<any> }): QueryAwareTransformer<any, any, any>

  /** Returns the registered range requests. */
  getRequests() {
    return this.requests
  }

  /** Returns the current field selection. */
  getFields() {
    return this.fields
  }

  /** Adds a block range (without a data request) to the query. */
  addRange(range: PortalRange): this {
    this.requests.push({ range: parsePortalRange(range) } as any)
    return this
  }

  /** Adds a request that a {@link RequestPlanner} picks while the stream runs. */
  addPlannedRequest({ range, planner }: { range: PortalRange; planner: RequestPlanner<R> }): this {
    this.planners.push({ range: parsePortalRange(range), request: planner })
    return this
  }

  /** Merges another query builder's requests and fields into this one. */
  merge(query?: QueryBuilder<F, R>) {
    if (!query) return this

    this.requests = [...query.requests, ...this.requests]
    this.planners = [...query.planners, ...this.planners]
    this.addFields(query.getFields())

    return this
  }

  /**
   * Resolves all registered requests into concrete block ranges.
   *
   * - Resolves `'latest'` to the current head block number via the portal.
   * - Resolves `Date` values to block numbers via `portal.resolveTimestamp`.
   * - Merges overlapping ranges and applies an optional `bound` to clip results.
   *
   * @returns `raw` — the full merged ranges; `bounded` — ranges clipped to `bound`.
   */
  async calculateRanges({
    portal,
    bound,
  }: {
    bound?: Range
    portal: Portal
  }): Promise<{ bounded: PlannedRangeRequest<R>[]; raw: PlannedRangeRequest<R>[] }> {
    // Planners take part in range merging as requests of their own, so each merged range knows
    // which planners cover it.
    const entries: RangeRequest<MergedRequest<R>, NaturalRange>[] = [
      ...this.requests.map((r) => ({ range: r.range, request: { request: r.request || ({} as R), planners: [] } })),
      ...this.planners.map((p) => ({ range: p.range, request: { request: {} as R, planners: [p.request] } })),
    ]

    const latest = entries.some((r) => r.range.from === 'latest') ? await portal.getHead() : undefined

    const resolvedTimestamps = await this.resolveTimestamps(portal, entries)

    const resolvedRequests = entries.map((r) => ({
      range:
        r.range.from === 'latest'
          ? {
              from: Math.min(latest?.number || 0, bound?.from || Infinity),
              ...(r.range.to ? { to: r.range.to } : {}),
            }
          : {
              from: resolveRequiredRangeValue(r.range.from, resolvedTimestamps),
              to: resolveOptionalRangeValue(r.range.to, resolvedTimestamps),
            },
      request: r.request,
    }))

    for (const r of resolvedRequests) {
      // non-strict comparison on purpose. `to` can be zero
      if (r.range.to != null && r.range.to < r.range.from) {
        throw new BlockRangeConfigurationError(
          `Invalid block range: 'from' (${r.range.from}) must be less than or equal to 'to' (${r.range.to})`,
        )
      }
    }

    const ranges = mergeRangeRequests<MergedRequest<R>>(resolvedRequests, (a, b) => ({
      request: this.mergeDataRequests(a.request, b.request),
      planners: [...a.planners, ...b.planners],
    }))

    if (!ranges.length) {
      // FIXME request should be optional
      return {
        raw: [{ range: { from: 0 } } as any],
        bounded: [{ range: bound || { from: 0 } } as any],
      }
    }

    return {
      raw: ranges.map(unwrapMergedRequest),
      bounded: applyRangeBound(ranges, bound).map(unwrapMergedRequest),
    }
  }

  private async resolveTimestamps(portal: Portal, requests: { range: NaturalRange }[]): Promise<Map<number, number>> {
    const timestamps = new Set<number>()

    for (const r of requests) {
      if (r.range.from instanceof Date) {
        timestamps.add(dateToSeconds(r.range.from))
      }
      if (r.range.to instanceof Date) {
        timestamps.add(dateToSeconds(r.range.to))
      }
    }

    if (timestamps.size === 0) return new Map()

    const resolved = new Map<number, number>()
    await Promise.all(
      [...timestamps].map(async (ts) => {
        try {
          resolved.set(ts, await portal.resolveTimestamp(ts))
        } catch (error) {
          if (error instanceof Error && error.message.toLowerCase().includes('no chunk found for timestamp')) {
            const date = new Date(ts * 1000).toISOString()
            throw new BlockRangeConfigurationError(
              `Failed to resolve timestamp ${date} to a block number. The block for this timestamp may not have been produced yet.`,
            )
          }
          throw error
        }
      }),
    )

    return resolved
  }
}

type MergedRequest<R> = { request: R; planners: RequestPlanner<R>[] }

function unwrapMergedRequest<R>({ range, request }: RangeRequest<MergedRequest<R>>): PlannedRangeRequest<R> {
  if (request.planners.length === 0) {
    return { range, request: request.request }
  }

  return { range, request: request.request, planners: request.planners }
}

/** Minimal portal interface needed by `QueryBuilder.calculateRanges` to resolve head and timestamps. */
export interface Portal {
  getHead(): Promise<{ number: number; hash: string } | undefined>
  resolveTimestamp(seconds: number): Promise<number>
}

function dateToSeconds(date: Date): number {
  return Math.floor(date.getTime() / 1000)
}

function resolveRequiredRangeValue(value: number | Date, resolved: Map<number, number>): number {
  if (typeof value === 'number') return value

  const timestamp = dateToSeconds(value)
  const blockNumber = resolved.get(timestamp)

  if (blockNumber === undefined) {
    throw new BlockRangeConfigurationError(`Failed to resolve date ${value.toISOString()} to a block number.`)
  }

  return blockNumber
}

function resolveOptionalRangeValue(
  value: number | Date | undefined,
  resolved: Map<number, number>,
): number | undefined {
  if (value === undefined) return undefined
  return resolveRequiredRangeValue(value, resolved)
}

/**
 * Merges overlapping range requests into a sorted list of non-overlapping ranges.
 * When ranges overlap, their requests are combined using the provided merge function.
 *
 * @param requests - Array of range requests to merge
 * @param merge - Function to merge two overlapping requests
 * @returns Array of non-overlapping range requests sorted by range.from
 *
 * @example
 * // Merge overlapping log requests
 * const requests = [
 *   { range: { from: 0, to: 100 }, request: { logs: [{address: '0x1'}] } },
 *   { range: { from: 50, to: 150 }, request: { logs: [{address: '0x2'}] } }
 * ]
 *
 * const merged = mergeRangeRequests(requests, mergeDataRequests)
 * // Result:
 * // [
 * //   { range: { from: 0, to: 49 }, request: { logs: [{address: '0x1'}] } },
 * //   { range: { from: 50, to: 100 }, request: { logs: [{address: '0x1'}, {address: '0x2'}] } },
 * //   { range: { from: 101, to: 150 }, request: { logs: [{address: '0x2'}] } }
 * // ]
 *
 * @example
 * // Merge requests with infinite ranges
 * const requests = [
 *   { range: { from: 0 }, request: { logs: [{address: '0x1'}] } },
 *   { range: { from: 100 }, request: { logs: [{address: '0x2'}] } }
 * ]
 *
 * const merged = mergeRangeRequests(requests, mergeDataRequests)
 * // Result:
 * // [
 * //   { range: { from: 0, to: 99 }, request: { logs: [{address: '0x1'}] } },
 * //   { range: { from: 100 }, request: { logs: [{address: '0x1'}, {address: '0x2'}] } }
 * // ]
 */
export function mergeRangeRequests<R>(requests: RangeRequest<R>[], merge: (r1: R, r2: R) => R): RangeRequest<R>[] {
  if (requests.length <= 1) return requests

  let union: RangeRequest<R>[] = []
  let heap = new Heap<RangeRequest<R>>((a, b) => a.range.from - b.range.from)

  heap.init(requests.slice())

  let top = heap.popStrict()
  let req: RangeRequest<R> | undefined = heap.peek()
  while (req) {
    let i = rangeIntersection(top.range, req.range)
    if (i == null) {
      union.push(top)
      top = heap.popStrict()
    } else {
      heap.pop()
      for (let range of rangeDifference(top.range, i)) {
        heap.push({ range, request: top.request })
      }
      for (let range of rangeDifference(req.range, i)) {
        heap.push({ range, request: req.request })
      }
      heap.push({
        range: i,
        request: merge(top.request, req.request),
      })
      top = heap.popStrict()
    }
    req = heap.peek()
  }
  union.push(top)
  return union
}

/**
 * Filters range requests by applying a boundary range and returns only intersecting requests.
 * For requests that partially intersect with the boundary, only the intersecting portion is kept.
 *
 * @param requests - Array of range requests to filter
 * @param range - Optional boundary range to apply. If not provided, returns original requests unmodified.
 * @returns Array of range requests that intersect with the boundary range
 *
 * @example
 * // Filter requests by range boundary
 * const requests = [
 *   { range: { from: 0, to: 100 }, request: { logs: [{address: '0x1'}] } },
 *   { range: { from: 150, to: 200 }, request: { logs: [{address: '0x2'}] } }
 * ]
 *
 * const filtered = applyRangeBound(requests, { from: 50, to: 150 })
 * // Result:
 * // [
 * //   { range: { from: 50, to: 100 }, request: { logs: [{address: '0x1'}] } }
 * // ]
 *
 * @example
 * // Filter requests with infinite ranges
 * const requests = [
 *   { range: { from: 0 }, request: { logs: [{address: '0x1'}] } },
 *   { range: { from: 100 }, request: { logs: [{address: '0x2'}] } }
 * ]
 *
 * const filtered = applyRangeBound(requests, { from: 50, to: 150 })
 * // Result:
 * // [
 * //   { range: { from: 50, to: 150 }, request: { logs: [{address: '0x1'}] } },
 * //   { range: { from: 100, to: 150 }, request: { logs: [{address: '0x2'}] } }
 * // ]
 */
export function applyRangeBound<R>(requests: RangeRequest<R>[], range?: Range): RangeRequest<R>[] {
  if (range == null) return requests
  let result: RangeRequest<R>[] = []
  for (let req of requests) {
    let i = rangeIntersection(range, req.range)
    if (i) {
      result.push({ range: i, request: req.request })
    }
  }
  return result
}

/**
 * Finds the intersection between two ranges. Returns undefined if there is no intersection.
 * If both ranges extend to infinity, the result will also extend to infinity.
 *
 * @param a - First range to intersect
 * @param b - Second range to intersect
 * @returns The intersecting range, or undefined if ranges don't intersect
 *
 * @example
 * // Intersect finite ranges
 * const a = { from: 0, to: 100 }
 * const b = { from: 50, to: 150 }
 * const intersection = rangeIntersection(a, b)
 * // Result: { from: 50, to: 100 }
 *
 * @example
 * // Intersect with infinite range
 * const a = { from: 0 }  // infinite range
 * const b = { from: 100, to: 200 }
 * const intersection = rangeIntersection(a, b)
 * // Result: { from: 100, to: 200 }
 *
 * @example
 * // No intersection case
 * const a = { from: 0, to: 50 }
 * const b = { from: 100, to: 150 }
 * const intersection = rangeIntersection(a, b)
 * // Result: undefined
 */
export function rangeIntersection(a: Range, b: Range): Range | undefined {
  let beg = Math.max(a.from, b.from)
  let end = Math.min(rangeEnd(a), rangeEnd(b))
  if (beg > end) return undefined
  if (end === Number.POSITIVE_INFINITY) {
    return { from: beg }
  }
  return { from: beg, to: end }
}

/**
 * Calculates the difference between two ranges, returning an array of non-intersecting ranges.
 * The difference consists of parts of range 'a' that do not overlap with range 'b'.
 *
 * @param a - The first range to compare
 * @param b - The second range to subtract from the first range
 * @returns Array of ranges representing the non-overlapping parts of range 'a'
 *
 * @example
 * // Get difference between finite ranges
 * const a = { from: 0, to: 100 }
 * const b = { from: 50, to: 75 }
 * const diff = rangeDifference(a, b)
 * // Result:
 * // [
 * //   { from: 0, to: 49 },
 * //   { from: 76, to: 100 }
 * // ]
 *
 * @example
 * // Get difference with infinite range
 * const a = { from: 0 }  // infinite range
 * const b = { from: 50, to: 100 }
 * const diff = rangeDifference(a, b)
 * // Result:
 * // [
 * //   { from: 0, to: 49 },
 * //   { from: 101 }  // infinite range
 * // ]
 *
 * @example
 * // No intersection case
 * const a = { from: 0, to: 50 }
 * const b = { from: 100, to: 150 }
 * const diff = rangeDifference(a, b)
 * // Result:
 * // [
 * //   { from: 0, to: 50 }  // original range 'a' returned unchanged
 * // ]
 */
export function rangeDifference(a: Range, b: Range): Range[] {
  let i = rangeIntersection(a, b)
  if (i == null) return [a]
  let result: Range[] = []
  if (a.from < i.from) {
    result.push({ from: a.from, to: i.from - 1 })
  }
  if (i.to != null && i.to < rangeEnd(a)) {
    let from = i.to + 1
    if (a.to) {
      result.push({ from, to: a.to })
    } else {
      result.push({ from })
    }
  }
  return result
}

/**
 * Returns the upper bound of a range, or `+Infinity` if the range is open-ended.
 */
export function rangeEnd(range: Range): number {
  return range.to ?? Number.POSITIVE_INFINITY
}

/**
 * Concatenates two optional arrays of query items. Returns `undefined` when both are empty/absent.
 */
export function concatQueryLists<T extends object>(a?: T[], b?: T[]): T[] | undefined {
  let result = [...(a || []), ...(b || [])]
  return result.length ? result : undefined
}

/**
 * Returns a hex-encoded SHA-256 hash of the query, excluding positional fields
 * (`fromBlock`, `toBlock`, `parentBlockHash`). Used as a cache key so that
 * identical queries over different block ranges share the same cache bucket.
 */
export async function hashQuery(query: Query): Promise<string> {
  const { fromBlock, toBlock, parentBlockHash, ...unique } = query

  return await sha256Hex(JSON.stringify(unique))
}

/**
 * UTF-8 encode a JS string to ArrayBuffer.
 * @internal
 */
export function stringToArrayBuffer(str: string): Uint8Array {
  if (typeof TextEncoder !== 'undefined') {
    return new TextEncoder().encode(str)
  }

  throw new Error(
    'TextEncoder is not supported in this environment. Please ensure you are running in a modern JavaScript environment that supports TextEncoder (Node.js 11+, modern browsers, or include a polyfill).',
  )
}

async function sha256Hex(data: string): Promise<string> {
  // globalThis.crypto is available in browsers, Node 18+, and Cloudflare Workers
  if (typeof crypto === 'undefined' || !crypto.subtle) {
    throw new Error(
      'crypto.subtle is not supported in this environment. Please ensure you are running in a modern JavaScript environment that supports crypto.subtle (Node.js 18+, modern browsers, or include a polyfill).',
    )
  }
  const d = await crypto.subtle.digest('SHA-256', stringToArrayBuffer(data))
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, '0')).join('')
}
