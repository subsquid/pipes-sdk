import type { AbiEvent, EventParams } from '@subsquid/evm-abi'
import { Codec, Sink } from '@subsquid/evm-codec'

import {
  DecodeErrorHook,
  PortalRange,
  ProfilerOptions,
  Transformer,
  defaultDecodeError,
  formatWarning,
  parsePortalRange,
  recordSuppressedDecode,
} from '~/core/index.js'
import { arrayify, findDuplicates } from '~/internal/array.js'
import { Log, LogRequest } from '~/portal-client/query/evm.js'

import { evmQuery } from './evm-query-builder.js'
import { Factory } from './factory.js'
import { FactoryLookahead } from './factory-lookahead.js'

export type FactoryEvent<T> = {
  contract: string
  blockNumber: number
  event: T
}

export type DecodedEvent<D = object, F = unknown> = {
  event: D
  contract: string
  block: {
    number: number
    hash: string
  }
  timestamp: Date
  factory?: F extends object ? FactoryEvent<F> : never
  rawEvent: Log<{
    address: true
    topics: true
    data: true
    transactionHash: true
    logIndex: true
    transactionIndex: true
  }>
}

/**
 * Defines how to capture an event — either all instances or filtered by indexed parameters.
 *
 * @example Simple form — capture all emitted instances:
 * ```ts
 * events: { transfers: commonAbis.erc20.events.Transfer }
 * ```
 *
 * @example Filtered form — match specific indexed parameter values:
 * ```ts
 * events: {
 *   transfers: {
 *     event: commonAbis.erc20.events.Transfer,
 *     params: { from: '0x...' },
 *   },
 * }
 * ```
 */
export type EventFilter<T extends AbiEvent<any> = AbiEvent<any>> = T | EventWithArgsInput<T>

export type Events = Record<string, EventFilter>

export type DecodeEventsFunctions<T extends Events> = ReturnType<ExtractEventType<EventsMap<T>[keyof T]>['decode']>

export type IndexedKeys<T> = {
  [K in keyof T]: T[K] extends { indexed: true } ? K : never
}[keyof T]

type CodecValueType<T extends AbiEvent<any>, K extends keyof T['params']> =
  T['params'][K] extends Codec<any, infer TOut> ? TOut : never

export type IndexedParamsInput<T extends AbiEvent<any>> = Partial<{
  [K in IndexedKeys<T['params']>]: CodecValueType<T, K> | CodecValueType<T, K>[]
}>

export type IndexedParams<T extends AbiEvent<any>> = Partial<{
  [K in IndexedKeys<T['params']>]: CodecValueType<T, K>[]
}>

export type EventWithArgsInput<T extends AbiEvent<any>> = {
  event: T
  params: IndexedParamsInput<T>
}

export type EventWithArgs<T extends AbiEvent<any>> = {
  event: T
  params: IndexedParams<T>
}

export type EventEntryFor<V> =
  // If the entry is an AbiEvent, allow either the raw AbiEvent or the `{ event, params }` form
  V extends AbiEvent<any>
    ? V
    : V extends {
          event: infer E extends AbiEvent<any>
          params: infer P extends Record<PropertyKey, unknown>
        }
      ? // Reject params that include non-indexed keys
        Exclude<keyof P, keyof Partial<IndexedParamsInput<E>>> extends never
        ? {
            event: E
            params: IndexedParamsInput<E>
          }
        : never
      : never

export type EventsMap<T> = {
  readonly [K in keyof T]: EventEntryFor<T[K]>
}

export type AbiDecodeEvent<T extends AbiEvent<any>> = ReturnType<T['decode']>

type ExtractEventType<V> = V extends AbiEvent<any> ? V : V extends { event: infer E extends AbiEvent<any> } ? E : never

export type EventResponse<T extends Events, F> = {
  [K in keyof T]: DecodedEvent<
    // child event - extract from EventsMap normalized type
    AbiDecodeEvent<ExtractEventType<EventsMap<T>[K]>>,
    // factory event
    F extends Factory<infer R> ? AbiDecodeEvent<AbiEvent<R>> : never
  >[]
}

type Contracts = Factory<any> | string[]

export type DecodedEventPipeArgs<T extends Events, C extends Contracts> = {
  range: PortalRange
  /** Optional contract addresses to filter events from. Can be a {@link Factory} instance or an array of addresses. */
  contracts?: C
  events: EventsMap<T>
  profiler?: ProfilerOptions
  onError?: DecodeErrorHook
}

const decodedEventFields = {
  block: {
    number: true,
    hash: true,
    timestamp: true,
  },
  transaction: {
    from: true,
    to: true,
    hash: true,
    sighash: true,
  },
  log: {
    address: true,
    topics: true,
    data: true,
    transactionHash: true,
    logIndex: true,
    transactionIndex: true,
  },
} as const

const DUPLICATED_EVENTS = (duplicates: { props: string[]; event: string }[]) => {
  const events = duplicates
    .map((d) =>
      `
Topic:      ${d.event}
Properties: ${d.props.join(', ')}

${d.props.slice(1).join(', ')} property will miss events due to the duplicate signature.
`.trim(),
    )
    .join('-----\n')

  return formatWarning({
    title: 'Duplicate event topics detected',
    content: [
      events,
      '', // empty line
      `Ensure each event has a unique signature to avoid decoding issues.`,
    ],
  })
}

export function isEventWithArgs<T extends AbiEvent<any>>(
  value: unknown,
): value is EventWithArgs<T> | EventWithArgsInput<T> {
  return typeof value === 'object' && value !== null && 'event' in value && 'params' in value
}

type LogParamTopics = Pick<LogRequest, 'topic0' | 'topic1' | 'topic2' | 'topic3'>

export function buildEventTopics<T extends AbiEvent<any>>(event: T, indexedParams: IndexedParams<T>): LogParamTopics {
  const params = event.params as EventParams<T>

  // Filter by indexed parameters to ensure correct topic assignment order
  const indexedParamKeys = Object.keys(params).filter((k) => {
    const param = params[k] as { indexed?: boolean }
    return param?.indexed === true
  })

  const paramsByTopicOrder: (string[] | undefined)[] = indexedParamKeys.map((k) => {
    const indexedParam = indexedParams[k as keyof typeof indexedParams]
    if (!indexedParam) return

    const topicParams: string[] = []
    for (const param of indexedParam) {
      const eventParams = params[k] as Codec<any, any>
      const sink = new Sink(1)
      eventParams['encode'](sink, param)
      topicParams.push(sink.toString())
    }

    return topicParams
  })

  return {
    topic0: [event.topic],
    topic1: paramsByTopicOrder[0],
    topic2: paramsByTopicOrder[1],
    topic3: paramsByTopicOrder[2],
  }
}

export function getNormalizedEventParams<T extends AbiEvent<any>>(params: IndexedParamsInput<T>): IndexedParams<T> {
  const entries = Object.entries(params)
    .map(([key, value]) => [key, arrayify(value).map((v) => (typeof v === 'string' ? v.toLowerCase() : v))])
    .filter(([_, value]) => value.length > 0)
  return Object.fromEntries(entries) as IndexedParams<T>
}

function getNormalizedEvents<T extends Events>(events: T): EventWithArgs<AbiEvent<any>>[] {
  const normalizedEvents: EventWithArgs<AbiEvent<any>>[] = []

  for (const eventName in events) {
    const event = events[eventName]

    if (isEventWithArgs(event)) {
      normalizedEvents.push({
        event: event.event,
        params: getNormalizedEventParams(event.params),
      } as EventWithArgs<AbiEvent<any>>)
    } else {
      normalizedEvents.push({
        event,
        params: {},
      } as EventWithArgs<AbiEvent<any>>)
    }
  }

  return normalizedEvents
}

function splitEvents<T extends AbiEvent<any>>(normalizedEvents: EventWithArgs<T>[]) {
  return {
    eventsWithoutParams: normalizedEvents.filter((event) => Object.keys(event.params).length === 0),
    eventsWithParams: normalizedEvents.filter((event) => Object.keys(event.params).length > 0),
  }
}

function getDuplicateEvents<T extends Events>(events: T, duplicates: string[]) {
  return duplicates.map((duplicate) => {
    const props = Object.keys(events).filter((name) => {
      const eventValue = events[name]
      const eventAbi = isEventWithArgs(eventValue) ? eventValue.event : eventValue
      return eventAbi.topic === duplicate
    })
    return { props, event: duplicate }
  })
}

/**
 * Decodes EVM events from portal data and optionally filters them by indexed parameters.
 *
 * This transformer extracts and decodes EVM events from blockchain data. You can either
 * capture all instances of an event or filter by indexed parameters to reduce data transfer
 * and processing overhead.
 *
 * @param args - Configuration object for the decoder
 * @param args.range - Block range to query. See {@link PortalRange} for format details.
 * @param args.contracts - Optional contract addresses to filter events from. Can be a {@link Factory} instance or an array of addresses
 * @param args.events - Map of event names to event definitions. Each entry can be:
 *   - An {@link AbiEvent} instance to capture all instances of that event
 *   - An {@link EventWithArgs} object with `event` and `params` to filter by indexed parameters
 * @param args.profiler - Optional {@link ProfilerOptions} configuration for performance monitoring
 * @param args.onError - Optional {@link DecodeErrorHook}; returning without throwing suppresses the record (counted in `sqd_decode_errors_skipped_total`)
 * @returns A {@link Transformer} that processes EVM portal data and returns {@link EventResponse} with decoded events
 *
 * @example
 * ```ts
 * evmEventDecoder({
 *   range: { from: 'latest' },
 *   events: {
 *     // Use the AbiEvent instance directly for convenience if you need all the emitted events
 *     approvals: commonAbis.erc20.events.Approval,
 *     // Or filter by any of the indexed parameters defined in the contract
 *     transfers: {
 *       event: commonAbis.erc20.events.Transfer,
 *       params: {
 *         // For every event param you can use an array to match multiple values
 *         from: ['0x87482e84503639466fad82d1dce97f800a410945'],
 *         // Or pass a single value directly
 *         to: '0x10b32a54eeb05d2c9cd1423b4ad90c3671a2ed5f',
 *       },
 *     },
 *   },
 * })
 * ```
 */
export function evmEventDecoder<T extends Events, C extends Contracts>({
  range,
  contracts,
  events,
  profiler,
  onError: onErrorHook,
}: DecodedEventPipeArgs<T, C>) {
  const decodedRange = parsePortalRange(range)
  const onError = onErrorHook || defaultDecodeError
  const normalizedEvents = getNormalizedEvents(events)
  const { eventsWithParams, eventsWithoutParams } = splitEvents(normalizedEvents)
  const eventTopic0 = eventsWithoutParams.map((event) => event.event.topic)
  const factory = Factory.isFactory(contracts) ? contracts : undefined
  const normalizedContracts =
    contracts && !Factory.isFactory(contracts) ? contracts.map((contract) => contract.toLowerCase()) : undefined

  const childRequests = (address?: string[]): LogRequest[] => {
    const requests: LogRequest[] = []

    if (eventsWithoutParams.length > 0) {
      requests.push({ address, topic0: eventTopic0, transaction: true })
    }

    for (const event of eventsWithParams) {
      requests.push({ ...buildEventTopics(event.event, event.params), address, transaction: true })
    }

    return requests
  }

  const lookaheadOptions = factory?.lookaheadOptions()
  const lookahead =
    factory && lookaheadOptions && childRequests().length > 0
      ? new FactoryLookahead({ factory, childRequests, ...lookaheadOptions })
      : undefined

  const query = evmQuery().addFields(decodedEventFields)

  if (factory) {
    query.addLogRequest({ range: decodedRange, request: factory.buildFactoryEventRequest() })
  }

  if (lookahead) {
    query.addPlannedRequest({ range: decodedRange, planner: lookahead.plan })
  } else {
    for (const request of childRequests(factory ? undefined : (contracts as string[] | undefined))) {
      query.addLogRequest({ range: decodedRange, request })
    }
  }

  return query
    .build({
      setupQuery: (config) => {
        const allEventTopics = normalizedEvents.map(({ event }) => event.topic)
        const duplicates = findDuplicates(allEventTopics)
        if (duplicates.length) {
          config.logger.error(DUPLICATED_EVENTS(getDuplicateEvents(events, duplicates)))
        }

        config.query.merge(query)
      },
    })
    .pipe({
      profiler: profiler ?? { name: 'EVM decoder' },
      start: async (ctx) => {
        if (Factory.isFactory(contracts)) {
          await contracts.migrate()
        }
      },
      transform: async (data, ctx): Promise<EventResponse<T, C>> => {
        const result = {} as EventResponse<T, C>
        // TODO: should use normalizedEvents instead of events here
        for (const eventName in events) {
          ;(result[eventName as keyof T] as DecodeEventsFunctions<T>[]) = []
        }

        if (Factory.isFactory(contracts)) {
          const span = ctx.profiler.start('factory event decode')
          for (const block of data) {
            if (!block.logs) continue
            for (const log of block.logs) {
              if (Factory.isFactory(contracts) && contracts.isFactoryEvent(log)) {
                try {
                  await contracts.decode(log, block.header.number)
                } catch (error) {
                  await onError(ctx, error)
                  recordSuppressedDecode(ctx)
                }
              }
            }
          }
          span.end()
        }

        const span = Factory.isFactory(contracts) ? ctx.profiler.start('child events decode') : undefined
        for (const block of data) {
          if (!block.logs) continue

          for (const log of block.logs) {
            let factoryEvent: FactoryEvent<any> | null = null
            if (Factory.isFactory(contracts)) {
              factoryEvent = await contracts.getContract(log.address)
              if (!factoryEvent) {
                continue
              }
            } else if (
              normalizedContracts &&
              // We have a list of contracts to filter by - skip non-matching addresses
              // this is needed because, when using the same topic hashes, portal may return logs from other contracts
              (normalizedContracts.length === 0 || !normalizedContracts.includes(log.address))
            ) {
              continue
            }

            for (const eventName in events) {
              const eventValue = events[eventName]
              let eventAbi: AbiEvent<any>

              if (isEventWithArgs(eventValue)) {
                eventAbi = eventValue.event
              } else {
                eventAbi = eventValue
              }

              const topic0 = log.topics[0]
              if (topic0 !== eventAbi.topic) {
                continue
              } else if (!eventAbi.is(log)) {
                continue
              }

              try {
                const decoded = eventAbi.decode(log)
                const eventArray = result[eventName as keyof T] as ReturnType<typeof eventAbi.decode>[]

                eventArray.push({
                  event: decoded,
                  contract: log.address,
                  rawEvent: log,
                  block: {
                    number: block.header.number,
                    hash: block.header.hash,
                  },
                  factory: factoryEvent,
                  timestamp: new Date(block.header.timestamp * 1000),
                })
              } catch (error) {
                await onError(ctx, error)
                recordSuppressedDecode(ctx)
              }
              break
            }
          }
        }
        span?.end()

        if (Factory.isFactory(contracts)) {
          const span = ctx.profiler.start('persist factory state')
          await contracts.persist()
          span.end()
        }

        return result
      },
      rollback(cursor) {
        if (!Factory.isFactory(contracts)) return

        lookahead?.rollback()

        return contracts.rollback(cursor)
      },
      stop() {
        lookahead?.stop()
      },
    })
}
