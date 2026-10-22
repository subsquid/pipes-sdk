import { AbiEvent } from '@subsquid/evm-abi'
import type { Codec } from '@subsquid/evm-codec'

import { BlockCursor, Logger, PortalRange, createTarget } from '~/core/index.js'
import { arrayify } from '~/internal/array.js'
import { PortalClient } from '~/portal-client/client.js'
import { Log, LogRequest } from '~/portal-client/query/evm.js'

import {
  EventWithArgs,
  EventWithArgsInput,
  FactoryEvent,
  IndexedParams,
  buildEventTopics,
  evmEventDecoder,
  getNormalizedEventParams,
  isEventWithArgs,
} from './evm-decoder.js'
import { evmPortalStream } from './evm-portal-source.js'

export type EventArgs = {
  [key: string]: Codec<any> & { indexed?: boolean }
}

/** @internal */
export type InternalFactoryEvent<T extends EventArgs> = {
  childAddress: string
  factoryAddress: string
  blockNumber: number
  transactionIndex: number
  logIndex: number
  event: DecodedAbiEvent<T>
}

export interface FactoryPersistentAdapter<T extends InternalFactoryEvent<any>> {
  all(): Promise<T[]>
  all(params: IndexedParams<AbiEvent<any>>): Promise<T[]>
  lookup(parameter: string): Promise<T | null>
  save(entities: T[]): Promise<void>
  remove(blockNumber: number): Promise<void>
  migrate(): Promise<void>
}

export type DecodedAbiEvent<T extends EventArgs> = ReturnType<AbiEvent<T>['decode']>

export type FactoryLookaheadOptions = {
  /**
   * The most child addresses one portal query may carry. Every child-event request repeats the
   * address list, so the limit counts the addresses of all of them together. When the children
   * up to a block no longer fit, child events from that block on are fetched by topic alone.
   *
   * @default 5_000
   */
  maxAddresses?: number
  /**
   * The fewest blocks an address-filtered query must cover. A new filter starts a new portal
   * stream, which is not worth it for a short stretch. Closer than this to the finalized head,
   * child events are fetched by topic alone for the rest of the run.
   *
   * @default 10_000
   */
  minSegmentBlocks?: number
}

const DEFAULT_LOOKAHEAD_MAX_ADDRESSES = 5_000
const DEFAULT_LOOKAHEAD_MIN_SEGMENT_BLOCKS = 10_000

export type ContractFactoryOptions<T extends EventArgs> = {
  address: string | string[]
  event: AbiEvent<T> | EventWithArgsInput<AbiEvent<T>>
  childAddressField: keyof T | ((data: DecodedAbiEvent<T>) => string | null)
  /**
   * It is safe to use `any` here because the FactoryPersistentAdapter generic argument
   * will be inferred in the constructor of `Factory`
   */
  database:
    | FactoryPersistentAdapter<InternalFactoryEvent<any>>
    | Promise<FactoryPersistentAdapter<InternalFactoryEvent<any>>>
  /**
   * Fetches child events with a server-side address filter instead of by topic alone.
   *
   * A second, light stream scans the factory events of finalized blocks ahead of the main stream.
   * The main stream then fetches each stretch of blocks filtered by every child created up to its
   * end. Near the finalized head, or when the children no longer fit into one query, it fetches
   * child events by topic alone, as it does for the whole range without this option.
   *
   * @default false
   */
  lookahead?: boolean | FactoryLookaheadOptions
}

export class Factory<T extends EventArgs> {
  #batch: InternalFactoryEvent<T>[] = []
  #db?: FactoryPersistentAdapter<InternalFactoryEvent<T>>
  readonly #addresses: Set<string>
  readonly #event: AbiEvent<T>
  readonly #params: IndexedParams<AbiEvent<T>>

  constructor(private options: ContractFactoryOptions<T>) {
    this.#addresses = new Set(arrayify(this.options.address).map((a) => a.toLowerCase()))

    if (isEventWithArgs(this.options.event)) {
      this.#event = this.options.event.event
      this.#params = getNormalizedEventParams(this.options.event.params)
    } else {
      this.#event = this.options.event
      this.#params = {}
    }
  }

  factoryAddress(): string[] {
    return Array.from(this.#addresses)
  }

  factoryTopic() {
    return this.#event.topic
  }

  factoryEventParams(): IndexedParams<AbiEvent<T>> {
    return this.#params
  }

  factoryEvent(): AbiEvent<T> {
    return this.#event
  }

  buildFactoryEventRequest(): LogRequest {
    const params = this.factoryEventParams()
    const hasParams = Object.keys(params).length > 0

    if (!hasParams) {
      return {
        address: this.factoryAddress(),
        topic0: [this.factoryTopic()],
      }
    }

    const topics = buildEventTopics(this.#event, params)
    return {
      address: this.factoryAddress(),
      ...topics,
    }
  }

  isFactoryEvent(log: any) {
    return this.#event.is(log)
  }

  private assertDb() {
    if (!this.#db) throw new Error('Database not initialized. Call migrate() first.')

    return this.#db
  }

  async migrate() {
    this.#db = this.options.database instanceof Promise ? await this.options.database : this.options.database

    return this.assertDb().migrate()
  }

  /** @internal */
  lookaheadOptions(): Required<FactoryLookaheadOptions> | null {
    const lookahead = this.options.lookahead ?? false
    if (lookahead === false) return null

    const options = lookahead === true ? {} : lookahead
    const maxAddresses = options.maxAddresses ?? DEFAULT_LOOKAHEAD_MAX_ADDRESSES
    const minSegmentBlocks = options.minSegmentBlocks ?? DEFAULT_LOOKAHEAD_MIN_SEGMENT_BLOCKS

    for (const [name, value] of Object.entries({ maxAddresses, minSegmentBlocks })) {
      if (!Number.isInteger(value) || value < 1) {
        throw new Error(`lookahead.${name} must be a positive integer, got ${value}`)
      }
    }

    return { maxAddresses, minSegmentBlocks }
  }

  /**
   * The child a factory-creation log announces, or `null` when it names none. Inline decoding and
   * the lookahead scan both read children through here, so they never disagree on one.
   *
   * @internal
   */
  childFromLog(log: Log, blockNumber: number): InternalFactoryEvent<T> | null {
    const decoded = this.#event.decode(log)
    const contract =
      typeof this.options.childAddressField === 'function'
        ? this.options.childAddressField(decoded)
        : String(decoded[this.options.childAddressField])

    if (!contract) return null

    return {
      childAddress: contract,
      factoryAddress: log.address,
      blockNumber,
      transactionIndex: log.transactionIndex,
      logIndex: log.logIndex,
      event: decoded,
    }
  }

  /**
   * Whether {@link getContract} accepts the child of this creation event: a stored creation event may
   * come from another factory or from a run with other params.
   *
   * @internal
   */
  ownsChild(child: InternalFactoryEvent<T>): boolean {
    return this.#addresses.has(child.factoryAddress) && this.matchesParams(child.event)
  }

  async decode(log: Log, blockNumber: number) {
    const child = this.childFromLog(log, blockNumber)
    if (!child) return

    this.#batch.push(child)
  }

  async getContract(address: string): Promise<FactoryEvent<DecodedAbiEvent<T>> | null> {
    const memory = this.#batch.find((b) => b.childAddress === address)
    if (memory) {
      if (!this.ownsChild(memory)) return null

      return this.transform(memory)
    }

    const fromDb = await this.assertDb().lookup(address)
    if (!fromDb) return null
    else if (!this.ownsChild(fromDb)) return null

    return this.transform(fromDb)
  }

  /**
   * Lowercased addresses of every stored child {@link getContract} accepts.
   *
   * Reads every row and filters it here: an adapter filtering by params on its own may drop a child
   * that {@link getContract} accepts.
   *
   * @internal
   */
  async storedChildAddresses(): Promise<string[]> {
    const addresses = new Set<string>()

    for (const child of await this.assertDb().all()) {
      if (!this.ownsChild(child)) continue

      addresses.add(child.childAddress.toLowerCase())
    }

    return [...addresses]
  }

  private transform(event: InternalFactoryEvent<T>): FactoryEvent<DecodedAbiEvent<T>> {
    return {
      contract: event.factoryAddress,
      blockNumber: event.blockNumber,
      event: event.event,
    }
  }

  private matchesParams(event: DecodedAbiEvent<T>): boolean {
    if (Object.keys(this.#params).length === 0) {
      return true
    }

    for (const [key, values] of Object.entries(this.#params)) {
      // This value is being casted because TS isn't being able to infer the value of expectedValues out of `this.#params`.
      const expectedValues = values as unknown[]
      const eventValue = event[key as keyof T]
      if (!expectedValues.some((ev) => String(ev).toLowerCase() === String(eventValue).toLowerCase())) {
        return false
      }
    }

    return true
  }

  /** @internal */
  async runPreindex({ portal, range, logger }: { portal: PortalClient; range: PortalRange; logger: Logger }) {
    const name = `factory preindexing ${this.factoryAddress().join(', ')}`

    logger.info(`Starting ${name}`)

    await evmPortalStream({
      id: name,
      portal,
      logger,
      outputs: evmEventDecoder({
        profiler: { name },
        contracts: this.factoryAddress(),
        range,
        events: {
          factory: Object.keys(this.#params).length > 0 ? { event: this.#event, params: this.#params } : this.#event,
        },
      }),
    }).pipeTo(
      createTarget({
        write: async ({ read }) => {
          for await (const { data } of read()) {
            const res: InternalFactoryEvent<T>[] = []

            for (const event of data.factory) {
              const contract =
                typeof this.options.childAddressField === 'function'
                  ? this.options.childAddressField(event.event)
                  : String(event.event[this.options.childAddressField])

              if (!contract) continue

              res.push({
                childAddress: contract,
                factoryAddress: event.contract,
                blockNumber: event.block.number,
                transactionIndex: event.rawEvent.transactionIndex,
                logIndex: event.rawEvent.logIndex,
                event: event.event,
              })
            }

            await this.assertDb().save(res)
          }
        },
      }),
    )

    logger.info(`Finished ${name}`)
  }

  async getAllContracts() {
    return this.assertDb().all(this.#params)
  }

  async persist() {
    await this.assertDb().save(this.#batch)
    // Flush memory batch
    this.#batch = []
  }

  async rollback(cursor: BlockCursor) {
    await this.assertDb().remove(cursor.number)
  }

  static isFactory(address: any) {
    return address instanceof Factory
  }
}

/**
 * Creates a Factory instance to track contract creation events and enable decoding events from dynamically created contracts.
 *
 * A Factory pattern is useful when you need to decode events from contracts that are created dynamically
 * by factory contracts (e.g., Uniswap pools created by a factory). The factory tracks creation events,
 * extracts child contract addresses, and stores them in a database for efficient lookup during event decoding.
 *
 * @param options - Configuration object for the factory
 * @param options.address - Factory contract address(es) to monitor for creation events. Can be a single address or an array of addresses
 * @param options.event - The factory event that signals contract creation. Can be:
 *   - An {@link AbiEvent} instance to capture all creation events
 *   - An {@link EventWithArgs} object with `event` and `params` to filter by indexed parameters
 * @param options.childAddressField - Field name or function to extract the child contract address from the decoded factory event.
 *   - If a string, it should be a key from the event's decoded data
 *   - If a function, it receives the decoded event data and should return the contract address or null
 * @param options.database - Database adapter for storing and querying factory events. Can be a {@link FactoryPersistentAdapter} instance or a Promise that resolves to one
 * @returns A {@link Factory} instance that can be used as the `contracts` parameter in {@link evmEventDecoder}
 *
 * @example
 * ```ts
 * contractFactory({
 *   address: '0x1f98431c8ad98523631ae4a59f267346ea31f984',
 *   event: factoryAbi.PoolCreated,
 *   childAddressField: 'pool',
 *   database: contractFactorySqliteStore({
 *     path: './uniswap3-eth-pools.sqlite',
 *   }),
 * })
 * ```
 *
 */
export function contractFactory<T extends EventArgs>(options: ContractFactoryOptions<T>) {
  return new Factory(options)
}
