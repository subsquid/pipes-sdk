import { Portal } from '~/core/query-builder.js'
import { BlockRef, PortalClient, StreamData } from '~/portal-client/index.js'

/**
 * A `Portal` with fixed answers and no server, for code that only reads the head or resolves a timestamp.
 *
 * @internal
 */
export function stubPortal(overrides: Partial<Portal> = {}): Portal {
  return {
    getHead: async () => ({ number: 1, hash: '0x' }),
    resolveTimestamp: async () => 0,
    ...overrides,
  }
}

export const STUB_HEAD = { number: 100, hash: '0x100' }

/**
 * A `PortalClient` that streams the given batches with no I/O, so a test owns every timer and every
 * batch boundary, a head-only batch included. `batches` gets the query of each `getStream` call.
 *
 * @internal
 */
export class StubPortalClient extends PortalClient {
  /** Every query `getStream` received, in order. */
  readonly reads: any[] = []
  readonly #batches: (query: any) => AsyncIterable<StreamData<any>>

  constructor(
    batches: (query: any) => AsyncIterable<StreamData<any>>,
    { url = 'http://portal.invalid', finalized }: { url?: string; finalized?: boolean } = {},
  ) {
    super({ url, finalized })

    this.#batches = batches
  }

  override async getMetadata() {
    return { dataset: 'test', aliases: [], real_time: true, start_block: 0 } as any
  }

  override async getHead(): Promise<BlockRef | undefined> {
    return STUB_HEAD
  }

  override getStream(query: any): any {
    this.reads.push(query)

    return this.#batches(query)
  }
}

/** One batch as the portal client hands it over; no block numbers make it a head-only batch. */
export function stubBatch(
  numbers: number[],
  head: StreamData<any>['head'],
  requests: Record<number, number> = {},
): StreamData<any> {
  return {
    blocks: numbers.map((number) => ({ header: { number, hash: `0x${number}`, timestamp: number * 1000 } })),
    head,
    meta: { bytes: numbers.length, requestedFromBlock: numbers[0] ?? 0, lastBlockReceivedAt: new Date(), requests },
  }
}
