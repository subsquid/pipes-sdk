import { Portal } from '~/core/query-builder.js'
import { PortalClient, StreamData } from '~/portal-client/index.js'

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
 * batch boundary, a head-only batch included.
 *
 * @internal
 */
export class StubPortalClient extends PortalClient {
  readonly #batches: () => AsyncIterable<StreamData<any>>

  constructor(batches: () => AsyncIterable<StreamData<any>>) {
    super({ url: 'http://portal.invalid' })

    this.#batches = batches
  }

  override async getMetadata() {
    return { dataset: 'test', aliases: [], real_time: true, start_block: 0 } as any
  }

  override async getHead() {
    return STUB_HEAD
  }

  override getStream(): any {
    return this.#batches()
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
