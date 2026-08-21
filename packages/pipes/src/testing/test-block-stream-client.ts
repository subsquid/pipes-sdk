import { BlockCursor } from '~/core/types.js'
import { BlockStreamClient, StreamData } from '~/portal-client/index.js'

export type MockBlockStreamClientOptions = {
  name?: string
  finalized?: boolean
  /** The batches `getStream` serves; receives the query it was called with. */
  stream?: (query: any) => AsyncGenerator<StreamData<any>>
  /** The independent head poll; defaults to "no head known". */
  getHead?: () => Promise<BlockCursor | undefined>
}

export type MockBlockStreamClient = BlockStreamClient & {
  /** Every query `getStream` received, in order — assert resume anchors and probe slices on it. */
  reads: any[]
}

/**
 * A scriptable in-memory {@link BlockStreamClient} — the mock to use when testing anything that
 * consumes the client contract directly (the fallback supervisor, capability probes) without HTTP.
 * For portal-over-HTTP behavior use `mockPortal` instead.
 */
export function mockBlockStreamClient(options: MockBlockStreamClientOptions = {}): MockBlockStreamClient {
  const name = options.name ?? 'mock'
  const stream =
    options.stream ??
    async function* (): AsyncGenerator<StreamData<any>> {
      /* serves nothing */
    }
  const reads: any[] = []

  return {
    reads,
    finalized: options.finalized ?? false,
    getUrl: () => `mock://${name}`,
    getMetadata: async () => ({ dataset: name, aliases: [], real_time: true, start_block: 0 }),
    getHead: async () => (options.getHead ? await options.getHead() : undefined),
    resolveTimestamp: async () => {
      throw new Error(`${name}: resolveTimestamp unsupported`)
    },
    getStream: (query: any) => {
      reads.push(query)
      return { [Symbol.asyncIterator]: () => stream(query)[Symbol.asyncIterator]() }
    },
  } as MockBlockStreamClient
}
