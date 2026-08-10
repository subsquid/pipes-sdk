import { createTarget } from '~/core/index.js'

export function createMemoryTarget<T extends { blockNumber: number }[]>({
  onData,
}: {
  onData: (data: T) => Promise<void> | void
}) {
  return createTarget<T>({
    // `onData` has always been handed finalized rows only, and a hot stream cannot promise that.
    requiresFinalizedStream: true,
    write: async ({ read }) => {
      for await (const batch of read()) {
        if (batch.data.length > 0) {
          await onData(batch.data as T)
        }
      }
    },
  })
}
