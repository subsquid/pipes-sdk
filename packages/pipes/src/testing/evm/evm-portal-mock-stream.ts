import { type MockPortal, type MockResponse, mockPortal } from '../test-portal.js'
import { type PortalBlock } from './mock-block.js'

export { type MockPortal } from '../test-portal.js'

/**
 * Creates a mock portal HTTP server that serves the given blocks as a stream response.
 *
 * @deprecated Use `mockPortal({ blocks })` from `@subsquid/pipes/testing`. This helper sends every
 * block in one response whatever the query asks for, so a test can pass on blocks outside its range.
 *
 * @example
 * ```ts
 * const event1 = encodeEvent({ abi, eventName: 'Transfer', address, args: { ... } })
 *
 * const portal = await mockEvmPortalStream({
 *   blocks: [
 *     mockBlock({ transactions: [{ logs: [event1] }] }),
 *     mockBlock({ transactions: [{ logs: [event1] }] }),
 *   ],
 * })
 * ```
 */
export async function mockEvmPortalStream({
  blocks,
  finalized,
}: {
  blocks: PortalBlock[]
  finalized?: { number: number; hash: string }
}): Promise<MockPortal> {
  const lastBlock = blocks[blocks.length - 1]
  const head = finalized ?? (lastBlock ? { number: lastBlock.header.number, hash: lastBlock.header.hash } : undefined)

  const response: MockResponse = {
    statusCode: 200,
    data: blocks,
    head: head ? { finalized: head } : undefined,
  }

  return mockPortal([response])
}
