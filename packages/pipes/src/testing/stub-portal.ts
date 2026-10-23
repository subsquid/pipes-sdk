import { Portal } from '~/core/query-builder.js'

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
