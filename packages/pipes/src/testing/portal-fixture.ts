// Test-only: it imports vitest, a dev dependency, so index.ts (the published `testing` entry) must not re-export it.
import { it } from 'vitest'

import { MockChainPortal, MockPortal, MockPortalOptions, MockResponse, mockPortal } from './test-portal.js'

type OpenPortal = {
  (responses: MockResponse[], options?: { finalized?: boolean }): Promise<MockPortal>
  (options: MockPortalOptions): Promise<MockChainPortal>
}

/** A test with a `portal` fixture: it opens mock portals the way `mockPortal` does and closes them after the test. */
export const withPortal = it.extend<{ portal: OpenPortal }>({
  portal: async ({}, use) => {
    const opened: MockPortal[] = []
    const open = async (input: MockResponse[] | MockPortalOptions, options?: { finalized?: boolean }) => {
      const portal = Array.isArray(input) ? await mockPortal(input, options) : await mockPortal(input)
      opened.push(portal)

      return portal
    }

    await use(open as OpenPortal)
    await Promise.all(opened.map((portal) => portal.close()))
  },
})
