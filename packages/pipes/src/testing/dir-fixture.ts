// Test-only: it imports vitest, a dev dependency, so index.ts (the published `testing` entry) must not re-export it.
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { withPortal } from './portal-fixture.js'

/** A test with a fresh output directory, removed after the test, beside the `portal` fixture. */
export const withDir = withPortal.extend<{ dir: string }>({
  dir: async ({}, use) => {
    const dir = await mkdtemp(path.join(tmpdir(), 'sqd-test-'))
    await use(dir)
    await rm(dir, { recursive: true, force: true })
  },
})
