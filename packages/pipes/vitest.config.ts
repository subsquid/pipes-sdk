import path from 'node:path'

import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    watch: false,
    // vitest 4 no longer excludes `dist` by default, and the build emits compiled tests there
    exclude: ['**/node_modules/**', '**/dist/**'],
    // conformance suites share global state ids (e.g. the `ct-s1` advisory lock), so files must
    // stay serialized in one fork: the vitest 3 `singleFork` equivalent is `maxWorkers: 1`
    maxWorkers: 1,
    isolate: false,
    maxConcurrency: 2,
    fileParallelism: true,
    testTimeout: 20_000,
    teardownTimeout: 1_000,
    slowTestThreshold: 10_000,
    hideSkippedTests: false,
    expandSnapshotDiff: true,
    bail: 1,
    coverage: {
      enabled: false,
      provider: 'v8',
      reportsDirectory: './coverage',
      include: ['src'],
      exclude: ['src/tests'],
    },
    pool: 'forks',
  },
  resolve: {
    alias: {
      '~': path.resolve(__dirname, './src'),
    },
  },
})
