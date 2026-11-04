import path from 'node:path'

import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    watch: false,
    // vitest 3 ran this suite in a single non-isolated fork (`singleFork`); vitest 4 spells it:
    maxWorkers: 1,
    isolate: false,
    testTimeout: 30_000,
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
