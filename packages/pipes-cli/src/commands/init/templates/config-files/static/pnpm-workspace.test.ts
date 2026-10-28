import { expect, it } from 'vitest'

import { pnpmWorkspaceTemplate } from './pnpm-workspace.js'

it('allows the same build scripts for pnpm 10 and pnpm 11', () => {
  expect(pnpmWorkspaceTemplate).toMatchInlineSnapshot(`
    "onlyBuiltDependencies:
      - better-sqlite3
      - bufferutil
      - es5-ext
      - esbuild
      - keccak
      - utf-8-validate
    allowBuilds:
      better-sqlite3: true
      bufferutil: true
      es5-ext: true
      esbuild: true
      keccak: true
      utf-8-validate: true
    "
  `)
})
