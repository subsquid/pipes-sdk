const builtDependencies = ['better-sqlite3', 'bufferutil', 'es5-ext', 'esbuild', 'keccak', 'utf-8-validate']

// pnpm 10 reads onlyBuiltDependencies; pnpm 11 ignores it, reads allowBuilds, and fails the install on any build it skips
export const pnpmWorkspaceTemplate = `onlyBuiltDependencies:
${builtDependencies.map((name) => `  - ${name}`).join('\n')}
allowBuilds:
${builtDependencies.map((name) => `  ${name}: true`).join('\n')}
`
