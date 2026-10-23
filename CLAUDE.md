# Project: @subsquid/pipes SDK

## Key conventions

- Package manager: **pnpm** (v10)
- Test runner: **vitest** (run from `packages/pipes/`)
- Linter/formatter: **Biome**
- Build: **tsup** via **turbo**

## Testing

- Tests live next to source files: `feature.ts` → `feature.test.ts`
- Always use the internal testing framework from `~/testing/` — do NOT create ad-hoc HTTP mocks
- EVM test helpers: `mockBlock`, `encodeEvent`, `resetMockBlockCounter` from `~/testing/evm/`
- General helpers: `mockPortal`, `readAll`, `testLogger` from `~/testing/`
- Mock a portal with `mockPortal({ blocks, latest, finalized, ... })`: it answers queries from the chain it holds. Use the list form `mockPortal(responses, { finalized? })` only for an exact response sequence. Do not use the deprecated `mockEvmPortalStream` or `finalizedMockPortal`
- Stub the `Portal` interface (head + timestamp lookup) with `stubPortal` from `~/testing/stub-portal.js`
- Create mock portals through a vitest `test.extend` fixture that closes them after the test, not in a `let` with `afterEach` or `try/finally`. Write new tests this way, and move an existing test over when you change it:
  ```ts
  const it = base.extend<{ portal: (options: MockPortalOptions) => Promise<MockChainPortal> }>({
    portal: async ({}, use) => {
      const opened: MockChainPortal[] = []
      await use(async (options) => {
        const portal = await mockPortal(options)
        opened.push(portal)

        return portal
      })
      await Promise.all(opened.map((portal) => portal.close()))
    },
  })
  ```
- Run tests: `pnpm vitest run src/path/to/file.test.ts` (from `packages/pipes/`)
- Some tests require external services (ClickHouse, PostgreSQL) — exclude with `--exclude` flags when running locally

## PR workflow

Before creating any PR, the agent MUST follow the `pr-quality-gate` skill:
1. Verify new/changed code has test coverage
2. Generate a coverage diff (base vs head) and include it in the PR body
3. Use the internal testing framework for all new tests

## Code style

### Grouping and blank lines

Group code into logical sections with blank lines between them.
Prefer to add a blank line before return statements / breaks/ continues for readability


### Commits

- Make commits messages descriptive of the change, not the implementation details. Focus on the "what" and "why", not the "how".
- Size of commits should be manageable for review — 500 characters max
