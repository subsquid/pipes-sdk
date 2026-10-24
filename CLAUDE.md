# Project: @subsquid/pipes SDK

## Key conventions

- Package manager: **pnpm** (v10)
- Test runner: **vitest** (run from `packages/pipes/`)
- Linter/formatter: **Biome**
- Build: **tsup** via **turbo**

## Testing

- Tests sit next to the source (`feature.ts` → `feature.test.ts`) and use `~/testing/`, never ad-hoc HTTP mocks: `mockPortal`, `readAll`, `testLogger`; `mockBlock`, `encodeEvent`, `resetMockBlockCounter` from `~/testing/evm/`; `stubPortal` from `~/testing/stub-portal.js` for the `Portal` interface
- `mockPortal({ blocks, latest, finalized })` answers from the chain it holds; the list form `mockPortal(responses)` is only for an exact response sequence. `mockEvmPortalStream` and `finalizedMockPortal` are deprecated
- Every resource a test opens is a vitest `test.extend` fixture released when the test ends, never a `let` + `afterEach` or `try/finally`; move an existing test over when you change it. A fixture several files need lives once in a test-only module (mock portals: `withPortal` from `~/testing/portal-fixture.js`), and such a module is never re-exported from `~/testing/index.ts`, because it imports vitest
- Run from `packages/pipes/`: `pnpm vitest run <file>`; `--exclude` the tests that need ClickHouse or Postgres when they are not running

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
