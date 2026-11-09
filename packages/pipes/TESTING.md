# Test placement and overlap

Choose a test's location from the behavior it proves, the boundary it exercises,
and how it obtains the expected result. A filename, a database connection, or a
`CT-*` label alone does not decide its role.

## Where a test belongs

| Role | Location | Expected result and examples |
|---|---|---|
| Component or adapter test | Beside the implementation in `src/` | Explicit examples of that component's behavior: query construction, a decoder filter, a rollback SQL statement, a schema mapping, or the arguments of a repair hook. It may use mocks, a real database, or the whole pipe when that is needed to exercise the adapter. |
| Pipeline conformance scenario | `conformance/<target>/` or a shared scenario suite | Observable pipeline guarantees over a controlled history: rows, cursor, finalized prefix, restart, fork, or process exit. Use the shared simulator and an independent expected result, with the existing oracle and store probes where applicable. |
| Test-support test | Beside its helper in `src/testing/` or `conformance/` | Prove that the simulator, oracle, validator, probe, or kill switch detects the errors it claims to detect. These are tests of the measuring tools, not evidence that the pipeline passes the corresponding contract. |
| Type assertion | Beside the API it checks | Check inferred types and invalid API use with the TypeScript compiler. A successful Vitest runtime invocation alone is not a type check. |

Unit/integration/end-to-end describe the execution boundary. Conformance describes
what is being proved. These are separate dimensions: an adapter integration test
can provide evidence for a specification requirement without belonging to the
shared conformance harness. The `CT-*` categories in [spec/13](../../spec/13-conformance-tdd.md)
classify checks, not exclusive directories.

New tests of general pipeline guarantees should extend the existing conformance
scenarios instead of building another full-pipeline fixture in a target suite.
A scenario may apply only to one durability class; it need not work on every sink.
Keep target-specific schema, protocol, callback, and client behavior beside the
target. Existing mixed suites need not be moved wholesale to follow this rule.

## Example: a Postgres fork

- `src/targets/drizzle/node-postgres/rollback.test.ts` checks the rollback SQL and
  column handling.
- `src/targets/drizzle/node-postgres/drizzle-target.test.ts` checks that updates,
  deletes, foreign keys, and generated columns restore correctly through the
  target. These conditions are not covered by an insert-only S3 scenario.
- `conformance/postgres/fork*.test.ts` checks the pipeline's rows and cursor against
  its history, including what a real process kill leaves and what a restart does.
- `conformance/postgres/probe.test.ts` checks that the probe reads a seeded store
  correctly. It does not prove that the target writes or recovers that state.

All four can mention rollback and all four can catch the same regression. That
alone does not make them redundant.

## When two tests are redundant

Compare all of the following, including fixture setup and imported helpers:

1. The implementation or binding under test.
2. Input data, configuration, persisted starting state, and event sequence.
3. The execution boundary and failure mechanism: direct call, full pipe, thrown
   exception, actual process kill, clean restart, or restart during recovery.
4. The source of the expected answer and the observations checked.
5. Any additional condition, such as batching, absent finality, row updates,
   cleanup timing, or callback ordering.

A test can be removed when another retained test exercises those conditions and
contains all its meaningful assertions. A parameterized case counts as a test for
this comparison. Equal titles, equal bodies with different fixtures, and equal
line coverage are not sufficient evidence.

Keep the same guarantee on different sinks: their commit and recovery mechanisms
can fail independently. Keep targeted component checks when they exercise cases
or diagnostics absent from the shared scenario. Keep a real-kill recovery test
when a similar target test only throws an exception: stack unwinding changes the
state left behind.

For expensive scenarios with the same setup, prefer one run with the necessary
observations over several runs with identical setup. For example, `runToEnd`
already checks the complete end state and validators. A no-op-restart test that
calls it before and after restarting also proves the initial successful run.
Retain any additional assertion before removing a separate baseline test.

## Extending a matrix

Name which file owns the original scenario and what the new matrix adds.
The default S3 fork belongs to each target's `fork.test.ts`; `fork-depth.test.ts`
adds other depths/response partitions and crashes at the finality boundary. It
omits the original `(S3.ancestor, S3.blocksPerResponse)` pair. Pub/Sub's original
fork also checks compensation ordering, so those assertions stay there.

Expected rows and the terminal cursor must come from the scenario, not from the
rows or cursor the pipe happened to produce. An oracle that only follows the
pipe can agree with a run that stops early; the fixed end-state check must remain.
Instrumentation may observe commit steps, but should not use the production
resolver or recovery implementation to compute its expected answer.

## Reviewing a test change

- State the new condition and assertion; identify the closest existing test.
- Reuse `src/testing/` and the conformance fixtures before introducing another mock
  portal, bus, database fixture, or process runner.
- If consolidating, name the surviving test and account for every removed
  assertion, including checks hidden in helpers. Preserve type assertions too.
- Record applicable specification coverage regardless of the test's directory.
  Do not mark a requirement covered just because its helper or validator is tested.
- Keep changes to scenarios separate from claims that all of a test class or
  implementation phase is complete.

## Running checks

From `packages/pipes`:

```sh
pnpm vitest run src/targets/drizzle/node-postgres/rollback.test.ts
pnpm vitest run conformance/postgres
pnpm test
```

[Conformance setup](conformance/README.md#running) describes each backend's test
requirements. [The test workflow](../../.github/workflows/test.yml) invokes
`pnpm test`, so conformance scenarios and helper tests are included automatically.
Project-dependent suites may skip; report skips separately from passes.

During a review, use CI results for the current head before running anything
locally. Run a targeted local check for a changed or suspected behavior when
needed; do not rebuild or rerun a green suite only to duplicate CI. Use CI for
runtime observations rather than adding a separate timing campaign.
