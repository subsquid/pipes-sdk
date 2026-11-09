# Conformance harness

These tests hold the pipe to the contract in [`spec/`](../../../spec/README.md), from the outside. A mock portal plays a scripted chain, the pipe streams it into a real store, and a reference model (the oracle) computes what the store must hold. The tests read the store directly and compare. The pipe's internals are never imported; it is built through the public API.

The test plan, the coverage matrix and the gap register are in [`spec/13-conformance-tdd.md`](../../../spec/13-conformance-tdd.md).

## Layout

```
oracle/       the reference model, the structural validators, the replay of the portal's log
scenarios/    the chains the tests play (spec/11) and the ERC-20 pipe every scenario streams
kill/         spawns a pipe in a child process that kills itself with SIGKILL
parquet/      class K: the Parquet sink
postgres/     class T: the Postgres sink
bigquery/     class W: the BigQuery sink, into a dataset of the test's own in a real project
clickhouse/   class A: the ClickHouse sink, with and without the author's repair hook
pubsub/       class C: the Pub/Sub sink, publishing to a stand-in bus in the test process
```

Store folders use the following files where the scenario applies:

| File | What it is |
|---|---|
| `pipe.ts` | the ERC-20 pipe into this store |
| `probe.ts` | reads the store back: state, rows, residue |
| `kill-switch.ts` | stops the child at one step of this store's commit protocol: a file operation for Parquet, an SQL statement for Postgres, a Storage Write append or a query job for BigQuery, an HTTP request for ClickHouse, a statement on the state file or a publish call for Pub/Sub |
| `killed-run.ts` | the child process: the pipe with the kill switch armed |
| `harness.ts` | fixtures, and `runToEnd`, `runKilled`, `crashAt`, `resumeToEnd` |
| `<scenario>.test.ts` | one uninterrupted run (CT-1) |
| `fork-depth.test.ts` | S3 at depths 1, 3 and 6 with different response partitions; crash/restart at the finality boundary |
| `fork-history.test.ts` | partial canonical windows with missing finality or cascading rollback (ClickHouse and BigQuery) |
| `<scenario>-crash.test.ts` | the run killed at each commit step, then restarted (CT-2) |

## What every run checks

The harness checks the scenario's end state on every run, so no test can leave it out:

- `runToEnd` fails unless the store and the oracle both end in exactly the scenario's end state, and the store passes every structural validator;
- `crashAt` fails unless the crash left a commit point intact (CN-40);
- `resumeToEnd` fails unless the restart starts right after the recovered cursor and then passes `runToEnd`.

The end state comes from the scenario, not from the pipe. An oracle that follows the pipe agrees with a pipe that stops early.

## Where a new test goes

Follow [Test placement and overlap](../TESTING.md). Pipeline scenarios belong here
when they check observable guarantees against a controlled history and an
independent expected result. Target-specific mechanisms and edge cases belong
beside the target, even when exercising them requires the whole pipe.

`oracle/*.test.ts`, `probe.test.ts`, `kill-switch.test.ts`, and `harness.test.ts`
test the measuring tools themselves. Their location does not make them evidence
that the pipeline passes the contract. A `CT-*` requirement may also have useful
coverage in a component or adapter suite.

The default S3 fork remains in `fork.test.ts`. Its depth/response-size combination
is omitted from `fork-depth.test.ts`, which adds the other combinations and
recovery at the finality boundary. Backfill restart tests use `runToEnd` before
and after the restart, so they also assert the initial uninterrupted end state.

`fork-history.test.ts` binds the shared missing-finalized and deep cascading-rollback
histories to ClickHouse and BigQuery. Both retain two rollback boundaries and
independent expected rows and state.

A new store gets a folder of its own with the files above, and runs the existing scenarios.

Pub/Sub cannot take a message back, so its store is two parts: the producer's state file and the bus. `bus.ts` is a Pub/Sub stand-in in the test process: it answers the Publisher calls of the Google client and keeps every message it acknowledged, so what a killed child published stays published. The probe reads both, and the rows are what a consumer holds once it has applied the bus and the outbox by version.

## Names

| Code | Meaning |
|---|---|
| CT-1, CT-2 | one uninterrupted run; a run killed at each commit step |
| class K, T, W, A, C | the durability classes (spec/06): Parquet, Postgres, BigQuery, ClickHouse, Pub/Sub |
| S1 (`backfill`) | a steady backfill over deep history |
| S3 (`fork`) | an unfinalized tail, a floor that rises once, and one fork below the head |

## Running

```
pnpm vitest run conformance                  # from packages/pipes
pnpm vitest run conformance/postgres         # one store
```

The Postgres tests need a server: `TEST_POSTGRES_DSN`, by default `postgresql://postgres:postgres@localhost:5432/postgres`. Each test makes a schema of its own and drops it.

The Pub/Sub tests need no server: the bus runs in the test process. Each test has a bus and a state file of its own.

The ClickHouse tests need a server too: `TEST_CLICKHOUSE_URL`, by default `http://localhost:10123`, with `TEST_CLICKHOUSE_USERNAME` and `TEST_CLICKHOUSE_PASSWORD` (both `default`). Each test makes a database of its own and drops it. Merges are stopped on its tables, and the pipe's client inserts with `optimize_on_insert` off, so a row the pipe stored twice reads twice, whether it came in two inserts or in one.

The BigQuery tests need a real project the tests may write to: `BIGQUERY_TEST_PROJECT`. Without it the whole class-W suite skips. Each test makes a dataset of its own (`ctw_*`) and drops it, tables included. Authentication is the Google client's own (`GOOGLE_APPLICATION_CREDENTIALS` or the ADC of the host); nothing project-related is committed to the repo. A DML job a killed child created runs on whatever the child does, so the parent waits for every job the child journaled before it reads the store. One point restarts at once and re-reads the store once the killed run's jobs finish; a controlled test runs the restart's recovery alone and then executes the killed run's rollback DELETE after it — by then the range is empty, so it must take nothing.

## What a second implementation can reuse

The mock portal speaks HTTP, the stand-in bus speaks the Pub/Sub Publisher protocol over gRPC (`bus.proto`), and the probes read the stores in the formats of spec/14, so `oracle/`, `scenarios/`, the bus and the probes do not depend on TypeScript. Three parts do: building the pipe (`pipe.ts`), the journal of batches and forks (an in-process hook), and the kill switches, which patch the file system, the `pg` client, the BigQuery client's job creation and Storage Write appends, the ClickHouse client's HTTP connection, and the SQLite statements and Pub/Sub client calls in the child process.
