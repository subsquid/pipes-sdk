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
clickhouse/   class A: the ClickHouse sink, with and without the author's repair hook
```

Each store folder holds the same files:

| File | What it is |
|---|---|
| `pipe.ts` | the ERC-20 pipe into this store |
| `probe.ts` | reads the store back: state, rows, residue |
| `kill-switch.ts` | stops the child at one step of this store's commit protocol: a file operation for Parquet, an SQL statement for Postgres, an HTTP request for ClickHouse |
| `killed-run.ts` | the child process: the pipe with the kill switch armed |
| `harness.ts` | fixtures, and `runToEnd`, `runKilled`, `crashAt`, `resumeToEnd` |
| `<scenario>.test.ts` | one uninterrupted run (CT-1) |
| `<scenario>-crash.test.ts` | the run killed at each commit step, then restarted (CT-2) |

## What every run checks

The harness checks the scenario's end state on every run, so no test can leave it out:

- `runToEnd` fails unless the store and the oracle both end in exactly the scenario's end state, and the store passes every structural validator;
- `crashAt` fails unless the crash left a commit point intact (CN-40);
- `resumeToEnd` fails unless the restart starts right after the recovered cursor and then passes `runToEnd`.

The end state comes from the scenario, not from the pipe. An oracle that follows the pipe agrees with a pipe that stops early.

## Where a new test goes

If the test can be stated without naming a store, it belongs here: "after a crash at step X the store holds a commit point, and the restart ends in the scenario's end state". If it is about one sink's mechanism ("recovery deletes the rows in [low, high]"), it belongs next to that sink in `src/targets/`.

A new store gets a folder of its own with the files above, and runs the existing scenarios.

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

The ClickHouse tests need a server too: `TEST_CLICKHOUSE_URL`, by default `http://localhost:10123`, with `TEST_CLICKHOUSE_USERNAME` and `TEST_CLICKHOUSE_PASSWORD` (both `default`). Each test makes a database of its own and drops it. Merges are stopped on its tables, and the pipe's client inserts with `optimize_on_insert` off, so a row the pipe stored twice reads twice, whether it came in two inserts or in one.

## What a second implementation can reuse

The mock portal speaks HTTP, and the probes read the stores in the formats of spec/14, so `oracle/`, `scenarios/` and the probes do not depend on TypeScript. Three parts do: building the pipe (`pipe.ts`), the journal of batches and forks (an in-process hook), and the kill switches, which patch the file system, the `pg` client and the ClickHouse client's HTTP connection in the child process.
