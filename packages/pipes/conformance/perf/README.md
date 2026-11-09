# Head-following and fork benchmark (CT-6, S2/S3)

Can a pipe follow a fast chain, and what does a fork cost it? A live chain produces blocks at a
fixed rate and forks on schedule. The pipe follows it into a real store. The bench measures the
head lag, the time each fork takes to resolve (SLI-4) and the target's share of it, and sweeps the
block rate to find where the pipe stops keeping up.

## Running

From `packages/pipes`:

```
node --import tsx conformance/perf/run.ts --sink postgres --rates 25,100,500 --fork-every 20 --rows 10
conformance/perf/sweep.sh            # the full sweep; JSON per run in $OUT (default ./perf-results)
```

Postgres and ClickHouse come from the same environment variables as the conformance suites
(`TEST_POSTGRES_DSN`, `TEST_CLICKHOUSE_URL`, ...). Each point makes a schema or database of its own
and drops it. Run the sweep on a quiet machine: one point is one process, and the numbers are only
as stable as the host.

| Option              | Default      | Meaning                                                                                    |
| ------------------- | ------------ | ------------------------------------------------------------------------------------------ |
| `--sink`            | `memory`     | `memory` (the pipe with no store: a fork-aware in-memory target), `postgres`, `clickhouse` |
| `--rates`           | `1,4,…,1000` | block rates to sweep, blocks/s                                                             |
| `--seconds`         | `20`         | chain time per point                                                                       |
| `--fork-every`      | `0`          | a fork every N produced blocks; 0 for none                                                 |
| `--fork-depth`      | `3`          | blocks a fork replaces below the head; below `--finality`                                  |
| `--finality`        | `32`         | latest − finalized                                                                         |
| `--rows`            | `1`          | transfers in a matching block: the rows a rollback has to remove                           |
| `--match`           | `0.3`        | fraction of blocks with transfers                                                          |
| `--previous-blocks` | `10`         | canonical blocks a 409 carries                                                             |
| `--hold`            | `5000`       | how long the portal holds a request at the head before a 204                               |
| `--rtt`             | `0`          | round-trip time between the pipe and the portal                                            |
| `--db-rtt`          | `0`          | round-trip time between the target and its store                                           |
| `--profiler`        | off          | run with the profiler on, as a pipe does outside `NODE_ENV=production`                     |
| `--stop-on-fail`    | off          | stop the sweep at the first rate the pipe does not keep up with                            |
| `--json`            |              | write every point's full result, per-rollback timeline included                            |

## What runs

- `portal-process.ts`: the portal, in a child process so that serving blocks takes no CPU from the
  pipe. It is the mock portal of `~/testing` driven by a clock: it appends blocks at the rate, forks
  by replacing the last `--fork-depth` blocks and adding the new head, and holds a request at the
  head until its block exists, as the real portal does. `--rtt` puts a TCP proxy in front of it.
- `point.ts`: one point of a sweep, in a process of its own. It opens the store (behind a proxy with
  `--db-rtt`), runs the pipe from `--fork-depth` blocks below the start head to the run's last block, and prints the result.
- `sinks.ts`: the pipe (WETH transfers into one table, keyed by block and log index) and the three
  targets. A wrapper times them without knowing the store: every target commits a batch before it
  asks for the next, so the request for the next batch marks the previous one committed.
- `metrics.ts`, `run.ts`: the summary of a point, and the sweep with its table.

## Reading the table

| Column          | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                 |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| keeps up        | `yes`: the pipe committed at least 5 produced blocks while the chain ran, was never silent longer than 2 s or 20 block intervals, its lag holds (slope below 20 ms/s), every fork resolved, and it commits in the first half of the unfinalized window. `at finality`: all that, but the lag holds only because the pipe fell behind until the blocks it reads are finalized and come in one batch. `no`: anything else |
| lag ms          | block produced → the batch holding it committed, while the chain ran and after its first tenth; the drain after it is `drain ms`                                                                                                                                                                                                                                                                                        |
| lag blocks      | the p50 lag in blocks at the chain's rate                                                                                                                                                                                                                                                                                                                                                                               |
| slope ms/s      | how fast the lag grows; a pipe that cannot keep up gains `1000·(1 − capacity/rate)`                                                                                                                                                                                                                                                                                                                                     |
| drain ms        | last block produced → committed                                                                                                                                                                                                                                                                                                                                                                                         |
| blocks/commit   | blocks per batch: the pipe batches more as the rate rises                                                                                                                                                                                                                                                                                                                                                               |
| commit ms       | the target's time on one batch                                                                                                                                                                                                                                                                                                                                                                                          |
| rollbacks/forks | 409s the pipe handled / forks the chain made; one 409 covers every fork since the last request                                                                                                                                                                                                                                                                                                                          |
| fork resolve    | fork produced → a block at or above its new head committed on its branch (SLI-4)                                                                                                                                                                                                                                                                                                                                        |
| detect          | fork produced → its 409 reached the target; the 409 names its fork by the branch of the canonical parent it carries                                                                                                                                                                                                                                                                                                     |
| rollback        | `resolveFork`: finding the ancestor and rolling the store back                                                                                                                                                                                                                                                                                                                                                          |
| resume          | rollback done → the next commit: the source re-reads the range from the ancestor                                                                                                                                                                                                                                                                                                                                        |
| responses       | stream responses by status                                                                                                                                                                                                                                                                                                                                                                                              |

## Results, 2026-11-09

A 96-core Linux host, with Postgres 18 and ClickHouse 25.10 in Docker on the same host. Rows 10,
match 0.3, finality 32, fork depth 3, a fork every 20 blocks, 20 s per point, unless the row
says otherwise. Times are p50 in ms. "Rollback" is the target's `resolveFork`; "resolve" is the
whole fork, produced → committed on the new branch.

**The pipe with no store** (memory sink) keeps up to 10,000 blocks/s with a fork every 20 blocks:
lag 1 ms, fork resolved in 1 ms. The SDK core is not the limit.

**Store on loopback.** Both targets keep up to 2,000 blocks/s.

| Sink       | Rollback | Resolve | Commit |
| ---------- | -------- | ------- | ------ |
| postgres   | 4–11     | 7–19    | 0–1    |
| clickhouse | 53–59    | 35–76   | 4–8    |

**Store at a distance.** The store's round trip decides the limit, through the commit path at the
head more than through forks. Postgres commits every unfinalized block in a transaction of its own,
about 6 round trips; ClickHouse commits a batch in 2.

| Sink, DB RTT      | Commit | Rollback | Highest rate that keeps up (`yes`)    |
| ----------------- | ------ | -------- | ------------------------------------- |
| postgres, 2 ms    | 12–15  | 27–30    | 100 blocks/s                          |
| postgres, 10 ms   | 54–64  | 106–111  | 25 blocks/s                           |
| clickhouse, 2 ms  | 7–12   | 74–82    | 500 blocks/s, the highest tried       |
| clickhouse, 10 ms | 15–30  | 143–156  | 200 blocks/s; 500 with finality 2,000 |

Above its rate, Postgres falls behind until the blocks it reads are finalized: with finality 32 its
lag stops at about the finality window. With finality 2,000 at a DB RTT of 10 ms it falls behind
for good at 50 blocks/s (lag +34 ms per second), and the forks of the run all land above its
commit head, so it rolls back none of them. ClickHouse keeps a 60 ms lag at 500 blocks/s there.

A rollback costs about 10–12 store round trips for either target. Depth (1 to 30 blocks) does not
change it. Rows per block change it only for Postgres at 1,000 rows (32 ms on loopback).
