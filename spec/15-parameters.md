# 15 — Parameter registry — MUTABLE

Last updated: 2026-07-19. Observed = current reference-implementation value.
Target = ratified contract value; ⚠ = proposed, pending ADR-14.

## Stream assembly & transport

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-STREAM-MAX-BYTES | batch assembly byte budget, counted in UTF-16 code units of raw lines (WP-11, PF-1, HZ-1) | 10 MiB | same |
| P-STREAM-MAX-IDLE-MS | assembly idle flush (WP-11) | 300 ms | same |
| P-STREAM-MAX-WAIT-MS | assembly total-wait flush (WP-11, LIV-3) | 5000 ms | same |
| P-HEAD-POLL-MS | re-poll delay at head (WP-13, LIV-3, HZ-5) | 0 ms | ⚠ review (HZ-5) |
| P-HTTP-TIMEOUT-MS | whole-request timeout (WP-14, LIV §0) | 20000 ms | same |
| P-BODY-TIMEOUT-MS | inter-chunk stall timeout (IB-51) | unset (∞) | ⚠ propose finite |
| P-RETRY-SCHEDULE-MS | backoff schedule (WP-14, IB-7) | 10, 100, 500, 2000, 10000, 20000 | same |
| P-RETRY-STATUS-SET | retryable statuses (WP-14, IB-7) | 429, 502, 503, 504, 521–524 | ⚠ + 529 (in-flight fix) |
| P-STREAM-RETRY-LIMIT | streaming retry cap (WP-14, LIV-7; ADR-10) | unbounded (non-streaming default: 0) | same |

## Multi-source fallback

Detection thresholds (16). All configurable per pipe; observed values are the defaults.

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| P-FB-MAX-STALENESS-MS | unproductive-wait window defining the `stale` verdict (WP-64, DEF-69, LIV-60) | 180000 ms | same |
| P-FB-MAX-LAG-BLOCKS | distance behind the independent head defining `lagging`, and the reclaim allowance (WP-63, DEF-67, ADR-26) | 10 blocks | same |
| P-FB-TICK-MS | stall re-check interval while a request is outstanding (WP-64) | 1000 ms | same |
| P-FB-COOLDOWN-MS | how long an unhealthy source waits before returning to `unknown` (WP-67) | 30000 ms | same |
| P-FB-LIVENESS-FAIL | consecutive liveness failures that flip a source unhealthy (WP-67) | 2 | same |
| P-FB-LIVENESS-RECOVER | consecutive liveness passes required for `healthy` (WP-67) | 3 | same |
| P-FB-PROBE-INTERVAL-MS | minimum gap between capability probes of one standby (WP-65) | 5000 ms | same |
| P-FB-PROBE-TIMEOUT-MS | capability probe time-box (WP-65) | 30000 ms | same |
| P-FB-HEAD-TTL-MS | head-poll cache lifetime (WP-66) | 5000 ms | same |
| P-FB-HEAD-TIMEOUT-MS | head-poll time-box, including the aggregate lookup (WP-66, FM-64) | 500 ms | same |
| P-FB-ALLDOWN-POLL-MS | gap between re-selections while every source is unhealthy (WP-60, LIV-64) | 1000 ms | same |
| P-FB-ALLDOWN-TIMEOUT-MS | how long an all-down gap is tolerated before the read fails (LIV-64) | unbounded | same |

## Sink bindings

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-CH-CURSOR-RETENTION | class-A cursor rows kept per key (WP-47, RP-6) | 10 000 rows | same |
| P-CH-CLEANUP-PERIOD | cleanup cadence in commits (PF-3, LIV-9, HZ-3) | 25 (also fires on the first save; every save when retention < 25) | same |
| P-PG-UNFINALIZED-RETENTION | class-T undo-log depth in blocks (WP-47, IB-21) | 1000 | same |
| P-PG-TX-RETRY | class-T transaction retry attempts (FM-21) | 3 | same |
| P-PG-TX-RETRY-PAUSE-MS | class-T retry pause | 100 ms | same |
| P-PQ-FILE-MAX-BYTES | class-K unit rollover byte trigger (LIV-8, HZ-4) | 128 MiB | same |
| P-PQ-ROW-GROUP-ROWS | class-K writer memory bound (PF-1) | 100 000 rows | same |
| P-BQ-APPEND-MAX-BYTES | class-W append request cap (IB-23, PF-1) | 16 MiB | same |
| P-BQ-RETRY-LIMIT | class-W transient retry attempts (FM-20) | 8 | same |
| P-BQ-RETRY-BASE-MS | class-W backoff base (exponential) | 250 ms | same |
| P-BQ-CURSOR-RETENTION | class-W WAL rows kept per key (WP-47, RP-6) | 10 000 rows | same |
| P-BQ-CLEANUP-PERIOD | class-W WAL cleanup cadence in commits (PF-3) | 25 (also fires on the first save) | same |

## Engine & observability

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-PROGRESS-INTERVAL-MS | progress sampling/log cadence (OB-4) | 5000 ms | same |
| P-RUNNER-RETRY | dev-runner restart attempts (GAP-32: explicit `retry: 0` currently coerced to the default) | 5 | same |
| P-METRICS-PORT | observability server port (IB-40) | 9090 | ⚠ review (collides with conventional scraper port) |
| P-PREVIEW-HISTORY | profiler/preview snapshots retained (OB-22, IB-43, PF-5) | 50 | same |
| P-PREVIEW-ARRAY-LIMIT | preview array truncation (IB-44, OB-23) | 10 (hardcoded literal in the reference) | same |

## Liveness & SLO targets (all ⚠ proposed — no recorded baselines; ADR-14)

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-STALL-BUDGET-S | max unsignalled zero-progress (LIV-1, LIV-2, SLI-6) | not measured | ⚠ 60 s |
| P-FORK-RESOLVE-S | fork signal → resumed streaming (LIV-4, SLI-4) | not measured | ⚠ 30 s |
| P-STARTUP-S | start → first commit post-crash (LIV-5, SLI-5) | not measured | ⚠ 60 s |
| P-SHUTDOWN-DRAIN-S | stop request → clean exit (LIV-6) | not measured | ⚠ 10 s |
| P-SLO-BACKFILL-BPS | S1 backfill throughput floor (SLI-1) | not measured | ⚠ set from first CT-6 baseline |
| P-SLO-COMMIT-P99-MS | S1 commit latency p99 (SLI-3) | not measured | ⚠ set from first CT-6 baseline |
