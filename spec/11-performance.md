# 11 — Performance (PF-n, SLI-n, HZ-n)

## SLI definitions (black-box measurable)

- **SLI-1 — Backfill throughput**: blocks committed per second, measured at the sink
  cursor over a steady 5-minute window.
- **SLI-2 — Ingest bandwidth**: portal bytes consumed per second (OB-11 counter delta).
- **SLI-3 — Batch commit latency**: time from batch emission to commit-point
  durability, per batch; report p50/p99.
- **SLI-4 — Fork resolution time**: fork signal → streaming resumed (LIV-4 witness).
- **SLI-5 — Recovery time**: process start → first new commit after a crash (LIV-5
  witness).
- **SLI-6 — Stall time**: longest zero-progress interval without an OB-13 signal
  (must be 0 by LIV-2; the SLI measures signalled stall duration).
- **SLI-7 — Peak resident memory** over a scenario run.

## SLO table

No baselines are recorded in the repository today (no committed benchmark results);
the Baseline column is honestly empty and MUST be filled from the first CT-6 run on
the reference implementation. All targets ⚠ provisional pending ADR-14.

| SLI | Scenario | Target | Known baseline |
|---|---|---|---|
| SLI-1 | S1 steady backfill | ⚠ P-SLO-BACKFILL-BPS | — none recorded |
| SLI-3 p99 | S1 | ⚠ P-SLO-COMMIT-P99-MS | — |
| SLI-4 | S3 fork storm | ⚠ P-FORK-RESOLVE-S | — |
| SLI-5 | S5 cold start | ⚠ P-STARTUP-S | — |
| SLI-6 | S1–S6 | 0 (unsignalled) | — |
| SLI-7 | S1, S6 | ⚠ derived bound (PF-1) | — |

## Resource-bound requirements

**PF-1 — Derivable memory ceiling.** [MUST] Peak memory ≤ assembly bound
(P-STREAM-MAX-BYTES + one chunk) + sink buffer (class-configured:
P-PQ-ROW-GROUP-ROWS, P-BQ-APPEND-MAX-BYTES, one batch for T/A) + constant overhead.
Finalized-only delivery waits upstream and adds no finality-depth-dependent in-process
term. No hidden unbounded term.

**PF-2 — End-to-end backpressure.** [MUST] = WP-12; under a saturated sink, portal
consumption throttles to sink throughput with bounded buffering.

**PF-3 — Maintenance budget.** [MUST] Retention cleanup runs at most once per cleanup
period (per binding: P-CH-CLEANUP-PERIOD, P-BQ-CLEANUP-PERIOD) and completes without
blocking the write path beyond one commit's latency. Two-sided: it must also run often
enough for LIV-9.

**PF-4 — Startup work scheduling.** [MUST] Recovery cost scales with crash residue
(one batch range, open units), never with total history (LIV-5).

**PF-5 — Observability overhead.** [SHOULD] The observability surface (scrapes,
preview retention P-PREVIEW-HISTORY) adds bounded, scrape-independent overhead;
profiling data retention is bounded per batch.

**PF-6 — Ingest/processing overlap.** [MUST] Transport consumption of batch N+1
proceeds concurrently with downstream processing of batch N (the single-slot lookahead
of WP-12): with per-batch network time L and processing time P, steady-state batch
cadence approaches max(L, P), never L+P. Memory cost is bounded by one extra assembling
batch (P-STREAM-MAX-BYTES). A sequential fetch-then-process implementation is
non-conforming even if it satisfies every ordering property.

## Workload model

| W-param | Meaning | Reference value |
|---|---|---|
| W-BLOCK-RATE | blocks/s offered by simulator | chain-profile dependent |
| W-BLOCK-SIZE | mean bytes per block | 10 KiB |
| W-MATCH-RATE | fraction of blocks with matching records | 0.3 |
| W-TABLES | sink tables written | 4 |
| W-SPARSITY | fraction of windows with zero rows for a table | 0.5 |
| W-FORK-DEPTH | blocks rolled back per fork | 1–finality depth |
| W-FORK-RATE | forks per hour (S3) | 60 |
| W-FINALITY-DEPTH | latest − finalized distance | 32 |

Reference scenarios: **S1** steady backfill (deep history, healthy portal) ·
**S2** head following (real-time, W-FINALITY-DEPTH lag) · **S3** fork storm (S2 +
W-FORK-RATE forks at varying depth) · **S4** sparse-table soak (class K, W-SPARSITY
high, hours) · **S5** cold start (large history, crash residue present) ·
**S6** slow-sink saturation (sink throttled below portal rate).

## Hazard register

- **HZ-1 — Batch overshoot.** Single-slot assembly can exceed P-STREAM-MAX-BYTES by one
  transport chunk; a pathological block inflates peak memory. Byte accounting counts
  UTF-16 code units of raw lines while the buffer holds parsed objects — the tracked
  figure is a proxy, not resident bytes. *Threatens:* PF-1.
  *Probe:* S1 with oversized-block injection (FM-17).
- **HZ-2 — Profiling retention.** Per-stage profiling retains references to stage
  outputs for the span's life; with profiling on, memory scales with batch size × stage
  count. *Threatens:* PF-1. *Probe:* S1 with profiling enabled vs off.
- **HZ-3 — Retention-cleanup churn.** Bookkeeping cleanup via append-mechanisms
  (cancel-row engines) doubles write volume during cleanup bursts. *Threatens:* SLI-3.
  *Probe:* S1 with P-CH-CLEANUP-PERIOD=1.
- **HZ-4 — Byte-only rollover stall.** Class K with only a byte trigger and a slow tail
  stalls cursor advancement (LIV-8). *Probe:* S4 with time/block triggers disabled.
- **HZ-5 — Head-poll tight loop.** P-HEAD-POLL-MS=0 at head degenerates to
  request-per-response against the portal. *Threatens:* SLI-2 efficiency, portal quota.
  *Probe:* S2 measuring request rate.
- **HZ-6 — Unbounded cache growth.** RS-24; disk exhaustion over long soaks.
  *Threatens:* LIV-9 environment. *Probe:* S4 with cache on, disk quota.
- **HZ-7 — Undo-log write amplification.** Class T snapshot triggers add a write per
  unfinalized-block row mutation. *Threatens:* SLI-1 near head. *Probe:* S2 vs S1
  throughput delta.
- **HZ-8 — Deep-rollback scan.** Fork depth near retention bound forces large ranged
  deletes/scans; unpartitioned stores degrade severely (the binding mandates
  partition/index guards). *Threatens:* SLI-4. *Probe:* S3 at max W-FORK-DEPTH.

## Benchmarking requirements

CT-6 MUST: baseline every SLI per scenario on the reference implementation before
optimization work; characterize the saturation knee (offered load vs SLI-1/SLI-3);
include overload phases (S6) and recovery phases (S5) — not only steady state; publish
results into the Baseline column and gate regressions against them.
