# 12 — Observability (OB-n)

Required signals. Concrete metric names, routes, and payload schemas: IB-40…IB-46.
Bands: 1–9 progress, 10–19 pressure & errors, 20–29 lifecycle, 30–39 alarm states.

## Progress

- **OB-1 — Processed-block gauge.** Current committed cursor number; equals `C`
  (INV-30). Sentinel "not started" is distinguishable from block 0 — satisfied by the
  /metrics −1 sentinel (IB-46); the /stats binding reads 0 pre-first-batch (IB-41).
- **OB-2 — Progress heartbeat.** A signal that distinguishes *idle-input* (at head,
  nothing to do) from *stalled-service* (work pending, no progress): the pair ⟨OB-1
  static, OB-6 static⟩ = idle; ⟨OB-1 static, OB-6 advancing⟩ = stalling.
- **OB-3 — End-block gauge.** Effective range end (`min(configured to, head)`) — the
  denominator of progress; a semantic gauge, not the raw chain head.
- **OB-4 — Progress ratio & ETA.** Derived ⟨percent, ETA⟩ from windowed throughput;
  ETA reads 0 only when synced *(the /stats binding also reads 0 pre-first-batch —
  IB-41)*.
- **OB-5 — Throughput.** Blocks/s and bytes/s over a sliding window; plus cumulative
  counters (blocks processed, bytes downloaded) for rate computation by scrapers.
- **OB-6 — Head gauge.** The portal-reported head (latest; finalized where available)
  as last observed. *(Currently neither bound in IB nor exported — GAP-28.)*

## Pressure & errors

- **OB-10 — Batch-size distributions.** Histograms of blocks/batch and bytes/batch —
  the observable of assembly behavior (WP-11).
- **OB-11 — Transfer counters.** Cumulative portal requests by ⟨classification:
  success | rate-limited | error⟩ and bytes downloaded.
- **OB-12 — Retry accounting.** Rate-limited and failed request counts are separable
  (error-budget accounting for FM-18).
- **OB-13 — Stall/retry signal.** While any retry loop is active, an observable,
  reason-coded signal is continuously present (LIV-7 witness); it clears on success.
  A global write halt (sink refusing) is directly observable, not inferred.
  *(Currently only a log warning after a fixed stall delay, neither bound in IB nor
  reason-coded — GAP-28.)*
- **OB-14 — Fork counter.** Increments exactly once per resolved fork (T-FORK);
  reason-coded fatal fork errors are distinguishable from resolved forks.

## Lifecycle & stage timing

- **OB-20 — Lifecycle timestamps.** Start, recovery-complete, first-batch, stop —
  observable with timestamps; readiness = recovery complete (LIV-5 witness).
  *(Currently neither bound in IB nor implemented — GAP-28.)*
- **OB-21 — Publication lag.** (Class K) The gap between processed cursor and
  durable/published cursor (LIV-8 witness). *(Currently neither bound in IB nor
  implemented — GAP-28.)*
- **OB-22 — Stage timing.** Per-batch, per-stage elapsed-time tree (ingest, decode,
  each transformer, commit) — the profiling surface; bounded retention
  (P-PREVIEW-HISTORY snapshots).
- **OB-23 — Data preview.** The last batch's per-stage output sample, size-bounded by
  the truncation rules (IB-44) — lets a dashboard show *what* the pipe produces
  without replaying it.

## Alarm states

- **OB-30 — Coded terminal errors.** A halted pipe exposes its terminal coded error
  (IB-50) as its final observable state — both as an edge event (log) and a level read
  (status). *(The level read is currently neither bound in IB nor implemented — GAP-28.)*
- **OB-31 — Cardinality bound.** Signals are labeled by pipe id plus fixed
  low-cardinality dimensions (`classification`/`status` on transfer counters, `table`
  on per-sink metrics); label cardinality is O(pipes × tables) at worst, never
  O(blocks) or O(distinct errors).
- **OB-32 — Capture-on-stall.** On LIV-2 violation or terminal error, the surface
  retains the last OB-22/OB-23 snapshot for post-mortem scraping. *(Currently
  unimplemented — the server closes on stop; GAP-28.)*

## Property → observable mapping

| Property | Decided by |
|---|---|
| LIV-1, LIV-2 | OB-1 + OB-2 + OB-13 |
| LIV-3 | OB-1 vs OB-6 |
| LIV-4 | OB-14 + OB-1 resumption |
| LIV-5 | OB-20 |
| LIV-6 | OB-20 (stop timestamp) |
| LIV-7 | OB-13 continuity |
| LIV-8 | OB-21 |
| LIV-9 | bookkeeping counts (store-level audit) |
| INV-30…INV-32 | scraper cross-check vs oracle state |

## The harness rule

Lying metrics are conformance failures: CT-1 runs a scraper alongside the oracle and
asserts OB-1/OB-3/OB-14 equal oracle state at quiescence. An implementation that
progresses correctly but reports wrongly fails conformance.

Non-interference: observers are read-only (trust model); a scraper at any rate MUST NOT
change pipeline behavior (FM-36).
