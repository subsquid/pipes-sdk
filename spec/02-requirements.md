# 02 — Product requirements

Bands: 1–9 core flow, 10–19 management, 20–29 quality. Acceptance status lives in
[13-conformance-tdd.md](13-conformance-tdd.md), not here.

**REQ-1 — Resumable block streaming.** [MUST]
A pipe streams every block of its configured range set from the portal, in ascending
block order, exactly once per run, from backfill through real-time head following.
*Acceptance:* for any range set and any portal history, the concatenation of delivered
batches is exactly the configured ranges (clipped by resume bound), strictly ascending,
gap-free within ranges.
*Trace:* WP-10…WP-16, INV-20; ADR-1.

**REQ-2 — Declarative queries with typed decoding.** [MUST]
A network module lets the author declare filters (addresses, topics, discriminators,
ranges) and field selections; the pipe merges all declarations into one portal query
and returns decoded, schema-validated records. Undecodable records follow the declared
skip/fatal rules.
*Acceptance:* a declared filter set yields exactly the matching records with exactly
the selected fields; an unselected field is absent; malformed portal data is rejected,
never silently coerced.
*Trace:* DEF-16, WP-20…WP-25, INV-21…INV-24.

**REQ-3 — Effective exactly-once sink delivery.** [MUST]
Sink output plus cursor always converge to "each block's data present exactly once, cursor
= last delivered block" — across crashes at any point, per the sink's durability class.
*Acceptance:* kill-point testing (CT-2) at every commit-protocol step yields, after
restart, output identical to an uninterrupted run.
*Trace:* RP-1…RP-6, CN-10…CN-24, INV-40…INV-44; ADR-5.

**REQ-4 — Automatic fork rollback.** [MUST]
On a portal fork signal from the full stream, the pipe locates the canonical ancestor,
removes all sink data above it, rewinds the cursor, notifies transformers, and resumes
from the ancestor — without operator action and without touching finalized data.
Finalized-only delivery receives no fork signal and therefore needs no sink rollback path.
*Acceptance:* fork conformance suite (CT-3): for every fork-capable sink, any fork depth
within retention resolves to the oracle's ancestor; post-rollback output equals a run
that never saw the orphaned blocks.
*Trace:* WP-40…WP-47, INV-13, INV-14; ADR-3.

**REQ-5 — Finality safety.** [MUST]
The finalized floor is monotonic across batches, forks, and restarts. Sinks with
immutable storage declare finalized-only delivery, causing the pipe to select the
portal's finalized head and stream before range resolution; no sink-side finality
buffer is used. On a dataset that does not define finality, the route cannot provide
reorg safety and the output is explicitly not reorg-safe (FM-13).
*Acceptance:* no sequence of portal head reports (including regressions and absences)
ever lowers the floor; a finalized-only target uses the finalized route even when the
pipe was configured for the full stream, resolves symbolic `latest` against the
finalized head, and receives every delivered row without a deferred tail.
*Trace:* WP-7, INV-2, INV-12, INV-25, DEF-15, IB-11; ADR-3, ADR-22.

**REQ-6 — Restart continuity.** [MUST]
A restarted pipe resumes from the recovered committed state exactly: the first requested
block is cursor+1, the floor is re-seeded from persisted finalized state, and partial
writes from the interrupted run are repaired before new data is written.
*Acceptance:* stop/crash + restart at any point produces the same final output as an
uninterrupted run (CT-2).
*Trace:* WP-1…WP-6, CN-40…CN-45, INV-40.

**REQ-7 — Multi-output composition.** [MUST]
Several independent outputs (decoders/transforms) compose into one pipe: their queries
merge into a single portal query (union of filters per overlapping range), and each batch
carries each output's results under its own key without cross-contamination.
*Acceptance:* N outputs run composed produce, per output, the same records as each run
alone over the same range.
*Trace:* WP-24, WP-25, INV-22.

**REQ-10 — Observability surface.** [MUST]
A running pipe exposes progress (current/end block, percent, ETA), throughput, fork
count, per-stage timing, and a last-batch data preview over a standard HTTP surface,
machine-readable, without disturbing the pipeline.
*Acceptance:* every LIV property except LIV-9 (store-level audit) is decidable from the
surface (OB mapping table); a dashboard can drive its full UI from the binding in
IB-40…IB-46.
*Trace:* OB-1…OB-32, IB-40…IB-46.

**REQ-11 — Pipe isolation.** [MUST]
Pipes are isolated by pipe id: cursor state, rollback records, retention cleanup, and
fork resolution never read or modify another pipe's state, even when pipes share one
sink store. A blank/absent id when connected to a sink is a startup error.
*Acceptance:* two pipes with distinct ids sharing a store run and fork independently;
blank id fails with the documented error code.
*Trace:* INV-15, INV-35, DEF-9; ADR-2.

**REQ-12 — Local replay cache.** [SHOULD]
An optional local cache stores finalized batches keyed by query shape and replays them
on re-runs, transparently falling back to the portal at the first gap. Cached replay is
byte-equivalent to portal replay.
*Acceptance:* second run over a cached range issues no portal data requests for covered
prefixes and produces identical output.
*Trace:* RS-20…RS-25; ADR-11.

**REQ-13 — Coded error surface.** [MUST]
Every error the pipe raises to the author carries a stable code from a closed, banded
taxonomy with a documentation link; programs match on codes, never message text.
*Acceptance:* the emitted code set equals the registry in IB-50…IB-52; each coded path
is triggerable.
*Trace:* INV-31, IB-50…IB-52; ADR-4.

**REQ-20 — Bounded memory.** [MUST]
Peak memory is derivable from configuration: batch assembly is bounded by
P-STREAM-MAX-BYTES plus one network chunk, and sink buffers by their configured limits.
Finalized-only delivery adds no finality-depth-dependent in-process buffer. No stage has
an unbounded queue.
*Acceptance:* soak run (CT-7) under W-* reference load shows memory plateau consistent
with the derived bound.
*Trace:* PF-1…PF-5, HZ-1.

**REQ-21 — Throughput.** [SHOULD]
Backfill throughput on the reference workload meets the SLO table; the pipeline is
pull-driven end to end so a slow sink throttles ingestion instead of growing queues,
while transport fetch of the next batch overlaps processing of the current one.
*Acceptance:* CT-6 benchmarks against SLI-1…SLI-3 targets; overlap cadence per PF-6.
*Trace:* PF-6, WP-12, 11-performance.

**REQ-22 — Liveness under transient faults.** [MUST]
Transient portal/sink faults (timeouts, retryable statuses, dropped connections) are
retried with backoff and surfaced as signals, never as pipe death; integrity violations
halt with a coded error. A stall never exceeds the stall budget silently.
*Acceptance:* fault-injection corpus (CT-4) — each FM row produces its required response.
*Trace:* FM-1…FM-43, LIV-2, LIV-7.

**REQ-23 — Cross-implementation conformance.** [MUST]
An implementation in any language is conforming iff it passes the CT suite and can
resume from persistent state (cursor rows, state files, cache) written by the reference
implementation, and vice versa.
*Acceptance:* CT-5 round-trip: state written by A, resumed by B, for every persistent
format in IB-20…IB-26.
*Trace:* G1; ADR-13.

## Explicitly unspecified

Deliberately left open — conformance tests MUST NOT pin these:

- Batch boundaries: how blocks are grouped into batches (only ordering/coverage is
  normative).
- Flush/rollover timing within the configured triggers; exact file rollover points.
- Log message text and log record layout.
- The in-process API surface (method names, types) — per-language; only observable
  behavior and persistent/wire formats are contract.
- Concurrency/parallelism strategy inside a stage (threads, async), provided observable
  ordering holds.
- Default process-level metrics beyond the named `sqd_*` set.
- Preview payload contents beyond the truncation rules of IB-44.

## Open questions

| # | Question | Owner |
|---|---|---|
| OQ-2 | Defined behavior for a fork reaching below the finalized floor (currently a coded fatal; is halt the intent?) — GAP-6. | SDK team |
| OQ-4 | SLO targets for SLI-1…SLI-7 are unratified (⚠ in 15-parameters). | SDK team |
| OQ-5 | Cache growth policy: unbounded append-only accepted, or add eviction/versioning? (HZ-6). | SDK team |
| OQ-6 | Must a duplicate output signature be a fatal startup error, or stay a logged report? (WP-24, GAP-15). | SDK team |
| OQ-7 | Cursor timestamps are network-dependent (tron reports ms — GAP-24): normalize to one unit, or spec per network? | SDK team |
| OQ-8 | Do tracked tables belong to one pipe (exclusive) or may co-resident pipes share them? Decides whether CN-44's orphan guard can run at all, and whether file sinks namespace data by pipe id (GAP-20, GAP-35, ADR-16 proposed). | SDK team |
