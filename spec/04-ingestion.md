# 04 — Ingestion, transitions, checkpointing (WP-n)

Bands: 1–9 init/resume, 10–19 streaming & batching, 20–29 transform/decode,
30–39 lifecycle, 40–49 fork.

## The conceptual loop

```
state ← sink.recover()                     # T-INIT: TargetState or ⊥
repair_partial_writes(state)               # per durability class (06)
F ← clamp-seed(state.finalized)            # never from state.latest
portal ← select(configured mode, sink finality capability)  # WP-7
ranges ← resolve(range set, portal) ⊓ [state.latest+1, ∞)
for range in ranges:
  request ← merged query + range + parent hash anchor
  loop:
    batch | head-only | fork | end ← portal.stream(request)
    on fork  → T-FORK; restart loop from ancestor  # full stream only
    on end   → next range
    on head-only → emit progress; poll after P-HEAD-POLL-MS
    F ← max(F, batch.head.finalized)       # clamp, INV-12
    RC ← blocks above F (with hashes)
    data ← transformers(batch)             # declaration order
    sink.commit(data, C←last(batch), F, RC)  # T-BATCH / T-CHECKPOINT
T-STOP (exactly once, every exit path)
```

## Init & resume

**WP-1 — Resume position.** [MUST] The first block requested after recovery is
`state.latest.number + 1`. The recovered cursor's hash anchors the first request's
parent-linkage check (IB-3).
*Why:* gap-free, duplicate-free continuation (REQ-6).

**WP-2 — Floor seeding.** [MUST] `F` is seeded only from `state.finalized`, never from
`state.latest`. Absent/null seeds ⊥.
*Why:* seeding from `latest` would mark unfinalized data safe on no-finality datasets.

**WP-3 — Pipe-id validation.** [MUST] A pipe connected to a sink with a blank or
missing pipe id fails startup with the documented configuration error (IB-50 band
E0xxx). *(Current code throws an uncoded error — GAP-4.)*

**WP-4 — Range resolution.** [MUST] Symbolic endpoints resolve before streaming:
`latest` → current head (bounded below by the resume bound), dates/timestamps → block
numbers via the portal. Unresolvable or inverted ranges fail startup with the coded
range-configuration error. Ranges then merge (overlaps unioned) and clip to the resume
bound.

**WP-5 — Repair before write.** [MUST] Recovery repair (06, CN-40…CN-44) completes
before the first new commit. No new data may land while orphan data above `C` exists.

**WP-6 — Query freeze.** [MUST] Query-aware transformers contribute filters/fields
during configuration, before range resolution; after streaming starts the query is
immutable for the life of the run.

**WP-7 — Finalized-only source selection.** [MUST] When a sink declares finalized-only
delivery (DEF-15), the pipe selects an effective finalized portal before resolving
symbolic ranges, starting transformers, consulting a cache, or requesting blocks. All
finality-sensitive operations use that same effective view: `latest` resolves against
the finalized head, the data request uses the finalized stream, a cache receives both
the finalized mode and a portal view pinned to it, and the sink is told the effective
mode. A configured full-stream preference is overridden with a warning. A sink that
does not declare the capability follows the configured stream mode unchanged (IB-11).

## Streaming & batching

**WP-10 — Ordering.** [MUST] Blocks are consumed in strictly ascending number order;
batches partition that order (no block spans two batches, no reordering).
*(Delegated linkage: ADR-1 — the pipe forwards a parent-hash anchor and trusts the
portal's fork signal rather than re-verifying hashes.)*

**WP-11 — Batch assembly bounds.** [MUST] Batch assembly flushes on any of: byte budget
P-STREAM-MAX-BYTES, idle gap P-STREAM-MAX-IDLE-MS, total wait P-STREAM-MAX-WAIT-MS, or
(near head) per-unfinalized-block immediate flush. Peak assembly memory ≤
P-STREAM-MAX-BYTES + one transport chunk.
*Notes:* the total-wait clock starts when the consumer demands the batch, not at its
first block; the byte budget counts UTF-16 code units of raw NDJSON lines (a proxy —
HZ-1); the first unfinalized block MAY co-batch with pending finalized rows — per-block
isolation is guaranteed only from the next one on.

**WP-12 — Pull-driven backpressure with single-slot prefetch.** [MUST] The pipeline is
demand-driven end to end, with exactly one slot of lookahead: while the consumer
processes batch N, the producer keeps assembling batch N+1 from the transport (PF-6);
it blocks only when the assembling batch reaches a flush trigger and the slot is still
occupied. No stage may buffer unboundedly (REQ-20).

**WP-13 — Head following.** [MUST] On a head-only signal the pipe emits progress
(OB-2), then re-requests after P-HEAD-POLL-MS. Backfill/real-time is one loop, not two
modes. *(Currently the 204's progress emission does not happen and its head report is
discarded — the floor stalls while idle at head; GAP-23.)*

**WP-14 — Transport retry.** [MUST] Transport-level faults (retryable statuses in
P-RETRY-STATUS-SET, timeouts P-HTTP-TIMEOUT-MS, dropped connections) are retried with
schedule P-RETRY-SCHEDULE-MS, honoring server-provided retry-after, up to
P-STREAM-RETRY-LIMIT for streaming. Retries resume from the already-advanced position —
blocks received before the drop are never re-delivered downstream.

**WP-15 — Duplicate/gap discipline.** [MUST] Within a run, no block number is delivered
twice and none is skipped inside a covered range. Configured inter-range gaps are
preserved, never backfilled.

**WP-16 — Malformed input.** [MUST] A batch line that fails schema validation is an
integrity fault (FM-11): fail with a diagnostic identifying the offending block; never
silently coerce or drop. *(Partial-line vs malformed-JSON discrimination is currently
unimplemented — GAP-5; schema-validation failures currently surface uncoded and without
block identification — GAP-25.)*

## Transform & decode

**WP-20 — Sequential composition.** [MUST] Transformers apply in declaration order;
each receives the previous output; batch context (head, cursors, ranges, progress,
query identity) is available read-only to every stage.

**WP-21 — Validation before decode.** [MUST] Every block is validated against the
schema derived from the *selected* fields before user code sees it; selected-but-absent
collections read as empty collections, unselected fields are absent (INV-23).

**WP-22 — Filter fidelity.** [MUST] Decoders re-check their filters locally (address
normalization, discriminator match) and drop portal over-returns silently; a record
matching no declared output is not an error.

**WP-23 — Decode-error policy.** [MUST] One uniform rule across every network module
(ADR-12): a decode failure is *fatal by default* (it halts the pipe). A user-supplied
`onError` hook that returns without throwing *suppresses* the offending record; the hook
re-throwing keeps the failure fatal. Suppressed records are skipped and counted in
`sqd_decode_errors_skipped_total`, labelled by pipe id (INV-31). The hook shape and these
semantics are part of the shared conformance surface.

**WP-24 — Attribution uniqueness.** [MUST] Within one module, each input record maps to
at most one output key. Declaring two outputs with an indistinguishable signature is
reported at startup. *(Currently a logged error in the evm module and undetected in
solana — GAP-15; whether the report must be fatal is OQ-6.)*

**WP-25 — Query union.** [MUST] Composed outputs merge into one portal query: for each
overlapping range sub-interval, the union of all outputs' filters; field selections
merge by union. Composition MUST NOT change any single output's results (REQ-7).

## Lifecycle

**WP-30 — Hooks exactly once.** [MUST] Start hooks run once before the first batch;
stop hooks run exactly once on every exit path (completion, error, cancellation) —
including when a start hook itself fails partway.

**WP-31 — Cancellation.** [MUST] Cancellation propagates to the transport (aborting
in-flight requests) and unwinds through T-STOP. A cancelled run leaves committed state
only (crash-equivalent or better).

**WP-32 — Rollback notification.** [MUST] After a sink completes T-FORK, every
transformer's rollback hook is invoked with the ancestor cursor before streaming
resumes (stateful transformers must discard state above it).

## Fork (T-FORK)

**WP-40 — Detection.** [MUST] A fork is signaled by the portal rejecting the
parent-hash anchor (IB-4) and supplying the canonical chain (DEF-10). The pipe treats
this as authoritative (ADR-1). This transition applies to the full stream; the
finalized-only route does not emit forks (DEF-15).

**WP-41 — Empty canonical chain.** [MUST] A fork signal with an empty canonical chain
is a portal-contract violation: fail with the coded error (E1xxx band), never guess.
*(Currently an uncoded crash inside exception construction can preempt the coded path —
GAP-8.)*

**WP-42 — Ancestor search.** [MUST] The search walks the persisted rollback records
newest → oldest, each record's chain descending, keeping a canonical **window** that
starts as the full canonical chain and, after each visited cursor, narrows to canonical
entries strictly below it. A visited cursor whose hash appears in the window is the
ancestor. If the window empties while retained cursors remain (the fork reaches below
the canonical chain), the next retained cursor below the exhausted window is the
ancestor — a **deep-fork restart**: streaming resumes from it and the portal supplies a
longer canonical chain if it is still not canonical. If that cursor would lie below `F`,
resolution fails — see WP-44. If a record is exhausted without a match and exactly one
window entry remains matching the record's finalized cursor, the floor itself is the
ancestor (rollback to `F`).

**WP-43 — Rollback effect.** [MUST] Given ancestor `A`: all sink data attributed to
blocks `> A.number` is removed (per class mechanism, 06); `C ← A`; `RC` is trimmed to
`≤ A.number`; `F` is unchanged. Then WP-32, then streaming resumes at `A.number + 1`
with `A.hash` as the new anchor.

**WP-44 — Finality conflict.** [MUST] A fork that cannot resolve above the finalized
floor halts the pipe with the coded fork error *(today the generic ancestor-unresolvable
code E1003 — the resolve layer returns ⊥ with an open TODO)*. Finalized data is never
rolled back (INV-13). *(Whether halt is the final intent is OQ-2/GAP-6.)*

**WP-45 — Sink capability.** [MUST] A fork arriving at a sink that does not implement
rollback halts with the coded not-supported error. Fork support is per full-stream sink,
declared, and part of its conformance surface. A finalized-only sink MAY omit rollback
because WP-7 selects a route on which no fork can arrive; a fork response on that route
is a portal-contract violation and still halts safely (FM-13).

**WP-46 — Rollback idempotence.** [MUST] Re-running a completed rollback (crash between
rollback and next commit, then recovery) is a no-op: the mechanism must tolerate its
own partial or duplicated application (per-class details in 06).

**WP-47 — Depth bound.** [MUST] Rollback is guaranteed only within the retained history:
class-specific retention (P-CH-CURSOR-RETENTION, P-PG-UNFINALIZED-RETENTION,
P-BQ-CURSOR-RETENTION, and the class-C rollback manifest down to `F`). A fork deeper
than retention (but above `F`) resolves via the deep-fork restart of WP-42. Classes K/∅
use finalized-only delivery and therefore do not retain fork history for rollback.

## Commit & acknowledgement

The commit point, atomicity unit, and visibility per durability class are normative in
[06-consistency-durability.md](06-consistency-durability.md) (CN-10…CN-24). Single-writer
rule: INV-15. No-partial-visibility: INV-3/CN-20…CN-24.

## Error handling

Transient vs integrity classification and required responses: [09-failure-model.md](09-failure-model.md).
The governing rule: transient → bounded-backoff retry with visible signals (never
silent-forever without OB-13); integrity/configuration → halt with a coded error
(closed taxonomy, IB-50). No input content may terminate the process without a coded
diagnostic (FM-1).
