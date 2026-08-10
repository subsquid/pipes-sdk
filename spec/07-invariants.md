# 07 — Safety invariants (INV-n)

Scope tags: `[state]` holds in every observable state · `[transition]` relates
consecutive states · `[response]` holds of every emitted result · `[recovery]` holds
across crash/restart. Bands: structural 1–9, transition legality 10–19, read/response
20–29, reporting 30–34, isolation 35–39, durability/recovery 40–49.

## Structural

**INV-1 — Rollback-chain well-formedness.** [state]
`RC` is strictly increasing by number; every entry has a hash; every entry `e`
satisfies `F < e.number ≤ C.number` (when `F`/`C` present). An empty `RC` is legal
(fully finalized position).
*Why:* the ancestor search (WP-42) is only sound over a sorted, hash-bearing, above-floor chain.
*Check:* CT-1 structural validator on every persisted state read.

**INV-2 — Floor dominance.** [state]
`F` never exceeds the portal's true finalized history's reach in a way the pipe invented:
`F` is always some previously reported finalized cursor (or ⊥). Every consumer of
finality reads the clamped `F`, never a raw report.
*Why:* an invented floor would admit reorg-able data to finalized output and prune
rollback history too early.
*Check:* CT-1 oracle comparison of floor against the simulator's report history.

**INV-3 — Data attribution.** [state]
Every committed row is attributed to exactly one block number within the pipe's
processed range; visible data respects the class visibility rule (CN-20…CN-24): on a
finalizing dataset, source-gated sinks receive rows only from the finalized route.
*Why:* rollback and coverage reasoning operate on block attribution.
*Check:* CT-1 validator: attribution column present, in-range, class-visible.

**INV-4 — Coverage partition.** [state]
(File sinks) Per table, published windows are pairwise disjoint and their union equals
the processed-and-covered range; `V` (next-window start) equals the end of the last
published window + 1.
*Why:* consumers must distinguish "empty" from "never indexed" (ADR-6).
*Check:* CT-7 sparse-stream soak; filename/window audit vs oracle.

**INV-5 — State-record well-formedness.** [state]
Every persisted state record is fully parseable per its IB format; required keys
present (`finalized` explicitly null when absent); no state record refers to a block
above the newest committed data's attribution bound.
*Why:* cross-implementation resume (G1) dies on tolerated corruption.
*Check:* CT-5 format round-trip + fuzzed-corruption rejection (CN-43).

## Transition legality

**INV-10 — Frame condition.** [transition]
No input event ⇒ no observable state change: without a delivered batch, fork signal, or
operator action, ⟨C, F, RC, D, V⟩ are constant. Background maintenance never changes
logical state (CN-24).
*Why:* catches phantom writes, cleanup bugs, clock-driven mutation.
*Check:* CT-1 quiescence comparison; CT-7 idle soak.

**INV-11 — Cursor legality.** [transition]
`C` changes only by: T-BATCH/T-CHECKPOINT (strictly increasing), T-FORK (to the
canonical ancestor), recovery repair (to the last commit point). No other transition
moves it.
*Why:* cursor is the exactly-once anchor; illegal movement is data loss or duplication.
*Check:* CT-1 oracle lockstep on every transition.

**INV-12 — Floor monotonicity.** [transition] [recovery]
`F' ≥ F` across every transition — batches (regressed/absent reports clamp), forks
(unchanged), restarts (re-seeded from persisted `F`), recovery.
*Why:* an un-finalize event would invalidate immutable published data.
*Check:* CT-1 with adversarial head reports (regressing, vanishing, genesis-0); CT-2 restart matrix.

**INV-13 — Finalized immutability.** [transition]
Data attributed to blocks `≤ F` is never modified or deleted by any fork, recovery, or
maintenance transition. Published immutable units are never rewritten.
*Why:* downstream consumers treat finalized output as append-only truth.
*Check:* CT-3 fork suite asserts byte-stability of `≤ F` output; CT-2 recovery ditto.

**INV-14 — Fork reach.** [transition]
T-FORK deletes exactly the data attributed to `(ancestor, C]` — nothing below the
ancestor, nothing outside the pipe's tracked tables, and the ancestor is hash-verified
canonical (or the WP-42 deep-fork current-cursor case).
*Why:* over-deletion loses committed history; under-deletion leaves orphan rows that
corrupt the canonical view.
*Check:* CT-3 oracle diff of pre/post-fork data at every depth.

**INV-15 — Single writer.** [state]
Per cursor key, at most one pipe instance commits. Where the binding provides a lock
(IB-24) the second instance fails with the coded lock error; elsewhere dual-instance
execution is undefined behavior explicitly excluded by NG2 — the spec requires only
that a binding's declared enforcement (or its absence) matches its IB entry.
*Why:* two writers interleave commit protocols and corrupt every class's crash-window
reasoning.
*Check:* CT-8 dual-instance test where a lock is declared.

**INV-16 — Destructive-ops enumeration.** [transition]
Committed data leaves the store only via: fork rollback (CN-30…CN-33), recovery repair
(CN-40…CN-44), or bounded retention cleanup of *bookkeeping* records (RP-6). No other
code path deletes.
*Why:* the whole-class bug of accidental deletion.
*Check:* CT-1 ledger reconciliation: every disappearance justified by an oracle event.

**INV-17 — Lifecycle exactly-once.** [transition]
Per run: start hooks fire exactly once before the first batch; stop hooks exactly once
on every exit path; a partially-failed start still triggers stop for the started
subset.
*Why:* double-stop breaks author resources (connections); missed stop leaks them.
*Check:* CT-1 hook ledger across normal/error/cancel exits (regression: past defect A2).

## Read/response

**INV-20 — Emission ordering.** [response]
Batches are emitted with strictly ascending, gap-free-within-range block coverage;
every batch's blocks are strictly ascending; batch metadata (head, cursors, chain)
is consistent with its content (last block = reported current cursor).
*Why:* downstream logic (aggregations, cursors) assumes order.
*Check:* CT-1 structural validator on every emission.

**INV-21 — Provenance fidelity.** [response]
Decoded records reflect the portal bytes faithfully: no invented fields, no dropped
selected fields, numeric widths preserved (big integers never truncated), absent
collections read as empty, unselected fields absent (never null-filled).
*Why:* silent coercion is undetectable downstream.
*Check:* CT-5 decode fixtures per network module; CT-9 fuzz.

**INV-22 — Determinism modulo free variables.** [response]
Two runs over the same portal history with the same configuration emit the same
records with the same attribution, differing only in declared free variables (batch
boundaries, timing, unit rollover points).
*Why:* replay purity (RS-10) and cross-implementation equivalence build on this.
*Check:* CT-1 dual-run diff; CT-5 cross-implementation diff.

**INV-23 — Selection contract.** [response]
The emitted record shape is a pure function of the declared field selection; reading an
unselected field is not an error but yields absence — and this behavior is uniform
across network modules.
*Why:* implementations must not diverge on shape defaults.
*Check:* CT-5 selection matrix fixtures.

**INV-24 — Filter soundness.** [response]
Every emitted record matches the declared filters of the output it appears under
(address/topic/discriminator re-checked locally); over-returned portal records never
leak through.
*Why:* portal over-fetch is an implementation detail, not an output.
*Check:* CT-1 with adversarial over-returning simulator.

**INV-25 — Finalized-source visibility.** [response]
(Classes K/∅) Finalized-only source selection occurs before range resolution and every
sink-visible row arrives through that effective route, in source order; the sink does
not defer or release rows based on finality locally. On a finalizing dataset, no
delivered row precedes its block's finalization. On a no-finality dataset, output is
explicitly not reorg-safe (DEF-15, FM-13).
*Why:* immutable storage + reorg-able data are incompatible.
*Check:* CT-1/CT-5 finalized-route selection, symbolic-latest, cache-view, immediate-
delivery, and no-finality scenarios.

## Reporting

**INV-30 — Metrics honesty.** [state]
Exported progress equals internal state: processed-block gauge = `C.number`, end-block
gauge = effective range end, fork counter increments exactly per T-FORK. A metric that
lies is a conformance failure (12 §harness rule).
*Check:* CT-1 scraper cross-check.

**INV-31 — Error soundness.** [response]
Every terminal failure surfaces exactly one coded error from the closed taxonomy
(IB-50); no partial batch is delivered alongside a terminal error; retried transients
are not surfaced as errors (they are signals, OB-13). A decode record suppressed by an
`onError` hook (WP-23) is not a terminal failure: it is skipped, counted in
`sqd_decode_errors_skipped_total`, and never delivered — a suppression that goes
uncounted is a conformance failure.
*Check:* CT-4 fault corpus: every injected fault maps to its FM-required code/signal;
suppress-hook fixture asserts skip + counter, not a delivered record or an error.

**INV-32 — Progress heartbeat.** [state]
The observability surface always distinguishes: progressing / idle-at-head /
stalled-retrying / halted-with-error (OB-2, OB-13).
*Check:* CT-4 stall scenarios.

## Isolation

**INV-35 — Pipe-key isolation.** [state] [transition]
All state reads, writes, cleanup, migration, and fork resolution are scoped to the
bound cursor key; a pipe never reads another key's rollback records (fork resolution
included) nor deletes another key's rows.
*Check:* CT-8 co-resident-pipes suite.

**INV-36 — Cache isolation.** [state]
Cache entries are keyed by query hash (DEF-13) + block range; a query-shape change
never replays another shape's bytes.
*Check:* CT-5 cache keying fixtures.

## Durability/recovery

**INV-40 — Recovery soundness.** [recovery]
Post-recovery state ≡ some committed state in every field (CN-40); final output after
any crash/restart schedule equals an uninterrupted run's output.
*Check:* CT-2 kill-point matrix (the class's every protocol step).

**INV-41 — Recovery idempotence.** [recovery]
Recovery re-run (crash during recovery) converges: same resulting state, no
accumulating side effects (CN-41, CN-42).
*Check:* CT-2 double-kill scenarios.

**INV-42 — No orphans post-recovery.** [recovery]
After recovery completes, no data above `C` exists in tracked tables, and no
temporary/in-flight residue remains (per class).
*Check:* CT-2 storage audit after each kill-point.

**INV-43 — Replay purity dependency.** [recovery]
(Class K) Recovery-triggered re-processing produces byte-identical units for finalized
blocks — the purity obligation RS-10 is load-bearing for INV-40 here.
*Check:* CT-2 re-fetch determinism diff.

**INV-44 — Refuse-before-delete.** [recovery]
When persisted state and stored data are mutually inconsistent beyond the class's
defined crash window (straddling units, unknown formats, orphan tracked data), recovery
halts with a coded integrity error **before** any destructive repair (CN-43, CN-44).
*Check:* CT-2 corrupted-state corpus.

## Reading the catalog in tests

- Every persisted-state read → INV-1…INV-5 validators (cheap, always on).
- Every oracle-lockstep transition → INV-10…INV-17.
- Every emitted batch → INV-20…INV-25 structural validators.
- Every scrape → INV-30…INV-32 cross-checks.
- Kill-points and fork storms → INV-40…INV-44, INV-13, INV-14.
