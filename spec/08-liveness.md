# 08 — Liveness (LIV-n)

## §0 Environmental definitions

Liveness claims hold only under these conditions:

- **Healthy portal**: responds within P-HTTP-TIMEOUT-MS, serves the requested ranges,
  transient faults are intermittent (not permanent).
- **Healthy sink storage**: accepts writes within its ordinary latency; transient
  faults intermittent.
- **Adequate resources**: process has memory per the REQ-20 bound and CPU to decode at
  the offered rate.
- **Quiescent**: no batch in flight, no fork pending, portal at head with no new blocks.

Each property: precondition → bound → witness observable → test class.

**LIV-1 — Ingest progress.** Healthy portal + sink, uncovered range remaining ⇒ the
cursor advances by ≥ 1 block within P-STALL-BUDGET-S.
*Witness:* OB-1 gauge strictly increases. *Test:* CT-1, CT-6.

**LIV-2 — Zero-progress stall budget.** Under healthy conditions, the maximum interval
with no cursor advance and no OB-13 stall signal is P-STALL-BUDGET-S. A stall beyond it
without a signal is a liveness failure — silence is the bug, not the stall.
*Witness:* OB-2 heartbeat + OB-13. *Test:* CT-4.

**LIV-3 — Head following.** Real-time dataset, pipe at head ⇒ a newly produced block is
delivered within P-HEAD-POLL-MS + P-STREAM-MAX-WAIT-MS + one round trip.
*Witness:* OB-1 tracks OB-6 (head gauge) within the bound. *Test:* CT-6 (S2).

**LIV-4 — Fork resolution terminates.** A fork signal with a canonical chain
intersecting retained history resolves (rollback complete, streaming resumed) within
P-FORK-RESOLVE-S, without operator action.
*Witness:* OB-14 fork counter + resumed OB-1 progress. *Test:* CT-3.

**LIV-5 — Startup bound.** Healthy conditions ⇒ recovery + repair + first batch
delivered within P-STARTUP-S, independent of total history size (recovery work scales
with crash residue, not with data volume).
*Witness:* OB-20 lifecycle timestamps. *Test:* CT-2, CT-6 (S5).

**LIV-6 — Shutdown drain.** A stop/cancel request completes (stop hooks done, resources
released) within P-SHUTDOWN-DRAIN-S; in-flight batch either commits or is discarded
whole (crash-equivalent or better). Past the budget, the run reports a coded failure,
and the work that cannot be cancelled may outlive it (WP-33).
*Witness:* process exit + OB-20. *Test:* CT-1 lifecycle suite, real-signal child processes.

**LIV-7 — Convergence-or-alarm.** Any retry loop either succeeds, or continuously emits
the stall/retry signal (OB-13) while retrying. Unbounded silent retry is forbidden;
bounded retry ends in a coded error. (Streaming transport retry is deliberately
unbounded-with-signal — ADR-10.)
*Witness:* OB-13 signal present whenever retrying. *Test:* CT-4.

**LIV-8 — Checkpoint keep-up.** (Class K) Under steady input meeting the workload
model, checkpoint triggers fire often enough that cursor lag behind processed blocks is
bounded by the configured trigger intervals; a byte-only trigger on a slow tail MUST
NOT stall the cursor indefinitely once a time/block trigger is configured.
*Witness:* OB-1 vs OB-21 (published-cursor gauge) gap bounded. *Test:* CT-7 (S4). *Hazard:* HZ-4.

**LIV-9 — Reclamation convergence.** Retention cleanup keeps bookkeeping bounded: under
steady state, cursor/undo record counts converge to their retention parameters within
one cleanup period (P-CH-CLEANUP-PERIOD commits).
*Witness:* bookkeeping row counts plateau. *Test:* CT-7.

**LIV-10 — No cross-pipe starvation.** Co-resident pipes (shared store, distinct keys)
each make LIV-1 progress; one pipe's fork storm or retention cleanup does not starve
another beyond store-level contention.
*Witness:* per-pipe OB-1. *Test:* CT-8.
