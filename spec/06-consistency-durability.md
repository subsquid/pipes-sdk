# 06 — Consistency & durability (CN-n)

Bands: 10–19 commit protocols (durability classes), 20–29 visibility & isolation,
30–34 fork mechanics per class, 40–49 recovery.

## Durability classes (ADR-5)

Every sink declares exactly one class. The class fixes the commit protocol, the crash
window, and the recovery obligation. Concrete engine bindings: IB-20…IB-26.

**CN-10 — Class T (transactional).** Data rows, cursor, floor, and rollback chain
commit in **one storage transaction** per batch, at an isolation level preventing lost
updates. Crash window: none — a batch is all-or-nothing. Recovery: none needed beyond
reading the last committed state. Delivery: exactly-once.

**CN-11 — Class W (write-ahead).** Per batch: (1) intent record `⟨IN-FLIGHT, range⟩`
persisted **before** any data write; (2) data appended with per-stream offsets that the
store deduplicates on retry; (3) commit record with the advanced cursor. Crash window:
between (1) and (3) — recovery finds the in-flight record, deletes the intent range
from every tracked table (idempotent), records the abort, and resumes from the
pre-batch cursor. Delivery: effectively exactly-once.

**CN-12 — Class K (checkpointed-immutable).** Only finalized rows are written (via the
hold-back buffer, DEF-15). At a checkpoint (DEF-17): open units are published
atomically (write-temp → sync → rename), **then** the cursor/state record is persisted
atomically. Crash window: units published above the persisted cursor — recovery deletes
every unit whose window end exceeds the cursor plus all temporary files, then re-fetches;
requires replay purity (RS-10). A unit *straddling* the cursor is an integrity fault —
refuse before deleting anything — **unless** it starts where the recorded coverage says
that table was next due to publish from, which identifies it as the interrupted
checkpoint's own unit (a sparse table's stretched unit straddles by construction) and so
as a remnant to delete. Delivery: effectively exactly-once.

**CN-13 — Class A (append-lagged).** Data is appended by author code first; the cursor
record is appended **after**, non-atomically. Crash window: data committed above the
cursor. Recovery: on every resume the sink invokes the repair hook (`recovery`, safe
cursor) which MUST remove rows above the cursor before streaming resumes. Delivery:
at-least-once at the storage layer, effectively exactly-once **iff** the repair
obligation (RP-42) is met. *(Hook optional by design — the binding is schema-blind; its
absence is warned, not repaired: an accepted deviation, ADR-15.)*

**CN-14 — Class ∅ (ephemeral).** No persistence; every run is a cold start; only
finalized rows are emitted to author code. Exists as the executable minimal model of
the hold-back contract.

**CN-17 — Class C (compensating append-only).** [MUST] For a medium that can neither
rewrite nor delete what it published (message buses). Output is a changelog of keyed
operations, not rows. Per batch, in **one local transaction**: assign each operation the
next producer sequence, enqueue it in a publish outbox, record it in a rollback manifest
(operation, row id, partition, filter attributes, and — for revisable rows — its encoded
value), append the block ledger, advance the cursor, fold newly-finalized revisable rows
into their baselines, and prune what finality has put out of a fork's reach. Only then
does the outbox go to the wire; confirmed rows leave the outbox in a second transaction.
Crash window: between the commit and the confirmation — recovery republishes the outbox
byte- and sequence-identically. The cursor can therefore never run ahead of the wire: a
crash leaves *unpublished* operations, never *unrecorded published* ones. Fork: CN-35.
Delivery: at-least-once end to end, converging **iff** consumers apply operations
idempotently and by version (RP-24). *(Numbered outside the 10–14 run: the class was
added after the band's commit-model entries, and ids are stable — ADR-21.)*

## Commit model

**CN-15 — Total order per pipe.** [MUST] Commits for one cursor key form a total order;
each commit's cursor strictly exceeds its predecessor's (except fork/recovery rewind).
There is no commit "version" separate from the cursor: cursor order **is** commit order.

**CN-16 — Single commit point.** [MUST] Each class has exactly one point after which a
batch is durable (T: transaction commit; W: commit record; K: state-record rename; A:
cursor append; C: the local outbox transaction). Before it, recovery discards the batch;
after it, recovery preserves it entirely.

## Visibility & isolation

**CN-20 — Atomic visibility (T).** [MUST] Readers of the store never observe a batch's
rows without its cursor or vice versa.

**CN-21 — Visibility (W/A/C).** [MUST] Readers may observe data rows before the
corresponding cursor record (the crash window is reader-visible); after recovery
completes, visible data and cursor agree. Consumers requiring exactness read ≤ the
committed cursor.

**CN-22 — Visibility (K/∅).** [MUST] Only finalized data is ever visible; published
units are immutable from the moment of publication — no in-place rewrite, ever
(INV-13 corollary).

**CN-23 — Monotonic reads.** [MUST] For a single reader, the observable cursor never
moves backward except across an observed fork/recovery repair.

**CN-24 — Maintenance transparency.** [MUST] Background work (retention cleanup, index
maintenance, part merges) never changes logical content: the query-visible data and
cursor are identical before and after (metamorphic check, CT-1).

## Fork mechanics per class

**CN-30 — T.** Undo is a row-snapshot log captured by triggers on unfinalized-block
writes — before-images for update/delete, the key alone for insert — keeping the
earliest image per row per block. Replay takes, for each row, the earliest record
strictly above the ancestor (insert→delete, update/delete→restore the before-image)
inside one transaction, then drops the undo records above the ancestor. Storing the
pre-change value is what lets a row whose prior value predates every snapshot — because
it was written in a finalized block — land back on that value. This holds only while a
record's block tag is exact, so a class-T sink must be fed batches carrying at most one
unfinalized block and never mixing finalized blocks into one (`perBlockUnfinalized`).

**CN-31 — W.** Fork is a ranged delete `(ancestor, max(canonical)]` on every tracked
table, bracketed by intent/commit WAL records; both bounds mandatory (bounded work,
partition pruning). Guarded by RP-43. (The delete range may exceed `C`; rows exist only
`≤ C` (INV-42), so the deleted data set equals INV-14's `(ancestor, C]`.)

**CN-32 — K.** Fork drops hold-back rows above the ancestor only; published units are
never touched (they contain only finalized rows).

**CN-33 — A.** Fork resolves the ancestor from own-key rollback records, then delegates
deletion to the repair hook (`fork`, ancestor). The storage-level mechanism MUST be
idempotent under duplicated application and MUST NOT corrupt tables that share the
store (engine-capability guards at startup).

**CN-35 — C.** Fork resolves the ancestor from own-key rollback records, then repairs
each affected row by **publishing**, never by deleting. One rule per row id: drop the
orphaned revision suffix and publish whatever state remains at the ancestor — the
surviving revision, else the finalized baseline, else the route-declared inverse (a
`delete` by default). Each compensation takes the partition's **next** sequence, never a
reused or rewound one, and carries the id and filter attributes of the operations it
repairs, so a filtered consumer receives the repair for its own slice. A write-once row
that also exists at or below the ancestor is still canonical there and MUST NOT be
compensated. Compensations enter the same outbox and go out before anything later on
their partition, and before the source re-streams the canonical blocks. On the finality
dead-end (WP-44), the entire rollbackable manifest is compensated back to the finalized
baselines before the sink returns ⊥ — fail-closed, not a convergence claim: if the fork
invalidated the persisted floor, local data cannot repair it and consumers need a
rebuild *(GAP-6)*.

**CN-34 — Ancestor persistence.** [MUST] After any class's fork completes, persisted
state reads ⟨C = ancestor, F unchanged, RC trimmed⟩ — even if the process crashes
immediately after (fork completion is itself a commit point). *(The append-lagged
binding persists nothing at fork completion — a crash before the next commit resumes
pre-fork; GAP-21.)*

## Recovery contract

**CN-40 — Recovered ≡ committed.** [MUST] After any crash, recovered state equals some
committed state **in every field** — cursor, floor, rollback chain, coverage map, and
data (after class repair). No field may be reconstructed approximately.

**CN-41 — Recovery idempotence.** [MUST] Recovery interrupted by another crash and
re-run converges to the same state; repair actions are idempotent.

**CN-42 — Residue convergence.** [MUST] Crash residue (temp files, in-flight records,
orphan rows) is bounded and removed by the next successful recovery; it never
accumulates across crash loops nor blocks retention cleanup.

**CN-43 — Format compatibility gate.** [MUST] A sink refuses to operate on persisted
state it cannot fully interpret (unknown format version, corrupt record) with a coded
integrity error — before modifying anything.

**CN-44 — Orphan-data guard.** [MUST — exclusive tracked tables only] Where cursor state
is absent but tracked tables contain data (state deleted, wrong key, half-migration),
the sink refuses with a coded error rather than restarting from scratch and duplicating
history. Applies only where the binding declares its tracked tables **exclusive** to one
cursor key (IB-27). On tables shared with co-resident pipes the condition is
undecidable — "table non-empty" is not evidence of *orphan* data, the rows may belong to
another key — and probing them would make one pipe's startup depend on another's data,
contrary to INV-35; a sink over shared tables MUST NOT run the guard. *(The one
implementation probes table-wide and so refuses a legitimate first run into a populated
shared table — GAP-20; exclusivity model ADR-16/OQ-8.)*

**CN-45 — Cross-implementation recovery.** [MUST] Recovery reads state via the formats
of IB-20…IB-26 only; any conforming implementation recovers state written by any other
(G1). Clock independence: no recovery or ordering decision may depend on wall-clock
comparability across writers *(two bindings order cursor rows by wall-clock timestamp —
GAP-10)*.

**CN-46 — Recovery (C).** [MUST] A class-C sink's resume is: open state → drain the
outbox → stream from the recovered cursor. No data repair runs and none is possible: the
commit point precedes the wire, so recovery has nothing published-but-unrecorded to
clean up, and every outbox row it republishes is byte- and sequence-identical to what a
first attempt would have sent. A cold start (no state at the configured path) MUST be
made observable — the sequencer is the state, and its loss is indistinguishable in-band
from a first run (ADR-21, GAP-38).

## Subsystem non-interference

| | Writer (commit) | Reader (consumer) | Maintenance (cleanup) |
|---|---|---|---|
| Writer | single writer per key (INV-15) | never blocks readers beyond store semantics | cleanup never deletes newest state (RP-6) |
| Reader | — | — | maintenance invisible (CN-24) |
| Co-resident pipe (other key) | fully isolated (INV-35) | fully isolated | own-key scope only |
