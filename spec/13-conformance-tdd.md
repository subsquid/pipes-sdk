# 13 — Conformance & TDD (CT-n, GAP-n) — MUTABLE

Last updated: 2026-11-04. Statuses reflect the actual TypeScript test inventory at
mainline `11c92d6` plus the conformance harness in `packages/pipes/conformance/`, not
aspiration.

## Harness architecture

```
┌─────────────────┐   scripted history    ┌──────────────────┐
│ portal simulator │──────────────────────▶│       SUT        │
│  (ledger: every  │  batches, heads,      │  (any language,  │
│   block/head/fork│  fork signals, faults │   black box)     │
│   it ever served)│◀──────────────────────│                  │
└─────────────────┘   requests (asserted   └──────┬───────────┘
        │              against IB-1…IB-11)        │ commits
        │                                          ▼
        │            ┌──────────────┐      ┌──────────────────┐
        └───────────▶│ reference    │      │ sink store probe │
                     │ model        │◀────▶│ (reads state +   │
                     │ (oracle)     │ diff │  data via IB-20…IB-28) │
                     └──────────────┘      └──────────────────┘
   observability scraper ── polls IB-40…IB-46 surface, cross-checks oracle (12 §harness rule)
   fault injectors ── portal faults (FM-10…FM-19), store faults (FM-20…FM-27), kill-points (FM-30…FM-34)
```

*Harness status:* the simulator is the reference implementation's mock portal in its
chain form. It answers every request from the chain it holds, keyed by the request
anchor (IB-3), so a post-restart re-request, a fork (409 with the canonical window), a
head move or a transport fault needs no script; a response-list form remains for exact
sequences, and logs every request with the answer it got and the head it reported. The
oracle, the structural validators, store probes and kill-point injectors for every class
live in `packages/pipes/conformance/`; CT-1 and CT-2 run S1
against classes K, T, A and W, and S3 reduced to one fork against classes T, A and W. On S3
the oracle replays the portal's request log: a batch's blocks and floor come from the
answer that served it, a fork's canonical chain from the 409, never from the pipe. A
kill is a true kill: a child process runs the pipe and stops itself with SIGKILL at one
step of the commit protocol or of recovery — a file operation for class K, an SQL
statement for class T, a Storage Write append or a query job for class W, an HTTP
request for class A — so nothing unwinds and the store
holds what a crash leaves. For class T the harness waits until the server has dropped
the child's connections, and with them any transaction it held open; for class A, until
the server runs none of the child's queries. The class-A probe reads each table net of
its cancel rows, with merges stopped and no folding on insert, so a row stored twice
reads twice. The child
journals every batch before the store sees it, and the oracle follows the journal. Class W
runs against a real BigQuery project the environment names (`BIGQUERY_TEST_PROJECT`); the
suite skips wholesale without it, and each test makes a dataset of its own. A DML job a
killed child created runs on whatever the child does, so the parent waits for every job
the child journaled before it reads the store. One point restarts at once, waits for the
killed run's outstanding jobs once the restart ends, and reads the store again: the end
state must stand. A separate controlled test pins the latest order BigQuery's same-table
DML serialization hands a restart — the killed run's rollback DELETE completing after
the restart's recovery: by then its range is empty, so it takes nothing. Every point
checks its
crash window: each row above the recovery cursor lies inside the range the in-flight WAL
row names (INV-42). Not
built yet: the corrupted-state corpus,
the observability scraper, and adversarial histories (duplicate or out-of-order blocks,
over-return).

*Language neutrality:* the simulator speaks HTTP (IB-1…IB-11) and the probes read the
persisted formats (IB-20…IB-28), so the simulator, the oracle, the validators and the
probes serve any implementation. Three parts are bound to the TypeScript
implementation: building the pipe, the batch journal (an in-process hook) and the kill
switches, which patch the file system, the Postgres client and the ClickHouse client's
HTTP connection inside the child. A
second implementation needs its own driver for them, or kill points injected from
outside its process. The harness layout is in `packages/pipes/conformance/README.md`.

**Quiescence** (comparison point): simulator drained, SUT idle-at-head (OB-2 idle), no
retry signal active, sink store stable for one poll interval. All oracle/SUT diffs are
evaluated at quiescence. Each scenario also fixes its expected end state (rows, cursor,
floor) from the simulator alone, and both the store and the oracle are diffed against it:
a lockstep oracle follows the SUT, so it agrees with a SUT that stops early.

**Crash point** (comparison point after a kill): the store must hold a commit point the
oracle wrote (CN-40), its committed part (units at or below `C`) must pass every
structural validator and hold exactly the scenario's rows up to `C`, and the crash window
(units above `C`, residue) must be where the class protocol puts it for that kill point.
The window is the evidence that the kill landed on the step the test names. The restart
then runs to quiescence and is diffed as above.

## Reference model (normative pseudocode)

```
state: C←⊥, F←⊥, RC←[], R←[], D←{}, V←{}           # DEF tuple + R: persisted ⟨RC, F⟩ records
mode ← configured stream mode

bind(target):                                      # WP-7 / RP-7
  if target requires finalized-only: mode ← finalized-only
  resolve symbolic ranges through portal with mode
  pass portal and mode explicitly to transformers and caches

recover(persisted):                                  # T-INIT
  require well-formed(persisted) else REFUSE          # INV-5, INV-44
  P ← commit point since the last recover whose ⟨C, F, RC, R⟩ = persisted   # CN-40
      # any batch end for K and ∅ (checkpoint timing is free); the last two for the others
  if P = ⊥ and persisted ≠ ⊥ → REFUSE                 # a state no commit wrote
  if P = ⊥ and D ≠ ∅ and tables exclusive → REFUSE    # CN-44
  C, F, RC, R, D ← P (or the empty state); V ← persisted   # class repair leaves D as P holds it
  assert no row in D above C                          # INV-42

batch(blocks, head):                                 # T-BATCH
  require blocks strictly ascending, first = C+1 (within range)   # INV-20
  if class ∈ {K, ∅}: require mode = finalized-only   # INV-25
  F ← max(F, head.finalized)                          # INV-12
  RC ← [b ∈ processed : b.number > F] if mode = full and F ≠ ⊥, else []   # INV-1; no fork history on finalized-only or below an absent floor (WP-47)
  rows ← transform(blocks)                            # pure per RS-10
  commit(D += rows, C ← last(blocks), F, RC)          # per class CN-10…CN-14, CN-17
  R ← R ++ [⟨RC, F⟩]                                  # one record per commit; retention trims (WP-47)
  wellformed_check()

fork(canonical):                                     # T-FORK
  require mode = full else CODED-ERROR                # WP-45 / FM-13
  require canonical ≠ ∅ else CODED-ERROR              # WP-41
  A ← search(R, canonical, F)                         # WP-42 (a partial window is tolerated, RP-43)
  if A = ⊥ → CODED-ERROR                              # no ancestor at or above F (E1003, WP-44)
  D ← D − rows(> A.number)                            # class C compensates instead (CN-35)
  C ← A; RC ← RC[≤ A.number]; R ← R ++ [⟨RC, F⟩]      # F unchanged; INV-13/14
  commit-point(fork)                                   # CN-34

search(R, canonical, F):                             # WP-42
  W ← canonical                                       # narrowing window
  walk: for rec ∈ R newest → oldest:
    for r ∈ rec.RC newest → oldest:
      if F ≠ ⊥ and r.number < F.number → break walk   # never below F (WP-44)
      if ∃w ∈ W : w.hash = r.hash → return w          # canonical match
      if W = ∅ → return r                             # deep-fork restart (below exhausted window)
      W ← {w ∈ W : w.number < r.number}
  if F ≠ ⊥ and ∃w ∈ W : w.hash = F.hash → return F    # floor fallback
  return ⊥

read_state(): return ⟨C, F, RC⟩                       # same taxonomy as the API

wellformed_check(): assert INV-1…INV-5 after every transition
```

**Free variables** (SUT may legitimately vary; oracle must not pin):
batch partitioning of the block sequence · flush/rollover/checkpoint timing within
configured triggers · published-unit boundaries (subject to INV-4 tiling) · retry
pacing within WP-14 · log text · preview sample content within IB-44 rules · the
timestamp of the cursor a fork rolled back to (the canonical chain carries none, so a
binding may keep the processed block's or none).
Everything else — record content, attribution, cursor/floor/chain evolution, coverage
windows, persisted formats — is deterministic against the oracle.

## Test-class taxonomy

These classes label the guarantees checked, not the directory a test must occupy.
Follow [test placement and overlap](../packages/pipes/TESTING.md) when adding or
consolidating tests. Helper tests validate the harness; they do not by themselves
establish pipeline conformance.

| CT | Class | Primary properties |
|---|---|---|
| CT-1 | pipeline property tests (simulator ↔ oracle lockstep, structural validators, quiescence diff) | INV-1…INV-5, INV-10…INV-17, INV-20…INV-25, INV-30, WP-*, RP-7, RP-20…RP-23 |
| CT-2 | crash-recovery kill-point matrix (kill at every class commit-protocol step; double-kill; corrupted-state corpus) | INV-40…INV-44, CN-*, REQ-3, REQ-6 |
| CT-3 | fork conformance (depth × timing × full-stream class; fork-storm; impossible-fork refusal on finalized-only routes) | REQ-4, WP-40…WP-47, INV-13, INV-14, LIV-4 |
| CT-4 | input/dependency fault corpus (every FM row) | FM-*, LIV-2, LIV-7, INV-31, INV-32 |
| CT-5 | interface conformance (wire fixtures IB-1…IB-11; persisted-format round-trips IB-20…IB-28 incl. cross-implementation resume; decode/selection fixtures; observability golden scrapes IB-40…IB-46; error-code registry sync IB-50…IB-52) | REQ-23, INV-5, INV-21, INV-23, CN-45 |
| CT-6 | performance benchmarks (S1–S6, SLO gates, saturation knee) | SLI-*, PF-*, LIV-3, LIV-5 |
| CT-7 | soak/endurance (S4 sparse coverage honesty, memory plateau, reclamation) | INV-4, REQ-20, LIV-8, LIV-9, CN-24 |
| CT-8 | isolation (co-resident pipes, dual-instance where lock declared, legacy-migration races) | INV-15, INV-35, LIV-10, RP-31, RP-32 |
| CT-9 | fuzz (portal wire bytes; decoder inputs; persisted-state bytes) | FM-1, FM-11, WP-16, CN-43 |

**Kill-point matrix (CT-2), per class**: T — before/inside/after transaction ·
W — after intent, mid-append, after append pre-commit-record, after commit record ·
K — pre-start-state, mid-unit-write, post-publish pre-state-rename, mid-rename, post-state ·
A — after data pre-cursor, after cursor ·
C — after the local commit pre-publish, mid-publish, after publish pre-confirmation ·
all — during recovery itself, during fork.

*Status (class K, S1):* every K point runs after a first commit: mid-unit-write (cut
after the header), post-publish before the state write and before the state rename,
mid-rename (one table published, the other finished in temp), post-publish of a unit
that straddles the cursor, and post-state. Before the first commit: before the start
state's rename, mid-unit-write over the start state, and post-publish before the first
commit's state rename, after which recovery leaves no unit. Recovery is killed before its
temp sweep and before its over-cursor sweep, from a commit and from a start state, and a
restart is killed again in its first checkpoint, from a commit and from a start state
(double kill, CN-42). "During fork" does not apply: the
finalized-only route takes no fork (WP-7). Not checked: INV-43's byte identity,
because unit boundaries after a restart are free.

*Status (class T, S1):* a batch transaction is killed before its `begin`, inside it
after the data writes and after the cursor write, with its `commit` sent and
unanswered, and after the `commit` is acknowledged; every kill leaves the last commit
the server acknowledged, and the unanswered commit leaves either commit point (CN-40).
The first batch transaction is killed inside, which leaves no state and a cold
restart. Recovery is killed inside the undo-log trigger set-up, on a fresh store and
after a crash (INV-41), and a restart is killed again in its first transaction
(double kill, CN-42). Kill points name a transaction by the block its batch tags the
undo log with, not by its place in the run.

*Status (class T, S3 with one fork):* a batch above the floor is killed inside, after
its rows and undo records, which leaves neither, and after its commit, which leaves
both. The fork's transaction is killed before its commit, which leaves the forked
branch and a restart that forks again, and again in that restart (WP-46). Killed after
the fork's commit, the store holds the ancestor with the current floor and a trimmed
chain. Not checked: the undo log's restore of an updated or deleted row, because S3
only inserts; retention's cleanup steps.

*Additional S3 coverage:* `fork-depth.test.ts` runs depths 1, 3 and 6 (the last
rolls back exactly to the finalized block), with one or seven blocks per response,
for classes T, A, C and W. It also kills the rollback at two protocol steps at the
finality boundary, restarts it, and repeats a pre-commit crash before resuming.
The same store/oracle/end-state checks used by the original S3 matrix apply.
Classes T and A additionally assert E1003 for a fork that replaces the finalized
block, with the stored rows and cursor preserved. Class W uses the existing
project-gated suite.

## Structural validators (kind-agnostic, always on)

Applicable to any emission/state without domain knowledge: decodable (parses per
format) · ordered (attribution never descends; one block may hold several rows) ·
linked (each batch strictly ascending inside the configured ranges, its first block the
next one the range set holds after the cursor) ·
items-belong-to-parent (rows within their unit's window) · in-range (attribution within
configured ranges) · coverage partition (file sinks: windows disjoint, no gap inside the
configured ranges, `V` = last window end + 1) · watermark coherence (RC above F; C
consistent with data bound) · route-mode coherence (classes K/∅ bound to finalized-only
delivery) · sequence coherence (class C: versions on a topic never rewound, and
gap-free unless the barrier is waived, RP-45; a repeated version is an outbox
republish and must repeat the same operation) · no residue (nothing in flight left in
the store, INV-42).

## Traceability matrix (2026-11-02)

Status: **C** covered · **P** partial · **U** unchecked. Suffix ⚠ = known-violated or
known-suspect in the current implementation (see gap register).

| Property | CT class | Status | Note |
|---|---|---|---|
| WP-1, WP-2 (resume, floor seed) | CT-1/2 | C | portal-source + watermark tests; CT-2 S1: every restart asks first for the block after the recovered cursor, anchored on it (classes K, T, A and C); class A: a restart whose portal reports a lower floor keeps the persisted one |
| WP-3 (id validation) | CT-1 | C | a blank id fails with E0001 (portal-source suite); a target driven without a source id or an explicit key fails with E0001 (cursor-key and target suites) |
| WP-4 (range resolution) | CT-1 | C | range-algebra + builder tests |
| WP-5/CN-40…CN-44 recovery-before-write | CT-2 | P ⚠ | classes K, T and A: the CT-2 kill-point matrix on S1, where the oracle requires the recovered state to be a commit point (CN-40), rollback records included for class T; before the first cursor write classes K and A recover over their start state (IB-22) and start cursor (ADR-15); class C: the CT-2 matrix on S1 and S3, where the oracle requires the stand-in bus and the outbox together to hold exactly the changelog up to the recovered cursor, and nothing the state did not record (CN-17); class W: the CT-2 matrix on S1 and S3 against a real BigQuery project (`BIGQUERY_TEST_PROJECT`), where the oracle requires the recovered state to be a commit point, with every row above the recovery cursor inside the range the in-flight WAL row names (CN-14); orphan guard: class K refuses data with no state (unit suite), class W probes table-wide rather than key-scoped (GAP-20) |
| INV-40…INV-42 (recovery soundness, idempotence, no orphans) | CT-2 | P ⚠ | classes K, T and A: every CT-2 point on S1, and for classes T and A on S3, ends in the scenario's end state with no residue, crashes during recovery and a second crash in the restart included; before the first commit classes K and A recover over their start state (IB-22) and start cursor (ADR-15); after a fork, class T holds the ancestor with the current floor and a trimmed chain; class C: every CT-2 point on S1 and S3 ends in the scenario's end state with an empty outbox, a second crash in the recovery publish and in the fork transaction included; class W: every CT-2 point on S1 and S3 ends in the scenario's end state with no residue, a crash in the restart's recovery, a second crash before the fork's WAL row and a restart that begins while the killed run's rollback DELETEs still run included |
| INV-43 (re-fetch byte identity) | CT-2 | P | finalized S1 reprocessing with fixed response/unit boundaries compares all stored bytes; arbitrary crash/repartition recovery still compares rows, not bytes |
| INV-44 (refuse-before-delete) | CT-2 | P | class K refuses a straddling unit the coverage does not explain (E2317, target suite); state suites now include malformed cursor corpora for T/A/W, malformed state fields for K, unsupported schema versions for C and invalid W recovery bounds, with preservation assertions; cross-field and older-record corruption remain open |
| WP-7, RP-7, DEF-15, INV-25, IB-11 (finalized-only selection) | CT-1/5 | C | portal-source asserts hot override + warning, finalized `latest` and its no-finality fallback, transformer-start view honouring an explicit hot head, per-pipe (non-sticky) selection, effective cache client and node-cache forwarding, quiet pre-finalized mode, and unchanged non-requiring mode; Parquet/memory assert direct delivery and the coded impossible-fork refusal; CT-1 S1 diffs the class-K store against the oracle under a lagging floor and with no finality |
| WP-10, WP-15, INV-20 (ordering) | CT-1 | P | buffer/split unit tests; the CT-1 S1 lockstep oracle refuses a batch whose cursor descends or is not the chain's block, and the diff against S1's fixed end state catches a skipped block that holds rows, and a run that ends early (class K); the linked validator checks block order within a batch, but no CT feeds it delivered batches yet; the portal client refuses a duplicate, out-of-order or out-of-range block (E1006, ADR-1), asserted with scripted responses at client and pipe level; the chain simulator has no adversarial mode |
| WP-11, HZ-1 (assembly bounds) | CT-1 | C | stream-buffer tests |
| WP-12 (backpressure) | CT-1/6 | P | unit-level only; no end-to-end saturation test |
| WP-14 (transport retry) | CT-4 | P | counts tested; reconnect-mid-stream unmerged |
| WP-16, FM-11 (malformed input) | CT-9 | P | the client refuses an invalid-JSON line and a line without a block number (E1007), requests a body cut inside a block again from it, and refuses a second cut at the same block, all against the chain simulator; a schema-violating block fails with E1008 naming block and field, asserted on the cast helper and end to end on evm; no fuzz corpus |
| WP-20…WP-22 (compose, validate, filter) | CT-1/5 | C | decoder + portal-source suites; the evm decoder drops a log another output requested that its indexed-parameter filter excludes |
| WP-23 (decode-error policy) | CT-5 | C | unified via `core/decode-error.ts` (ADR-12); fatal-default + suppress-and-count asserted on both modules |
| WP-24 (attribution uniqueness) | CT-5 | C | evm refuses entries that can match one log (E0004) and accepts disjoint filters and differing topic counts; solana refuses a shared discriminator and an instruction that is not one of its `program` (E0003), and warns when the deprecated `programId` lists several addresses |
| WP-25 (query union) | CT-1 | C | merge/heap tests + multi-output isolation |
| WP-30…WP-32 (lifecycle) | CT-1 | P | stop-once + partial-start tested; across a resolved fork, the start and stop hooks and the metrics server run once each |
| WP-40…WP-43 (fork core) | CT-3 | C ⚠ | core fork, portal-source, and fork-capable target suites; WP-42 text normativized from the tested deep-fork/floor-fallback semantics; K/∅ excluded by WP-7 |
| WP-41 (empty canonical) | CT-4 | C | a 409 with an empty or absent chain fails with E1002, asserted with scripted responses at client level |
| WP-44 (finality conflict) | CT-3 | P | Resolver unit tests: the search stops at the first block below `F`, so a match, a deep-fork restart or an older floor below `F` gives ⊥. `F` is the highest floor of all records, also of a record with an empty chain or after the match. `F` is the ancestor only when no older record holds a canonical block above it. The oracle pins the halt and the fallback. A pipe test stops with E1003 when the fork replaces the finalized block. CT-3 asserts E1003 and preservation of stored rows and cursor for classes T, A and W (W project-gated); class C follows CN-35 instead: compensate to the finalized baseline, drain, then halt |
| WP-46 (rollback idempotence) | CT-2/3 | P | netting idempotence tested (one binding); classes T, A and C: CT-2 S3 kills the fork's rollback twice in a row and converges; others U |
| WP-47 (depth bound) | CT-3 | P ⚠ | retention pruning tested; deep-fork restart e2e U |
| RP-1…RP-6 (write loop) | CT-1 | C | per-target state suites |
| RP-21, RP-22, INV-4 (coverage) | CT-7 | C | parquet coverage-naming suites: sparse stretch, trailing empty unit, disjoint-range gap, resume mid-gap |
| RP-30…RP-32 (keying, migration) | CT-8 | P ⚠ | happy path C; concurrent migration U (GAP-7) |
| RP-42 (delegated repair) | CT-2 | P ⚠ | with the hook, every class-A CT-2 point recovers, a crash after one table's cancel rows included; without it, the restart warns and stores the crash window a second time: the accepted deviation, pinned (ADR-15); before the first cursor write the hook is called with the start cursor and the restart converges; a hook that binds the cursor as unsigned fails at a range start of 0 with nothing removed (ADR-15) |
| RP-43 (rollback completeness) | CT-3 | C | the oracle accepts a canonical window below the cursor and resumes per WP-42; class W bounds the fork delete by its own top (CN-31), so a short window deletes every orphan row |
| CN-10 T atomicity | CT-2 | C | live tx tests; CT-2 on S1 kills the batch transaction before, inside and after it, with its commit unanswered too, and on S3 a batch above the floor with its undo records; every crash leaves the last commit the server acknowledged; a serialization failure is retried and commits, an integrity fault and the lock refusal (E2103) halt at the first attempt; serializable isolation is the default, and an explicit weaker level reaches Drizzle unchanged; batch commits stay atomic, but serial equivalence with other writers is not guaranteed |
| CN-11 W protocol | CT-2 | P | unit/lifecycle C; integration gated off CI |
| CN-12 K protocol | CT-1/2 | C | finalized-route selection plus crash-safety + recovery suites; straddle refusal asserted (E2317); CT-2 kills S1 at every protocol step, a stretched unit's straddle and the start state included |
| CN-13 A protocol | CT-2 | C ⚠ | CT-2 kills S1 before, between and after the data inserts, before, during and after the cursor write, and in recovery's cancel rows; S3 in a batch above the floor and in the fork's rollback; in the first batch, after its start cursor, after its data, after its cursor write, in its recovery's cancel rows and again in its replay; the no-hook window is an accepted deviation (ADR-15) |
| CN-17, CN-35, CN-46 C protocol | CT-2/3 | P ⚠ | CT-1 and CT-2 on S1 and S3 against a Pub/Sub stand-in that keeps what it acknowledged: a kill at each statement of the batch, confirmation and fork transactions and at each publish leaves the outbox the bus lacks, and the restart publishes it first, sequence- and byte-identically; the compensations go out before the new branch; fold-per-id, baseline restore and the route-declared inverse (materialized routes) only against a publisher seam; the lost-sequencer exposure is operational only (GAP-38) |
| RP-24, RP-44 (changelog contract) | CT-5 | P ⚠ | canonical codec tested as a contract (accept/reject table + golden bytes); attribute inheritance through forks asserted; a consumer that applies the stand-in bus by version ends on the scenario's rows after every class-C CT-1 and CT-2 run (event routes); untested against a real subscription |
| RP-45 (shared-feed contract) | CT-5 | P ⚠ | one payload shape, the split-counter and backwards-block refusals, and the finality reference (stamped on every message, on compensations, and on a restart's recovery drain) asserted against a publisher seam, and on every message of the class-C CT-1 and CT-2 runs; the quiet-table stall and the consumer's max-read are stated limits, untested against a real subscription |
| CN-34 (fork commit point) | CT-2 | P ⚠ | class T: CT-2 S3 kills inside and after the fork transaction; after it, the store holds the ancestor, the current floor and the trimmed chain; class A: CT-2 S3 kills before and after the ancestor write, inside and after rollback, and in the next batch; each crash leaves the ancestor, the floor unchanged and the chain trimmed, with only class-A residue above the cursor; class C: CT-2 S3 kills inside and after the fork transaction; after it, the state holds the ancestor, the floor unchanged and the chain trimmed, with the compensations in the outbox; other classes U |
| CN-45 (cross-impl, clock indep.) | CT-5 | U ⚠ | single implementation; the class-A and class-W bindings resume from the last commit under a clock set back across a restart; the cache reads a value that any codec or `compress` option wrote |
| INV-1, INV-3, INV-5 (state well-formedness) | CT-1 | P | structural validators and the diff against S1's fixed end state on the class-K, class-T and class-A stores (two tables) at quiescence, class K with and without finality, and on the committed part after every CT-2 kill; on the class-C state and the rows a consumer of the stand-in bus and the outbox holds, at quiescence and after every CT-2 kill; on the class-W store (its WAL rows and tracked tables) against the project `BIGQUERY_TEST_PROJECT` names, at quiescence and on the committed part after every CT-2 kill |
| INV-2, INV-12 (floor) | CT-1/2 | C | adversarial regression/genesis tests; source, cache and class-T fork tests split a response without the finalized header against `F`, also when the cache delivered `F`; class T keeps the current floor at a fork |
| INV-10 (frame condition) | CT-1 | P | CT-1 S1: a restart with nothing left to read leaves the class-K store byte-identical, the reads of the class-T and class-A stores unchanged, and the class-C state and bus unchanged; no idle soak (CT-7) |
| INV-13, INV-14 (fork safety) | CT-3 | C | at batch boundaries; mid-flush U; the fork search stops at `F`, and the pipe halts before the sink removes data (WP-44) |
| INV-15, INV-35, LIV-10 (isolation) | CT-8 | U ⚠ | no co-resident/dual-instance tests; the Parquet binding's refusal of a directory under another key's state is unit-tested, two first runs started together pass it (ADR-16) |
| INV-21, INV-23 (fidelity, selection) | CT-5 | C | validator fixtures incl. chain edge shapes |
| INV-22 (determinism) | CT-1 | P | CT-1 S1: two runs under different response partitionings commit the same rows (class K); no cross-implementation diff |
| INV-30…INV-32 (reporting) | CT-1/4 | P ⚠ | metric registration tested; honesty cross-check U; OB signals unimplemented, OB-13 log-only (GAP-28) |
| INV-36 (cache) | CT-5 | C | keying/gap/latest-exclusion tests |
| PF-6, WP-12 (ingest overlap) | CT-6 | P | slot semantics unit-tested; no cadence benchmark |
| LIV-1…LIV-9 | CT-4/6/7 | U | no liveness harness yet; CT-1 S1 asserts only that a bounded run ends at its range end, not the stall budget |
| WP-31, WP-33, LIV-6, FM-33, RP-8 (signal-managed shutdown) | CT-1/2 | P ⚠ | the stop semantics are asserted in process against the chain simulator: idle reads, retries, startup, a commit and a rollback in progress, repeated signals, several pipes, callback order and failures, the deadline, no abort listener or batch retained per batch. Real SIGTERM and SIGINT reach child processes: exit codes, the opt-out, the listeners gone after the deadline. Class C also runs signal, restart and forced-kill runs against the stand-in bus on S1. Class W drains against fake clients only, and its forced-kill guarantee rests on its CT-2 matrix; class K stops and restarts in process with no gap or repeat; classes T and A have no stop run of their own, and class A asserts only that the author's client stays open; the budget's P-SHUTDOWN-DRAIN-S value is not measured |
| RS-10, RS-11 (purity, stateful) | CT-2 | P | K recovery relies on it; factory rollback C |
| RS-20…RS-25 (cache) | CT-5 | C | except overlap re-insert (U) |
| FM corpus | CT-4 | P | scattered unit coverage; no systematic corpus |
| SLI/PF | CT-6 | P | S2/S3 head-following and fork sweep (`packages/pipes/conformance/perf`): SLI-4 with its detect, rollback and resume parts, head lag and the saturation knee per class (memory, T, A), with portal and store latency injected; S1, S4–S6, SLI-7 and SLO gates not built |
| IB-1…IB-11 wire | CT-5 | P | client-side tested, including finalized route selection; CT-1 S1 checks the IB-3 anchor of every request; no golden wire fixtures |
| IB-20…IB-28 formats | CT-5 | P ⚠ | per-binding tests plus fresh-reader writer-output round-trips for T/A/W and file reopen for K/C; class-T timestamp-unit handling is covered by `conformance/postgres/cursor-timestamp`; second-implementation compatibility is unverified |
| IB-40…IB-46 observability | CT-5 | P ⚠ | the CORS origin check and the /profiler `enabled` flag are tested over HTTP; consumed by dashboard, no golden scrapes (GAP-16) |
| IB-50…IB-52 error registry | CT-5 | C | declared codes synced with `docs/errors.md` and the IB-50 table by the spec check, run on specification, error-source and error-document changes |

## Gap register (2026-11-02)

Priorities: P0 active production risk · P1 correctness hole, plausible trigger ·
P2 bounded/rare · P3 polish. "First test" = cheapest failing-test-first entry point.

| GAP | Statement | Violated | Pri | First test |
|---|---|---|---|---|
| GAP-7 | Legacy cursor migration in one binding is unlocked: concurrent first runs can both adopt legacy state and inherit a foreign monotonic floor that never self-corrects | RP-32, INV-35 | P2 | two concurrent first runs against legacy-keyed store; ≤ 1 adopts |
| GAP-16 | Observability payloads are consumed by the dashboard but have no golden fixtures; shapes drift silently (a version-drift accommodation already exists in the dashboard) | IB-40…IB-46 | P3 | golden scrape of /stats, /metrics, preview against a scripted run |
| GAP-20 | The orphan-data guard is table-wide where it must be key-scoped: the write-ahead binding (E2212) probes the tracked table with no key filter while reading its sync row by key, so a co-resident pipe's legitimate first run into a populated shared table is refused — one pipe's startup made dependent on another's data, blocking a configuration INV-35/RP-41 support. The other bindings run no guard at all and restart from scratch over genuinely orphan data | CN-44, INV-35, REQ-3 | P2 | second pipe id, first run into a table another pipe populates → assert start, not refusal; then on declared-exclusive tables delete cursor state and assert coded refusal per binding |
| GAP-28 | Required observability signals with no IB binding and no implementation: OB-6 head gauge, OB-20 lifecycle timestamps, OB-21 publication lag, OB-30 terminal-error level read, OB-32 capture-on-stall. OB-13 is partial: a stall watchdog logs a warning after a fixed delay, but exposes nothing a scraper can read and carries no reason code. /stats pre-first-batch zeros defeat OB-1/OB-4 on that surface | OB-6/13/20/21/30/32, REQ-10 | P2 | bind each in IB-41/IB-42, then golden scrapes (with GAP-16) |
| GAP-38 | A class-C sink's producer sequence lives only in its local state, and the wire carries no producer-lineage field: a state that was lost or restored from a backup restarts the sequence, republishes low versions under ids consumers already hold at higher ones, and every affected row silently freezes while event topics keep looking healthy. The producer refuses to start on an empty state file (E2420), but a state restored from a backup opens as an ordinary resume and rewinds the sequence undetected; a bootstrap declared with `allowColdStart` under a namespace that has already published cannot be detected by either side in-band either. Both are handled operationally (`sqd_pubsub_cold_start` gauge + runbook), not structurally (ADR-21) | CN-17, CN-46, RP-24 | P2 | publish, wipe the state, resume under the same namespace → assert the refusal; restore an older state file → assert detection; declare the bootstrap and assert a consumer cannot distinguish the stale stream from a healthy one (documents the residual exposure) |
| GAP-40 | The class-C block-order check walks a batch in sequencing order, which is route by route rather than block by block, so a second route on the same topic is refused (E2429) whenever it emits an operation below the previous route's last block in the batch. Nothing the mapper emits can avoid it — the concatenation order is the sink's — so several routes per topic and the sequence barrier are mutually exclusive in practice, currently by refusal and documentation rather than by ordering the operations before they are sequenced | RP-45, IB-28 | P2 | two routes on one topic, one batch spanning blocks *N*…*N+2* → assert the operations are sequenced in block order, not E2429 |

## Build order

Remaining work, in dependency order. Completed work is recorded in the traceability
matrix above.

1. **Correctness core (CT-2/3/5):** extend the corruption corpus to cross-field
   contradictions, older rollback records and Pub/Sub cursor/outbox bytes; verify
   cross-implementation format compatibility. Retain the existing kill-point and
   fork matrices. Exit: INV/CN/WP matrix rows at least P everywhere, C on the
   fork/recovery core.
2. **Robustness (CT-4/8/9):** adversarial simulator histories (over-return,
   duplicate and out-of-order delivery, head regressions), a systematic FM corpus,
   isolation tests and fuzzing.
3. **Performance and endurance (CT-6/7):** baselines in
   [11-performance.md](11-performance.md), SLO ratification (ADR-14) and soaks.

Update the traceability matrix and open gap register as each item is completed.
