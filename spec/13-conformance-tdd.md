# 13 — Conformance & TDD (CT-n, GAP-n) — MUTABLE

Last updated: 2026-10-24. Statuses reflect the actual TypeScript test inventory at
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
oracle, the structural validators, store probes for classes K, T and A and kill-point
injectors for all three live in `packages/pipes/conformance/`; CT-1 and CT-2 run S1
against classes K, T and A, and S3 reduced to one fork against classes T and A. On S3
the oracle replays the portal's request log: a batch's blocks and floor come from the
answer that served it, a fork's canonical chain from the 409, never from the pipe. A
kill is a true kill: a child process runs the pipe and stops itself with SIGKILL at one
step of the commit protocol or of recovery — a file operation for class K, an SQL
statement for class T, an HTTP request for class A — so nothing unwinds and the store
holds what a crash leaves. For class T the harness waits until the server has dropped
the child's connections, and with them any transaction it held open; for class A, until
the server runs none of the child's queries. The class-A probe reads each table net of
its cancel rows, with merges stopped and no folding on insert, so a row stored twice
reads twice. The child
journals every batch before the store sees it, and the oracle follows the journal. Not
built yet: kill points and store probes for classes W and C, the corrupted-state corpus,
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
  portal ← effective-view(mode)
  resolve symbolic ranges through portal
  pass the same portal view to transformers and caches

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
  RC ← [b ∈ processed : b.number > F] if mode = full, else []   # INV-1; finalized-only keeps no fork history (WP-47)
  rows ← transform(blocks)                            # pure per RS-10
  commit(D += rows, C ← last(blocks), F, RC)          # per class CN-10…CN-14, CN-17
  R ← R ++ [⟨RC, F⟩]                                  # one record per commit; retention trims (WP-47)
  wellformed_check()

fork(canonical):                                     # T-FORK
  require mode = full else CODED-ERROR                # WP-45 / FM-13
  require canonical ≠ ∅ else CODED-ERROR              # WP-41
  require max(canonical).number ≥ C.number else CODED-ERROR   # RP-43 (class W enforces today — GAP-9)
  A ← search(R, canonical, F)                         # WP-42
  if A = ⊥ → CODED-ERROR                              # no ancestor at or above F (E1003, WP-44)
  D ← D − rows(> A.number)                            # class C compensates instead (CN-35)
  C ← A; RC ← RC[≤ A.number]; R ← R ++ [⟨RC, F⟩]      # F unchanged; INV-13/14
  commit-point(fork)                                   # CN-34 (class A deviates — GAP-21)

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
K — mid-unit-write, post-publish pre-state-rename, mid-rename, post-state ·
A — after data pre-cursor, after cursor ·
C — after the local commit pre-publish, mid-publish, after publish pre-confirmation ·
all — during recovery itself, during fork.

*Status (class K, S1):* every K point runs after a first commit: mid-unit-write (cut
after the header), post-publish before the state write and before the state rename,
mid-rename (one table published, the other finished in temp), post-publish of a unit
that straddles the cursor, and post-state. Before the first commit, mid-unit-write
converges and post-publish before the state rename is GAP-41's pin. Recovery is killed
before its temp sweep and before its over-cursor sweep, and a restart is killed again
in its first checkpoint (double kill, CN-42). "During fork" does not apply: the
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
the fork's commit, the store holds the ancestor's own commit point, not the fork's:
GAP-43, pinned. Not checked: the undo log's restore of an updated or deleted row,
because S3 only inserts; retention's cleanup steps.

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

## Traceability matrix (2026-10-24)

Status: **C** covered · **P** partial · **U** unchecked. Suffix ⚠ = known-violated or
known-suspect in the current implementation (see gap register).

| Property | CT class | Status | Note |
|---|---|---|---|
| WP-1, WP-2 (resume, floor seed) | CT-1/2 | C | portal-source + watermark tests; CT-2 S1: every restart asks first for the block after the recovered cursor, anchored on it (classes K, T and A); class A: a restart whose portal reports a lower floor keeps the persisted one |
| WP-3 (id validation) | CT-1 | P ⚠ | rejects, but uncoded (GAP-4) |
| WP-4 (range resolution) | CT-1 | C | range-algebra + builder tests |
| WP-5/CN-40…CN-44 recovery-before-write | CT-2 | P ⚠ | classes K, T and A: the CT-2 kill-point matrix on S1, where the oracle requires the recovered state to be a commit point (CN-40), rollback records included for class T; before the first cursor write class K skips its sweep (GAP-41) and class A its repair hook (GAP-44), both pinned by CT-2; classes W and C have no kill-point matrix (GAP-14); orphan guard W-only and table-wide rather than key-scoped (GAP-20) |
| INV-40…INV-42 (recovery soundness, idempotence, no orphans) | CT-2 | P ⚠ | classes K, T and A: every CT-2 point on S1, and for classes T and A on S3, ends in the scenario's end state with no residue, crashes during recovery and a second crash in the restart included; GAP-41 and GAP-44 break classes K and A before the first commit; after a fork, GAP-43 leaves class T and GAP-21 class A a state no commit wrote; classes W and C U (GAP-14) |
| INV-43 (re-fetch byte identity) | CT-2 | U | unit boundaries after a restart are free, so CT-2 diffs rows, not bytes |
| INV-44 (refuse-before-delete) | CT-2 | P | class K refuses a straddling unit the coverage does not explain (E2317, target suite); no corrupted-state corpus |
| WP-7, RP-7, DEF-15, INV-25, IB-11 (finalized-only selection) | CT-1/5 | C | portal-source asserts hot override + warning, finalized `latest` and its no-finality fallback, transformer-start view honouring an explicit hot head, per-pipe (non-sticky) selection, effective cache client and node-cache forwarding, quiet pre-finalized mode, and unchanged non-requiring mode; Parquet/memory assert direct delivery and the coded impossible-fork refusal; CT-1 S1 diffs the class-K store against the oracle under a lagging floor and with no finality |
| WP-10, WP-15, INV-20 (ordering) | CT-1 | P | buffer/split unit tests; the CT-1 S1 lockstep oracle refuses a batch whose cursor descends or is not the chain's block, and the diff against S1's fixed end state catches a skipped block that holds rows, and a run that ends early (class K); the linked validator checks block order within a batch, but no CT feeds it delivered batches yet; the portal client refuses a duplicate, out-of-order or out-of-range block (E1006, ADR-1), asserted with scripted responses at client and pipe level; the chain simulator has no adversarial mode |
| WP-11, HZ-1 (assembly bounds) | CT-1 | C | stream-buffer tests |
| WP-12 (backpressure) | CT-1/6 | P | unit-level only; no end-to-end saturation test |
| WP-14 (transport retry) | CT-4 | P | counts tested; reconnect-mid-stream unmerged |
| WP-16, FM-11 (malformed input) | CT-9 | U ⚠ | malformed and truncated final lines surface as a raw parse error (GAP-5); validation errors uncoded (GAP-25) |
| WP-20…WP-22 (compose, validate, filter) | CT-1/5 | C | decoder + portal-source suites; the evm decoder drops a log another output requested that its indexed-parameter filter excludes |
| WP-23 (decode-error policy) | CT-5 | C | unified via `core/decode-error.ts` (ADR-12); fatal-default + suppress-and-count asserted on both modules |
| WP-24 (attribution uniqueness) | CT-5 | C | evm refuses entries that can match one log (E0004) and accepts disjoint filters and differing topic counts; solana refuses a shared discriminator (E0003) |
| WP-25 (query union) | CT-1 | C | merge/heap tests + multi-output isolation |
| WP-30…WP-32 (lifecycle) | CT-1 | P ⚠ | stop-once + partial-start tested; fork path re-fires start/stop per segment (GAP-22) |
| WP-40…WP-43 (fork core) | CT-3 | C ⚠ | core fork, portal-source, and fork-capable target suites; WP-42 text normativized from the tested deep-fork/floor-fallback semantics; K/∅ excluded by WP-7; a full stream without finality keeps no chain to search (GAP-42) |
| WP-41 (empty canonical) | CT-4 | U ⚠ | an empty or absent chain surfaces as a raw 409 HTTP error, pinned by a client test (GAP-8) |
| WP-44 (finality conflict) | CT-3 | P | Resolver unit tests: the search stops at the first block below `F`, so a match, a deep-fork restart or an older floor below `F` gives ⊥. `F` is the highest floor of all records, also of a record with an empty chain or after the match. `F` is the ancestor only when no older record holds a canonical block above it. The oracle pins the halt and the fallback. A pipe test stops with E1003 when the fork replaces the finalized block. No built-in target or CT-3 test asserts the halt |
| WP-46 (rollback idempotence) | CT-2/3 | P | netting idempotence tested (one binding); classes T and A: CT-2 S3 kills the fork's rollback twice in a row and converges; others U |
| WP-47 (depth bound) | CT-3 | P ⚠ | retention pruning tested; deep-fork restart e2e U; a full stream without finality retains nothing (GAP-42) |
| RP-1…RP-6 (write loop) | CT-1 | C | per-target state suites |
| RP-21, RP-22, INV-4 (coverage) | CT-7 | C | parquet coverage-naming suites: sparse stretch, trailing empty unit, disjoint-range gap, resume mid-gap |
| RP-30…RP-32 (keying, migration) | CT-8 | P ⚠ | happy path C; concurrent migration U (GAP-7) |
| RP-42 (delegated repair) | CT-2 | P ⚠ | with the hook, every class-A CT-2 point recovers, a crash after one table's cancel rows included; without it, the restart warns and stores the crash window a second time: the accepted deviation, pinned (ADR-15); before the first cursor write the hook is never called (GAP-44) |
| RP-43 (contract guard) | CT-3 | P ⚠ | one binding only (GAP-9) |
| CN-10 T atomicity | CT-2 | C ⚠ | live tx tests; CT-2 on S1 kills the batch transaction before, inside and after it, with its commit unanswered too, and on S3 a batch above the floor with its undo records; every crash leaves the last commit the server acknowledged; retry and isolation ungated (GAP-26) |
| CN-11 W protocol | CT-2 | P | unit/lifecycle C; integration gated off CI |
| CN-12 K protocol | CT-1/2 | C ⚠ | finalized-route selection plus crash-safety + recovery suites; straddle refusal asserted (E2317); CT-2 kills S1 at every protocol step, a stretched unit's straddle included; first-checkpoint crash window open (GAP-41) |
| CN-13 A protocol | CT-2 | C ⚠ | CT-2 kills S1 before, between and after the data inserts, before, during and after the cursor write, and in recovery's cancel rows; S3 in a batch above the floor and in the fork's rollback; the first batch's window is never repaired (GAP-44); the no-hook window is an accepted deviation (ADR-15) |
| CN-17, CN-35, CN-46 C protocol | CT-2/3 | P ⚠ | fold-per-id, baseline restore, route-declared inverse, seq monotonicity across forks and the commit-then-publish crash window are asserted against a publisher seam; no live-bus conformance run, and the lost-sequencer exposure is operational only (GAP-38) |
| RP-24, RP-44 (changelog contract) | CT-5 | P ⚠ | canonical codec tested as a contract (accept/reject table + golden bytes); attribute inheritance through forks asserted; consumer-side convergence untested against a real subscription |
| RP-45 (shared-feed contract) | CT-5 | P ⚠ | one payload shape, the split-counter and backwards-block refusals, and the finality reference (stamped on every message, on compensations, and on a restart's recovery drain) asserted against a publisher seam; the quiet-table stall and the consumer's max-read are stated limits, untested against a real subscription |
| CN-34 (fork commit point) | CT-2 | P ⚠ | class T: CT-2 S3 kills inside and after the fork transaction; after it, the store holds the ancestor's own floor and chain (GAP-43); class A: CT-2 S3 kills inside and after the fork's rollback; both leave the forked branch's cursor over rolled-back rows (GAP-21); other classes U |
| CN-45 (cross-impl, clock indep.) | CT-5 | U ⚠ | single implementation; clock ordering in two bindings (GAP-10); cache codec undiscriminated (GAP-27) |
| INV-1, INV-3, INV-5 (state well-formedness) | CT-1 | P | structural validators and the diff against S1's fixed end state on the class-K, class-T and class-A stores (two tables) at quiescence, class K with and without finality, and on the committed part after every CT-2 kill; no store probe for classes W and C |
| INV-2, INV-12 (floor) | CT-1/2 | C ⚠ | adversarial regression/genesis tests; class T lowers the persisted floor at a fork (GAP-43) |
| INV-10 (frame condition) | CT-1 | P | CT-1 S1: a restart with nothing left to read leaves the class-K store byte-identical and the reads of the class-T and class-A stores unchanged; no idle soak (CT-7) |
| INV-13, INV-14 (fork safety) | CT-3 | C | at batch boundaries; mid-flush U; the fork search stops at `F`, and the pipe halts before the sink removes data (WP-44) |
| INV-15, INV-35, LIV-10 (isolation) | CT-8 | U ⚠ | no co-resident/dual-instance tests; Parquet recovery acts on a co-resident pipe's files (GAP-35) |
| INV-21, INV-23 (fidelity, selection) | CT-5 | C | validator fixtures incl. chain edge shapes |
| INV-22 (determinism) | CT-1 | P | CT-1 S1: two runs under different response partitionings commit the same rows (class K); no cross-implementation diff |
| INV-30…INV-32 (reporting) | CT-1/4 | P ⚠ | metric registration tested; honesty cross-check U; OB signals unimplemented, OB-13 log-only (GAP-28) |
| INV-36 (cache) | CT-5 | C | keying/gap/latest-exclusion tests |
| PF-6, WP-12 (ingest overlap) | CT-6 | P | slot semantics unit-tested; no cadence benchmark |
| LIV-1…LIV-9 | CT-4/6/7 | U | no liveness harness yet; CT-1 S1 asserts only that a bounded run ends at its range end, not the stall budget |
| RS-10, RS-11 (purity, stateful) | CT-2 | P | K recovery relies on it; factory rollback C |
| RS-20…RS-25 (cache) | CT-5 | C | except overlap re-insert (U) |
| FM corpus | CT-4 | P | scattered unit coverage; no systematic corpus |
| SLI/PF | CT-6 | U | no benchmarks recorded |
| IB-1…IB-11 wire | CT-5 | P | client-side tested, including finalized route selection; CT-1 S1 checks the IB-3 anchor of every request; no golden wire fixtures |
| IB-20…IB-28 formats | CT-5 | P ⚠ | per-binding tests; no round-trip corpus; timestamp units per-network (GAP-24) |
| IB-40…IB-46 observability | CT-5 | U ⚠ | consumed by dashboard, no golden scrapes (GAP-16) |
| IB-50…IB-52 error registry | CT-5 | P ⚠ | declared codes synced with `docs/errors.md` by the spec check; the IB-50 table is outside the sync (GAP-13) |

## Gap register (2026-10-24)

Priorities: P0 active production risk · P1 correctness hole, plausible trigger ·
P2 bounded/rare · P3 polish. "First test" = cheapest failing-test-first entry point.

| GAP | Statement | Violated | Pri | First test |
|---|---|---|---|---|
| GAP-4 | Blank-pipe-id guard throws an uncoded error; the documented code exists but is dead | REQ-13, WP-3 | P3 | blank id + sink → assert the coded configuration error |
| GAP-5 | Malformed and truncated NDJSON lines are not told apart, and neither is coded: a line split across chunks is rejoined, but a malformed line — or an unterminated final line at a clean end of body — surfaces as a raw parse error | WP-16, FM-11 | P2 | send one malformed line (must fail coded, naming the block); end a body mid-line (must re-request or fail coded) |
| GAP-7 | Legacy cursor migration in one binding is unlocked: concurrent first runs can both adopt legacy state and inherit a foreign monotonic floor that never self-corrects | RP-32, INV-35 | P2 | two concurrent first runs against legacy-keyed store; ≤ 1 adopts |
| GAP-8 | A fork response with an empty or absent canonical chain is not read as a fork: it surfaces as a raw 409 HTTP error, so the coded guard is unreachable, and a client test pins the uncoded error | WP-41, FM-14 | P3 | 409 with an empty `previousBlocks` → assert the coded contract violation, not an HTTP error |
| GAP-9 | Canonical-chain-below-cursor guard enforced by one binding only (E1004). The other fork-capable bindings delete everything above the ancestor without an upper bound, so no orphan rows remain, but they accept the contract violation instead of refusing it | RP-43, FM-15 | P3 | fork with max(canonical) < cursor against each class; assert coded refusal |
| GAP-10 | ClickHouse and BigQuery order cursor records by wall-clock timestamp (resume + fork-record + retention ordering; BigQuery's per-process monotonic counter resets on restart); clock regression can misorder — violates clock independence. ClickHouse's millisecond key has no tie-breaker either, so two commits in one millisecond tie with no clock regression at all | CN-45, CN-15 | P2 | two commits under a regressed clock, and two in one millisecond, per binding; resume must pick commit-order latest |
| GAP-12 | Class-W binding assumes single-writer (client-assigned monotonic timestamps) with no lock and no detection; dual instance corrupts undetected | INV-15 (declaration) | P3 | document in IB-24; optional fencing probe |
| GAP-13 | The spec check syncs the codes declared in source with `docs/errors.md` but not with the IB-50 table, so the table can drift unnoticed; CI runs the check only when `spec/**` changes, so a code-only change skips it | REQ-13, IB-50, IB-52 | P3 | diff declared codes against IB-50 in the check; run it on code changes |
| GAP-14 | Classes K, T and A have kill-point matrices (CT-2 on S1, classes T and A also on S3 with one fork, class A with and without its repair hook). Class W has none for now, by decision: its store is reachable only in a real BigQuery project, which the harness does not use. Class C has none yet: it needs a bus, or a stand-in for one, whose messages outlive the killed child | INV-40…INV-42, CN-34 | P1 | the class-C injector: kill between the local commit and the publish confirmation; restart and assert the outbox republishes byte- and sequence-identically (CN-17, CN-46) |
| GAP-16 | Observability payloads are consumed by the dashboard but have no golden fixtures; shapes drift silently (a version-drift accommodation already exists in the dashboard) | IB-40…IB-46 | P3 | golden scrape of /stats, /metrics, preview against a scripted run |
| GAP-20 | The orphan-data guard is table-wide where it must be key-scoped: the write-ahead binding (E2212) probes the tracked table with no key filter while reading its sync row by key, so a co-resident pipe's legitimate first run into a populated shared table is refused — one pipe's startup made dependent on another's data, blocking a configuration INV-35/RP-41 support. The other bindings run no guard at all and restart from scratch over genuinely orphan data | CN-44, INV-35, REQ-3 | P2 | second pipe id, first run into a table another pipe populates → assert start, not refusal; then on declared-exclusive tables delete cursor state and assert coded refusal per binding |
| GAP-21 | Append-lagged fork completion persists nothing (no rewound cursor row): until the next cursor row, a crash leaves the forked branch's cursor over rows already rolled back to the ancestor, and once the next batch's data is in, over rows of the new branch — states no commit wrote — and the restart converges only because its first request meets the fork again. Persisting the ancestor once the rollback ends would leave the same mismatch for a crash inside the rollback: the ancestor must be persisted before the repair hook removes rows, so a crash leaves only the class's own window above the cursor. CT-2 pins all three points (`conformance/clickhouse/fork-crash.test.ts`). The transactional binding has its own variant (GAP-43) | CN-34, CN-40 | P2 | flip the CT-2 S3 pins: kill inside and after the fork's rollback and in the next batch, assert C = ancestor, F unchanged, RC trimmed, and only rows not yet cancelled or not yet committed above C |
| GAP-22 | A fork tears down and restarts the whole lifecycle (stop + start hooks, metrics server) per streaming segment — hooks fire per segment, not exactly once per run | WP-30, INV-17 | P2 | count start/stop hook invocations across one resolved fork; assert one each |
| GAP-23 | Head-only (204) responses are dropped before the progress tracker (no per-signal OB-2 emission) and their finalized-head report is discarded — the floor stalls while idle at head, so persisted finality and rollback-retention state lag on quiet chains | WP-13, DEF-6 | P2 | serve 204 with a raised finalized header; assert floor advance + progress emission |
| GAP-24 | Cursor timestamps are portal-verbatim and network-dependent — tron and substrate report milliseconds (hyperliquid presumed ms). Lag metrics normalize at the point of use (`blockTimestampSeconds`), but the cursor itself carries whichever unit the portal sent, and the class-T sync column converts it as if it were seconds | DEF-1, IB-20…IB-28 | P2 | tron fixture cursor round-trip asserting the declared unit; normalization decision is OQ-7 |
| GAP-25 | Block schema-validation failures surface as an uncoded `DataValidationError` without identifying the offending block | FM-11, WP-16, REQ-13 | P2 | stream one schema-violating block; assert a coded error naming the block |
| GAP-26 | Class-T batch-transaction retry is ungated (any error retried, including integrity faults and the coded lock refusal) and the serializable-isolation default is operator-downgradable without a guard | FM-21, FM-2, CN-10 | P2 | throw an integrity error in onData → assert no retry; configure read-committed → assert refusal or documented degradation |
| GAP-27 | Cache blobs carry no codec marker; the reader picks zstd vs gzip by runtime feature-detection, so a store written under gzip fails to decode, with an uncoded zlib error, on a zstd-capable runtime — breaks cross-runtime/implementation resume. The `compress` flag is not recorded either, so an uncompressed store fails the same way under the default | IB-25, REQ-23 | P2 | write a gzip cache, read on a zstd runtime; assert correct decode (sniff content magic) |
| GAP-28 | Required observability signals with no IB binding and no implementation: OB-6 head gauge, OB-20 lifecycle timestamps, OB-21 publication lag, OB-30 terminal-error level read, OB-32 capture-on-stall. OB-13 is partial: a stall watchdog logs a warning after a fixed delay, but exposes nothing a scraper can read and carries no reason code. /stats pre-first-batch zeros defeat OB-1/OB-4 on that surface | OB-6/13/20/21/30/32, REQ-10 | P2 | bind each in IB-41/IB-42, then golden scrapes (with GAP-16) |
| GAP-31 | Uncoded error escapes outside the registry: invalid date strings and unparseable block numbers in range config (an invalid `Date` object is not checked at all and reaches the portal as NaN), a plain TableNotFoundError from ClickHouse rollback, and a hard process exit on DDL failure in the ClickHouse store utility | REQ-13, FM-1, IB-52 | P3 | trigger each; assert coded surfaces (with GAP-13's registry sync) |
| GAP-32 | Dev-runner coerces `retry: 0` to the default 5 via a falsy-default — fail-fast intent silently ignored (open fix PR #70) | FM-40 | P3 | retry: 0 → assert a single attempt |
| GAP-33 | Transformer-id dedup collides for 3+ same-id transformers: the third gets the second's id (open fix PR #73) | OB-22 | P3 | three same-id transformers get distinct ids |
| GAP-34 | Observability-server hygiene: CORS accepts any origin containing "localhost" as a substring, with credentials allowed (and rejects 127.0.0.1); stop() clears the global metrics registry; /profiler hardcodes `enabled: true` | IB-40, IB-43 | P3 | origin `http://localhost.evil.com` → assert rejected |
| GAP-35 | Parquet data files carry no pipe-id namespace while the state sidecar does (`_sqd_parquet_state.<id>.json`), and the binding documents a shared directory as supported. Recovery treats every file in a table directory as its own: it deletes every temp file, and its over-cursor sweep deletes another pipe's committed files above its own cursor — or refuses (E2317) on one that straddles it. Temp names restart per process, so two writers can open the same temp path, and overlapping windows collide on publish (E2309) | IB-22, IB-27, INV-35, INV-16 | P1 | two pipe ids, one directory, the second ahead of the first; restart the first → assert the second's files survive, or a coded exclusivity refusal at startup |
| GAP-36 | One-program-per-decoder (ADR-17) is enforced by proxy: `programId` is decoder-level, so nothing binds an instruction definition to its program, and the guards only reject collisions visible in the discriminator set. Two programs with disjoint tracked discriminators still pass and cross-decode | INV-24, REQ-2 | P2 | decoder over `[A, B]` with `{ aSwap: A.swap, bDeposit: B.deposit }`; feed B's on-chain `swap` → assert refusal or no emission under `aSwap`, not a decode |
| GAP-37 | Date-range resolution failures surface uncoded: `resolveTimestamps` matches `error.message` against the portal's `"No chunk found for timestamp"` prose, but the only `Portal` impl throws `HttpError` whose message is `` `Got ${status} from ${url}` `` and nothing wraps it — the body never reaches `.message`, so the match can never succeed and E0002 `BlockRangeConfigurationError` is dead at that site. A `from`/`to` date preceding the dataset's first block raises a raw 404 `HttpError` at the author instead. Matching on message text is itself what REQ-13 forbids; the builder test passes only because its mock throws that prose as a plain error | REQ-13, IB-50, IB-52 | P3 | date range with `from` before `start_block`; simulator answers `GET timestamps/{seconds}/block` (IB-1) with 404 → assert E0002, not `HttpError` |
| GAP-38 | A class-C sink's producer sequence lives only in its local state, and the wire carries no producer-lineage field: a state that was lost or restored from a backup restarts the sequence, republishes low versions under ids consumers already hold at higher ones, and every affected row silently freezes while event topics keep looking healthy. The producer refuses to start on an empty state file (E2420), but a state restored from a backup opens as an ordinary resume and rewinds the sequence undetected; a bootstrap declared with `allowColdStart` under a namespace that has already published cannot be detected by either side in-band either. Both are handled operationally (`sqd_pubsub_cold_start` gauge + runbook), not structurally (ADR-21) | CN-17, CN-46, RP-24 | P2 | publish, wipe the state, resume under the same namespace → assert the refusal; restore an older state file → assert detection; declare the bootstrap and assert a consumer cannot distinguish the stale stream from a healthy one (documents the residual exposure) |
| GAP-40 | The class-C block-order check walks a batch in sequencing order, which is route by route rather than block by block, so a second route on the same topic is refused (E2429) whenever it emits an operation below the previous route's last block in the batch. Nothing the mapper emits can avoid it — the concatenation order is the sink's — so several routes per topic and the sequence barrier are mutually exclusive in practice, currently by refusal and documentation rather than by ordering the operations before they are sequenced | RP-45, IB-28 | P2 | two routes on one topic, one batch spanning blocks *N*…*N+2* → assert the operations are sequenced in block order, not E2429 |
| GAP-41 | Class-K recovery skips the over-cursor sweep when no cursor was ever persisted: the state read returns before the sweep when the state file is absent. A crash between the first publish and the first state write leaves a published file no cursor covers; the restart either collides with it (E2309) or publishes an overlapping file beside it — silently duplicated rows — depending on batch partitioning, a declared free variable. Deleting unconditionally is not the fix while data files carry no pipe namespace (GAP-35). CT-2 pins it (`conformance/parquet/backfill-crash.test.ts`): killed before the first state rename, recovery keeps both published units and the restart does not converge | CN-12, INV-42, REQ-3 | P2 | crash after the first publish, before the first state write; restart → assert output equals an uninterrupted run (flip the CT-2 pin) |
| GAP-42 | A full stream on a dataset without finality keeps no fork history: the source builds each batch's rollback chain only from blocks above a reported finalized head, so with no report every persisted record is empty, and the first fork, however shallow, halts as unresolvable (E1003) instead of rolling back. The model keeps every processed block above an absent floor, bounded by retention (WP-47) | WP-42, WP-47 | P2 | full stream over a dataset without finality, fork two blocks below the head → assert rollback to the common ancestor, not E1003 |
| GAP-43 | The transactional binding writes nothing at fork completion: it drops the sync rows above the ancestor, so the ancestor's own row becomes the persisted state, with the floor and the chain of that older commit. When the floor rose after the ancestor was committed, a crash before the next commit recovers a lower floor and a chain that holds finalized blocks again; the next batch reports the floor again, so the run converges. CT-2 S3 pins it (`conformance/postgres/fork-crash.test.ts`): killed after the fork's commit, the store holds ⟨C = 26, F = 19, RC = 20…26⟩ where CN-34 wants F = 23 and RC = 24…26 | CN-34, INV-12 | P2 | flip the CT-2 S3 pin: kill after the fork's commit, assert ⟨C = ancestor, F unchanged, RC trimmed⟩ |
| GAP-44 | Class-A recovery calls the repair hook only over a persisted cursor. A crash between the first batch's data and its cursor row leaves rows no state accounts for, and the restart, hook or not, stores them again: duplicated rows, no error. GAP-41 is the class-K sibling, and the fix shares its blocker: removing every row from the range start is safe only on tables declared exclusive (OQ-8, ADR-16). CT-2 pins it (`conformance/clickhouse/backfill-crash.test.ts`): killed before the first cursor write, the restart ends with the first batch's rows twice | CN-13, CN-44, RP-42, REQ-3 | P2 | crash after the first batch's data, before its cursor row; restart → assert output equal to an uninterrupted run, or a coded refusal on exclusive tables (flip the CT-2 pin) |

## Build order

- **Phase 0 — harness skeleton**: ledger mode (the chain-form mock portal), the
  reference model (oracle), the structural validators, the class-K store probe and
  class-K kill-point injection are in place; CT-1 is green on the reference
  implementation for S1, and CT-2 runs the class-K matrix on S1 — the exit criteria
  are met. Open: adversarial histories (over-return for INV-24, duplicate and out-of-order
  delivery, head regressions), which build on the same chain model.
- **Phase 1 — P1 gaps**: GAP-35 (decide tracked-table exclusivity with OQ-8/ADR-16,
  which also unblocks GAP-41 and GAP-44), GAP-14 (the matrices of classes K, T and A
  run, class A with and without its repair hook; open is class C, and class W has none
  for now). Exit:
  register updated, fixes landed or accepted as documented deviations.
- **Phase 2 — correctness core**: full CT-2 matrix all classes; CT-3 fork suite; CT-5
  format round-trips (this unblocks the second-language implementation). Exit: matrix
  rows for INV/CN/WP ≥ P everywhere, C on the fork/recovery core.
- **Phase 3 — robustness**: CT-4 systematic FM corpus; CT-8 isolation; CT-9 fuzz.
- **Phase 4 — performance regime**: CT-6 baselines into 11-performance, SLO
  ratification (ADR-14), CT-7 soaks.
Each phase ends by updating this matrix and register.
