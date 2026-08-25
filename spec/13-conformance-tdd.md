# 13 — Conformance & TDD (CT-n, GAP-n) — MUTABLE

Last updated: 2026-08-11. Statuses reflect the actual TypeScript test inventory at this
change's mainline merge base plus the finalized-only target suites in this change, not
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
                     │ (oracle)     │ diff │  data via IB-20…IB-26) │
                     └──────────────┘      └──────────────────┘
   observability scraper ── polls IB-40…IB-46 surface, cross-checks oracle (12 §harness rule)
   fault injectors ── portal faults (FM-10…FM-19), store faults (FM-20…FM-27), kill-points (FM-30…FM-34)
```

*Simulator status:* the reference implementation covers the wire surface and the
per-request assertion, but selects responses by request ordinal rather than from a
ledger — the Phase-0 delta.

**Quiescence** (comparison point): simulator drained, SUT idle-at-head (OB-2 idle), no
retry signal active, sink store stable for one poll interval. All oracle/SUT diffs are
evaluated at quiescence.

## Reference model (normative pseudocode)

```
state: C←⊥, F←⊥, RC←[], D←{}, V←{}                # per DEF tuple
mode ← configured stream mode

bind(target):                                      # WP-7 / RP-7
  if target requires finalized-only: mode ← finalized-only
  portal ← effective-view(mode)
  resolve symbolic ranges through portal
  pass the same portal view to transformers and caches

recover(persisted):                                  # T-INIT
  require well-formed(persisted) else REFUSE          # INV-5, INV-44
  C, F, RC, V ← persisted; repair(D, class, C)        # CN-40…CN-44
  assert no row in D above C                          # INV-42

batch(blocks, head):                                 # T-BATCH
  require blocks strictly ascending, first = C+1 (within range)   # INV-20
  if class ∈ {K, ∅}: require mode = finalized-only   # INV-25
  F ← max(F, head.finalized)                          # INV-12
  RC ← [b ∈ processed : b.number > F]                 # INV-1
  rows ← transform(blocks)                            # pure per RS-10
  commit(D += rows, C ← last(blocks), F, RC)          # per class CN-10…CN-14
  wellformed_check()

fork(canonical):                                     # T-FORK
  require mode = full else CODED-ERROR                # WP-45 / FM-13
  require canonical ≠ ∅ else CODED-ERROR              # WP-41
  require max(canonical).number ≥ C.number else CODED-ERROR   # RP-43 (class W enforces today — GAP-9)
  W ← canonical                                       # narrowing window
  for r ∈ RC newest → oldest:                          # WP-42
    if ∃w ∈ W : w.hash = r.hash → A ← w; break         # canonical match
    if W = ∅:
      if F ≠ ⊥ and r.number < F.number → CODED-ERROR   # WP-44 finality conflict
      A ← r; break                                     # deep-fork restart (below exhausted window)
    W ← {w ∈ W : w.number < r.number}
  if A unset and |W| = 1 and W.hash = F.hash → A ← F   # floor fallback (WP-42)
  if A unset → CODED-ERROR                             # no ancestor (E1003)
  D ← D − rows(> A.number)
  C ← A; RC ← RC[≤ A.number]                           # F unchanged; INV-13/14
  commit-point(fork)                                   # CN-34 (class A deviates — GAP-21)

read_state(): return ⟨C, F, RC⟩                       # same taxonomy as the API

wellformed_check(): assert INV-1…INV-5 after every transition
```

**Free variables** (SUT may legitimately vary; oracle must not pin):
batch partitioning of the block sequence · flush/rollover/checkpoint timing within
configured triggers · published-unit boundaries (subject to INV-4 tiling) · retry
pacing within WP-14 · log text · preview sample content within IB-44 rules.
Everything else — record content, attribution, cursor/floor/chain evolution, coverage
windows, persisted formats — is deterministic against the oracle.

## Test-class taxonomy

| CT | Class | Primary properties |
|---|---|---|
| CT-1 | pipeline property tests (simulator ↔ oracle lockstep, structural validators, quiescence diff) | INV-1…INV-5, INV-10…INV-17, INV-20…INV-25, INV-30, WP-*, RP-7, RP-20…RP-23 |
| CT-2 | crash-recovery kill-point matrix (kill at every class commit-protocol step; double-kill; corrupted-state corpus) | INV-40…INV-44, CN-*, REQ-3, REQ-6 |
| CT-3 | fork conformance (depth × timing × full-stream class; fork-storm; impossible-fork refusal on finalized-only routes) | REQ-4, WP-40…WP-47, INV-13, INV-14, LIV-4 |
| CT-4 | input/dependency fault corpus (every FM row) | FM-*, LIV-2, LIV-7, INV-31, INV-32 |
| CT-5 | interface conformance (wire fixtures IB-1…IB-11; persisted-format round-trips IB-20…IB-26 incl. cross-implementation resume; decode/selection fixtures; observability golden scrapes IB-40…IB-46; error-code registry sync IB-50…IB-52) | REQ-23, INV-5, INV-21, INV-23, CN-45 |
| CT-6 | performance benchmarks (S1–S6, SLO gates, saturation knee) | SLI-*, PF-*, LIV-3, LIV-5 |
| CT-7 | soak/endurance (S4 sparse coverage honesty, memory plateau, reclamation) | INV-4, REQ-20, LIV-8, LIV-9, CN-24 |
| CT-8 | isolation (co-resident pipes, dual-instance where lock declared, legacy-migration races) | INV-15, INV-35, LIV-10, RP-31, RP-32 |
| CT-9 | fuzz (portal wire bytes; decoder inputs; persisted-state bytes) | FM-1, FM-11, WP-16, CN-43 |

**Kill-point matrix (CT-2), per class**: T — before/inside/after transaction ·
W — after intent, mid-append, after append pre-commit-record, after commit record ·
K — mid-unit-write, post-publish pre-state-rename, mid-rename, post-state ·
A — after data pre-cursor, after cursor · all — during recovery itself, during fork.

## Structural validators (kind-agnostic, always on)

Applicable to any emission/state without domain knowledge: decodable (parses per
format) · ordered (ascending attribution) · linked (batch first = cursor+1) ·
items-belong-to-parent (rows within their unit's window) · in-range (attribution within
configured ranges) · watermark coherence (RC above F; C consistent with data bound) ·
route-mode coherence (classes K/∅ bound to finalized-only delivery).

## Traceability matrix (2026-08-11)

Status: **C** covered · **P** partial · **U** unchecked. Suffix ⚠ = known-violated or
known-suspect in the current implementation (see gap register).

| Property | CT class | Status | Note |
|---|---|---|---|
| WP-1, WP-2 (resume, floor seed) | CT-1/2 | C | portal-source + watermark tests |
| WP-3 (id validation) | CT-1 | P ⚠ | rejects, but uncoded (GAP-4) |
| WP-4 (range resolution) | CT-1 | C | range-algebra + builder tests |
| WP-5/CN-40…CN-44 recovery-before-write | CT-2 | P ⚠ | class K covered; T/W/A unchecked (GAP-14); orphan guard W-only and table-wide rather than key-scoped (GAP-20) |
| WP-7, RP-7, DEF-15, INV-25, IB-11 (finalized-only selection) | CT-1/5 | C | portal-source asserts hot override + warning, finalized `latest` and its no-finality fallback, transformer-start view honouring an explicit hot head, per-pipe (non-sticky) selection, effective cache client and node-cache forwarding, quiet pre-finalized mode, and unchanged non-requiring mode; Parquet/memory assert direct delivery and the coded impossible-fork refusal |
| WP-10, WP-15, INV-20 (ordering) | CT-1 | P | buffer/split unit tests; no adversarial simulator (GAP-29) |
| WP-11, HZ-1 (assembly bounds) | CT-1 | C | stream-buffer tests |
| WP-12 (backpressure) | CT-1/6 | P | unit-level only; no end-to-end saturation test |
| WP-14 (transport retry) | CT-4 | P | counts tested; reconnect-mid-stream, 529 unmerged |
| WP-16, FM-11 (malformed input) | CT-9 | U ⚠ | partial-line FIXME open (GAP-5); validation errors uncoded (GAP-25) |
| WP-20…WP-22 (compose, validate, filter) | CT-1/5 | C | decoder + portal-source suites |
| WP-23 (decode-error policy) | CT-5 | C | unified via `core/decode-error.ts` (ADR-12); fatal-default + suppress-and-count asserted on both modules |
| WP-24 (attribution uniqueness) | CT-5 | P ⚠ | collision only logged in evm, undetected in solana (GAP-15, OQ-6) |
| WP-25 (query union) | CT-1 | C | merge/heap tests + multi-output isolation |
| WP-30…WP-32 (lifecycle) | CT-1 | P ⚠ | stop-once + partial-start tested; fork path re-fires start/stop per segment (GAP-22) |
| WP-40…WP-43 (fork core) | CT-3 | C | core fork, portal-source, and fork-capable target suites; WP-42 text normativized from the tested deep-fork/floor-fallback semantics; K/∅ excluded by WP-7 |
| WP-41 (empty canonical) | CT-4 | U ⚠ | ctor crash preempts coded path (GAP-8) |
| WP-44 (finality conflict) | CT-3 | P ⚠ | null result tested; downstream halt untested (GAP-6) |
| WP-46 (rollback idempotence) | CT-2/3 | P | netting idempotence tested (one binding); others U |
| WP-47 (depth bound) | CT-3 | P | retention pruning tested; deep-fork restart e2e U |
| RP-1…RP-6 (write loop) | CT-1 | C | per-target state suites |
| RP-21, RP-22, INV-4 (coverage) | CT-7 | C | parquet coverage-naming suites: sparse stretch, trailing empty unit, disjoint-range gap, resume mid-gap |
| RP-30…RP-32 (keying, migration) | CT-8 | P ⚠ | happy path C; concurrent migration U (GAP-7) |
| RP-42 (delegated repair) | CT-2 | U ⚠ | hook optional by design, absence made loud (ADR-15); no-hook recovery kill-point owed (GAP-14) |
| RP-43 (contract guard) | CT-3 | P ⚠ | one binding only (GAP-9) |
| CN-10 T atomicity | CT-2 | P ⚠ | live tx tests; kill-points U (GAP-14) |
| CN-11 W protocol | CT-2 | P | unit/lifecycle C; integration gated off CI |
| CN-12 K protocol | CT-1/2 | C | finalized-route selection plus crash-safety + recovery suites; straddle refusal asserted (E2317) |
| CN-13 A protocol | CT-2 | U ⚠ | recovery/fork with a hook untested; no-hook window is an accepted deviation (ADR-15); kill-point owed (GAP-14) |
| CN-17, CN-35, CN-46 C protocol | CT-2/3 | P ⚠ | fold-per-id, baseline restore, route-declared inverse, seq monotonicity across forks and the commit-then-publish crash window are asserted against a publisher seam; no live-bus conformance run, and the lost-sequencer exposure is operational only (GAP-38) |
| RP-24, RP-44 (changelog contract) | CT-5 | P ⚠ | canonical codec tested as a contract (accept/reject table + golden bytes); attribute inheritance through forks asserted; consumer-side convergence untested against a real subscription |
| CN-34 (fork commit point) | CT-2 | U ⚠ | crash-during-fork untested; class A persists nothing at fork (GAP-21) |
| CN-45 (cross-impl, clock indep.) | CT-5 | U ⚠ | single implementation; clock ordering in two bindings (GAP-10); cache codec undiscriminated (GAP-27) |
| INV-2, INV-12 (floor) | CT-1/2 | C | adversarial regression/genesis tests |
| INV-13, INV-14 (fork safety) | CT-3 | C | at batch boundaries; mid-flush U |
| INV-15, INV-35, LIV-10 (isolation) | CT-8 | U | no co-resident/dual-instance tests |
| INV-21, INV-23 (fidelity, selection) | CT-5 | C | validator fixtures incl. chain edge shapes |
| INV-22 (determinism) | CT-1 | U | no dual-run diff harness |
| INV-30…INV-32 (reporting) | CT-1/4 | P ⚠ | metric registration tested; honesty cross-check U; six OB signals unimplemented (GAP-28) |
| INV-36 (cache) | CT-5 | C | keying/gap/latest-exclusion tests |
| PF-6, WP-12 (ingest overlap) | CT-6 | P | slot semantics unit-tested; no cadence benchmark |
| LIV-1…LIV-9 | CT-4/6/7 | U | no liveness harness yet |
| RS-10, RS-11 (purity, stateful) | CT-2 | P | K recovery relies on it; factory rollback C |
| RS-20…RS-25 (cache) | CT-5 | C | except overlap re-insert (U) |
| FM corpus | CT-4 | P | scattered unit coverage; no systematic corpus |
| INV-60…INV-64 (fallback stream safety) | CT-1/2 | C | switch continuity, fork propagation across a switch, conservative and raise-only commitment, floor across a switch |
| INV-65…INV-69 (fallback isolation, faults) | CT-2 | C | concurrent-read refusal, late verdict discarded, ineligible-head exclusion, strategy fault surfacing, cause attribution |
| LIV-60…LIV-64 (handoff, reclaim) | CT-2 | P | handoff, reclaim, no-permanent-demotion, fresh start and all-down covered against a simulated frontier; no soak |
| WP-60…WP-69 (driving the list) | CT-2 | P | selection, resume, boundary/stall decisions, probing and range resolution covered; head-poll cadence not benchmarked |
| OB-60…OB-66 (fallback signals) | CT-1 | P | gauges and per-pipe labelling asserted; log-field redaction covered by the diagnostics suite, not end to end |
| SLI/PF | CT-6 | U | no benchmarks recorded |
| IB-1…IB-11 wire | CT-5 | P | client-side tested, including finalized route selection; no golden wire fixtures |
| IB-20…IB-26 formats | CT-5 | P ⚠ | per-binding tests; no round-trip corpus; timestamp units per-network (GAP-24) |
| IB-40…IB-46 observability | CT-5 | U ⚠ | consumed by dashboard, no golden scrapes (GAP-16) |
| IB-50…IB-52 error registry | CT-5 | U ⚠ | no registry-sync test (GAP-13) |

## Gap register (2026-08-11)

Priorities: P0 active production risk · P1 correctness hole, plausible trigger ·
P2 bounded/rare · P3 polish. "First test" = cheapest failing-test-first entry point.

| GAP | Statement | Violated | Pri | First test |
|---|---|---|---|---|
| GAP-4 | Blank-pipe-id guard throws an uncoded error; the documented code exists but is dead | REQ-13, WP-3 | P3 | blank id + sink → assert the coded configuration error |
| GAP-5 | Malformed NDJSON line vs incomplete line are not discriminated; a partial line can surface as a raw parse crash | WP-16, FM-11 | P2 | simulator splits a line across chunks (must continue) and sends one malformed line (must fail coded) |
| GAP-6 | Fork below the finalized floor is an acknowledged open TODO; halt is provisional intent (OQ-2) | WP-44 | P2 | canonical chain entirely below floor → coded halt, state intact |
| GAP-7 | Legacy cursor migration in one binding is unlocked: concurrent first runs can both adopt legacy state and inherit a foreign monotonic floor that never self-corrects | RP-32, INV-35 | P2 | two concurrent first runs against legacy-keyed store; ≤ 1 adopts |
| GAP-8 | Fork-signal handling crashes uncoded on an empty canonical chain (exception constructor) before the coded guard runs | WP-41, FM-14 | P2 | fork response with empty chain → assert coded error, not raw crash |
| GAP-9 | Canonical-chain-below-cursor guard enforced by one binding only; others can strand orphan rows above the new chain | RP-43, FM-15 | P2 | fork with max(canonical) < cursor against each class; assert coded refusal |
| GAP-10 | ClickHouse and BigQuery order cursor records by wall-clock timestamp (resume + fork-record + retention ordering; BigQuery's per-process monotonic counter resets on restart); clock regression can misorder — violates clock independence | CN-45, CN-15 | P2 | two commits under a regressed clock, per binding; resume must pick commit-order latest |
| GAP-12 | Class-W binding assumes single-writer (client-assigned monotonic timestamps) with no lock and no detection; dual instance corrupts undetected | INV-15 (declaration) | P3 | document in IB-24; optional fencing probe |
| GAP-13 | No automated sync between thrown error codes and the registry; the "all codes cross-checked" claim is manual | REQ-13, IB-50 | P3 | enumerate codes from source; diff against IB-50 table |
| GAP-14 | Crash-recovery kill-point coverage exists only for class K; classes T/W/A have no kill-point tests | INV-40…INV-42 | P1 | Phase-0 ledger mode first (an ordinal script cannot answer the post-restart re-request), then the kill-point harness at the T transaction boundary |
| GAP-15 | Indistinguishable-output collision (duplicate event signature) only logs in the evm module and is entirely undetected in solana; later outputs silently miss records | WP-24 | P2 | two outputs, same signature → startup diagnostic asserted fatal (pending OQ-6) |
| GAP-16 | Observability payloads are consumed by the dashboard but have no golden fixtures; shapes drift silently (a version-drift accommodation already exists in the dashboard) | IB-40…IB-46 | P3 | golden scrape of /stats, /metrics, preview against a scripted run |
| GAP-20 | The orphan-data guard is table-wide where it must be key-scoped: the write-ahead binding (E2212) probes the tracked table with no key filter while reading its sync row by key, so a co-resident pipe's legitimate first run into a populated shared table is refused — one pipe's startup made dependent on another's data, blocking a configuration INV-35/RP-41 support. The other bindings run no guard at all and restart from scratch over genuinely orphan data | CN-44, INV-35, REQ-3 | P2 | second pipe id, first run into a table another pipe populates → assert start, not refusal; then on declared-exclusive tables delete cursor state and assert coded refusal per binding |
| GAP-21 | Append-lagged fork completion persists nothing (no rewound cursor row); a crash after resolveFork and before the next commit resumes from pre-fork state | CN-34 | P2 | kill between resolveFork and the next commit; assert recovered C = ancestor |
| GAP-22 | A fork tears down and restarts the whole lifecycle (stop + start hooks, metrics server) per streaming segment — hooks fire per segment, not exactly once per run | WP-30, INV-17 | P2 | count start/stop hook invocations across one resolved fork; assert one each |
| GAP-23 | Head-only (204) responses are dropped before the progress tracker (no per-signal OB-2 emission) and their finalized-head report is discarded — the floor stalls while idle at head, so persisted finality and rollback-retention state lag on quiet chains | WP-13, DEF-6 | P2 | serve 204 with a raised finalized header; assert floor advance + progress emission |
| GAP-24 | Cursor timestamps are portal-verbatim and network-dependent — tron reports milliseconds (hyperliquid presumed ms). Lag metrics now normalize at the point of use (`blockTimestampSeconds`), so the cursor itself still carries whichever unit the portal sent | DEF-1, IB-20…IB-26 | P2 | tron fixture cursor round-trip asserting the declared unit; normalization decision is OQ-7 |
| GAP-25 | Block schema-validation failures surface as an uncoded `DataValidationError` without identifying the offending block | FM-11, WP-16, REQ-13 | P2 | stream one schema-violating block; assert a coded error naming the block |
| GAP-26 | Class-T batch-transaction retry is ungated (any error retried, including integrity faults) and the serializable-isolation default is operator-downgradable without a guard | FM-21, FM-2, CN-10 | P2 | throw an integrity error in onData → assert no retry; configure read-committed → assert refusal or documented degradation |
| GAP-27 | Cache blobs carry no codec marker; the reader picks zstd vs gzip by runtime feature-detection, so a store written under gzip mis-decodes on a zstd-capable runtime — breaks cross-runtime/implementation resume | IB-25, REQ-23 | P2 | write a gzip cache, read on a zstd runtime; assert correct decode (sniff content magic) |
| GAP-28 | Six required observability signals have no IB binding and no implementation: OB-6 head gauge, OB-13 stall/retry signal, OB-20 lifecycle timestamps, OB-21 publication lag, OB-30 terminal-error level read, OB-32 capture-on-stall; /stats pre-first-batch zeros defeat OB-1/OB-4 on that surface | OB-6/13/20/21/30/32, REQ-10 | P2 | bind each in IB-41/IB-42, then golden scrapes (with GAP-16) |
| GAP-29 | No local detection of portal ordering violations: duplicate or out-of-order blocks would flow through silently instead of failing | FM-16, WP-15 | P3 | adversarial simulator emits a duplicate block; assert coded halt |
| GAP-30 | The effective-end computation applies no min(configured to, head) — a single-argument `Math.min` no-op takes the configured `to` even beyond head, skewing percent/ETA | OB-3, DEF-18 | P3 | range.to beyond head; assert end-block gauge = head |
| GAP-31 | Uncoded error escapes outside the registry: invalid date strings in range config, a plain TableNotFoundError from ClickHouse rollback, and a hard process exit on DDL failure in the ClickHouse store utility | REQ-13, FM-1, IB-52 | P3 | trigger each; assert coded surfaces (with GAP-13's registry sync) |
| GAP-32 | Dev-runner coerces `retry: 0` to the default 5 via a falsy-default — fail-fast intent silently ignored (open fix PR #70) | FM-40 | P3 | retry: 0 → assert a single attempt |
| GAP-33 | Profiling-surface defects: batch spans leak on empty batches and stream end (open fix PR #71); transformer-id dedup collides for 3+ same-id transformers (open fix PR #73) | OB-22 | P3 | span onStart count = onEnd count across empty batches; three same-id transformers get distinct ids |
| GAP-34 | Observability-server hygiene: CORS accepts any origin containing "localhost" as a substring (and rejects 127.0.0.1); stop() clears the global metrics registry; /profiler hardcodes `enabled: true` | IB-40, IB-43 | P3 | origin `http://localhost.evil.com` → assert rejected |
| GAP-36 | One-program-per-decoder (ADR-17) is enforced by proxy: `programId` is decoder-level, so nothing binds an instruction definition to its program, and the guards only reject collisions visible in the discriminator set. Two programs with disjoint tracked discriminators still pass and cross-decode | INV-24, REQ-2 | P2 | decoder over `[A, B]` with `{ aSwap: A.swap, bDeposit: B.deposit }`; feed B's on-chain `swap` → assert refusal or no emission under `aSwap`, not a decode |
| GAP-35 | Parquet data files are named by block window with no pipe-id namespacing while the state sidecar is namespaced (`_sqd_parquet_state.<id>.json`): two pipes covering overlapping ranges in one directory collide on filename and the second fails E2309 — sharing a directory is refused by accident of naming rather than declared policy | IB-22, IB-27, INV-35 | P2 | two pipe ids, one directory, overlapping ranges → assert the declared outcome (namespaced files or a coded exclusivity refusal), not a publish collision |
| GAP-38 | A class-C sink's producer sequence lives only in its local state, and the wire carries no producer-lineage field: a state that was lost or restored from a backup restarts the sequence, republishes low versions under ids consumers already hold at higher ones, and every affected row silently freezes while event topics keep looking healthy. Neither side can detect it in-band — handled operationally (cold-start warning + `sqd_pubsub_cold_start` gauge + runbook), not structurally (ADR-21) | CN-17, CN-46, RP-24 | P2 | publish, wipe the state, resume under the same namespace → assert the cold-start signal fires; then assert a consumer cannot distinguish the stale stream from a healthy one (documents the exposure) |
| GAP-39 | Class C publishes no finality signal: consumers converge but never learn when a row became reorg-proof, so "act only on finalized values" is not expressible on the wire. Deliberately deferred pending demand — candidates are an optional finality attribute, a periodic watermark operation, or a separate finalized-stream pipe | CN-17, RP-24 | P3 | consumer asks "is this row final?" → assert the contract states the answer is unavailable, not approximated |
| GAP-37 | Date-range resolution failures surface uncoded: `resolveTimestamps` matches `error.message` against the portal's `"No chunk found for timestamp"` prose, but the only `Portal` impl throws `HttpError` whose message is `` `Got ${status} from ${url}` `` and nothing wraps it — the body never reaches `.message`, so the match can never succeed and E0002 `BlockRangeConfigurationError` is dead at that site. A `from`/`to` date preceding the dataset's first block raises a raw 404 `HttpError` at the author instead. Matching on message text is itself what REQ-13 forbids | REQ-13, IB-50, IB-52 | P3 | date range with `from` before `start_block`; simulator answers `GET timestamps/{seconds}/block` (IB-1) with 404 → assert E0002, not `HttpError` |

## Build order

- **Phase 0 — harness skeleton**: the reference implementation already serves the
  simulator's wire surface from an HTTP fixture (200 NDJSON · 204 · 409 with canonical
  chain · 5xx · head headers, IB-4/IB-5) with a per-request assertion hook — sufficient
  as-is for request-shape work (resume-anchor scoping, ADR-20). The Phase-0 delta is **ledger mode**:
  derive responses from the request anchor (IB-3) against a held chain instead of
  selecting them by request ordinal. An ordinal script has no answer for the re-request
  a restarted SUT issues from its recovered cursor, so this gates the entire CT-2
  matrix; it is also what adversarial histories require (over-return for INV-24,
  duplicate/out-of-order for GAP-29, head regressions). Then, all new: reference model
  (oracle), structural validators, sink store probes, and kill-point injection for one
  class (K — cheapest, file-based; the only crash imitation today is a pre-commit
  abort, one point of the CT-2 matrix). Exit: CT-1 green on the reference
  implementation for S1, ledger mode answering post-restart re-requests.
- **Phase 1 — P1 gaps**: range anchors (landed under ADR-20), GAP-14 kill-point harness for class T
  (and the class-A no-hook recovery window, now an accepted deviation under ADR-15). Exit:
  register updated, fixes landed or accepted as documented deviations.
- **Phase 2 — correctness core**: full CT-2 matrix all classes; CT-3 fork suite; CT-5
  format round-trips (this unblocks the second-language implementation). Exit: matrix
  rows for INV/CN/WP ≥ P everywhere, C on the fork/recovery core.
- **Phase 3 — robustness**: CT-4 systematic FM corpus; CT-8 isolation; CT-9 fuzz.
- **Phase 4 — performance regime**: CT-6 baselines into 11-performance, SLO
  ratification (ADR-14), CT-7 soaks.
Each phase ends by updating this matrix and register.
