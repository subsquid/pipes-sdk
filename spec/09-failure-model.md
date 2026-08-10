# 09 — Failure model (FM-n)

Response verbs: **mask** (absorb, no author-visible effect) · **degrade** (continue
with reduced service, visibly) · **fail-safe** (halt with a coded error, state intact)
· **alarm** (emit the OB signal; combinable with the others).

## Global requirements

**FM-1 — No externally-triggered termination.** [MUST] No input content — portal
bytes, chain data, head reports — may terminate the process without a coded
diagnostic. Malformed input is fail-safe + alarm, never a raw crash.

**FM-2 — Transient vs integrity.** [MUST] Every fault is classified: *transient*
(retry with backoff + alarm while retrying, LIV-7) or *integrity/configuration*
(fail-safe with a coded error; state untouched beyond the last commit point). No fault
class is silently ignored.

**FM-3 — Blast radius.** [MUST] A fault in one pipe never corrupts another pipe's
state (INV-35); a fault in author code aborts the current commit protocol cleanly
(RP-5); a halted pipe leaves recoverable committed state (CN-40).

## Input-side faults (portal)

Bands 10–19.

| # | Fault | Required response |
|---|---|---|
| FM-10 | slow / unresponsive portal | mask via retry (P-RETRY-SCHEDULE-MS, unbounded for streaming) + alarm OB-13 while stalled |
| FM-11 | malformed batch line (invalid JSON, schema-violating block) | fail-safe + alarm, diagnostic names the offending block (WP-16; GAP-5, GAP-25) |
| FM-12 | regressing / vanishing finalized head | mask: clamp at floor (INV-12); never un-finalize |
| FM-13 | finalized-only delivery cannot establish finality (no-finality dataset), or receives a fork signal | no floor: degrade with the explicit "not reorg-safe" limitation; fork: fail-safe because the sink intentionally has no rollback path (WP-45) |
| FM-14 | fork signal with empty canonical chain | fail-safe with coded contract violation (WP-41) |
| FM-15 | canonical chain entirely below persisted cursor | fail-safe with coded contract violation (RP-43) |
| FM-16 | duplicate / out-of-order blocks within a stream | fail-safe: violates the trusted-ordering premise (ADR-1); MUST NOT be silently reordered *(no local detection exists today — GAP-29)* |
| FM-17 | oversized single block (> assembly budget) | degrade: deliver as its own batch (bounded overshoot, WP-11); alarm if it exceeds hard memory bounds |
| FM-18 | retryable status / retry-after from portal | mask: honor server pacing (WP-14), count in OB-12 |
| FM-19 | equivocating heads across reconnects (different finalized reports for same height) | mask via clamp; the floor keeps the maximum; data content conflicts surface as FM-16 |

## Sink/storage faults

Bands 20–29.

| # | Fault | Required response |
|---|---|---|
| FM-20 | transient storage unavailability / timeout | mask via class retry (bounded, e.g. P-PG-TX-RETRY, P-BQ-RETRY-LIMIT) + alarm; then fail-safe |
| FM-21 | transaction conflict / serialization failure (class T) | mask via bounded retry of the batch transaction *(retry currently ungated by fault class — GAP-26)* |
| FM-22 | storage rejects rows (schema mismatch, oversized) | fail-safe with the coded sink error; batch not acknowledged |
| FM-23 | lock contention (declared single-writer lock) | fail-safe for the losing instance with the coded lock error (INV-15) |
| FM-24 | disk full / quota (class K state or units) | fail-safe; on restart recovery repairs partial units (CN-12) |
| FM-25 | engine capability missing (unsupported table engine/partitioning) | fail-safe at startup with the coded capability error — never degrade into unsafe rollback |
| FM-26 | corrupt / unparseable persisted state | fail-safe before any destructive action (CN-43, INV-44) |
| FM-27 | orphan tracked data without cursor state | fail-safe (CN-44) — never silent re-processing |

## Process faults

Bands 30–34.

| # | Fault | Required response |
|---|---|---|
| FM-30 | crash at any commit-protocol step | recovery per class (CN-10…CN-14): converge to committed state (INV-40…INV-42) |
| FM-31 | crash during recovery | recovery idempotent (INV-41) |
| FM-32 | crash during fork rollback | fork completion is a commit point (CN-34); recovery either sees pre-fork state (redo fork on next signal) or post-fork state; partial rollback must be idempotent under re-application (WP-46) |
| FM-33 | cancellation mid-batch | crash-equivalent or better (LIV-6) |
| FM-34 | author-code exception in a callback | abort commit protocol for the batch (RP-5); surface the error; committed state intact |

## Client/consumer faults

Bands 35–39.

| # | Fault | Required response |
|---|---|---|
| FM-35 | slow consumer (sink slower than portal) | mask via end-to-end backpressure (WP-12); no queue growth |
| FM-36 | observability scraper absent / slow | mask: pipeline never blocks on observers (OB non-interference) |

## Operator faults

Bands 40–49.

| # | Fault | Required response |
|---|---|---|
| FM-40 | invalid configuration (ranges, blank id, bad options) | fail-safe at startup with coded configuration errors (WP-3, WP-4) |
| FM-41 | dual instance on the same cursor key | fail-safe where a lock is declared (FM-23); explicitly undefined (NG2) where not — the binding's declaration (IB-24) is normative |
| FM-42 | restored/older state file alongside newer data | fail-safe via refuse-before-delete (INV-44) |
| FM-43 | retention set below fork depth needs | fork beyond retention resolves via deep-fork restart (WP-47); alarm on OB-14 |

## Fault → property → test-class cross-reference

| Fault family | Properties exercised | Test class |
|---|---|---|
| FM-10…FM-19 | INV-12, INV-20, INV-24, WP-14…WP-16, LIV-7 | CT-4 (input-fault corpus) |
| FM-20…FM-27 | RP-*, CN-43, CN-44, INV-44 | CT-4 (dependency faults), CT-2 |
| FM-30…FM-34 | INV-40…INV-44, CN-34 | CT-2 (kill-point matrix) |
| FM-35…FM-36 | WP-12, REQ-20 | CT-6 (S6), CT-7 |
| FM-40…FM-43 | WP-3, WP-4, INV-15, INV-44 | CT-4, CT-8 |
