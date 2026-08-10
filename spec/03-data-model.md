# 03 — Data model

Definitions only; operational semantics live in 04/05, invariants in 07.

## Primitives

**DEF-1 — Block cursor.** `Cursor ≡ ⟨number: ℕ, hash: string | ⊥, timestamp: ℕ | ⊥⟩`.
`number` identifies chain height; `hash` identifies the block among competing blocks at
one height; `timestamp` is the portal block-header timestamp **verbatim** — its unit is
network-dependent: epoch seconds on evm/solana/bitcoin, epoch **milliseconds** on tron
(hyperliquid unverified, presumed milliseconds); normalization is an open decision
(GAP-24, OQ-7). Equality: two cursors denote the same
block iff numbers are equal and, when both hashes are present, hashes are equal.
Ordering is by `number`.

**DEF-2 — Pipe.** The principal entity: one named stream instance, identified by a
non-empty string **pipe id**. All persistent state is scoped by pipe id (via DEF-9).

**DEF-3 — Portal / dataset.** The upstream HTTP gateway serving one chain dataset. A
dataset MAY be *real-time* (serves unfinalized head blocks) or lag the head; it MAY be
*no-finality* (never reports a finalized head).

**DEF-4 — Batch.** One delivered unit: `Batch ≡ ⟨blocks: sequence, head: Head, meta⟩`
where `blocks` is a non-empty, strictly-ascending-by-number block sequence. Batch
boundaries are a free variable (02 §Explicitly unspecified).

**DEF-5 — Head.** `Head ≡ ⟨finalized: Cursor | ⊥, latest: ⟨number⟩ | ⊥⟩` — the portal's
report of chain state accompanying each batch.

**DEF-6 — Finalized floor `F`.** The pipe's monotonic high-watermark of finality:
the greatest finalized cursor observed, clamped so it never decreases (INV-2, INV-12).
`F = ⊥` means "nothing known finalized"; on a no-finality dataset `F` stays ⊥ forever.
Every consumer of finality information reads the clamped floor, never the raw report.
The clamp compares by `number` only: an equal-number report with a different hash does
not replace the stored cursor.

**DEF-7 — Rollback chain `RC`.** The ordered sequence of cursors (with hashes) of
processed blocks strictly above `F` — the candidate ancestors for fork resolution.
Persisted with each cursor advance; trimmed as blocks finalize.

**DEF-8 — Sink resume state.** `TargetState ≡ ⟨latest: Cursor, finalized: Cursor | null⟩`
— the handshake a sink returns on resume. `finalized` is a **required key**, explicitly
`null` when absent; it re-seeds `F`. `latest` is the commit cursor `C`; streaming resumes
at `C.number + 1`.

**DEF-9 — Cursor key.** The identifier under which a sink stores resume state:
an explicit per-sink key if configured, else the pipe id. The legacy constant key
(`stream`) survives only as a migration source (RP-31): WP-3 makes a source-connected
pipe without an id a startup error, so no new state is written under it; a target driven
without a source currently falls back to it silently (GAP-4 family). Binding rules and
one-time legacy migration: RP-30…RP-32; ADR-2.

**DEF-10 — Canonical chain.** The portal's list of canonical cursors delivered with a
fork signal (wire name `previousBlocks`), newest-last. The ground truth against which
`RC` is matched to find the rollback ancestor.

**DEF-11 — Sink.** The delivery endpoint. Every sink declares a **durability class**
(policy table below) that fixes its commit protocol and recovery obligations (06).

**DEF-12 — Transformer.** A composable stage applied to each batch in declaration
order; may be asynchronous and stateful; participates in lifecycle events (start, stop,
rollback). A **query-aware transformer** additionally contributes filters/fields to the
portal query before streaming starts.

**DEF-13 — Query.** `Query ≡ ⟨type, fields, requests, range set⟩`: `type` is the network
tag; `fields` a per-entity field-selection tree; `requests` per-entity filter lists.
**Query hash**: a digest over the query excluding positional fields (`fromBlock`,
`toBlock`, parent hash) — the identity used for cache keying (RS-21).

**DEF-14 — Range set / coverage window.** The configured block ranges (inclusive,
possibly open-ended), after resolution of symbolic endpoints (`latest`, dates), merged
and clipped by the resume bound. A **coverage window** is the contiguous block interval
one published output unit (e.g. a file) accounts for: "window covered" asserts *the pipe
processed this interval for this table*, not that rows exist in it.

**DEF-15 — Finalized-only delivery.** A sink policy requiring every finality-sensitive
source operation to use the finalized mode. The pipe selects that mode before range
resolution and passes the same effective portal view through transformer startup,
caching, and streaming (WP-7, IB-11). On a finalizing dataset, the portal admits a block
only after finalization and emits no fork on that route, so the sink feeds delivered
rows into its ordinary commit protocol without a finality hold-back and needs no
rollback handler. On a no-finality dataset the route cannot establish reorg safety;
delivered rows pass through under the dataset's semantics and the output is not
reorg-safe (FM-13).

**DEF-16 — Network module.** A chain family binding: a query builder (typed filters +
field selection + request merging), a block schema validator, and optionally a decoder
with declared skip/error semantics. The closed set of network tags lives in IB-10.

**DEF-17 — Checkpoint.** For file sinks: the atomic point at which open output units are
published and the cursor advances together; the only place the cursor moves (CN-12).

**DEF-18 — Progress point.** The externally observable pair `⟨C, end⟩` where `C` is the
committed cursor and `end` the effective range end (`min(configured to, head)`), from
which percent/ETA derive (OB-3…OB-5).

## Core state tuple

Per pipe: `S ≡ ⟨C, F, RC, D, V⟩` —

| Component | Meaning | Persistence |
|---|---|---|
| `C` | committed cursor (DEF-1); ⊥ before first commit | sink store |
| `F` | finalized floor (DEF-6) | sink store (as last clamped value) |
| `RC` | rollback chain (DEF-7) | sink store |
| `D` | committed sink data: per-table multiset of rows, each attributed to one block number | sink store |
| `V` | coverage map: per-table next-window start (file sinks) | sink store |

Well-formedness is defined by the structural invariants INV-1…INV-5 (single source of
truth; not restated here). The **snapshot** unit of read isolation for downstream
consumers is per durability class (CN-20…CN-24).

## Policies

| Policy | Variants | Where fixed |
|---|---|---|
| Durability class | `T` transactional · `W` write-ahead · `K` checkpointed-immutable · `A` append-lagged · `∅` ephemeral | per sink family, CN-10…CN-14; ADR-5 |
| Finality mode | finalizing dataset · no-finality dataset | per dataset, DEF-3 |
| Visibility | immediate after commit (T/W/A/C) · source-gated finalized-only (K/∅) | per class, CN-20…CN-24 |
| Decode-error policy | fatal-by-default, hook may skip-with-count (uniform, ADR-12) | all network modules, WP-23 |
| Stream mode | full (unfinalized included, fork-capable) · finalized-only (configured or target-required) | per pipe, DEF-15, IB-11 |
| Cache | off · local finalized-batch cache | per pipe, RS-20 |

## Input events

| Event | Content | Meaning | Delivery |
|---|---|---|---|
| Data batch | blocks + head | next slice of the stream | ordered, at-least-once across reconnects; duplicates excluded by resume bound |
| Head-only signal | head, no blocks | caught up ("on head"); poll again | at-most-once per request |
| Fork signal | canonical chain (DEF-10) | requested parent is not canonical; rollback required | replaces a full-stream data response; absent on finalized-only delivery |
| Range end | — | requested range exhausted or data absent upstream | terminates one range |
| Finality report | `head.finalized` | raises `F` after clamping | with every batch; may regress or vanish (clamp absorbs) |

## Transition summary

Semantics in [04-ingestion.md](04-ingestion.md).

| Transition | One line |
|---|---|
| T-INIT | recover state, repair partial writes, compute resume bound |
| T-BATCH | ingest + transform + commit one batch; advance `C`; clamp `F`; refresh `RC` |
| T-RELEASE | the finalized route admits a newly finalized block upstream; it reaches the pipe as an ordinary T-BATCH, with no local release state |
| T-CHECKPOINT | (class K) publish open windows + persist cursor |
| T-FORK | resolve canonical ancestor; delete above it; rewind `C`; trim `RC` |
| T-STOP | run stop lifecycle exactly once; release resources |

## Terminology cross-reference (codebase term → DEF)

| Code term | Spec term |
|---|---|
| `BlockCursor` / `BlockRef` | DEF-1 |
| `id` (source/pipe id) | DEF-2 |
| `PortalStream` | the pipe's source stage (04) |
| `StreamData` / `PortalBatch` | DEF-4 |
| `head.finalized` / `head.latest` | DEF-5 |
| `FinalizedWatermark`, "floor", `clamp` | DEF-6 |
| `rollbackChain` (batch ctx carries a per-batch fragment; the accumulated chain is assembled sink-side) | DEF-7 |
| `TargetState` | DEF-8 |
| `CursorKey`, legacy id `stream` | DEF-9 |
| `canonicalBlocks` (wire: `previousBlocks`) | DEF-10 |
| `Target` | DEF-11 (sink) |
| `Transformer`, `QueryAwareTransformer` | DEF-12 |
| `Query`, `hashQuery` | DEF-13 |
| "coverage" (file naming) | DEF-14 |
| `requiresFinalizedStream` (reference binding) | DEF-15 |
| network dir (`evm/`, `solana/`, …) | DEF-16 |
| "checkpoint" (file sink) | DEF-17 |
| `resolveFork` (sink op) | T-FORK entry point (RP-40) |
| `resolveForkCursor` | ancestor search (WP-42) |
