# 10 — Replay, purity & cache (RS-n)

The pipeline-shape lifecycle doc: what "processing the same blocks again" means.
Bands: 1–9 replay semantics, 10–19 purity, 20–29 cache.

## Replay semantics

**RS-1 — Replay equivalence.** [MUST] Re-running a pipe over already-processed
finalized ranges (fresh sink) produces output equal to the first run modulo declared
free variables. Replay is the definition of correctness for recovery (CN-12), cache
(RS-20), and cross-implementation checks (REQ-23).

**RS-2 — Backlog definition.** The pipe's backlog is `effective end − C`. Backlog is
observable (OB-3…OB-5) and MUST shrink under healthy conditions (LIV-1); it is the
quantity ETAs are computed from.

**RS-3 — Re-fetch after repair.** [MUST] Recovery that discards data (CN-11 intent
range, CN-12 over-cursor units) re-fetches the discarded blocks from the portal (or
cache) — repair never reconstructs data from residue.

## Purity

**RS-10 — Purity obligation.** [MUST for class K; SHOULD elsewhere] Author transform
code is a pure function of ⟨batch content, static configuration⟩ for finalized blocks:
no wall-clock, no randomness, no external mutable reads that can change between runs.
The sink cannot verify purity; the spec makes it a *stated author obligation* whose
violation voids INV-43 (byte-identical regeneration).
Finalized-only source selection does not weaken this obligation: it removes the local
finality buffer, but class-K crash recovery still re-fetches and regenerates rows.

**RS-11 — Stateful transformers across replay.** [MUST] A transformer holding state
MUST rebuild it deterministically from the stream (its rollback hook discards state
above the ancestor; its start hook rebuilds from persisted adapters where provided).
Auxiliary transformer persistence (e.g. discovered child contracts) follows the same
fork/rollback discipline as sink data: rows above the ancestor are removed on rollback.

## Cache

**RS-20 — Cache contents.** [MUST] Only finalized batches are cached — a batch's
cacheable prefix is its blocks `≤` the reported finalized head at receipt time.
Unfinalized blocks pass through uncached. (Immutability of finalized data is what makes
an invalidation-free cache sound — ADR-11.)

**RS-21 — Keying.** [MUST] Cache identity is ⟨query hash (DEF-13), block interval⟩.
The query hash excludes positional fields, so one logical query shares a bucket across
ranges; any change to filters/fields/type changes the hash and misses cleanly (INV-36).

**RS-22 — Contiguous replay.** [MUST] A cached read serves stored batches only while
each next batch starts exactly where the previous ended (+1); at the first gap the
reader switches to the live portal from the last served cursor. Served bytes are
decoded identically to live bytes (RS-1).

**RS-23 — Fall-forward.** [SHOULD] After switching to live, an implementation MAY
return to the cache at later covered intervals; the reference implementation does not
(stated so tests don't pin either behavior).

**RS-24 — Growth.** Cache storage is append-only with no TTL/eviction in the reference
implementation; growth is unbounded (accepted trade-off ADR-11, hazard HZ-6, open
question OQ-5). Overlapping re-insertion of an already-covered interval MUST NOT
corrupt existing entries (the reference store rejects an exact duplicate via its primary
key; a partially overlapping interval inserts alongside without touching existing rows).

**RS-25 — Cache correctness gate.** [MUST] A conforming cache never serves bytes that
differ from what the portal served for the same ⟨query hash, interval⟩ at caching time.
There is no staleness dimension — finalized data is immutable (NG5).
