# ADR-6 — Immutable file sinks: finalized-only content, coverage-window naming

Status: Accepted — implemented for the parquet binding

## Context

Files are immutable — a reorg cannot rewrite them. And files named after the min/max
block of their *rows* left gaps indistinguishable from never-indexed ranges when
tables were sparse (issue #122). Alternatives: name by row content (the bug), name
naively by cursor (breaks on sparse tables, trailing windows, disjoint ranges).

## Decision

File sinks write only finalized rows (hold-back buffer upstream) and name every
published unit for the **coverage window** the pipe processed for that table —
persisted per-table beside the cursor. Sparse tables stretch windows; stream-end
windows publish even when empty; state/data disagreement is refused before any
deletion.

## Consequences

Consumers can distinguish "empty" from "not indexed" (INV-4, RP-21); fork handling
reduces to dropping hold-back rows (CN-32). Costs: rows lag finality (deferred
visibility, INV-25), no-finality datasets lose reorg safety (FM-13), and recovery
depends on the author-purity obligation (RS-10, INV-43). Shapes DEF-14, DEF-15,
CN-12, IB-22.

Landed for parquet in PR #123 (issue #122): coverage map persisted beside the cursor,
straddle refusal E2317, invalid-range refusal E2316, empty units at stream end. Existing
datasets keep their row-min/max names — the scheme is not retroactive, and recovery
parses both. Files stay un-namespaced by pipe id, so a directory belongs to one pipe
(ADR-16).
