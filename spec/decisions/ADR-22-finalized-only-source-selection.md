# ADR-22 — Finalized-only sinks select the finalized source

Status: Accepted

## Context

Immutable and ephemeral finalized-only sinks previously consumed the full stream and
held rows above the reported finalized floor in memory. That mechanism cannot make a
bounded run complete correctly: the full stream ends when it has delivered the
configured range, not when every delivered block has finalized. A range ending above
the floor can therefore leave rows in the sink's memory while the pipe reports
successful completion.

The buffer also makes finality a sink concern even though the portal already exposes a
finalized head and stream. It adds memory proportional to finality depth and forces a
sink that must never publish reorg-able data to implement a fork path solely for rows
it has not published.

## Decision

A sink that accepts only finalized delivery declares that capability before reading.
The pipe then selects one effective finalized portal view before symbolic range
resolution and uses it consistently for transformer startup, caches, and block reads
(WP-7, IB-11). A configured full-stream preference is overridden with a warning; the
sink is told the effective mode.

On a finalizing dataset, the finalized route admits blocks only after finalization and
does not emit forks. The sink therefore feeds every delivered row directly into its
ordinary commit protocol, without a finality hold-back, and may omit fork resolution
(RP-7, CN-32). Full-stream sinks retain the existing rollback contract unchanged. A
dataset with no notion of finality cannot make the output reorg-safe; that limitation
remains explicit, and an unexpected fork on the finalized route fails safely (FM-13).

This decision supersedes only the finality-mechanism portions of two accepted
decisions:

- ADR-6's sink-side hold-back and fork-drop mechanism is replaced by source selection.
  Its finalized-only output and coverage-window naming decisions remain in force.
- ADR-18's assignment of finalization buffering and fork handling to the Parquet sink
  is replaced by source selection. Its engine seam, staging, checkpoint, recovery,
  naming, and publication decisions remain in force.

## Consequences

A bounded finalized-only run cannot finish with an uncommitted finality tail. Memory no
longer contains a term proportional to finality depth, and finalized-only targets need
no rollback state or handler. Range resolution, custom caches, and transformers see the
same effective source mode as the sink, so `latest` and data reads cannot disagree.

The finality buffer API is removed. Custom finalized-only targets declare the new
capability and write rows as delivered; custom caches use the effective portal view
passed to them. Reorg safety now depends directly on the dataset's finalized-route
semantics, which makes the no-finality limitation visible rather than approximating it
inside a sink.

Shapes DEF-15, WP-7, RP-7, CN-12, CN-14, CN-22, CN-32, INV-25, FM-13, PF-1, IB-11.
