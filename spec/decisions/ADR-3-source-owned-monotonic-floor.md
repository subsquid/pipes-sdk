# ADR-3 — The source owns a monotonic finalized floor; sinks persist it verbatim

Status: Accepted (historical)

## Context

Different portal replicas and sources disagree on finality depth; a replica swap or
transient absence of a finalized head could "un-finalize" data already released to
immutable storage. Clamping could live in every sink (N implementations, N bugs) or
once in the source.

## Decision

The source maintains the single monotonic finalized watermark, clamping every reported
head upward against the floor and re-seeding it from persisted sink state (`finalized`
only, never `latest`). Sinks persist the clamped values verbatim and never re-derive
finality.

## Forks below the floor

A fork with no common ancestor at or above the floor halts the pipe (E1003). The pipe
halts before the sink removes data.

A reorganization below the reported finality can occur. If a sink can roll back
correctly, a rollback is better than a halt. But today no sink can prove that it can
roll back below the floor:

- The transactional sink keeps undo records only for blocks that were unfinalized when
  the sink wrote them.
- The replay cache keeps finalized blocks and never invalidates them (ADR-11).
- The compensating sink keeps baselines only down to the floor.

A later decision can let a sink roll back below the floor when the sink declares how
deep it can undo. Until then, the pipe halts.

Before this decision, the ancestor search did not check the floor correctly. It
compared each block with the floor of its own record, and the chain of a record is
always above that floor. So the check never stopped the search, and a canonical match
below the floor was the result.

The floor fallback also had an error. It used the floor only when the floor was the
last entry in the canonical window. When the window held lower blocks, the search went
to an older record and found a block below the floor. Because of this, a shallow fork
rolled back a finalized block that was still canonical.

Now the resolver reads every record first and uses the highest floor of all records.
Records do not always arrive in commit order, so an older record can hold a higher
floor. The search stops at the first retained block below that floor. A record holds
only the blocks of its own batch, so older records can hold blocks above the floor, and
the search reads them first. When the search stops, the floor is the ancestor if the
canonical window holds it. If the window does not hold it, the pipe halts.

## Consequences

Finality logic exists exactly once (INV-2, INV-12, WP-2, RP-4); immutable sinks are
safe against head regressions. Trade-off: a floor poisoned too high (e.g. adopted from
a foreign pipe's state, GAP-7) never self-corrects — monotonicity is deliberate and
absolute. Shapes DEF-6, REQ-5, WP-42, WP-44.
