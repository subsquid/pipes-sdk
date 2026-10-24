# ADR-1 — Chain integrity is the portal's job, not the pipe's

Status: Accepted (historical)

## Context

A streaming client could re-verify parent-hash linkage of every received block, or
trust the gateway. Verification in the client duplicates work the portal already does,
requires hash access on every chain family, and cannot detect a portal serving a
consistent-but-wrong chain anyway.

Order against the pipe's own request is a different matter. The request names its start
and end, and the client moves its position to one past each block it receives, so a
duplicate or a lower block left unchecked is written to the sink and then fetched again:
duplicated rows, no error. Checking it needs no chain knowledge, only the request.

## Decision

The pipe sends a parent-hash anchor with each stream request and advances it per block;
the portal validates linkage and signals divergence with a 409 + canonical chain. The
pipe never independently verifies hashes or finality assignment.

The pipe does check each block number against its own request: a block at or below one
already delivered for the request, below the requested start or above the requested end
halts with a coded error (E1006) before any stage sees it. A gap between block numbers
stays legal, since the portal skips blocks with no matching data.

## Consequences

Simpler, chain-agnostic core; single source of chain truth. The portal is fully trusted
(trust model in 01); portal contract violations must be *detected as inconsistencies*
and halt (WP-41, RP-43, FM-14…FM-16) rather than be repaired. Shapes NG1, NG5, WP-10,
WP-15, WP-40, IB-3, IB-4.
