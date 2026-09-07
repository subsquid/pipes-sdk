# ADR-26 — Reclaim is gated on why the source was left

Status: Accepted

## Context

Eager preference returns the pipe to the most preferred healthy source. Two situations
that call for opposite answers look identical at the moment of the decision, because in
both the candidate's head sits just under the pipe's position:

A source dropped for a transport fault, now recovered. It trails by ordinary ingestion
lag — a source ingesting the chain is always a little behind whatever is reading the tip.
Refusing to reclaim it strands the pipe on a more expensive standby indefinitely.

A source dropped for not progressing, still exhausted. Reclaiming it stalls the pipe
until it is dropped again, and repeats. Measured on a synthetic frontier, the loop
produced fifteen switches where one was correct.

Distance alone cannot separate them, and neither can a capability probe: the probe counts
a stream that ends without data as capable, which is precisely what an exhausted source
returns.

## Decision

The reason the source was abandoned selects the question asked before reclaiming it.

Abandoned for not progressing: it must be able to serve the pipe's position before it is
read again. Being *level* with the pipe is sufficient — requiring it to be ahead is a bar
nothing can clear while another source drives the tip, and would demote it permanently
(LIV-62).

Abandoned for a fault: only a structural gap disqualifies it. The allowance is the same
"acceptable distance behind" that governs lag failover, so ordinary jitter never blocks a
reclaim.

The verdict is computed by detection, which owns both thresholds and the abandonment
reason, and is published as a per-source `behind` flag (DEF-67). A head and the position
it is compared against are sampled together: comparing a cached head with a live position
would count everything the chain produced while the head sat in cache, which on a
sub-second chain exceeds the entire allowance.

## Consequences

A source dropped for not progressing that never catches up is never reclaimed, which is
the intent. One that recovers fully is reclaimed on the next boundary after its head
reaches the pipe.

The gate applies only to reclaiming a *preferred* source. Selection after a failure stays
optimistic and ignores it: with nothing else to read, an optimistic attempt beats refusing
to read at all.
