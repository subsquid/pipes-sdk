# ADR-23 — A shared class-C feed carries one payload shape, and finality rides metadata

Status: Accepted

## Context

A class-C topic that several parties consume is not just a changelog; it is a substrate.
The BigQuery CDC binding (IB-28) exists so a consumer can attach a subscription and land
the whole table with no code at all, and that same topic must also serve consumers the
substrate cannot serve on its own: an aggregator folding rows into state, and a validator
checking a block's rows are all present. Serving the second and third must cost the first
nothing.

Two properties of the substrate decide the shape of everything below. A subscriber's
server-side filter reads message metadata, never the body. And that filter is fixed when
the subscription is created, by whoever creates it — so a filter cannot be added later to a
subscription somebody else already owns.

The sink previously published a second kind of message beside the rows: first as a separate
payload shape on a "signals" route, then as the producer's own record about the feed, which
carried the finalized head. Both are the same violation. A schema-mapped subscription
rejects a body carrying columns its table does not have and stalls, so every zero-code
landing would need a filter that excludes it — a filter that must exist before the first
such message is ever published, and that the producer cannot add on the consumer's behalf.
There is no safe moment to publish the first one.

The finalized head still has to reach a stateful consumer. It is the one input the row
stream does not contain: there is no count, no end marker, and on an unordered subscription
silence is not proof, so nothing in the rows answers "have all corrections below this
height already arrived?" — and without an answer, per-block state can never be compacted.

## Decision

A topic carries one payload shape. Everything published on it is a row change of the sink's
output; no watermark record, no fork announcement, no status record (RP-45). What the
producer needs to say about the feed rides message metadata, which is what a filter reads
and what a schema-mapped landing ignores for free.

The source's finalized head therefore rides an attribute on every message, as a reference
value. Read by taking the maximum, it needs no version comparison, which makes duplicate and
reordered delivery harmless and lets the producer keep no durable record of what it last
published; a cold consumer has a floor to reason against from its first message. It is an
input to whatever confirmation policy the consumer chose, never a proof about the row it
travels with — a contract permitting a per-row reading would reinstate exactly the barrier
the version run already provides.

The version run carries the rest. It was already gap-free and block-ordered by deployment;
both become stated and enforced: a configuration splitting the producer-wide counter across
topics is refused, a batch stepping backwards in block order is refused, and either may be
waived only by declaring that no consumer reads the barrier. What the barrier does not say
is stated with it — it is about transport, not about the source, the mapping, or finality —
and so is the fact that a server-side filter trades it away.

A fork announcement is deliberately given up rather than deferred. Its body is no table's
row, so this decision puts it out of reach: a landing converges on the compensations, a
folding consumer has the rule that a `DELETE` at block *N* with version *S* retracts every
contribution from a row with block ≥ *N* and version < *S*, and a validator has the block
boundary. What is given up with it is one message retiring a whole fork — the rule converges
only as the last compensation lands — and coverage of a revisable route, which can pass
through a fork without a single `DELETE` for a `DELETE`-based rule to see. The second is why
a feed relying on that rule pins write-once ids rather than treating them as a default.

## Consequences

Finality becomes expressible on the wire — as a reference value with stated limits rather
than an approximation of a proof — which closes the registered gap that a class-C consumer
could never learn when a row became reorg-proof. A zero-code landing needs no subscription
filter at all, and a filter it does create can never be the thing that breaks it.

The watermark now stalls while the output is quiet, where a producer-paced record did not.
That is accepted and stated: on a sink that has caught up, the next message a consumer
receives is both the next watermark and the next row that could add to what it holds, so
quiet delays compaction without growing what waits to be compacted.

Riding rows does not make the value a statement about the sink's own progress either. It is
the source's head, so a backfilling sink stamps one far above the rows it is publishing. The
reference bounds retraction, the version run bounds arrival, and a consumer retiring state
holds below both — which is the price of the sink keeping no durable record of its own
position, and is stated with the contract rather than left to be discovered.

The removed signals route leaves the internal events feed without a publication path on this
sink. Its rollback needs differ — it carries no changelog envelope and no compensations — so
it needs a mechanism of its own rather than a second shape on a shared feed.

Shapes RP-24, RP-44, RP-45, CN-17, CN-35, CN-46, IB-28, GAP-38.
