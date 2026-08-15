# ADR-21 — Compensating append-only sinks as their own durability class

Status: Accepted

## Context

ADR-5 defines four durability classes over one axis: what the *store* lets a sink take
back. T rewrites in a transaction, W deletes a WAL range, K deletes unpublished files,
A delegates the delete to author code. Class K's alternative — publish nothing until it
is finalized (RP-20, CN-12) — is what an append-only medium normally gets.

A message bus is the case none of them covers. Published messages cannot be read back,
updated, or deleted, so no rollback mechanism exists at all; and holding data back until
finality defeats the point of a live feed, whose whole value is latency. The two
existing answers are therefore both unavailable: nothing can be rewritten, and waiting
is not an option worth taking.

What such a medium *can* do is publish more. If the output is modelled as a changelog of
keyed operations rather than as rows, a fork is repairable by appending the operations
that undo it — provided the sink can still name, at fork time, every row it published
above the fork point, and provided consumers apply operations in an order-insensitive,
idempotent way.

## Decision

A fifth durability class, **C (compensating append-only)**: publish immediately,
unfinalized data included, and repair a fork by publishing compensating operations
(CN-17, CN-35). Its preconditions are normative, not conventions:

1. **A local combined state.** Cursor, rollback manifest, finalized baselines, publish
   outbox and sequence counters commit in one local transaction per batch, before
   anything is published. The manifest is what makes an already-published operation
   nameable after a fork; the outbox is what keeps the cursor from ever running ahead
   of the wire.
2. **A per-row version the producer owns.** Every operation carries a monotone sequence
   that never rewinds — not across restarts, not across forks — so a compensation
   always dominates the operation it repairs and a republished operation is recognisably
   stale rather than newer.
3. **Compensations inherit the identity and the filter attributes of what they repair**,
   so a consumer subscribed to a filtered slice receives the repair for its own slice
   without a control-message clause.
4. **Fork-capability is proven, not assumed.** A dataset that reports no finalized head
   is refused unless the operator explicitly declares it fork-free: without a watermark
   there is no manifest, and this medium cannot retract a bad publish.

Losing the local state is a producer-identity event, not a restart: the sequencer is
gone, and recovery means a fresh id namespace plus a consumer re-bootstrap.

## Consequences

The class buys tip-latency delivery on a medium that cannot rewrite, at three costs the
other classes do not pay.

**Consumers own convergence.** A class-C sink's guarantee ends at "every operation, and
its repair, is published in a recoverable order". Whether the downstream state converges
depends on the consumer applying operations idempotently and by version. The contract is
small enough to state as one conditional statement, but it is a contract the other
classes do not impose at all.

**Duplicates are structural.** Delivery is at-least-once end to end: the crash window
between commit and publish is drained by republishing byte- and sequence-identical
operations. Exactly-once is out of reach and is not claimed (REQ-3's effective
exactly-once holds at the *sink*, not at the bus).

**Failures degrade to silence rather than to noise.** A lost sequencer publishes
low versions under ids consumers already hold at higher ones; every affected row
silently freezes, and nothing on the wire distinguishes that from health. A durable
producer epoch carried with every operation would close it in-band (version compared as
`(epoch, seq)`); it was declined to keep the published metadata minimal and the consumer
contract a single scalar comparison. The exposure is handled operationally instead — a cold-start
warning, an alertable gauge, and a runbook (GAP-38) — and remains an additive change if
it ever bites.

Shapes CN-17, CN-35, CN-46, RP-24, RP-44, IB-28.
