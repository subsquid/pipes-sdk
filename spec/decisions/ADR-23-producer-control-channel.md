# ADR-23 — A producer control channel of changelog-shaped records, over a general application-message route

Status: Accepted

## Context

Class C repairs a fork per row (RP-44): every orphaned id gets a compensating operation
carrying the identity and attributes of what it repairs. A consumer that mirrors a table
applies the repairs by version and converges.

A consumer that folds an aggregate instead has two further questions, and they turn out to
have different answers.

**Undoing a fork does not need a new message type.** Because a compensation carries the
orphaned row's own value and is sequenced ahead of the operation that replaces it (RP-46),
*discard every contribution from an operation with block ≥ N and version < S, on seeing a
deletion at block N with version S* is correct under unordered delivery. The same version
sequence answers the block boundary: a contiguous run reaching the first operation of a
higher block is missing nothing below it. Both properties were already true of the sink and
promised nowhere, which is what RP-46 fixes.

**Deciding when state is safe to compact does.** Folding per-block state into a base a
retraction can no longer reach takes an input the operation stream does not contain: the
source's finalized head. The sink holds it — it is what decides which operations enter the
rollback manifest at all — and nothing publishes it (GAP-39).

The first answer shipped was a second route kind that published an application-defined
payload with no changelog envelope, with its own per-batch mapping and two declared fork
strategies. It worked, and it was too much. A route with an arbitrary payload is a second
data plane: it duplicates what the changelog route already carries, and a destination
that applies a changelog cannot be attached to it at all.

## Decision

Drop the second data plane; keep one optional, producer-wide control channel whose records
are ordinary changelog operations — same envelope, same producer version, same id
namespacing (RP-45). It carries two kinds, and the reason each is there is different:

- a **finality watermark** (RP-47), because no consumer can derive the value from the
  operation stream;
- a **fork announcement**, because the sink's fork resolution is the only point at which a
  record can be emitted atomically with the rewind — an optimisation over the rule above,
  buying latency and covering the revisable case where a fork produces no deletion at all.

Four consequences follow from the shape rather than from convention:

1. **Everything on the wire is one format.** A destination that applies changelogs lands a
   control record like any other row. No consumer needs a second parser, and no record is
   off-limits to the zero-code landing path.
2. **The channel is producer-wide, not per stream.** Both a fork and a finalized head are
   properties of the producer's chain view, not of any one output.
3. **It rides the data destinations, marked in filterable metadata.** Both kinds are
   statements about the operations already there, not a separate feed. Keeping them on the
   same destination gives a folding consumer one subscription rather than two, and — because
   every copy shares one version — leaves a single-destination producer's version sequence
   gap-free, which is what makes RP-46's barrier usable at all. The marker cannot be a
   payload field: a subscriber's filter language reads metadata only, so a field would force
   every subscriber to parse what it wants to skip. It is carried by **every** operation
   rather than only by control records — a subscriber whose selection is fixed at creation
   must be able to name what it wants, not what it does not, and a rule written as an
   absence silently admits whatever is added next.
4. **The epoch is readable where ordinary operations are built.** Stamping it on data
   operations is what lets an unordered consumer fence a branch that has been abandoned;
   without that, an announcement raises an epoch nothing else carries.

The channel is additive: RP-44's compensations are still published, because they are what
keeps a mirroring destination correct. Neither consumer pays for the other — the
compensations inherit the pre-fork epoch, so a folding consumer's fence discards them.

## Consequences

**A fork is announced once, or not at all.** The announcement lives in the fork
transaction, so a crash leaves it unpublished, never unrecorded; the recovery drain
replays it. There is no "announced but not rewound" state and no restart protocol to
recover one — the window is unrepresentable rather than handled.

**The watermark needs no such machinery, and gets none.** It is monotone and read by
maximum, so a record lost to a rolled-back commit or a restart costs nothing: the next one
carries a higher value. Emission bookkeeping is therefore in-memory, and the republication
rate is a chattiness setting rather than a correctness one. It is emitted on the sink's own
commit, not on operation traffic — stamped on operations it would stall exactly when a
sparse feed's consumer needs it (RP-47).

**A schema-applying subscriber must be configured before the first control record.** Its
filter has to exclude them, and in the reference binding that filter is immutable once the
subscription exists — so adding a control channel to a live feed means recreating those
subscriptions, not amending them. Note this now binds on the watermark, which starts flowing
immediately, rather than on a fork that may be weeks away. The route may name a separate
destination to avoid it, which is the escape hatch for a feed whose subscribers cannot be
recreated; it costs the gap-free sequence and the single subscription.

**Publishing an arbitrary payload is no longer expressible.** A producer whose consumers
genuinely need a non-changelog schema has to model it as a changelog row (`_id` plus the
body it wants) or use a different sink. This is the cost of the decision, accepted
because the alternative was carrying a whole second data plane, and its only unique
capability was the fork-time hook this keeps.

**Finality is published as a reference, and stays one.** GAP-39 closes on the transport,
not on the question behind it: the number is the source's view, an input to a consumer's own
confirmation policy. Stating it as a proof about a row would put back the exact barrier
RP-46 shows the sequence does not need.

Shapes RP-45, RP-46, RP-47, IB-28, CN-35.
