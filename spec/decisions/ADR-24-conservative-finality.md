# ADR-24 — A mixed source list reports itself hot

Status: Accepted

## Context

Sources in one list may differ in commitment. The useful case is deliberate: a
finalized-only source is cheap and reorg-free, so it can carry a backfill, while a
hot source follows the chain tip afterwards.

The pipe publishes a single "this stream never forks" flag. Downstream that flag decides
whether a sink keeps its rollback machinery, and whether a sink that accepts only
finalized data forces the finalized route (ADR-22). One flag must therefore describe a
list whose members disagree.

## Decision

The list reports itself finalized only if every member is finalized-only. One hot member
makes the whole list hot.

The direction is not symmetric and the conservative one is forced: claiming finality for
a list that can serve reorg-able blocks would let a sink skip fork handling and then
receive a fork. Claiming hot for a list that happens to be all-finalized only costs an
unused rollback path.

A consumer may raise the effective commitment of a read — a finalized-only sink still
pins every source to its finalized stream — but a consumer's *lower* commitment is not
propagated: a source declared finalized-only is never read at the tip because the list as
a whole is hot (INV-63).

## Consequences

A mixed list keeps a sink's rollback machinery armed for the whole run, including the
long finalized-only backfill where it cannot fire. That cost is accepted; the alternative
is a flag that is wrong exactly when it matters.

Mixing is otherwise unrestricted. An earlier revision rejected mixed lists at
construction, which also removed the topology this decision exists to support.
