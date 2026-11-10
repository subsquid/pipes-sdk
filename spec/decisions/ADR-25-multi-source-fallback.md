# ADR-25 — Multi-source fallback

Status: Accepted

## Context

A pipe may read from an ordered list of sources (module 16): portals, JSON-RPC endpoints or
custom clients, switching between them on failure and back on recovery. Several choices in
that design look arbitrary or contradict single-source behaviour unless the reason is
recorded. Each section below is one of them.

## 1. Detection senses, strategy decides

A flat options object where one threshold both defines a measurement and commits to a
reaction makes it impossible to change one without the other.

**Detection** owns every threshold and measurement: probes, head polls, liveness, cooldowns
and the freshness conditions. It publishes verdicts (`lagging`, `stale`, `behind`) on the
events it raises. **Strategy** owns decisions and holds no thresholds. It is consulted when
nothing is being read, after a batch, and while a request is outstanding, and answers `use`,
`failover`, `hold` or `abort`. Its only options are `preferPrimary` and `allDownTimeoutMs`.

The strategy is internal. A user-supplied strategy function had no caller, and it forced
special cases on the engine: head polls and the stall ticker always on, a decision computed
and then offered for veto, and index validation for a programmer error. It can return as a
public seam when a concrete switching rule (cost-aware routing, pinning) needs it.

## 2. A mixed list reports itself hot

The pipe publishes one "never forks" flag, which decides whether a sink keeps its rollback
machinery and whether a finalized-only sink forces the finalized route (ADR-22). The list is
finalized only if every member is. Claiming finality for a list that can serve reorg-able
blocks lets a sink skip fork handling and then receive a fork; the opposite error only costs
an unused rollback path.

A consumer may raise a read's commitment but never lower it: a source declared finalized-only
is never read at the tip because the list as a whole is hot (INV-63). The cost is rollback
machinery armed during a finalized-only backfill, where it cannot fire.

## 3. Staleness measures unproductive wait

A per-request clock misses a finalized-only source parked at its frontier: it answers
"nothing yet" promptly, forever, and each answer restarts the clock. A clock from the last
delivered block counts consumer time, so a slow sink would fail over a healthy source.

Staleness is the time the active source spends answering without delivering a block:
accumulated across consecutive empty answers, reset by the first block, and counted only while
a request is outstanding. It is evaluated both mid-request and at batch boundaries, because a
source answering promptly with nothing never keeps a request open long enough for the first.

## 4. Reclaim is gated on why the source was left

A source dropped for a fault and since recovered trails by ordinary ingestion lag; refusing to
reclaim it strands the pipe on a costlier standby. A source dropped for not progressing and
still exhausted stalls the pipe again if reclaimed — on a synthetic frontier this looped
fifteen times where one switch was correct. Distance alone cannot tell them apart, nor can a
capability probe, which counts an empty stream as capable.

So the abandonment reason selects the check (DEF-67). Left for not progressing: it must be
level with the pipe's position; requiring it to be ahead is a bar nothing can clear (LIV-62).
Left for a fault: only a gap beyond `P-FB-MAX-LAG-BLOCKS` disqualifies it. Head and position
are sampled together, since a cached head against a live position counts the chain's progress
in between as lag. Before a stream delivers its first block, its position is the block it resumes
after. With no head sampled at a known position, a source left for not progressing counts as
behind: it has not shown it is level again.

The verdict and the abandonment reason outlive the read. A fork or a range boundary restarts
the stream, and a selection that forgot them would drive the exhausted source again and stall
for the whole staleness window on every restart. So selection also passes over a source that is
`behind` while another source is selectable; with none, it takes the `behind` one rather than
wait: a source that stalls can still catch up, and the wait gains nothing.

## 5. A source in a list retries less than a lone source

A lone portal retries indefinitely (ADR-10): there is nothing else to read. In a list that
means a struggling source is waited on and the standby is reached only when the staleness
window expires. Zero retries is the opposite error: every transient status costs a switch and
a cooldown.

A source built for a list gets a short bounded budget (`P-FB-SOURCE-RETRIES`). Settings a
caller supplies for a source win, and a ready-made transport client passes through untouched.

## 6. A source that is not a portal checks linkage in its adapter

ADR-1 gives the linkage check to the portal. The pipe sends a parent-hash anchor, and the
portal sends a fork signal when the anchor is not on its chain. A JSON-RPC endpoint has no
such check. If the pipe core did the check, the core would need chain knowledge, and ADR-1
keeps that knowledge out of the core.

So the source adapter does the check. The JSON-RPC adapter makes sure that each block links
to the block before it, the anchor included. It reports a mismatch as the same fork signal
that a portal sends. The pipe core does not change: it trusts the active source as it
trusts a portal (01), and INV-62 sends the signal to the fork path. A custom client has
the same duty (DEF-60).

The signal carries the canonical blocks below the fork (`P-FB-RPC-FORK-WINDOW`), as a
portal signal does. With only the parent of the forked block, the pipe steps back one
block for each fork, and a reorg of k blocks costs k forks.

## Consequences

The strategy is a pure function of an event and a snapshot, testable without machinery.
