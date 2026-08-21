# ADR-27 — A source in a list retries less than a lone source

Status: Accepted

## Context

A lone portal source retries a retryable status indefinitely (ADR-10). It has to: there
is nothing else to read, so giving up would end the pipe over a transient overload.

Inside a source list that reasoning inverts. Retrying forever means a struggling source
is waited on rather than handed over, and the standby that exists precisely for this is
never reached — the pipe would only move once the no-progress window expires, minutes
later. The opposite extreme is what an unconfigured transport does: zero retries, so a
single transient status hands over immediately, burning a switch and a cooldown on a blip
that would have cleared in under a second. Portals return such statuses routinely under
load.

Neither end is right, and the two are not equally wrong in the same direction: waiting
forever defeats the feature, while switching instantly makes it noisy.

## Decision

A source constructed for a list gets a short, bounded transport retry budget — enough to
ride out a brief blip, short enough that a source which is actually struggling is handed
over rather than waited on. The budget is a registered parameter
(`P-FB-SOURCE-RETRIES`), not a constant buried in construction.

Settings a caller supplies for a source win over this default, and a ready-made transport
client is passed through untouched: its owner has already decided. Sources in a list also
log through the pipe's logger, like everything else the pipe owns.

## Consequences

Transport faults now reach the fallback machinery at roughly the timescale it was
designed for, instead of either never (unbounded retry) or on the first packet loss (no
retry).

A caller who wants the lone-source behaviour for a particular source can ask for it
explicitly. The default deliberately differs from the single-source pipe, which is a
divergence a reader could mistake for an oversight — hence this record.
