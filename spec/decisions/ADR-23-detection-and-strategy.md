# ADR-23 — Detection senses, strategy decides

Status: Accepted

## Context

A fallback needs two separable things: machinery that notices a source is in trouble, and
a rule for what to do about it. Bundling them produces a single configuration surface
where a threshold like "how far behind is too far" simultaneously defines a measurement
and commits to a reaction, so an operator cannot change one without the other. It also
leaves no seam for the users who want their own switching rule — cost-aware routing,
pinning, never-switch-back — and who would otherwise have to reimplement probes, health
tracking and head polling to get it.

An earlier shape exposed one flat `policy` object holding both. Two knobs in it
(`maxStalenessMs`, `maxLagBlocks`) were read in two places for two different purposes,
which is what made the confusion concrete rather than theoretical.

## Decision

Split the surface in two along the sense/decide line.

**Detection** owns every threshold and every measurement: probes, head polls, liveness
counting, cooldowns, and the freshness conditions. It publishes *verdicts* — `lagging`,
`stale`, `behind` — attached to the events it raises.

**Strategy** owns the decisions and holds no thresholds. It is consulted at three points
— nothing is being read, a batch was delivered, a request is outstanding — and answers
`use`, `failover`, `hold` or `abort`. It may be supplied as plain options that tune the
stock rule, or as a function that replaces it. A custom function receives the stock
decision alongside the measurements, so overriding one case does not require restating
the others, and returning nothing keeps stock behaviour.

Safety is not delegated. Fork propagation, resume continuity, boundary-only switching and
the finalized floor hold whatever the strategy returns (INV-61…INV-64).

## Consequences

The stock rule becomes a pure function of an event plus a snapshot, so it is testable
without any machinery. Adding a proactive check later means adding a verdict and an
event, which is a compatible extension for strategies that ignore unknown events, and a
breaking one for strategies that exhaustively switch on them.

A custom strategy can starve or thrash the pipe. That is accepted: it is the same class
of freedom as a custom sink, and the safety invariants bound the damage to liveness.
