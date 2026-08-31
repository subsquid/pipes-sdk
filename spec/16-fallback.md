# 16 — Multi-source fallback (DEF-60…, WP-60…, INV-60…, LIV-60…, FM-60…, OB-60…)

A pipe may read from an ordered list of block sources instead of one. This module
specifies how the list is driven: which source is read, when the read moves to another
source, and what must remain true of the delivered stream while it moves. It extends the
single-source ingestion contract (04) — every property there continues to hold of the
stream a source list produces — and adds nothing to the sink contract: a sink cannot tell
a multi-source pipe from a single-source one (INV-60).

Bands reserved for this module: `DEF-60…69`, `WP-60…79`, `INV-60…69`, `LIV-60…64`,
`FM-60…69`, `OB-60…66`.

## Model

**DEF-60 — Source.** A block-stream provider satisfying the ingestion read contract
(WP-10…WP-16): given a start position and an optional parent-hash anchor it yields
batches in ascending block order, and signals a fork rather than serving a divergent
chain. A source carries a **commitment** (DEF-61) and a **name** used only for reporting.

**DEF-61 — Commitment.** Whether a source serves only finalized blocks (`finalized`) or
serves to the chain tip and may fork (`hot`). A source's commitment is a property of the
source; a consumer of the pipe may *raise* the effective commitment of a read but never
lower it (INV-63).

**DEF-62 — Source list.** A non-empty sequence of sources in descending preference.
Position 0 is the **primary**; the rest are **standbys**. All sources in a list MUST
answer the same query, so that their outputs are interchangeable at any block boundary.

**DEF-63 — Active source.** The single source the pipe is currently reading. At most one
source is read at a time; a list is not a fan-out.

**DEF-64 — Health.** A per-source value in {`healthy`, `unhealthy`, `unknown`}. `unknown`
is the initial and post-cooldown value and means *not yet disproven*: it is eligible to
be read, because reading is the fastest test of a source. `healthy` additionally requires
confirmed capability (DEF-65).

**DEF-65 — Capability.** Evidence that a source can serve *this pipe's query* at the
indexing frontier, not merely that it is reachable. Delivering a batch is evidence; so is
a successful probe of a one-block slice of the same query. Liveness alone is not.

**DEF-66 — Detection.** The sensing half of the machinery: the probes, polls and clocks
that produce a source's health and the freshness **verdicts** (DEF-67). Detection owns
every threshold.

**DEF-67 — Verdict.** A boolean conclusion detection attaches to an event, so that the
deciding half never re-derives it: `lagging` (the pipe is further behind an independent
chain-head reference than the configured allowance, and has previously reached the tip),
`stale` (the active source has spent longer than the configured window answering without
delivering a block), `behind` (a source's reach has fallen under the pipe such that
reading it would only stall).

**DEF-68 — Strategy.** The deciding half: a total function from an event and a snapshot
of detection's output to one of `use(i)`, `failover`, `hold`, `abort`. It chooses; it
never senses (ADR-23).

**DEF-69 — Unproductive wait.** Time the active source has spent answering without
delivering a block, accumulated across consecutive empty batches and reset by the first
batch that carries one. It excludes time the *consumer* spends between batches: a slow
sink MUST NOT make a healthy source look stalled.

## Driving the list (WP-60…)

**WP-60 — Selection.** When no source is being read — at start, or after the active
source was abandoned — the pipe consults the strategy with a `select` event carrying the
classified cause of the previous failure, if any. `use(i)` reads source `i`; `hold` waits
`P-FB-ALLDOWN-POLL-MS` and asks again; `abort` fails the read. A source index outside the
list is a programmer error and MUST fail the read rather than be clamped or ignored.

**WP-61 — Reading and resuming.** The pipe reads the active source from the position
after the last block delivered to the sink, anchored to that block's hash. This is the
same resume the ingestion contract already specifies (WP-12), applied unchanged when the
position was reached through a different source.

**WP-62 — Switch points.** A switch MUST occur only between batches: after a batch has
been delivered and before the next is requested. Mid-batch switching is not permitted
even when the strategy asks for it, because a partially consumed batch has no resume
position (INV-61).

**WP-63 — Boundary decision.** After each delivered batch the pipe refreshes the other
sources' heads (subject to `P-FB-HEAD-TTL-MS`), updates the freshness gauges, and consults
the strategy with a `batch` event carrying the `lagging` and `stale` verdicts. The refresh
runs only when something can consume it — lag detection enabled, the `stale` verdict
raised, a custom strategy installed, or a more-preferred source available to reclaim;
otherwise the boundary MUST NOT poll standbys (at tip pace the batch cadence outruns
`P-FB-HEAD-TTL-MS`, so an unconditional refresh degenerates to one poll per batch per
standby). `use(i)`
switches to a different source without penalising the active one; `failover` abandons
the active source, recording a cause; `hold` continues.

**WP-64 — Stall decision.** While a request to the active source is outstanding, the pipe
consults the strategy every `P-FB-TICK-MS` with a `stall` event carrying the elapsed
unproductive wait (DEF-69) and the `stale` verdict. `failover` abandons the source;
anything else keeps waiting. A source that keeps *answering* without progressing — a
finalized-only source parked at its finality frontier answers "nothing yet" indefinitely
— MUST be recognised by this path or the boundary path; a definition of staleness that
resets on an empty answer does not satisfy this (ADR-25).

**WP-65 — Probing.** A standby's capability is re-tested no more often than
`P-FB-PROBE-INTERVAL-MS`, never concurrently with itself, and time-boxed by
`P-FB-PROBE-TIMEOUT-MS`. A probe is fire-and-forget: it MUST NOT block a batch boundary. A
probe verdict that arrives after the read it was issued for has ended MUST be discarded
(INV-66).

**WP-66 — Head polling.** Each source's head is polled at *its own* commitment unless the
read has a forced commitment, in which case that one applies to every source. Polls are
cached for `P-FB-HEAD-TTL-MS` and time-boxed by `P-FB-HEAD-TIMEOUT-MS`; a timed-out poll counts
as a liveness failure and contributes no head. A head and the pipe position it is
compared against MUST be sampled at the same instant. Only the head *number* is consumed,
so a source offering a number-only poll (`eth_blockNumber` on an RPC source) MUST be
polled through it rather than through a full block-reference lookup.

**WP-67 — Health transitions.** A stream error, a capability failure, or
`P-FB-LIVENESS-FAIL` consecutive liveness failures make a source `unhealthy` for
`P-FB-COOLDOWN-MS`, after which it returns to `unknown`. `P-FB-LIVENESS-RECOVER` consecutive
liveness passes *and* confirmed capability make it `healthy`. Going unhealthy discards a
source's capability confirmation, so a source that stays reachable while failing the real
query cannot flap back on liveness alone.

**WP-68 — Read-through cache exclusivity.** A source list is not compatible with the
read-through block cache, which keys and fetches through a single portal identity. The
combination MUST be rejected when the pipe is constructed rather than silently ignoring
the cache or caching one source's answers under another's identity.

**WP-69 — Symbolic range resolution.** Resolving a symbolic range bound (`latest`, or a
timestamp) over a source list uses the highest head any source reports, and each poll is
time-boxed (`P-FB-HEAD-TIMEOUT-MS`). The result MUST distinguish *"no source could be
asked"* from *"there is no head"*: the former fails the read, because a resolver that
reads an unanswered lookup as "no head" resolves `latest` to the genesis block and
silently backfills the entire chain. A timestamp bound requires at least one source able
to resolve it.

**WP-70 — Transport retry budget.** A source in a list retries a retryable transport
failure a bounded number of times (`P-FB-SOURCE-RETRIES`, configurable for the list)
before the failure is reported to the machinery, rather than the unbounded retry a lone
source performs (ADR-10). Both
extremes are wrong here: retrying forever waits on a struggling source instead of using
the standby that exists for it, and not retrying at all spends a switch on every transient
status. Settings supplied for a specific source override this, and a pre-built transport
client is used as given.

## Deliberately unspecified

These are free for an implementation to choose; conformance tests MUST NOT pin them.

- Which source's dataset metadata (name, aliases, start block) the list reports, and
  whether that value changes across a switch. Only the *block* stream is specified to be
  indistinguishable (INV-60).
- The order in which standby heads are polled, and whether polls run concurrently.
- Whether a strategy is consulted for sources it can never select.
- The wording of any diagnostic message; only the bounded fields of a cause (OB-61) and
  the fact of a single transition log (OB-66) are contractual.

## Safety (INV-60…)

**INV-60 — Sink indistinguishability.** [response]
The sequence of blocks a sink receives from a source list is a sequence it could have
received from a single source: ascending, gapless, without duplicates across a switch,
and with fork signals delivered unchanged.
*Why:* the whole construction is only admissible if it cannot be observed downstream.
*Check:* CT-1 oracle comparison of a multi-source run against a single-source run over
the same synthetic chain.

**INV-61 — Switch continuity.** [transition]
A switch preserves the resume position exactly: the next source is asked for the block
after the last one delivered, anchored to its hash. No block is delivered twice and none
is skipped, whatever the strategy returns.
*Why:* a gap is silent data loss; an overlap corrupts append-only sinks.
*Check:* CT-2 forced-switch scenarios asserting the requested start of every source read.

**INV-62 — Fork propagation.** [transition]
A fork signalled by any source propagates to the pipe's fork path unchanged. It is never
converted into a source failure, never triggers a switch, and never selects a different
source as a way of avoiding it.
*Why:* a fork is a statement about the chain, not about the source; switching would
resume from an invalid position (ADR-1).
*Check:* CT-2 fork injected on the active source and on a source reached after a switch.

**INV-63 — Commitment is raise-only.** [state]
A source list reports itself finalized only if *every* member is finalized-only. A
consumer's declared commitment may raise a read's finality but MUST NOT lower a source's
own: a source declared finalized-only never serves reorg-able blocks because the list as
a whole is hot.
*Why:* the reported value gates whether a sink keeps its rollback machinery, and whether
a finalized-only sink forces the finalized route (ADR-24).
*Check:* CT-1 assert the reported commitment over each combination; CT-2 assert a
finalized-only source is never read at a lower commitment.

**INV-64 — Finality floor across sources.** [state]
The monotonic finalized floor is a property of the pipe, not of a source. A switch MUST
NOT lower it, and a source reporting a shallower or absent finalized head after a switch
MUST NOT un-finalize what was already committed.
*Why:* sources may disagree on finality depth; the shallowest must not win retroactively
(ADR-3).
*Check:* CT-1 oracle over a switch between sources with different finality depths.

**INV-65 — One read at a time.** [state]
A source list drives one read at a time. A second concurrent read MUST be refused rather
than interleaved, because the freshness state and the reported gauges describe the read
in flight.
*Why:* two reads silently reinterpret each other's commitment and freshness.
*Check:* CT-2 second concurrent read rejected.

**INV-66 — Verdicts describe the read that asked.** [response]
A capability verdict is applied only to the read that requested it. A verdict arriving
after that read ended MUST NOT change any source's health.
*Why:* probes outlive their read; a verdict about a previous query — possibly at a
different commitment — would rule a source in or out for a query it was never asked
about.
*Check:* CT-2 probe resolved after the read completes; health unchanged.

**INV-67 — Evidence must be eligible.** [state]
A source that is `unhealthy` contributes no freshness evidence: its last known head MUST
NOT be treated as a fresher alternative, because it is not a source the pipe may switch
to.
*Why:* otherwise the pipe abandons a working source in favour of one selection will
refuse, and walks into an all-down gap.
*Check:* CT-2 standby poisoned by capability failure while holding a head above the
cursor; active source is retained.

**INV-68 — Strategy faults are not source faults.** [response]
A strategy that throws, aborts, or names a source outside the list ends the read with
that error. It MUST NOT be attributed to the active source, retried, or allowed to
re-drive the list.
*Why:* attributing a programmer error to a source produces an unbounded retry loop and
hides the defect.
*Check:* CT-2 strategy throwing and aborting; error surfaces to the caller.

**INV-69 — Reported cause matches the trigger.** [response]
The cause recorded when a source is abandoned names what actually tripped: a freshness
verdict, a classified transport or protocol failure, or the strategy. A cause MUST NOT be
attributed to a verdict that did not fire.
*Why:* the cause is both an operator-facing signal and an input to later decisions
(WP-63, ADR-26); a mislabel corrupts both.
*Check:* CT-2 assert the recorded cause for each abandonment path.

## Liveness (LIV-60…)

**LIV-60 — Handoff.** If the active source stops delivering while another eligible source
can serve the pipe's position, the pipe eventually reads that source.
*Precondition:* at least one other source is eligible and its head exceeds the pipe
position. *Bound:* `P-FB-MAX-STALENESS-MS` + one `P-FB-TICK-MS`. *Witness:* the active source
changes and delivery resumes.

**LIV-61 — Reclaim.** A preferred source that recovers and can serve the pipe's position
is eventually read again, without operator action.
*Precondition:* `preferPrimary` is eager; the source is `healthy` and not `behind`.
*Bound:* one batch boundary after those hold. *Witness:* the active source index
decreases.

**LIV-62 — No permanent demotion.** A source abandoned for not progressing regains
eligibility for reclaim once it can serve the pipe's position. The rule that gates its
return MUST be satisfiable: being *level* with the pipe is sufficient, since a source can
never be ahead of a pipe that is following the chain tip through another source.
*Precondition:* the source's head reaches the pipe position. *Bound:* one head poll plus
one boundary. *Witness:* the source is read again.

**LIV-63 — Fresh start.** A source that begins to be read starts with a fresh stall
clock. Inherited unproductive wait MUST NOT make a newly promoted source stale before it
has had the chance to deliver anything.
*Precondition:* a source is selected. *Bound:* immediate. *Witness:* the reported
staleness is zero at the first boundary of that read.

**LIV-64 — All-down progress.** While no source is eligible the pipe keeps re-selecting
at `P-FB-ALLDOWN-POLL-MS` intervals; it either resumes when one recovers, or fails with an
all-sources-down error once `P-FB-ALLDOWN-TIMEOUT-MS` has elapsed. It does not block
silently forever unless configured to wait indefinitely.
*Precondition:* every source `unhealthy`. *Bound:* `P-FB-ALLDOWN-TIMEOUT-MS` when finite.
*Witness:* delivery resumes, or the read fails with the all-down error.

## Failure model (FM-60…)

| ID | Fault | Required response |
|---|---|---|
| FM-60 | Active source errors mid-stream (transport, protocol, decode) | Classify, mark unhealthy with cause, re-select, resume at the same position (WP-60, INV-61) |
| FM-61 | Active source stops delivering (hangs, or answers without progress) | Stall path; hand off if a fresher eligible source exists, otherwise hold and report a chain stall (WP-64, LIV-60) |
| FM-62 | Active source falls behind an independent head reference | `lagging` verdict at the next boundary; stock strategy fails over (WP-63) |
| FM-63 | Standby reachable but cannot serve the query | Capability failure keeps it out of `healthy`, so it is never switched *up* to; it remains selectable as a last resort (DEF-64, WP-67) |
| FM-64 | Standby head poll hangs | Time-boxed; counts as a liveness failure; MUST NOT delay the active source's next batch (WP-66) |
| FM-65 | Every source unhealthy | All-down handling (LIV-64) |
| FM-66 | A source signals a fork | Propagate unchanged (INV-62) |
| FM-67 | Strategy throws, aborts, or returns an invalid index | End the read with that error (INV-68) |
| FM-68 | Sources disagree on commitment | Permitted; the list reports itself hot (INV-63) |
| FM-69 | Consumer forces a commitment the source would not choose | Raising is honoured for every source; lowering is ignored (INV-63) |

## Observability (OB-60…)

**OB-60 — Active source.** Which source is being read, per source and per pipe.

**OB-61 — Per-source health.** Each source's health, and for an unhealthy source the
cause as bounded labels: which check failed, a coarse reason, and a status or protocol
code where one exists. The unbounded detail — endpoint, request, message — is a log
field, never a metric label.

**OB-62 — Switch count.** Cumulative switches, per pipe.

**OB-63 — Lag.** Blocks the pipe is behind the independent chain-head reference. It MUST
be *absent* — not zero — whenever it cannot be computed: before the pipe has a position to
measure from, when no reference is available, and while no source is being read. Zero
means "level with the chain", which is a different claim from "unknown".

**OB-64 — Staleness.** The active source's current unproductive wait (DEF-69).

**OB-65 — Chain stall.** Set while the active source is stale and no eligible source is
ahead of the pipe — the state where switching cannot help.

**OB-66 — Switch log.** Every transition to `unhealthy` is logged once, at warning level,
with the source name and the bounded cause fields, through the pipe's own logger so that
a pipe configured silent stays silent. Credentials MUST be redacted from every field.

Every metric above carries the pipe identity, so several pipes sharing one metrics
surface remain individually observable.

## Related decisions

- [ADR-23 — detection senses, strategy decides](decisions/ADR-23-detection-and-strategy.md)
- [ADR-24 — a mixed source list reports itself hot](decisions/ADR-24-conservative-finality.md)
- [ADR-25 — staleness measures unproductive wait](decisions/ADR-25-staleness-is-unproductive-wait.md)
- [ADR-26 — reclaim is gated on why the source was left](decisions/ADR-26-reclaim-gating.md)
- [ADR-27 — a source in a list retries less than a lone source](decisions/ADR-27-source-retry-budget.md)
