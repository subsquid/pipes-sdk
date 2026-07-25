# ADR-20 — The resume anchor binds only the range that continues from the cursor

Status: Accepted — settles anchor semantics on a resumed multi-range run

## Context

A pipe may configure several disjoint block ranges. On resume, WP-1 seeds the first
request from the recovered cursor and IB-3 carries that cursor's hash as the
parent-linkage anchor. IB-3 left one case open: which of several ranges the anchor
attaches to.

The reference binding sent the cursor's hash as the initial anchor for *every*
configured range. For the range that continues directly from the cursor that is correct
— the cursor's hash is the parent of its first block. For any later disjoint range it is
not: the cursor is block N, the range starts at some M ≫ N+1, and block N does not
precede block M. Under ADR-1 the portal validates linkage and answers a mismatch with a
409, so the resumed run took a spurious fork on the later range — a P1 correctness hole.

## Decision

The recovered cursor's hash anchors **only the range that continues directly from it**,
identified by `fromBlock = cursor.number + 1`. Every later range starts **unanchored**
(no `parentBlockHash`); IB-3 then re-establishes the anchor from that range's own first
block as it arrives. A gap between the cursor and a range's start means there is no known
parent to assert, so none is sent.

This is a scoping refinement of IB-3, not a change to the trust model: the portal still
owns linkage (ADR-1). An unanchored request asks the portal to begin at `fromBlock` and
resume per-block linkage from there, exactly as a fresh (non-resumed) range already does.

## Consequences

A resumed multi-range run no longer faults a 409 against a block the cursor never
precedes; the continuation range keeps its linkage check unchanged. Fork detection
(ADR-1, IB-4) is unaffected — it still runs within every range once that range's own
anchor is established. IB-3 states the scoping normatively. Shapes WP-1, IB-3, IB-4,
REQ-6.
