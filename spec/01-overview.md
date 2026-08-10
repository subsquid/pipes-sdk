# 01 — Overview

## What it is

A pipe is a long-running unidirectional dataflow: it requests block data from a
**portal** (an HTTP gateway over a blockchain data lake), receives ordered batches,
applies user-composed decoding/transformation, and delivers the result to a **sink**
that persists both the data and a resume cursor. The hot path is: portal stream →
batch assembly → decode/transform → sink commit → cursor advance — repeated from the
resume point to the head of the chain, then following the head in real time.

The system exists for one job: converting raw on-chain history into user-shaped
datasets **exactly once per block**, surviving process crashes, portal outages, and
chain reorganizations without human intervention.

## Actors

| Actor | Role | Direction |
|---|---|---|
| Portal | block-data producer; sole source of chain truth (ordering, linkage, finality, canonical chain on fork) | in |
| Pipeline author | declares queries, transforms, sink configuration; supplies `onData`-style callbacks | in (code) |
| Sink storage | the system the sink writes to (database, warehouse, files) | out |
| Dashboard / monitoring | consumes the observability HTTP surface and metrics | out |
| Operator | starts/stops processes, sets parameters, reads alarms | control |
| Downstream data consumer | reads sink output (tables, files) independently of the pipe | out |

## Design goals

- **G1 — Interchangeable implementations.** Any conforming implementation can resume
  from persistent state written by any other. → REQ-23, IB-20…IB-26, CN-45.
- **G2 — Effective exactly-once delivery.** After any crash/restart sequence, sink
  output contains each block's data exactly once. → REQ-3, REQ-6, CN-10…CN-24, INV-40…INV-44.
- **G3 — Automatic fork correctness.** On fork-capable streams, chain reorganizations
  are detected and rolled back to the canonical ancestor without operator action.
  → REQ-4, WP-40…WP-47, INV-13, INV-14.
- **G4 — Finality safety.** On datasets that define finality, immutable storage receives
  only the finalized stream; the finalized floor never regresses.
  → REQ-5, INV-2, INV-12, INV-25.
- **G5 — Unattended liveness.** Transient faults are retried with visible progress
  signals; only integrity violations halt the pipe. → REQ-22, LIV-1…LIV-8, FM-1.
- **G6 — Observable progress.** A dashboard can read progress, throughput, ETA, and
  per-stage timing from a standard HTTP surface. → REQ-10, OB-1…OB-32.

## Non-goals

- **NG1 — Chain-integrity verification.** The pipe does not independently verify
  parent-hash linkage or block authenticity; the portal is trusted for ordering,
  linkage, and canonical-chain selection (ADR-1).
- **NG2 — Multi-writer coordination.** Two concurrently running pipes with the same
  pipe id are not a supported configuration; behavior is defined only where a binding
  provides an explicit lock (see INV-15, IB-24).
- **NG3 — Cross-sink atomicity.** A pipe writes one sink; distributing one stream
  atomically across several sinks is out of scope.
- **NG4 — Query planning/optimization.** The pipe forwards the user's merged query to
  the portal verbatim; it does not rewrite filters for efficiency.
- **NG5 — Historical-data correction.** Data already finalized and committed is never
  revised, even if the portal later serves different bytes for the same block (ADR-1).
- **NG6 — Sink schema migration.** Evolving user table schemas between runs is the
  author's responsibility; the pipe only validates compatibility at startup.

## Trust model

| Party | Verified | Trusted | Must never be able to cause |
|---|---|---|---|
| Portal | response envelope shape (schema-validated per block); fork signal consistency at the level of CN/INV guards (e.g. INV-14's ancestor rule) | block ordering, parent linkage, finality assignment, canonical chain content | silent data corruption that violates a structural invariant — contract violations MUST halt with a coded error, not corrupt state (FM-11, FM-13…FM-16) |
| Pipeline author code | nothing (arbitrary code in-process) | purity where declared (RS-10), schema of produced rows (validated per binding) | corruption of *another* pipe's state; violation of cursor/data atomicity provided by the sink harness |
| Sink storage | startup compatibility checks (engine/schema preconditions) | transactional/append semantics it advertises | — (its failures map to FM-20…FM-27) |
| Dashboard | — | read-only consumer | any state change (the observability surface is read-only) |
| Operator | configuration validated at startup (coded errors) | parameter choices | undetected dual-instance corruption where a lock exists (INV-15) |

## Lifecycle at a glance

```
            ┌────────────┐   batches    ┌───────────────┐  rows   ┌────────────┐
 portal ───▶│ ingest +   │─────────────▶│ decode /      │────────▶│ sink       │──▶ storage
  (HTTP)    │ batch      │              │ transform     │         │ commit +   │
            │ assembly   │◀── backpressure (single-slot, pull) ───│ cursor     │
            └────────────┘              └───────────────┘         └────────────┘
                 │ fork signal (conflict)      ▲                        │
                 └─────────── rollback to canonical ancestor ◀──────────┘
```

Entity lifecycle (one pipe): `INIT/RESUME → {BATCH | CHECKPOINT}* → (FORK → BATCH…)* → STOP`,
with `CRASH → RESUME` possible at any point. `FORK` occurs only on the full stream;
finalized-only delivery applies the finality gate upstream (T-RELEASE).

Process lifecycle: start → recover committed state → repair partial writes → stream → clean stop (hooks exactly once) or crash (recovery restores a committed state).
