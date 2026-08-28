# @subsquid/pipes — behavioral specification

`@subsquid/pipes` is a blockchain-data streaming pipeline: it pulls block batches from a
portal (data-lake gateway) over HTTP, decodes and transforms them through composable
network modules, and delivers them to sinks with resumable cursors, finality tracking,
and automatic fork rollback.

This spec is the **language-neutral contract** for the pipeline. The current TypeScript
implementation is the reference; future implementations (Rust, Go, Python) MUST satisfy
the same properties and the same persistent/wire formats so that implementations are
interchangeable mid-stream — an implementation may resume from state written by another.

**Tier: conformance** (full depth). **Shape: data pipeline / ETL.**

## Document map

| Doc | Contents | Normative? |
|---|---|---|
| [01-overview.md](01-overview.md) | context, actors, goals, non-goals, trust model | Yes |
| [02-requirements.md](02-requirements.md) | product requirements REQ-n | Yes |
| [03-data-model.md](03-data-model.md) | definitions DEF-n, state tuple, policies | Yes |
| [04-ingestion.md](04-ingestion.md) | ingestion loop, transitions, checkpointing WP-n | Yes |
| [05-sinks.md](05-sinks.md) | the sink contract RP-n | Yes |
| [06-consistency-durability.md](06-consistency-durability.md) | commit model, durability classes CN-n | Yes |
| [07-invariants.md](07-invariants.md) | safety catalog INV-n | Yes |
| [08-liveness.md](08-liveness.md) | liveness properties LIV-n | Yes |
| [09-failure-model.md](09-failure-model.md) | fault families and required responses FM-n | Yes |
| [10-replay.md](10-replay.md) | replay, purity, cache, coverage RS-n | Yes |
| [11-performance.md](11-performance.md) | SLIs/SLOs, workload model, hazards HZ-n | Yes |
| [12-observability.md](12-observability.md) | required signals OB-n | Yes |
| [13-conformance-tdd.md](13-conformance-tdd.md) | reference model, CT taxonomy, matrix, gap register | **Mutable** |
| [14-interface-binding.md](14-interface-binding.md) | wire protocol, state formats, HTTP surface IB-n | Yes |
| [15-parameters.md](15-parameters.md) | parameter registry P-* | **Mutable** |
| decisions/ | ADR log (append-only; index = folder listing) | Yes |

## Sink bindings

Where to read about each sink. Navigation only — enforcement (locks, legacy migration,
tracked-table exclusivity) is tabulated normatively in **IB-27**, and per-binding status
lives in [13-conformance-tdd.md](13-conformance-tdd.md).

| Binding | Durability class | State format | Fork mechanics | Sink-specific decisions |
|---|---|---|---|---|
| ClickHouse | [A — append-lagged](06-consistency-durability.md#durability-classes-adr-5) | [IB-20](14-interface-binding.md#persisted-state-formats) | [CN-33](06-consistency-durability.md#fork-mechanics-per-class) | [ADR-7 — sign-netting rollback](decisions/ADR-7-clickhouse-sign-netting-rollback.md) · [ADR-15 — class-A repair ownership](decisions/ADR-15-mandatory-repair-hook.md) |
| Postgres | [T — transactional](06-consistency-durability.md#durability-classes-adr-5) | [IB-21](14-interface-binding.md#persisted-state-formats) | [CN-30](06-consistency-durability.md#fork-mechanics-per-class) | — |
| Parquet | [K — checkpointed-immutable](06-consistency-durability.md#durability-classes-adr-5) | [IB-22](14-interface-binding.md#persisted-state-formats) | [CN-32](06-consistency-durability.md#fork-mechanics-per-class) | [ADR-6 — coverage-window naming](decisions/ADR-6-coverage-window-naming.md) · [ADR-18 — pluggable engine seam](decisions/ADR-18-parquet-pluggable-engine-seam.md) · [ADR-19 — duckdb engine (external)](decisions/ADR-19-duckdb-segment-writer-engine.md) · [ADR-22 — finalized-only source selection](decisions/ADR-22-finalized-only-source-selection.md) |
| BigQuery | [W — write-ahead](06-consistency-durability.md#durability-classes-adr-5) | [IB-23](14-interface-binding.md#persisted-state-formats) | [CN-31](06-consistency-durability.md#fork-mechanics-per-class) | — |
| Pub/Sub | [C — compensating append-only](06-consistency-durability.md#durability-classes-adr-5) | [IB-28](14-interface-binding.md#persisted-state-formats) | [CN-35](06-consistency-durability.md#fork-mechanics-per-class) | [ADR-21 — compensating append-only sinks](decisions/ADR-21-compensating-append-only-sinks.md) · [ADR-23 — producer control channel](decisions/ADR-23-producer-control-channel.md) |

Binding-independent: the contract every sink implements is
[05-sinks.md](05-sinks.md) (RP-n); the decisions binding all of them are
[ADR-2 — cursor keyed by pipe id](decisions/ADR-2-cursor-keyed-by-pipe-id.md) ·
[ADR-4 — coded error taxonomy](decisions/ADR-4-coded-error-taxonomy.md) ·
[ADR-5 — durability classes](decisions/ADR-5-durability-classes.md) ·
[ADR-16 — tracked-table exclusivity](decisions/ADR-16-tracked-table-exclusivity.md)
*(proposed)* ·
[ADR-22 — finalized-only source selection](decisions/ADR-22-finalized-only-source-selection.md).

## Conventions

- RFC 2119 keywords (MUST/SHOULD/MAY) carry their standard meaning.
- **IDs** are stable and banded per category: `REQ` requirements, `DEF` definitions,
  `WP` ingestion/write-path properties, `RP` sink-contract properties, `CN`
  consistency/durability, `INV` invariants, `LIV` liveness, `FM` failure model, `RS`
  replay, `PF`/`SLI`/`HZ` performance, `OB` observability, `IB` interface binding, `CT`
  test classes, `GAP` gap register, `ADR` decisions. Numbering is banded so additions
  never renumber; ADRs alone are sequential.
- **Parameters**: every constant appears in normative text only as a `P-NAME` symbol;
  concrete values live in [15-parameters.md](15-parameters.md). ⚠ marks proposed targets
  awaiting ratification by ADR.
- Math notation: ℕ naturals, ⊥ absent/undefined, ⟨…⟩ tuples, sequences are ordered.
- **Mutability rule**: only 13 and 15 record status, dates, or current values.
  `decisions/` is append-only — accepted ADRs are never edited, only superseded. All
  other docs change only when *intended behavior* changes.
- **Machine check**: `node spec/check-spec.mjs` validates the ID system (definitions,
  references, ranges) and every relative link (target file exists, anchor matches a
  heading); CI runs it on every spec change.

## How to use

Conformance order: build the harness (portal simulator + reference model + structural
validators, [13-conformance-tdd.md](13-conformance-tdd.md) §Harness) → close the P0/P1
entries of the gap register → walk the traceability matrix from U to C. A new-language
implementation starts from [14-interface-binding.md](14-interface-binding.md) (formats
it must read/write) and [07-invariants.md](07-invariants.md) (properties it must hold),
and is done when the CT suite passes against it unchanged.
