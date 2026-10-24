# ADR-16 — Tracked data may be shared; a sink acts only on what it can attribute

Status: Accepted — settles whether co-resident pipes may share tracked tables

## Context

CN-44's orphan-data guard refuses to start when cursor state is absent but tracked
tables hold data. Written table-wide, it assumes each table belongs to one pipe. The
spec assumes the opposite elsewhere: INV-35 scopes every read, write and deletion to the
bound cursor key, RP-41 leaves co-resident pipes untouched, and NG2 excludes only two
pipes sharing an *id*. Several pipes writing one table is a supported configuration,
and on ClickHouse a common one.

The write-ahead binding reads its sync row by key but probes the tracked table without
one, so a second pipe's first run into a populated shared table is refused (GAP-20).
The file binding contradicts itself. It documented one pipe per directory from the
start; keying cursors by pipe id (ADR-2) then namespaced its state file and documented a
shared directory as supported, but left data and temp files keyless. Its recovery
therefore deletes another pipe's files, and two pipes collide on filename (GAP-35).

The underlying obstacle: absent per-row key attribution, "are these rows mine?" is
unanswerable. Rows generally carry no pipe key. The same question blocks recovery from a
crash before the first cursor write: what that crash leaves may be removed only if the
sink can tell it from another pipe's data (GAP-41, GAP-44).

## Decision

Co-resident pipes may share tracked data unless the binding declares its tracked tables
exclusive to one cursor key (IB-27). A sink deletes, refuses on, or guards only data it
can attribute to its own cursor key; it never infers ownership from what a table or
directory happens to hold. Attribution takes one of three forms:

- **By name, where the binding names the data.** A file sink puts its cursor key in the
  name of every file it writes, temp files included (IB-22). Recovery, the over-cursor
  sweep, the straddle check and publish consider only files that carry the key, so two
  pipes share a directory without touching each other's files. Coverage tiling (RP-21)
  holds per table and cursor key; how a reader combines several keys' files is the
  reader's business, not the sink's.
- **By the author, where rows carry no key.** A class-A sink removes nothing itself: its
  repair hook does (RP-42, ADR-15), and over a shared table the hook scopes the removal
  to the pipe's own rows (INV-35). The author knows how their rows are keyed; the
  schema-blind binding does not.
- **By declaration.** Over tables declared exclusive, everything they hold is the key's.
  The declaration is the binding's, in IB-27, or operator configuration where the binding
  exposes it.

CN-44 runs where the data is attributable by name or by declaration. Elsewhere the guard
MUST NOT run, and the residual duplication risk on a lost cursor is the operator's.

Upgrading a file directory. Files written before the key was in the name are not
renamed: readers may hold lists of them. A pipe whose own state predates keyed names
runs its first recovery over the keyless files as the binding did under one pipe per
directory, then records keyed naming in its state; from then on it touches only files
that carry its key. A pipe that starts without state leaves keyless files alone: they are
unattributable, so the guard does not cover them. Ownership of keyless files is read from
the pipe's own state only, never from another key's.

Alternatives considered. *One pipe per directory, enforced by refusing a directory that
holds another key's state file* — cheaper, with no format change and no upgrade path;
rejected because it withdraws, for files alone, a configuration the spec supports
elsewhere, and the file binding is the one where attribution costs nothing. *Add a
pipe-key column to every tracked table* — makes the question decidable for rows,
rejected for now: it changes user-owned schemas, breaks existing deployments, and taxes
every write for a guard that fires on operator error. *Drop the guard to a warning* —
rejected: on exclusive tables it prevents silent duplication of whole history, which is
REQ-3's business. *Infer exclusivity from a table having only one writer observed so
far* — rejected as unsound, absence of evidence.

This follows INV-15's existing pattern: the spec requires that a binding's declared
enforcement, or its declared absence, match its IB entry — not that every binding
enforce the same thing.

## Consequences

CN-44 gains a precondition and stops contradicting INV-35. The file binding needs no
exclusivity: with its key in every name, its recovery and publish stop touching
co-resident pipes' files, and its orphan guard runs over its own (GAP-35). The pipe id
becomes part of a file name, so the binding encodes it (IB-22). A crash before the first
state write becomes attributable (GAP-41); to repair it rather than refuse it as orphan
data, the binding must tell it from a lost state, which takes a state write before the
first publish.

The class-A repair hook may be called over the range start (GAP-44), since the removal it
makes is the author's to scope. The binding's documented hook removes every row above
the cursor; over a shared table that removes other pipes' rows as well, and the
documentation must say so. The write-ahead binding's guard must either become key-scoped
or declare the exclusivity it already assumes (GAP-20). Operators gain a knob they can
set wrongly where a binding exposes one — declaring exclusivity over a shared table is
undetectable, and the spec says so rather than pretending otherwise.
