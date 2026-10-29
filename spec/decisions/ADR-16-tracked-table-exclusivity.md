# ADR-16 — Tracked-table exclusivity is declared, not inferred

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
The file binding contradicted itself. It documented one pipe per directory from the
start; keying cursors by pipe id (ADR-2) then namespaced its state file and documented a
shared directory as supported, but left data and temp files keyless. Its recovery
therefore deleted another pipe's files, and two pipes over one table collided on
filename.

The underlying obstacle: absent per-row key attribution, "are these rows mine?" is
unanswerable. Rows generally carry no pipe key. The same question blocks recovery from a
crash before the first cursor write: what that crash leaves may be removed only if the
sink can tell it from another pipe's data (for class A, ADR-15). The file binding could
not: it read a directory with data and no state as a cold start, so it neither removed
that crash's files nor refused data whose state was lost.

## Decision

Exclusivity is a **declaration**, surfaced per binding in IB-27, not something a sink
infers by probing. CN-44 applies only where the tracked tables are declared exclusive to
one cursor key; over shared tables the guard MUST NOT run, and the residual duplication
risk on a lost cursor is the operator's. A sink removes only data it can attribute to
its own cursor key:

- **Over shared tables, by the author.** A class-A sink removes nothing itself: its
  repair hook does (RP-42, ADR-15), and over a shared table the hook scopes the removal
  to the pipe's own rows (INV-35). The author knows how their rows are keyed; the
  schema-blind binding does not. ClickHouse tables stay shareable.
- **Over exclusive tables, everything they hold.** The declaration is the binding's, in
  IB-27, or operator configuration where the binding exposes it.

The file binding declares its output directory, and every table in it, exclusive to one
cursor key: one pipe per directory, as it documented first. Its recovery acts on every
file in the directory, so it enforces the declaration: before it touches any file,
startup refuses a directory that holds another key's state sidecar (IB-22, E2321). The
sidecar is the one mark of another pipe that a keyless directory carries. The check runs
at startup only, so two first runs started together on an empty directory both pass it,
and the next start of either refuses.

The file binding tells its own crash residue from orphaned data by a **start state**
(IB-22): before its first checkpoint publishes anything, a pipe with no state writes a
state that records no commit. Recovery from a start state deletes every published file
and starts cold. Published files with no state at all can then only have outlived their
state, so startup refuses them, with a coded error, before it touches any file (CN-44).

A directory that two pipes share today, each with its own tables, refuses at its next
start; moving one pipe's state file and table directories to a directory of its own
resolves it. A pipe whose key changed (a renamed source id, or a state from before pipe
ids) refuses the same way, and renaming its state file to the new key resumes it.

Alternatives considered. *The cursor key in every data and temp file name* — lets pipes
share a directory and even a table, each recovery touching only its key's files;
deferred, not rejected: it changes the published name format, needs the key encoded for
a file name and an upgrade path for keyless files, and nobody has asked to write several
pipes into one table of files. The declaration does not preclude it. *One owner per
table directory, recorded in a marker file* — keeps a directory shareable across
disjoint tables; deferred for the same reason, and it needs a race-safe marker. *Add a
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

CN-44 gains a precondition and stops contradicting INV-35. The file binding's recovery
stops acting on a co-resident pipe's files, since it refuses to run beside one; this
closes the shared-directory gap. Its exclusivity also makes CN-44 apply to it: the
refusal under another key's sidecar covers a wrong key, the refusal of data with no state
a lost state, and a crash in the first checkpoint recovers like any other. A directory
whose last run crashed in its first checkpoint under a release that wrote no start state
refuses once after the upgrade; deleting its table directories re-indexes it.

The class-A repair hook is called over the range start after a crash in the first batch
(ADR-15), since the removal it makes is the author's to scope. The binding's documented
removal bounds that call to the first batch's blocks; a hook that writes its own condition
removes every row from the range start on, over a shared table other pipes' rows as well, and
the documentation says so. The write-ahead
binding's guard must either become key-scoped or declare the exclusivity it already
assumes (GAP-20). Operators gain a knob they can set
wrongly where a binding exposes one — declaring exclusivity over a shared table is
undetectable, and the spec says so rather than pretending otherwise.
