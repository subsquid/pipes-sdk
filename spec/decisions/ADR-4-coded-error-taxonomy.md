# ADR-4 — Coded, banded error taxonomy as the stable error surface

Status: Accepted (historical)

## Context

Errors were heterogeneous bare exceptions; programs matched on message text, which
rewording broke. The docs site needed one authoritative error reference.

After the codes existed, some author-visible failures still escaped them: a raw parse
error for an unreadable stream line, a raw HTTP 409 for a fork signal with no canonical
chain, a validation error that named no block, a raw HTTP 404 for a date after the
dataset's head (matched on message text that never arrived), plain errors for unparseable
range bounds, a library call that exited the process, and a code (E0001) nothing raised.

## Decision

Every author-visible error carries a stable `Exxxx` code from banded ranges (E0xxx
configuration, E1xxx fork, E2yxx per sink binding) plus a documentation URL. Codes are
append-only; messages are free to change; matching is on code/type only.

Input the portal sends is covered too, and each diagnostic names the block: a fork signal
without a canonical chain halts with E1002, a line the client cannot read as a block with
E1007, a block that violates the schema of the selected fields with E1008. A body that
ends inside a line is a transport fault, requested again from that block; a second cut
at the same block in a row is E1007. A failure is told apart by its structure (HTTP
status, parse result), never by message text: a 404 for a date is E0002. Library code
never ends the process; it throws a coded error the author can handle.

## Consequences

Stable programmatic error handling across versions and languages (REQ-13, INV-31,
IB-50). Obligation: the emitted set must stay in sync with the registry — currently
unenforced by CI (GAP-13).
