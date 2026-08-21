# ADR-25 — Staleness measures unproductive wait

Status: Accepted

## Context

"The active source has stopped making progress" needs a definition that survives contact
with real sources, and two obvious ones do not.

Timing a single outstanding request catches a source that hangs, but not one that keeps
answering without progressing. A finalized-only source parked at its finality frontier is
exactly that: asked for the next block it replies "nothing yet", promptly, forever. Each
answer restarts a per-request clock, so the pipe never concludes anything is wrong and
waits indefinitely while another source is ready to take over.

Timing from the last delivered block catches that case, but counts time the *consumer*
spends between batches. A sink that takes longer than the window to commit a batch would
make a perfectly healthy source look stalled and be failed over — the pipe would blame
the source for the sink's latency.

## Decision

Staleness is the time the active source has spent answering without delivering a block:
accumulated across consecutive empty answers, reset by the first answer carrying a block,
and measured only while a request is outstanding, so consumer time is excluded.

The verdict is evaluated both while a request is outstanding and at batch boundaries. The
second is necessary rather than redundant: a source answering promptly with nothing never
keeps a request outstanding long enough to reach the first path.

## Consequences

The stall window now bounds "no progress" rather than "no response", so a slow-but-
progressing source is never failed over for being slow, and a fast-but-empty source is no
longer invisible.

A source alternating one block with long empty stretches resets the accumulator on each
block and may never trip the window. This is intended: it is making progress.
