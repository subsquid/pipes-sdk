# ADR-24 — A run stops itself on a signal, and never ends the process

Status: Accepted — settles the exit policy and signal coexistence of graceful shutdown

## Context

A pipe is usually a long-lived process that an orchestrator stops with SIGTERM, then kills
with SIGKILL after a deadline. Without a handler, SIGTERM ends the process at once: nothing
is lost (the commit protocols are crash-safe), but a batch in flight is thrown away, class C
leaves an outbox to republish, and every author has to write the same wrapper to do better.

Handling the signal in the library raises questions a wrapper answers implicitly. A listener
replaces the default action of the signal, so something must end the process afterwards. An
interrupted bounded run must not look finished. A library may be embedded in a larger process
that has its own plans for the signal.

## Decision

`pipeTo` manages SIGTERM and SIGINT by default (WP-33). `shutdown: false` opts a run out, and a
function or options object sets the callback and the budget.

**The library never ends the process.** ADR-4 already says so, and a deadline does not change
it: an embedded application may have other work to finish, and only it knows. When the budget
expires, `pipeTo` rejects with E3002. In a standalone script an unhandled rejection then ends
the process with code 1, which is the force-exit that case needs, with no option to configure.
An application that catches the error decides for itself.

**The callback follows the drain.** `onShutdown` runs once after draining and resource cleanup
settle, also when they fail. A deadline limits the caller's wait; it never starts the callback
while draining is still in progress. If the drain settles later and the process is still alive,
the callback then runs. If the drain never settles, the callback does not run.

**Listeners exist only while a managed run does.** One shared pair is installed when the first
managed run starts, never on import, and removed when the last one settles. So after a deadline
the default action of the signal is back: a process that keeps running dies on the next signal.
With no process-wide exit policy, runs with different budgets do not interact: each has its own
timer, from the first signal.

**A stop rejects.** `pipeTo` resolves only when the range completed. A successful stop rejects
with E3001, so code after `await pipeTo(...)` cannot run by accident. A signal that arrives
while the sink finishes a complete range still counts as a stop: the process was told to end,
so completion-only work should not start. Rejecting costs a plain script exit code 1 and a
stack trace on a clean stop, and catching E3001 is how to get code 0. The band E3xxx joins the
registry for these two codes, since neither is a configuration, fork or sink error.

**A sink sees the end of a stream.** The source stops reading and the sink's loop ends as it
does at a range end, so each sink keeps one flush and close path, and a custom sink needs no
change. The cost is that a sink cannot tell a stop from a completion, which it has no need to.

**Application listeners coexist and are not coordinated.** All listeners run on a signal. An
application handler that calls `process.exit()` ends the drain. An application that wants to
own shutdown opts out.

## Consequences

SIGTERM and SIGINT no longer end a pipe process at once: this is a default behavior change.
A pipe with `shutdown: false` beside a managed one still loses the signal's default action
while the managed one runs, and must stop itself. Sinks may no longer close a client the author
passed in (RP-8), because one stopping pipe would break the others sharing it; an author who
relied on that closes the client after `pipeTo`. Work that cannot be cancelled may outlive the
rejection of E3002, so the cursor guarantee rests on the commit protocols (CN-40), not on the
deadline, and the budget must be set below the sender's SIGKILL deadline to be useful.
