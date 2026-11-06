# Graceful shutdown

`pipeTo` handles SIGTERM and SIGINT by default, for every target. On a signal, the pipe finishes the
batch it is writing, saves its cursor, closes its resources and stops.

```ts
await stream.pipeTo(target)
```

## Options

```ts
// A cleanup callback, with the default timeout.
await stream.pipeTo(target, {
  shutdown: async () => {
    await server.close()
  },
})

// A timeout, and an optional callback.
await stream.pipeTo(target, {
  shutdown: {
    timeout: '60s',
    onShutdown: async () => {
      await server.close()
    },
  },
})

// The application handles signals itself.
await stream.pipeTo(target, { shutdown: false })
```

| Option | Default | Meaning |
| ------------ | ------- | ------------------------------------------------------------------------------- |
| `timeout` | `'30s'` | The total time from the first signal, for draining, cleanup and `onShutdown`. |
| `onShutdown` | none | Runs once after the drain and cleanup, sync or async. It runs on a signal only. |

A `timeout` is a duration such as `'500ms'`, `'30s'`, `'1m'`, `'2m 30s'` or `'1h'`. An invalid, zero or
negative duration, or a number, rejects with [E0005](./errors.md#e0005--invalid-shutdown-option) before
the pipe starts.

Set `timeout` below the external termination deadline so the pipe has time to drain before SIGKILL.

## What happens on a signal

1. The pipe stops reading. A read that waits for the next block, a retry and any batch read ahead are
   cancelled, including during startup.
2. The batch the target is writing is finished, and its cursor is saved. A commit is never cancelled,
   and neither is a fork rollback in progress. Retries of a commit go on.
3. Transformers stop, and the target flushes and closes what it opened. Parquet closes its open files
   at the last written batch.
4. `onShutdown` runs after draining and cleanup settle, also if they failed.
5. `pipeTo` rejects. See below.

The timeout limits how long `pipeTo` waits. If it expires during draining, `pipeTo` rejects immediately;
the callback still waits for draining and cleanup to settle. If the drain never settles, the callback
never runs. Further signals do not restart the timeout.

### Portal reads in hooks and caches

Start hooks receive the original portal client, the run's effective `finalized` mode and
an optional `signal`. Pass the signal as `abort` to cancel additional portal requests and retries:

```ts
start: async ({ portal, finalized, signal }) => {
  const head = await portal.getHead({ finalized, abort: signal })
}
```

Custom caches receive the same settings. On a cache miss, forward them with
`portal.getStream(query, { finalized, request: { abort: signal } })`.
The built-in cache does this automatically. Client defaults stay unchanged.
Custom caches must stop iteration on the signal. Work that ignores it can outlive the shutdown timeout.

## How `pipeTo` settles

`pipeTo` resolves only when the range is complete, so code after `await pipeTo(...)` can trust that.

| What happened | `pipeTo` |
| ------------------------------------- | --------------------------------------------------------------------------- |
| The range completed | resolves, and `onShutdown` does not run |
| A failure with no signal | rejects with the failure, and `onShutdown` does not run |
| A signal, shutdown succeeded | rejects with `PipelineStoppedError` ([E3001](./errors.md#e3001--pipe-stopped-by-a-signal)) |
| A signal, the drain failed | rejects with the failure |
| A signal, `onShutdown` failed | rejects with the failure of the callback |
| A signal, both failed | rejects with an `AggregateError` that holds both |
| A signal, the `timeout` expired | rejects with `PipelineShutdownTimeoutError` ([E3002](./errors.md#e3002--shutdown-timed-out)) |

A signal that arrives while a complete range is being closed also counts as a stop.

## Exit codes

The SDK never ends the process. What the process does next depends on your code.

An unhandled `await stream.pipeTo(target)` exits with code 1 and prints the error on every stop. To exit
with code 0 on a clean stop, catch `PipelineStoppedError`. The process then ends when nothing keeps it
alive:

```ts
import { PipelineStoppedError } from '@subsquid/pipes'

try {
  await stream.pipeTo(target)
} catch (error) {
  if (!(error instanceof PipelineStoppedError)) throw error
}
```

After a timeout, work that cannot be cancelled may still be running and keep the process alive.
Catching `PipelineShutdownTimeoutError` does not stop that work; the application decides when to exit.
The SDK removes its signal listeners once no managed runs remain. Other application signal handlers
continue to apply.

## Resources

The SDK closes what it created. Clients you pass in stay open, because other pipes may share them:

| Target | Closed by the SDK | Stays open for you to close |
| ---------- | ------------------------------------------------------ | ------------------------------------------ |
| BigQuery | write streams, and a `WriterClient` the target created | `client.bigquery`, a `client.writer` you passed |
| Pub/Sub | the state file, a client built from a config | a `PubSub` instance you passed |
| ClickHouse | nothing | the client |
| Postgres | nothing | `db` |
| Parquet | open files | |

This holds whatever ended the run, including a range that completed.

Close shared resources in one place. With several pipes on one client, use the callback of one pipe or
close it after all the pipes have settled:

```ts
const results = await Promise.allSettled([a.pipeTo(targetA), b.pipeTo(targetB)])

await client.close()
```

## Several pipes and your own signal handlers

One signal stops all the pipes in the process, and each runs its own callback once with its own
timeout. A pipe with `shutdown: false` does not stop, so your code must stop it. While another pipe is
managed, a signal does not end the process either.

Your own `process.on('SIGTERM', ...)` handlers still run, next to the SDK's. A handler that calls
`process.exit()` ends the process before the drain is done: move that cleanup to `onShutdown`, or use
`shutdown: false` and handle the signal yourself.

The SDK listens only while a pipe with shutdown enabled is running.

## SIGKILL and crashes

SIGKILL cannot be caught, so there is no drain, and the same goes for a crash or a lost machine. Nothing
here promises exactly-once delivery to an arbitrary sink. On restart, each target recovers to its saved
cursor as follows, and the pipe resumes after it:

| Target | After a SIGKILL |
| ---------- | ---------------------------------------------------------------------------------------------- |
| Postgres | The batch and the cursor commit in one transaction, so a batch is there entirely or not at all. |
| BigQuery | Recovery deletes the rows of an unfinished batch, then the batch is read again. |
| ClickHouse | `onRollback` with `reason: 'recovery'` removes rows above the cursor. Without it, those rows are duplicated. |
| Parquet | Unpublished temporary files are discarded, and blocks since the last checkpoint are read again. |
| Pub/Sub | Unpublished messages go out first, and a message may repeat. Consumers apply messages by version. |
| Custom | What the target does. Save the cursor only after the data is confirmed. |

The same holds for a stop that outlasts its `timeout` and is then killed.
