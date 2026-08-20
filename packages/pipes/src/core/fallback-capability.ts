import { isForkException } from '~/portal-client/index.js'

import { safeReturn, withTimeout } from './fallback-async.js'
import { SourceErrorInfo, capabilityFailure, classifyError } from './fallback-diagnostics.js'
import { FallbackUnderlyingSource } from './fallback-source.js'
import { BlockCursor } from './types.js'

export interface CapabilityProbeOptions {
  /** Report not-capable if the probe slice stays outstanding longer than this. Default 30s. */
  timeoutMs?: number
}

/** A capability probe's verdict: `ok`, plus *why* it failed (for logs + metrics) when it didn't. */
export interface ProbeResult {
  ok: boolean
  cause?: SourceErrorInfo
}

const DEFAULT_TIMEOUT_MS = 30_000

/**
 * Build a generic `probeCapability` for any {@link FallbackUnderlyingSource}. It pulls a single
 * batch of *exactly the data the source is configured to serve* — the query (fields + request) is
 * baked into the source, so one `read(cursor)` batch re-exercises the whole pipeline (logs, traces,
 * state diffs) — starting just past the indexing frontier, and reports whether the source could
 * serve it.
 *
 * This catches the reachable-but-incapable failures liveness alone misses: an RPC node with the
 * trace/`debug_` API disabled or pruned state at that depth fails the slice, as does a Portal that
 * answers HTTP 400 to a query that passed type-level validation. The supervisor anchors `atCursor`
 * to the frontier, so during a backfill the probe verifies capability *at the depth the source is
 * about to read in bulk*.
 *
 * Capable iff the slice yields a batch or the stream ends without a non-fork error. A
 * `ForkException` counts as capable — the source served data and detected a reorg, a chain event
 * rather than an inability to serve. Any other error, or exceeding `timeoutMs`, reports not-capable,
 * with the cause (classified for logs + metrics) attached.
 *
 * (Unlike the Squid probe, which requests a one-block `{from, to}` slice, the Pipes `read` contract
 * is unbounded — so the probe takes only the first batch and closes the stream.)
 */
export function makeCapabilityProbe<T>(
  source: FallbackUnderlyingSource<T>,
  options: CapabilityProbeOptions = {},
): (atCursor?: BlockCursor) => Promise<ProbeResult> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS

  return async (atCursor?: BlockCursor): Promise<ProbeResult> => {
    const iterator = source.read(atCursor)[Symbol.asyncIterator]()
    try {
      // One batch (or a clean stream end) is enough: it proves the source served the slice. The
      // timeout rejects with a ready-made cause (a `SourceErrorInfo`, not a bare Error).
      await withTimeout(iterator.next(), timeoutMs, () =>
        capabilityFailure(`probe timed out after ${timeoutMs}ms`, 'timeout'),
      )

      return { ok: true }
    } catch (e) {
      if (isForkException(e)) return { ok: true }
      // The timeout rejects with a ready-made cause; a thrown error gets classified.
      const cause = isErrorInfo(e) ? e : classifyError('capability', e)
      return { ok: false, cause }
    } finally {
      safeReturn(iterator)
    }
  }
}

function isErrorInfo(e: unknown): e is SourceErrorInfo {
  return typeof e === 'object' && e != null && 'reason' in e && 'check' in e
}
