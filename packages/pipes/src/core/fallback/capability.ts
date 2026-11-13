import { BlockStreamClient, PortalBlockStreamOptions, PortalQuery, isForkException } from '~/portal-client/index.js'

import { BlockCursor } from '../types.js'
import { safeReturn, withTimeout } from './async.js'
import { SourceErrorInfo, classifyError } from './diagnostics.js'

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
 * A generic capability probe: it reads a one-block slice of the full query at the pipe's position.
 * This catches a source that is reachable but cannot serve the data, for example an RPC node without
 * the trace API or with pruned state. A fork counts as capable: the source served data.
 */
export function makeCapabilityProbe(
  client: BlockStreamClient,
  query: PortalQuery,
  options: CapabilityProbeOptions = {},
  /** The stream's own options, so a forced commitment is probed as the stream will read it. */
  streamOptions?: PortalBlockStreamOptions,
): (atCursor?: BlockCursor) => Promise<ProbeResult> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS

  return async (atCursor?: BlockCursor): Promise<ProbeResult> => {
    // At the last delivered block, never past it: `atCursor + 1` parks the slice at the tip, where
    // an RPC source re-fetches its head every ~100ms.
    const from = atCursor ? atCursor.number : query.fromBlock
    // No `parentBlockHash`: a probe that faults on a reorg would misreport it as incapability.
    const probeQuery = { ...query, fromBlock: from, toBlock: from, parentBlockHash: undefined }

    const iterator = client.getStream(probeQuery as never, streamOptions)[Symbol.asyncIterator]()
    try {
      await withTimeout(iterator.next(), timeoutMs, () => new Error(`probe timed out after ${timeoutMs}ms`))

      return { ok: true }
    } catch (e) {
      if (isForkException(e)) {
        return { ok: true }
      }

      return { ok: false, cause: classifyError('capability', e) }
    } finally {
      safeReturn(iterator)
    }
  }
}
