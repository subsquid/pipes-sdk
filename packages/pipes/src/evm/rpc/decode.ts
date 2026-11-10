import type { Block as NormalizedBlock } from '@subsquid/evm-normalization'
import { toJSON } from '@subsquid/util-internal-json'
import { cast } from '@subsquid/util-internal-validation'

import { Block, FieldSelection, getBlockSchema } from '~/portal-client/query/evm.js'

import { shimWireBlock } from './shim.js'

/**
 * Force the structurally-required fields the cursor + filter engine always need (and which the
 * Portal query always includes), so they survive `project` regardless of the user's selection.
 */
export function withRequiredFields(fields: FieldSelection): FieldSelection {
  return {
    block: { ...fields.block, number: true, hash: true, parentHash: true },
    transaction: { ...fields.transaction, transactionIndex: true },
    log: { ...fields.log, logIndex: true, transactionIndex: true },
    trace: { ...fields.trace, transactionIndex: true, traceAddress: true },
    // `kind` is the stateDiff tagged-union discriminator and an always-present required field, so it
    // must survive projection — otherwise a `stateDiffs: [{ kind: [...] }]` where-clause (which reads
    // `kind`) never matches when the user didn't also select it for output.
    stateDiff: { ...fields.stateDiff, transactionIndex: true, address: true, key: true, kind: true },
  }
}

/**
 * Decode an already-serialized wire block (the `toJSON` of a normalized block) into Pipes' typed
 * `Block<F>` by reusing the exact Portal decoder — `cast(getBlockSchema(fields), …)` — preceded by
 * the small pre-cast shim. Reusing the Portal decoder is what makes the RPC source's output match
 * the Portal source's.
 */
export function decodeWireBlock<F extends FieldSelection>(wire: unknown, fields: F): Block<F> {
  const shaped = shimWireBlock(wire)

  return cast(getBlockSchema(fields), shaped)
}

/** Decode a normalized RPC block (`mapRpcBlock` output) into Pipes' `Block<F>`. */
export function decodeBlock<F extends FieldSelection>(block: NormalizedBlock, fields: F): Block<F> {
  return decodeWireBlock(toJSON(block), fields)
}
