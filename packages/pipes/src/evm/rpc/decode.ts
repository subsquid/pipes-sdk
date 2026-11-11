import { FieldSelection } from '~/portal-client/query/evm.js'

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
