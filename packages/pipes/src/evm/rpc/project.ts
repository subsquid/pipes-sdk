import { DataRequest, FieldSelection } from '~/portal-client/query/evm.js'

/**
 * Augment a field selection with the fields a request's where-clauses need to *evaluate* — even
 * when not selected for output — so the filter engine can match on them (otherwise it sees
 * `undefined` and throws). Trace where-keys map 1:1 to trace field-selection keys; stateDiff
 * address/key/kind are always-present required fields. Mirrors the Squid evm-rpc-stream augment.
 */
export function augmentFields(fields: FieldSelection, req: DataRequest): FieldSelection {
  const log: any = { ...fields.log }
  const transaction: any = { ...fields.transaction }
  const trace: any = { ...fields.trace }

  for (const it of req.logs ?? []) {
    if (it.address) log.address = true
    if (it.topic0 || it.topic1 || it.topic2 || it.topic3) log.topics = true
  }
  for (const it of req.transactions ?? []) {
    if (it.to) transaction.to = true
    if (it.from) transaction.from = true
    if (it.sighash) transaction.sighash = true
    if (it.type) transaction.type = true
  }
  for (const it of req.traces ?? []) {
    if (it.createFrom) trace.createFrom = true
    if (it.callTo) trace.callTo = true
    if (it.callFrom) trace.callFrom = true
    if (it.callSighash) trace.callSighash = true
    if (it.suicideRefundAddress) trace.suicideRefundAddress = true
    if (it.rewardAuthor) trace.rewardAuthor = true
  }

  return { ...fields, log, transaction, trace }
}

const SELECTION_TYPES = ['block', 'transaction', 'log', 'trace', 'stateDiff'] as const

/** True if `augmented` selects any field that `original` does not (augmentation only adds). */
export function selectionGrew(augmented: FieldSelection, original: FieldSelection): boolean {
  for (const t of SELECTION_TYPES) {
    const aug = (augmented as any)[t] ?? {}
    const orig = (original as any)[t] ?? {}
    for (const k in aug) {
      if (aug[k] && !orig[k]) return true
    }
  }

  return false
}

/**
 * Keep the items of `projected` whose positionally-aligned item in `pre` (the pre-filter decode of
 * the same block) survived filtering into `kept`. `projected[i]` and `pre[i]` are the same on-chain
 * item decoded at two field selections, so a surviving `pre[i]` means keep `projected[i]`. Alignment
 * is by position + object identity — never a synthesized structural key — so structurally identical
 * items can't be confused by a shared/ambiguous key. Mirrors the Squid evm-rpc-stream projection.
 */
export function keptByPosition<P, Q>(projected: P[], pre: Q[], kept: Q[]): P[] {
  const survived = new Set(kept)

  return projected.filter((_, i) => survived.has(pre[i]))
}

interface FilterableBlock {
  header: { number: number }
  logs: unknown[]
  transactions: unknown[]
  traces: unknown[]
  stateDiffs: unknown[]
}

/**
 * Match the Portal's empty-block handling for the RPC source (which fetches full blocks, so every
 * block in a range arrives even when filtering leaves it empty): drop a block left empty by
 * filtering, EXCEPT the batch's boundary blocks (first + last) and — when `includeAllBlocks` is set
 * — every block. Keeping the last block matters beyond parity: it is the batch's progress cursor, so
 * dropping it would stall resume. Mirrors the Squid evm-rpc-stream `dropEmptyBlocks`.
 */
export function dropEmptyBlocks<B extends FilterableBlock>(blocks: B[], includeAllBlocks = false): B[] {
  if (includeAllBlocks) return blocks

  return blocks.filter((b, i) => {
    if (i === 0 || i === blocks.length - 1) return true // boundary blocks: always present
    return b.logs.length > 0 || b.transactions.length > 0 || b.traces.length > 0 || b.stateDiffs.length > 0
  })
}
