import { assertNotNull, groupBy } from '@subsquid/util-internal'
import { EntityFilter, FilterBuilder } from '@subsquid/util-internal-processor-tools'

import {
  Block,
  DataRequest,
  Log,
  LogRequest,
  StateDiff,
  StateDiffRequest,
  Trace,
  TraceRequest,
  Transaction,
  TransactionRequest,
} from '~/portal-client/query/evm.js'

/**
 * Client-side equivalent of the Portal server's filtering, for the RPC source: the Portal filters
 * server-side, but RPC fetches full blocks, so we match items + expand relations here. Ported from
 * the legacy `@subsquid/evm-processor` `ds-rpc/filter.ts` (and mirrors the Squid evm-rpc-stream
 * port) onto Pipes' flat `Block`/`DataRequest` model. Relations are kept in side-maps so the
 * yielded blocks stay plain projections.
 */

type AnyBlock = Block<any>
type AnyLog = Log<any>
type AnyTransaction = Transaction<any>
type AnyTrace = Trace<any>
type AnyStateDiff = StateDiff<any>

export interface Relations {
  txByIndex: Map<number, AnyTransaction>
  logsByTx: Map<number, AnyLog[]>
  tracesByTx: Map<number, AnyTrace[]>
  stateDiffsByTx: Map<number, AnyStateDiff[]>
  traceParent: Map<AnyTrace, AnyTrace>
  traceChildren: Map<AnyTrace, AnyTrace[]>
}

function traceAddressOf(trace: AnyTrace): number[] {
  return (trace as any).traceAddress ?? []
}

function traceCompare(a: AnyTrace, b: AnyTrace): number {
  return a.transactionIndex - b.transactionIndex || addressCompare(traceAddressOf(a), traceAddressOf(b))
}

function addressCompare(a: number[], b: number[]): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const order = a[i] - b[i]
    if (order) return order
  }

  return a.length - b.length
}

function isDescendant(parent: AnyTrace, child: AnyTrace): boolean {
  const pa = traceAddressOf(parent)
  const ca = traceAddressOf(child)
  if (parent.transactionIndex !== child.transactionIndex) return false
  if (pa.length >= ca.length) return false
  for (let i = 0; i < pa.length; i++) {
    if (pa[i] !== ca[i]) return false
  }

  return true
}

export function setUpRelations(block: AnyBlock): Relations {
  const traces = [...block.traces].sort(traceCompare)

  const relations: Relations = {
    txByIndex: new Map(block.transactions.map((tx) => [tx.transactionIndex, tx])),
    logsByTx: groupBy(block.logs, (log) => log.transactionIndex),
    tracesByTx: groupBy(traces, (trace) => trace.transactionIndex),
    stateDiffsByTx: groupBy(block.stateDiffs, (diff) => diff.transactionIndex),
    traceParent: new Map(),
    traceChildren: new Map(),
  }

  for (let i = 0; i < traces.length; i++) {
    const rec = traces[i]
    const children: AnyTrace[] = []
    for (let j = i + 1; j < traces.length; j++) {
      const next = traces[j]
      if (isDescendant(rec, next)) {
        children.push(next)
        if (traceAddressOf(next).length === traceAddressOf(rec).length + 1) {
          relations.traceParent.set(next, rec)
        }
      } else {
        break
      }
    }
    relations.traceChildren.set(rec, children)
  }

  return relations
}

function buildLogFilter(req: LogRequest[] = []) {
  const items = new EntityFilter<
    AnyLog,
    { transaction?: boolean; transactionLogs?: boolean; transactionStateDiffs?: boolean; transactionTraces?: boolean }
  >()
  for (const { address, topic0, topic1, topic2, topic3, ...relations } of req) {
    const filter = new FilterBuilder<AnyLog>()
    filter.propIn('address' as any, address)
    filter.getIn((log) => assertNotNull((log as any).topics)[0], topic0)
    filter.getIn((log) => assertNotNull((log as any).topics)[1], topic1)
    filter.getIn((log) => assertNotNull((log as any).topics)[2], topic2)
    filter.getIn((log) => assertNotNull((log as any).topics)[3], topic3)
    items.add(filter, relations)
  }

  return items
}

function buildTransactionFilter(req: TransactionRequest[] = []) {
  const items = new EntityFilter<AnyTransaction, { logs?: boolean; traces?: boolean; stateDiffs?: boolean }>()
  for (const { to, from, sighash, type, ...relations } of req) {
    const filter = new FilterBuilder<AnyTransaction>()
    filter.propIn('to' as any, to)
    filter.propIn('from' as any, from)
    filter.propIn('sighash' as any, sighash)
    filter.propIn('type' as any, type)
    items.add(filter, relations)
  }

  return items
}

function buildTraceFilter(req: TraceRequest[] = []) {
  const items = new EntityFilter<
    AnyTrace,
    { transaction?: boolean; transactionLogs?: boolean; subtraces?: boolean; parents?: boolean }
  >()
  for (const {
    type,
    createFrom,
    callTo,
    callFrom,
    callSighash,
    suicideRefundAddress,
    rewardAuthor,
    ...relations
  } of req) {
    const filter = new FilterBuilder<AnyTrace>()
    filter.propIn('type' as any, type)
    // Read action fields nullable-safely: `getIn` treats `undefined` as a non-match, and several of
    // these are legitimately absent on valid traces (a `call` with empty input has no `sighash`; a
    // `suicide`'s `refundAddress` is nullable) — asserting non-null would crash the whole stream.
    filter.getIn((t) => (t.type === 'create' ? (t as any).action?.from : undefined), createFrom)
    filter.getIn((t) => (t.type === 'call' ? (t as any).action?.to : undefined), callTo)
    filter.getIn((t) => (t.type === 'call' ? (t as any).action?.from : undefined), callFrom)
    filter.getIn((t) => (t.type === 'call' ? (t as any).action?.sighash : undefined), callSighash)
    filter.getIn(
      (t) => (t.type === 'suicide' ? ((t as any).action?.refundAddress ?? undefined) : undefined),
      suicideRefundAddress,
    )
    filter.getIn((t) => (t.type === 'reward' ? (t as any).action?.author : undefined), rewardAuthor)
    items.add(filter, relations)
  }

  return items
}

function buildStateDiffFilter(req: StateDiffRequest[] = []) {
  const items = new EntityFilter<AnyStateDiff, { transaction?: boolean }>()
  for (const { address, key, kind, ...relations } of req) {
    const filter = new FilterBuilder<AnyStateDiff>()
    filter.propIn('address' as any, address)
    filter.propIn('key' as any, key)
    filter.propIn('kind' as any, kind)
    items.add(filter, relations)
  }

  return items
}

class IncludeSet {
  logs = new Set<AnyLog>()
  transactions = new Set<AnyTransaction>()
  traces = new Set<AnyTrace>()
  stateDiffs = new Set<AnyStateDiff>()

  addLog(log?: AnyLog) {
    if (log) this.logs.add(log)
  }
  addTransaction(tx?: AnyTransaction) {
    if (tx) this.transactions.add(tx)
  }
  addTrace(trace?: AnyTrace) {
    if (trace) this.traces.add(trace)
  }
  addTraceStack(relations: Relations, trace?: AnyTrace) {
    while (trace) {
      this.traces.add(trace)
      trace = relations.traceParent.get(trace)
    }
  }
  addStateDiff(diff?: AnyStateDiff) {
    if (diff) this.stateDiffs.add(diff)
  }
}

export function filterBlock(block: AnyBlock, req: DataRequest, relations: Relations): void {
  const logFilter = buildLogFilter(req.logs)
  const transactionFilter = buildTransactionFilter(req.transactions)
  const traceFilter = buildTraceFilter(req.traces)
  const stateDiffFilter = buildStateDiffFilter(req.stateDiffs)

  const include = new IncludeSet()

  if (logFilter.present()) {
    for (const log of block.logs) {
      const rel = logFilter.match(log)
      if (rel == null) continue
      include.addLog(log)
      if (rel.transaction) include.addTransaction(relations.txByIndex.get(log.transactionIndex))
      if (rel.transactionLogs) {
        for (const sibling of relations.logsByTx.get(log.transactionIndex) ?? []) include.addLog(sibling)
      }
      if (rel.transactionTraces) {
        for (const trace of relations.tracesByTx.get(log.transactionIndex) ?? []) include.addTrace(trace)
      }
      if (rel.transactionStateDiffs) {
        for (const diff of relations.stateDiffsByTx.get(log.transactionIndex) ?? []) include.addStateDiff(diff)
      }
    }
  }

  if (transactionFilter.present()) {
    for (const tx of block.transactions) {
      const rel = transactionFilter.match(tx)
      if (rel == null) continue
      include.addTransaction(tx)
      if (rel.logs) {
        for (const log of relations.logsByTx.get(tx.transactionIndex) ?? []) include.addLog(log)
      }
      if (rel.traces) {
        for (const trace of relations.tracesByTx.get(tx.transactionIndex) ?? []) include.addTrace(trace)
      }
      if (rel.stateDiffs) {
        for (const diff of relations.stateDiffsByTx.get(tx.transactionIndex) ?? []) include.addStateDiff(diff)
      }
    }
  }

  if (traceFilter.present()) {
    for (const trace of block.traces) {
      const rel = traceFilter.match(trace)
      if (rel == null) continue
      include.addTrace(trace)
      if (rel.parents) include.addTraceStack(relations, relations.traceParent.get(trace))
      if (rel.subtraces) {
        for (const sub of relations.traceChildren.get(trace) ?? []) include.addTrace(sub)
      }
      if (rel.transaction) include.addTransaction(relations.txByIndex.get(trace.transactionIndex))
      if (rel.transactionLogs) {
        for (const log of relations.logsByTx.get(trace.transactionIndex) ?? []) include.addLog(log)
      }
    }
  }

  if (stateDiffFilter.present()) {
    for (const diff of block.stateDiffs) {
      const rel = stateDiffFilter.match(diff)
      if (rel == null) continue
      include.addStateDiff(diff)
      if (rel.transaction) include.addTransaction(relations.txByIndex.get(diff.transactionIndex))
    }
  }

  block.logs = block.logs.filter((log) => include.logs.has(log))
  block.transactions = block.transactions.filter((tx) => include.transactions.has(tx))
  block.traces = block.traces.filter((trace) => include.traces.has(trace))
  block.stateDiffs = block.stateDiffs.filter((diff) => include.stateDiffs.has(diff))
}
