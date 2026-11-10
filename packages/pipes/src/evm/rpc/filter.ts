import { groupBy } from '@subsquid/util-internal'

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
 * Client-side equivalent of the Portal server's filtering, for the RPC source, which fetches full
 * blocks. Ported from `@subsquid/evm-processor` `ds-rpc/filter.ts` onto Pipes' flat
 * `Block`/`DataRequest` model. It runs on the wire block, so hex values are compared in lowercase:
 * the wire keeps the case the node sent.
 */

type AnyBlock = Pick<Block<any>, 'logs' | 'transactions' | 'traces' | 'stateDiffs'>
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

type Predicate<T> = (item: T) => boolean

function lower(value: unknown): unknown {
  return typeof value === 'string' ? value.toLowerCase() : value
}

/** One where-clause: every field it lists must match one of the listed values. */
class FilterBuilder<T> {
  readonly #predicates: Predicate<T>[] = []
  #never = false

  getIn(get: (item: T) => unknown, values: readonly unknown[] | undefined): this {
    if (values == null) {
      return this
    }
    if (values.length === 0) {
      this.#never = true
    }

    const accepted = new Set(values.map(lower))
    this.#predicates.push((item) => accepted.has(lower(get(item))))

    return this
  }

  /** `undefined` when the clause can never match. */
  build(): Predicate<T> | undefined {
    if (this.#never) {
      return undefined
    }

    const predicates = this.#predicates

    return (item) => predicates.every((p) => p(item))
  }
}

/** All where-clauses for one item kind; a match returns the union of the matched clauses' relations. */
class EntityFilter<T, R extends object> {
  readonly #clauses: { match: Predicate<T>; relations: R }[] = []

  add(builder: FilterBuilder<T>, relations: R): void {
    const match = builder.build()
    if (match) {
      this.#clauses.push({ match, relations })
    }
  }

  present(): boolean {
    return this.#clauses.length > 0
  }

  match(item: T): R | undefined {
    let merged: R | undefined
    for (const clause of this.#clauses) {
      if (!clause.match(item)) {
        continue
      }

      merged = { ...merged, ...pickTrue(clause.relations) } as R
    }

    return merged
  }
}

function pickTrue<R extends object>(relations: R): Partial<R> {
  return Object.fromEntries(Object.entries(relations).filter(([, on]) => on)) as Partial<R>
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
    if (order) {
      return order
    }
  }

  return a.length - b.length
}

function isDescendant(parent: AnyTrace, child: AnyTrace): boolean {
  const pa = traceAddressOf(parent)
  const ca = traceAddressOf(child)
  if (parent.transactionIndex !== child.transactionIndex) {
    return false
  }
  if (pa.length >= ca.length) {
    return false
  }
  for (let i = 0; i < pa.length; i++) {
    if (pa[i] !== ca[i]) {
      return false
    }
  }

  return true
}

function setUpRelations(block: AnyBlock): Relations {
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
    filter.getIn((it) => (it as any).address, address)
    filter.getIn((log) => (log as any).topics?.[0], topic0)
    filter.getIn((log) => (log as any).topics?.[1], topic1)
    filter.getIn((log) => (log as any).topics?.[2], topic2)
    filter.getIn((log) => (log as any).topics?.[3], topic3)
    items.add(filter, relations)
  }

  return items
}

function buildTransactionFilter(req: TransactionRequest[] = []) {
  const items = new EntityFilter<AnyTransaction, { logs?: boolean; traces?: boolean; stateDiffs?: boolean }>()
  for (const { to, from, sighash, type, ...relations } of req) {
    const filter = new FilterBuilder<AnyTransaction>()
    filter.getIn((it) => (it as any).to, to)
    filter.getIn((it) => (it as any).from, from)
    filter.getIn((it) => (it as any).sighash, sighash)
    filter.getIn((it) => (it as any).type, type)
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
    filter.getIn((it) => (it as any).type, type)
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
    filter.getIn((it) => (it as any).address, address)
    filter.getIn((it) => (it as any).key, key)
    filter.getIn((it) => (it as any).kind, kind)
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
    if (log) {
      this.logs.add(log)
    }
  }
  addTransaction(tx?: AnyTransaction) {
    if (tx) {
      this.transactions.add(tx)
    }
  }
  addTrace(trace?: AnyTrace) {
    if (trace) {
      this.traces.add(trace)
    }
  }
  addTraceStack(relations: Relations, trace?: AnyTrace) {
    while (trace) {
      this.traces.add(trace)
      trace = relations.traceParent.get(trace)
    }
  }
  addStateDiff(diff?: AnyStateDiff) {
    if (diff) {
      this.stateDiffs.add(diff)
    }
  }
}

function buildFilters(req: DataRequest) {
  return {
    logFilter: buildLogFilter(req.logs),
    transactionFilter: buildTransactionFilter(req.transactions),
    traceFilter: buildTraceFilter(req.traces),
    stateDiffFilter: buildStateDiffFilter(req.stateDiffs),
  }
}

type BlockFilters = ReturnType<typeof buildFilters>

/** Build the request's filters once; the result keeps, in place, the items of a block that the request matches. */
export function createBlockFilter(req: DataRequest): (block: AnyBlock) => void {
  const filters = buildFilters(req)

  return (block) => filterBlock(block, filters)
}

function filterBlock(
  block: AnyBlock,
  { logFilter, transactionFilter, traceFilter, stateDiffFilter }: BlockFilters,
): void {
  const relations = setUpRelations(block)
  const include = new IncludeSet()

  if (logFilter.present()) {
    for (const log of block.logs) {
      const rel = logFilter.match(log)
      if (rel == null) {
        continue
      }
      include.addLog(log)
      if (rel.transaction) {
        include.addTransaction(relations.txByIndex.get(log.transactionIndex))
      }
      if (rel.transactionLogs) {
        for (const sibling of relations.logsByTx.get(log.transactionIndex) ?? []) {
          include.addLog(sibling)
        }
      }
      if (rel.transactionTraces) {
        for (const trace of relations.tracesByTx.get(log.transactionIndex) ?? []) {
          include.addTrace(trace)
        }
      }
      if (rel.transactionStateDiffs) {
        for (const diff of relations.stateDiffsByTx.get(log.transactionIndex) ?? []) {
          include.addStateDiff(diff)
        }
      }
    }
  }

  if (transactionFilter.present()) {
    for (const tx of block.transactions) {
      const rel = transactionFilter.match(tx)
      if (rel == null) {
        continue
      }
      include.addTransaction(tx)
      if (rel.logs) {
        for (const log of relations.logsByTx.get(tx.transactionIndex) ?? []) {
          include.addLog(log)
        }
      }
      if (rel.traces) {
        for (const trace of relations.tracesByTx.get(tx.transactionIndex) ?? []) {
          include.addTrace(trace)
        }
      }
      if (rel.stateDiffs) {
        for (const diff of relations.stateDiffsByTx.get(tx.transactionIndex) ?? []) {
          include.addStateDiff(diff)
        }
      }
    }
  }

  if (traceFilter.present()) {
    for (const trace of block.traces) {
      const rel = traceFilter.match(trace)
      if (rel == null) {
        continue
      }
      include.addTrace(trace)
      if (rel.parents) {
        include.addTraceStack(relations, relations.traceParent.get(trace))
      }
      if (rel.subtraces) {
        for (const sub of relations.traceChildren.get(trace) ?? []) {
          include.addTrace(sub)
        }
      }
      if (rel.transaction) {
        include.addTransaction(relations.txByIndex.get(trace.transactionIndex))
      }
      if (rel.transactionLogs) {
        for (const log of relations.logsByTx.get(trace.transactionIndex) ?? []) {
          include.addLog(log)
        }
      }
    }
  }

  if (stateDiffFilter.present()) {
    for (const diff of block.stateDiffs) {
      const rel = stateDiffFilter.match(diff)
      if (rel == null) {
        continue
      }
      include.addStateDiff(diff)
      if (rel.transaction) {
        include.addTransaction(relations.txByIndex.get(diff.transactionIndex))
      }
    }
  }

  block.logs = block.logs.filter((log) => include.logs.has(log))
  block.transactions = block.transactions.filter((tx) => include.transactions.has(tx))
  block.traces = block.traces.filter((trace) => include.traces.has(trace))
  block.stateDiffs = block.stateDiffs.filter((diff) => include.stateDiffs.has(diff))
}
