type EvmLog = { address: string; topics: string[]; transactionIndex: number }

/** `to` is `null` on a contract creation. */
type EvmTransaction = { transactionIndex: number; from?: string; to?: string | null; sighash?: string; type?: number }

type EvmTrace = {
  type: string
  transactionIndex: number
  traceAddress: number[]
  action?: { from?: string; to?: string; sighash?: string; refundAddress?: string | null; author?: string }
}

type EvmStateDiff = { transactionIndex: number; address: string; key: string; kind: string }

type LogRequest = {
  address?: string[]
  topic0?: string[]
  topic1?: string[]
  topic2?: string[]
  topic3?: string[]
  transaction?: boolean
  transactionLogs?: boolean
  transactionTraces?: boolean
  transactionStateDiffs?: boolean
}

type TransactionRequest = {
  from?: string[]
  to?: string[]
  sighash?: string[]
  type?: number[]
  logs?: boolean
  traces?: boolean
  stateDiffs?: boolean
}

type TraceRequest = {
  type?: string[]
  createFrom?: string[]
  callTo?: string[]
  callFrom?: string[]
  callSighash?: string[]
  suicideRefundAddress?: string[]
  rewardAuthor?: string[]
  transaction?: boolean
  transactionLogs?: boolean
  subtraces?: boolean
  parents?: boolean
}

type StateDiffRequest = {
  address?: string[]
  key?: string[]
  kind?: string[]
  transaction?: boolean
}

/** Transactions, by index, whose own item or whole item lists a relation flag pulls in. */
type Related = {
  transactions: Set<number>
  logs: Set<number>
  traces: Set<number>
  stateDiffs: Set<number>
}

function list<T>(source: Record<string, unknown>, key: string): T[] {
  return (source[key] ?? []) as T[]
}

function matches(values: string[] | undefined, value: string | null | undefined): boolean {
  if (values === undefined) {
    return true
  }
  if (value == null) {
    return false
  }

  return values.some((v) => v.toLowerCase() === value.toLowerCase())
}

function matchesLog(request: LogRequest, log: EvmLog): boolean {
  return (
    matches(request.address, log.address) &&
    matches(request.topic0, log.topics[0]) &&
    matches(request.topic1, log.topics[1]) &&
    matches(request.topic2, log.topics[2]) &&
    matches(request.topic3, log.topics[3])
  )
}

function matchesTransaction(request: TransactionRequest, tx: EvmTransaction): boolean {
  const typeMatches = request.type === undefined || (tx.type !== undefined && request.type.includes(tx.type))

  return (
    matches(request.from, tx.from) && matches(request.to, tx.to) && matches(request.sighash, tx.sighash) && typeMatches
  )
}

/** A filter that names one trace type's field matches only traces of that type. */
function matchesTraceField(
  values: string[] | undefined,
  trace: EvmTrace,
  type: string,
  value: string | null | undefined,
) {
  if (values === undefined) {
    return true
  }

  return trace.type === type && matches(values, value)
}

function matchesTrace(request: TraceRequest, trace: EvmTrace): boolean {
  const action = trace.action ?? {}
  const typeMatches = request.type === undefined || request.type.includes(trace.type)

  return (
    typeMatches &&
    matchesTraceField(request.createFrom, trace, 'create', action.from) &&
    matchesTraceField(request.callTo, trace, 'call', action.to) &&
    matchesTraceField(request.callFrom, trace, 'call', action.from) &&
    matchesTraceField(request.callSighash, trace, 'call', action.sighash) &&
    matchesTraceField(request.suicideRefundAddress, trace, 'suicide', action.refundAddress) &&
    matchesTraceField(request.rewardAuthor, trace, 'reward', action.author)
  )
}

function matchesStateDiff(request: StateDiffRequest, diff: EvmStateDiff): boolean {
  const kindMatches = request.kind === undefined || request.kind.includes(diff.kind)

  return matches(request.address, diff.address) && matches(request.key, diff.key) && kindMatches
}

/** True when `ancestor` is a strict prefix of `descendant`: a trace above it in one call tree. */
function isAbove(ancestor: number[], descendant: number[]): boolean {
  return ancestor.length < descendant.length && ancestor.every((step, i) => descendant[i] === step)
}

/**
 * The EVM items of a block that a query selects: what its requests match, plus what their relation
 * flags pull in — the transaction of an item, all logs, traces or state diffs of that transaction,
 * and the subtraces or parents of a trace.
 */
export function selectEvm(items: Record<string, unknown>, query: Record<string, unknown>): Record<string, unknown> {
  const logs = list<EvmLog>(items, 'logs')
  const transactions = list<EvmTransaction>(items, 'transactions')
  const traces = list<EvmTrace>(items, 'traces')
  const stateDiffs = list<EvmStateDiff>(items, 'stateDiffs')

  const related: Related = { transactions: new Set(), logs: new Set(), traces: new Set(), stateDiffs: new Set() }
  const selectedLogs = new Set<EvmLog>()
  const selectedTraces = new Set<EvmTrace>()
  const selectedStateDiffs = new Set<EvmStateDiff>()

  for (const log of logs) {
    for (const request of list<LogRequest>(query, 'logs')) {
      if (!matchesLog(request, log)) {
        continue
      }

      const tx = log.transactionIndex
      selectedLogs.add(log)
      if (request.transaction) {
        related.transactions.add(tx)
      }
      if (request.transactionLogs) {
        related.logs.add(tx)
      }
      if (request.transactionTraces) {
        related.traces.add(tx)
      }
      if (request.transactionStateDiffs) {
        related.stateDiffs.add(tx)
      }
    }
  }

  for (const transaction of transactions) {
    for (const request of list<TransactionRequest>(query, 'transactions')) {
      if (!matchesTransaction(request, transaction)) {
        continue
      }

      const tx = transaction.transactionIndex
      related.transactions.add(tx)
      if (request.logs) {
        related.logs.add(tx)
      }
      if (request.traces) {
        related.traces.add(tx)
      }
      if (request.stateDiffs) {
        related.stateDiffs.add(tx)
      }
    }
  }

  for (const trace of traces) {
    for (const request of list<TraceRequest>(query, 'traces')) {
      if (!matchesTrace(request, trace)) {
        continue
      }

      const tx = trace.transactionIndex
      selectedTraces.add(trace)
      if (request.transaction) {
        related.transactions.add(tx)
      }
      if (request.transactionLogs) {
        related.logs.add(tx)
      }

      for (const other of traces) {
        if (other.transactionIndex !== tx) {
          continue
        }

        const isSubtrace = request.subtraces && isAbove(trace.traceAddress, other.traceAddress)
        const isParent = request.parents && isAbove(other.traceAddress, trace.traceAddress)
        if (isSubtrace || isParent) {
          selectedTraces.add(other)
        }
      }
    }
  }

  for (const diff of stateDiffs) {
    for (const request of list<StateDiffRequest>(query, 'stateDiffs')) {
      if (!matchesStateDiff(request, diff)) {
        continue
      }

      selectedStateDiffs.add(diff)
      if (request.transaction) {
        related.transactions.add(diff.transactionIndex)
      }
    }
  }

  const selected: Record<string, unknown> = {}

  const outLogs = logs.filter((log) => selectedLogs.has(log) || related.logs.has(log.transactionIndex))
  const outTransactions = transactions.filter((tx) => related.transactions.has(tx.transactionIndex))
  const outTraces = traces.filter((trace) => selectedTraces.has(trace) || related.traces.has(trace.transactionIndex))
  const outStateDiffs = stateDiffs.filter(
    (diff) => selectedStateDiffs.has(diff) || related.stateDiffs.has(diff.transactionIndex),
  )

  if (outLogs.length > 0) {
    selected['logs'] = outLogs
  }
  if (outTransactions.length > 0) {
    selected['transactions'] = outTransactions
  }
  if (outTraces.length > 0) {
    selected['traces'] = outTraces
  }
  if (outStateDiffs.length > 0) {
    selected['stateDiffs'] = outStateDiffs
  }

  return selected
}
