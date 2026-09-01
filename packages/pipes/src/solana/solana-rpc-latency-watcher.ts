import { RpcLatencyWatcher, WebSocketListener, rpcLatencyWatcher } from '~/monitoring/index.js'
import { solanaQuery } from '~/solana/solana-query-builder.js'

export type { LatencySample, RpcObservation } from '~/monitoring/index.js'

type Notification = {
  params?: {
    result?: {
      /*
      firstShredReceived
        The validator has received the very first shred (fragment of data) belonging to this block/slot.
        👉 Means the block is starting to arrive over the network.

      completed
        All shreds for this block have been received, and the block data is now complete.
        👉 The validator has the full ledger entry for this slot.

      createdBank
        A “bank” (in-memory state machine that processes transactions) has been created for this slot.
        👉 Execution of transactions for this block can start.

      frozen
        The bank for this slot is frozen, meaning no more transactions will be added.
        The ledger entry for this block is finalized locally on this validator.
        👉 A validator has finished executing and sealing the block.

      dead
        The bank was marked dead, usually because it was abandoned (e.g., a fork lost the vote, or missing data made it impossible to complete).
        👉 This block will not become part of the canonical chain.

      optimisticConfirmation
        The block has reached optimistic confirmation — meaning a supermajority (>66%) of the cluster has voted on a descendant, so it’s very likely to stay in the ledger, but not absolutely final yet.
        👉 This is the “fast finality” Solana uses.

      root
        The block has been rooted: a descendant of this block is finalized and cannot be reverted.
        👉 This is the final, irreversible state in Solana’s ledger.
      */
      type: 'firstShredReceived' | 'completed' | 'createdBank' | 'frozen' | 'dead' | 'optimisticConfirmation' | 'root'
      timestamp: number
      slot: number
    }
  }
}

class SolanaRpcLatencyWatcher extends RpcLatencyWatcher {
  watch(url: string) {
    const listener = new WebSocketListener(url)

    const payload = {
      jsonrpc: '2.0',
      id: 1,
      method: 'slotsUpdatesSubscribe',
    }

    listener.subscribe(payload, (message: Notification) => {
      const res = message.params?.result

      if (res?.type !== 'optimisticConfirmation') return

      this.addBlock(url, {
        number: res.slot,
        timestamp: new Date(res.timestamp),
        receivedAt: new Date(),
      })
    })

    return listener
  }
}

export function solanaRpcLatencyWatcher({ rpcUrl, resolveTimeoutMs }: { rpcUrl: string[]; resolveTimeoutMs?: number }) {
  const transformer = rpcLatencyWatcher({
    watcher: new SolanaRpcLatencyWatcher(rpcUrl),
    resolveTimeoutMs,
  })

  return solanaQuery()
    .addFields({
      block: {
        number: true,
        timestamp: true,
      },
    })
    .addRange({ from: 'latest' })
    .build()
    .pipe(transformer.options)
}
