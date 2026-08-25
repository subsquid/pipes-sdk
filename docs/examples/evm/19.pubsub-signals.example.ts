/**
 * Google PubSub signal routes — publish application messages without the CDC envelope.
 *
 * A `topics` route (see `18.pubsub.example.ts`) publishes row mutations: every message carries
 * `_id`, `_CHANGE_TYPE`, and `_CHANGE_SEQUENCE_NUMBER`, and a fork is repaired per row with a
 * compensating `DELETE` or restoring `UPSERT`. That is the right shape when the consumer mirrors
 * a table — a BigQuery subscription applies it with no code.
 *
 * A `signals` route publishes the payload verbatim instead. Use it when the consumer is not a
 * table: a stateful fold, a control channel, a watermark announcement. The route owns the whole
 * schema, so anything the consumer needs — a type tag, an id, the fork epoch — has to be in
 * `data`.
 *
 * The trade is fork repair. A signal has no row identity and no compensating message, so a reorg
 * cannot be repaired per message. Each route declares how it copes:
 *
 * - `fork: { mode: 'boundary', map }` — the route publishes unfinalized data and accepts that
 *   some of it gets orphaned. On a fork the target publishes ONE message built by `map`, ahead of
 *   every CDC compensation, carrying the new epoch and the block the feed rewound to. The
 *   consumer discards what it received above that block from the previous epoch. That is the
 *   whole repair: one boundary message instead of one compensation per row.
 *
 * - `fork: { mode: 'finalized-only' }` — the route never publishes anything a fork could orphan,
 *   so no boundary message is needed. Enforced, not assumed: a draft above the finalized head is
 *   a fatal `E2427` rather than an unretractable message on the wire.
 *
 * `epoch` is a durable counter incremented once per fork, in the same transaction that rewinds
 * the cursor. It is what lets an unordered consumer tell a late duplicate from live data: a
 * message stamped with an epoch below the last boundary it saw is from an orphaned branch.
 *
 * Delivery stays at-least-once, as for CDC routes. The guarantee is one durable boundary message
 * per route per committed fork — not exactly-once delivery, and not ordering across topics.
 *
 * A signal topic must NOT be attached to a BigQuery subscription: the payload has no CDC fields,
 * so every message would fail schema validation and land in the dead-letter topic.
 *
 * A stream may feed both a `topics` route and a `signals` route — they are independent maps over
 * the same batch, publishing to different topics.
 *
 * To run:
 *
 * ```bash
 * GOOGLE_CLOUD_PROJECT=my-project tsx docs/examples/evm/19.pubsub-signals.example.ts
 * ```
 */

import { PubSub } from '@google-cloud/pubsub'
import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'
import { pubsubTarget } from '@subsquid/pipes/targets/pubsub'

const PROJECT = process.env['GOOGLE_CLOUD_PROJECT'] ?? 'demo'
const TRANSFERS_TOPIC = process.env['PUBSUB_TRANSFERS_TOPIC'] ?? 'evm.base.erc20-transfers.raw'
const STATE = process.env['PUBSUB_STATE'] ?? './state/base-erc20-signals.sqlite'

async function main() {
  await evmStream({
    id: 'base-erc20-signals',
    source: 'https://portal.sqd.dev/datasets/base-mainnet',
    outputs: evmEventDecoder({
      range: { from: 'latest' },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
    }),
  }).pipeTo(
    pubsubTarget({
      pubsub: new PubSub({ projectId: PROJECT }),
      state: { path: STATE },
      namespace: 'base-erc20',

      signals: {
        transfers: {
          topic: TRANSFERS_TOPIC,

          // Pure and deterministic, exactly as for a CDC route: a replay after a crash must
          // reproduce identical bytes, or the consumer cannot recognise the duplicate.
          map: ({ data, epoch }) =>
            data.map((t) => ({
              // Published verbatim. No `_id`, no `_CHANGE_TYPE` — the consumer reads this shape.
              data: {
                type: 'transfer',
                // Stamped so a message from a branch the feed has since abandoned is
                // recognisable after the boundary message raises the consumer's epoch.
                epoch,
                token: t.rawEvent.address,
                from: t.event.from,
                to: t.event.to,
                amount: t.event.value.toString(),
                block: t.block.number,
                logIndex: t.rawEvent.logIndex,
              },
              // Required: drives the go-live cut and the fork-mode check.
              block: t.block,
              // Subscription filters work the same as on a CDC route.
              attributes: {
                token: t.rawEvent.address,
              },
            })),

          // The alternative is `fork: { mode: 'finalized-only' }`, with no `map`: it publishes
          // nothing a fork can orphan, so the consumer never unwinds — at the cost of trailing
          // the finalized head. Reading the finalized stream satisfies it trivially.
          fork: {
            mode: 'boundary',
            // Called once per fork, inside the transaction that rewinds the cursor and persists
            // the new epoch — so this message and the rewind are committed together or not at
            // all. `rollbackTo` is the last block that survived; `deadEnd` means nothing local
            // proved which branch is canonical and the consumer needs a full rebuild.
            map: ({ epoch, rollbackTo, deadEnd }) => ({
              data: {
                type: 'fork',
                epoch,
                rollbackTo: rollbackTo?.number ?? null,
                deadEnd,
              },
            }),
          },
        },
      },
    }),
  )
}

void main()
