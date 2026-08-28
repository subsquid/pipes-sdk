/**
 * Google PubSub control route — the fork announcement and the finality watermark.
 *
 * A `topics` route (see `18.pubsub.example.ts`) is a changelog: every orphaned id gets a
 * compensating `DELETE` or a restoring `UPSERT`, and a BigQuery subscription applies them with no
 * code. The `control` route carries what is *not* a table row — statements about the feed itself.
 *
 * There are two of them, and they earn their place for different reasons.
 *
 * ## The fork announcement is an optimisation
 *
 * A consumer folding an aggregate can undo a fork from the rows alone. A compensation carries the
 * orphaned row's own body and is sequenced ahead of the row that replaces it, so:
 *
 *   on a DELETE at block N with sequence S, drop every contribution from a row with
 *   block ≥ N and sequence < S — including rows that arrive after the DELETE.
 *
 * That is correct under unordered delivery, needs no epoch and no announcement. What the
 * announcement adds is latency — one message retires a whole fork, where the rule converges only
 * as the last compensation lands — and generality: a `materialized` route compensates by restoring
 * the surviving revision, so such a topic can pass through a fork without a single `DELETE`, and a
 * consumer inferring forks from `DELETE`s is blind to it.
 *
 * It is enqueued INSIDE the transaction that rewinds the cursor and raises the epoch, so a rewound
 * cursor with an unannounced epoch cannot exist. A crash leaves it unpublished, never unrecorded.
 *
 * ## The finality watermark is not
 *
 * A consumer cannot keep per-block state forever; at some point it folds old blocks into a base a
 * retraction can no longer reach. Nothing in a row stream says when that is safe: there is no
 * count, no end marker, and silence is not proof. The producer holds the source's finalized head
 * and is the only party that does — so it publishes it.
 *
 * As a **reference value**, not an authority over a row. Finality is a policy: providers disagree
 * about it and a pipe may widen its window deliberately, so the number is an input to whatever
 * confirmation policy the consumer chose. Reading it needs no ceremony — it is monotone, so a
 * consumer keeps the maximum of what it has seen, with no sequence comparison involved. It comes
 * off the batch commit rather than off row traffic, so it keeps advancing while the table is
 * quiet; stamped on rows it would leave a sparse table's consumer as stale as its last row.
 *
 * ## What both rely on
 *
 * Every operation takes one number from one producer-wide counter, so on a single-topic producer
 * the topic's sequence is gapless — which is what lets a consumer tell "nothing more is coming"
 * from "it has not arrived yet", for a fork's compensations and for a whole block alike. Control
 * records ride the data topics for exactly that reason: a number burned on another topic is a hole
 * in this one. Naming `control.topic` trades that away, and so does a server-side filter — every
 * removed message reads as a gap. A BigQuery landing may filter, because it uses no barrier; a
 * consumer that does cannot.
 *
 * Every message carries a reserved `_type` attribute — `cdc` for a row change, `control` for these
 * — so a BigQuery subscription takes the rows with `attributes._type = "cdc"`. PubSub filters match
 * attributes and never the body, which is why the discriminator is an attribute, and why it says
 * only data-or-control: a subscription's filter is IMMUTABLE after creation, so it has to stay
 * correct as new kinds of control record appear. Their own payload discriminates them.
 *
 * `epoch` is the durable fork counter, raised once per fork. `map` receives it too, so data rows
 * can carry the epoch they were published under.
 *
 * Delivery stays at-least-once. The guarantee is one durable announcement per committed fork —
 * not exactly-once delivery, and not ordering across topics.
 *
 * To run:
 *
 * ```bash
 * GOOGLE_CLOUD_PROJECT=my-project tsx docs/examples/evm/19.pubsub-control.example.ts
 * ```
 */

import { PubSub } from '@google-cloud/pubsub'
import { commonAbis, evmEventDecoder, evmPortalStream } from '@subsquid/pipes/evm'
import { pubsubTarget } from '@subsquid/pipes/targets/pubsub'

const PROJECT = process.env['GOOGLE_CLOUD_PROJECT'] ?? 'demo'
const TRANSFERS_TOPIC = process.env['PUBSUB_TRANSFERS_TOPIC'] ?? 'evm.base.erc20-transfers'
const STATE = process.env['PUBSUB_STATE'] ?? './state/base-erc20-control.sqlite'

async function main() {
  await evmPortalStream({
    id: 'base-erc20-control',
    portal: 'https://portal.sqd.dev/datasets/base-mainnet',
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

      topics: {
        transfers: {
          topic: TRANSFERS_TOPIC,

          // Pure and deterministic: a replay after a crash must reproduce identical bytes, or the
          // consumer cannot recognise the duplicate.
          map: ({ data, epoch }) =>
            data.map((t) => ({
              data: {
                // Stamped so a row from a branch the feed has since abandoned stays recognisable
                // after the announcement raises the consumer's epoch. A consumer that only mirrors
                // the table can ignore it — the per-row compensation already repaired that.
                epoch,
                token: t.rawEvent.address,
                from: t.event.from,
                to: t.event.to,
                amount: t.event.value.toString(),
              },
              block: t.block,
              attributes: {
                token: t.rawEvent.address,
              },
            })),
        },
      },

      control: {
        // No `topic`: control records ride TRANSFERS_TOPIC, tagged `_type: "control"`.
        //
        // Called once per fork, inside the transaction that rewinds the cursor and persists the
        // new epoch. `rollbackTo` is the last block that survived; `deadEnd` means nothing local
        // proved which branch is canonical and the consumer needs a full rebuild.
        fork: ({ epoch, rollbackTo, deadEnd }) => ({
          data: {
            type: 'fork',
            epoch,
            rollbackTo: rollbackTo?.number ?? null,
            deadEnd,
          },
          // The target adds `_type` and nothing else. A subscriber filtering on `token` would
          // not see this announcement at all — mirror whatever your consumers filter on that is
          // constant for the topic. `token` is per-row here, so a filtered subscriber has to
          // admit it: `attributes.token = "0x…" OR attributes._type = "control"`.
          attributes: { chain: 'base' },
        }),

        // The source's own finalized head, republished every 100 blocks it advances. A skipped
        // watermark costs nothing — the next one carries a higher value — so this is a chattiness
        // knob, not a correctness one.
        finality: {
          everyBlocks: 100,
          map: ({ finalized, observedAt }) => ({
            data: {
              type: 'finality',
              // A reference value. The consumer decides what to do with it — this producer is not
              // claiming the row at that height is immutable.
              finalBlock: finalized.number,
              observedAt: observedAt.number,
            },
            attributes: { chain: 'base' },
          }),
        },
      },
    }),
  )
}

void main()
