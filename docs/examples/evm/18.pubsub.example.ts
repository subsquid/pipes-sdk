/**
 * Google PubSub target — publish a BigQuery CDC-compatible stream of ERC20 transfers.
 *
 * The target flattens each row into the message body and adds:
 *
 * ```json
 * {
 *   "token": "0x…",
 *   "from": "0x…",
 *   "to": "0x…",
 *   "amount": "1000000",
 *   "_id": "0xblockhash:12",
 *   "_CHANGE_TYPE": "UPSERT",
 *   "_CHANGE_SEQUENCE_NUMBER": "2A"
 * }
 * ```
 *
 * A `bigint` encodes as a decimal string and a `Date` as RFC 3339. Use `STRING` for an unconstrained
 * `uint256` amount (`BIGNUMERIC` is narrower) and `TIMESTAMP` for the date — not `INT64`.
 *
 * Fork repairs use `DELETE` or a restoring `UPSERT` with a larger change sequence number. The
 * sequence is one durable producer-wide counter; it is independent of PubSub ordering keys and
 * is not a gap detector.
 *
 * Business attributes remain on the PubSub message for subscription filters. A compensation
 * copies the attributes of the operation it repairs, so a filter such as
 * `attributes.token = "0x…"` also receives the matching repair.
 *
 * For a direct BigQuery subscription, enable `use_table_schema` and declare `_id` as the
 * destination table's primary key. `docs/pubsub-bigquery.md` has the table DDL, the subscription
 * flags, the IAM grants for a subscriber in another project, and the limits. PubSub message
 * ordering is optional: set
 * `publish.messageOrdering: true` only when another subscriber needs ordered delivery, and enable
 * message ordering on that subscription too. The topic name becomes the default ordering key,
 * and `MessageDraft.orderingKey` can shard it explicitly.
 *
 * `publish.uidAttribute: true` below makes the SDK add a stable `_uid` attribute for every
 * operation. When Dataflow performs deduplication, configure its PubSub source `idAttribute` to
 * `_uid`; enabling the SDK option alone does not configure Dataflow.
 *
 * The state file holds the cursor, rollback data, outbox, and sequence counter. Keep it on durable
 * storage and run one producer per path.
 *
 * `19.pubsub-control.example.ts` covers the control route — the fork announcement, for consumers
 * that fold their own state instead of mirroring rows.
 *
 * To run:
 *
 * ```bash
 * GOOGLE_CLOUD_PROJECT=my-project PUBSUB_TOPIC=evm.base.erc20-transfers \
 *   tsx docs/examples/evm/18.pubsub.example.ts
 * ```
 */

import { PubSub } from '@google-cloud/pubsub'
import { commonAbis, evmEventDecoder, evmPortalStream } from '@subsquid/pipes/evm'
import { pubsubTarget } from '@subsquid/pipes/targets/pubsub'

const PROJECT = process.env['GOOGLE_CLOUD_PROJECT'] ?? 'demo'
const TOPIC = process.env['PUBSUB_TOPIC'] ?? 'evm.base.erc20-transfers'
const STATE = process.env['PUBSUB_STATE'] ?? './state/base-erc20-transfers.sqlite'

async function main() {
  await evmPortalStream({
    id: 'base-erc20-transfers',
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
      // Cursor + rollback manifest + outbox + sequence counters, one transaction per batch.
      state: { path: STATE },
      // The id space consumers see. Pinned explicitly so renaming the pipe does not silently
      // start a new one (the default is the pipe id).
      namespace: 'base-erc20',
      publish: {
        // The target generates the value. Dataflow must separately be told to use `_uid` as
        // its PubSub id attribute.
        uidAttribute: true,
      },

      topics: {
        transfers: {
          topic: TOPIC,
          // Pure and deterministic: replays must reproduce the same logical rows.
          map: ({ data }) =>
            data.map((t) => ({
              // A plain object becomes the flattened CDC message body.
              data: {
                // Stable row identity. Hash-based, so a fork's re-streamed events get fresh ids
                // instead of aliasing the orphaned ones. Omit it to use the generated fallback.
                _id: `${t.block.hash}:${t.rawEvent.logIndex}`,
                token: t.rawEvent.address,
                from: t.event.from,
                to: t.event.to,
                amount: t.event.value,
                block: t.block.number,
                timestamp: t.timestamp,
              },
              // Required: every operation is attributed to its block, and that attribution is
              // what makes fork compensation possible.
              block: t.block,
              // Filter attributes. A fork's `delete` carries these unchanged, so a filtered
              // subscription receives the repair for its own slice.
              attributes: {
                token: t.rawEvent.address,
                from: t.event.from,
                to: t.event.to,
              },
            })),
        },
      },
    }),
  )
}

void main()
