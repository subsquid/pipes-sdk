/**
 * Google Pub/Sub target — publish a live changelog of ERC20 transfers.
 *
 * Unlike the database targets, Pub/Sub is write-only: a published message cannot be read back,
 * updated, or deleted. So the target publishes a **changelog** of two operations on keyed rows —
 * `upsert` and `delete` — and repairs chain forks by publishing compensating operations rather
 * than by rewriting anything.
 *
 * What that buys you:
 *   - **Hot blocks, no hold-back.** Unfinalized data is published as it arrives. When the chain
 *     forks, the target emits one `delete` per orphaned event (or, for a row that is revised
 *     rather than written once, an `upsert` restoring the value that survives at the fork point).
 *   - **Filters that keep working through a fork.** A compensation inherits the user attributes of
 *     the operation it repairs, so an ordinary subscription filter (`attributes.token = "0x…"`)
 *     receives the repair for its own slice with no special casing. Never filter on `_op`.
 *   - **Order-free consumption.** The default `lww` profile uses no Pub/Sub ordering keys and has
 *     no throughput cap. Each message carries `_seq`, a per-row version: a consumer applies an
 *     operation only when its `_seq` beats the one it holds for that `_id`. Delivery order stops
 *     mattering, and several producers can share one topic as long as ids do not overlap.
 *   - **Durable resume with no external database.** Cursor, rollback manifest, publish outbox and
 *     sequence counters live in one local SQLite file, committed in one transaction per batch.
 *
 * The consumer contract is one conditional statement:
 *
 * ```sql
 * INSERT INTO rows (id, seq, data) VALUES (?, ?, ?)
 * ON CONFLICT (id) DO UPDATE SET seq = excluded.seq, data = excluded.data
 * WHERE excluded.seq > rows.seq;   -- a `delete` is the same statement with data = NULL
 * ```
 *
 * Keep those tombstones for at least as long as the subscription's message retention (7 days by
 * default, up to 31): a late redelivery of an orphaned upsert must still lose to its delete.
 *
 * Two things to know before running this against a real feed:
 *   - **The state file is the producer's sequencer.** Put it on a persistent volume and run one
 *     instance per path. Losing it is an incident, not a restart — recovery means a fresh
 *     `namespace` plus a consumer re-bootstrap. The target logs a warning and raises
 *     `sqd_pubsub_cold_start` whenever it starts with an empty state; alert on that.
 *   - **This is a live changelog, not a backfill channel.** `publishFrom` defaults to `'latest'`
 *     (resolved once, then persisted), so a pipe reading deep history publishes nothing until it
 *     reaches its go-live block. History belongs in a queryable target; late consumers bootstrap
 *     from a snapshot of it and then apply the changelog.
 *
 * Prerequisites:
 *   - `@google-cloud/pubsub` is an optional peer dependency — install it: `npm i @google-cloud/pubsub`.
 *   - The topic must exist (the default `topicSetup: 'validate'` fails fast otherwise), and the
 *     subscription must exist BEFORE the first publish — Pub/Sub retains messages per
 *     subscription, so anything published earlier is simply gone.
 *
 * To run:
 * ```bash
 * # against a real project (Application Default Credentials)
 * GOOGLE_CLOUD_PROJECT=my-project PUBSUB_TOPIC=evm.base.erc20-transfers \
 *   tsx docs/examples/evm/18.pubsub.example.ts
 *
 * # or against the emulator
 * gcloud beta emulators pubsub start --project=demo
 * PUBSUB_EMULATOR_HOST=localhost:8085 GOOGLE_CLOUD_PROJECT=demo \
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

      topics: {
        transfers: {
          topic: TOPIC,
          // Pure and deterministic: a replay after a crash or a fork must reproduce identical
          // bytes, which is what lets a consumer recognise the duplicate and drop it.
          map: ({ data }) =>
            data.map((t) => ({
              // A plain object goes through the canonical codec — bigint-safe, key-order stable.
              data: {
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
              // Stable row identity. Hash-based, so a fork's re-streamed events get fresh ids
              // instead of aliasing the orphaned ones.
              id: `${t.block.hash}:${t.rawEvent.logIndex}`,
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
