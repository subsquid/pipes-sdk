import type { BatchPublishOptions, ClientConfig, FlowControlOptions, PubSub, Topic } from '@google-cloud/pubsub'

import { Logger } from '~/core/index.js'

import { PUBSUB_ERROR_CODES, PubsubTargetError } from './errors.js'
import { DeliveryProfile, OutboxRow } from './pubsub-state.js'

export type TopicSetup = 'validate' | 'create' | 'none'

export type PublisherOptions = {
  delivery: DeliveryProfile
  batching?: BatchPublishOptions
  flowControl?: FlowControlOptions
  topicSetup: TopicSetup
}

export type PublishMessage = {
  topic: string
  orderingKey: string
  attributes: Record<string, string>
  payload: Uint8Array
}

/**
 * Result of one outbox drain. `confirmed` holds the row ids whose publish was acknowledged —
 * only those may leave the outbox. A partition stops at its first failure, so the rows behind
 * it stay queued with their original seq and bytes.
 */
export type DrainResult = {
  confirmed: number[]
  bytes: number
  error?: unknown
}

/**
 * The client seam. The target speaks only this interface, so the whole write/fork protocol is
 * testable without a Pub/Sub endpoint, and a different transport stays a drop-in.
 */
export interface Publisher {
  /** Called once at start: validate (default), create (dev), or skip topic administration. */
  setup(topics: string[], logger: Logger): Promise<void>
  /** Publish the outbox in the given order, partition by partition. */
  drain(rows: (OutboxRow & { attributes: Record<string, string> })[]): Promise<DrainResult>
  close(): Promise<void>
}

const partitionOf = (row: { topic: string; orderingKey: string }) => `${row.topic} ${row.orderingKey}`

/**
 * Groups an outbox drain into partitions, preserving each partition's enqueue order.
 * Under `lww` the ordering key is empty, so a topic is one partition — publishing stays
 * concurrent across topics and the client keeps batching within each.
 */
export function partitionRows<T extends { topic: string; orderingKey: string }>(rows: T[]): T[][] {
  const groups = new Map<string, T[]>()

  for (const row of rows) {
    const key = partitionOf(row)
    const group = groups.get(key)
    if (group) {
      group.push(row)
    } else {
      groups.set(key, [row])
    }
  }

  return [...groups.values()]
}

export class GooglePubsubPublisher implements Publisher {
  readonly #client: PubSub
  readonly #options: PublisherOptions
  readonly #topics = new Map<string, Topic>()

  constructor(client: PubSub, options: PublisherOptions) {
    this.#client = client
    this.#options = options
  }

  async setup(topics: string[], logger: Logger): Promise<void> {
    if (this.#options.topicSetup === 'none') return

    for (const name of topics) {
      const topic = this.#topic(name)
      const [exists] = await topic.exists()
      if (exists) continue

      if (this.#options.topicSetup === 'validate') {
        throw new PubsubTargetError(PUBSUB_ERROR_CODES.TOPIC_NOT_FOUND, [
          `Topic "${name}" does not exist in project "${this.#client.projectId}".`,
          'Create it first, or set `topicSetup: "create"` (dev convenience — it needs admin IAM). ' +
            '`topicSetup: "none"` skips the check entirely for least-privilege deployments.',
        ])
      }

      await topic.create()
      logger.info(`created topic "${name}"`)
    }
  }

  #topic(name: string): Topic {
    const existing = this.#topics.get(name)
    if (existing) return existing

    const topic = this.#client.topic(name, {
      batching: this.#options.batching,
      flowControlOptions: this.#options.flowControl,
      messageOrdering: this.#options.delivery === 'ordered',
    })
    this.#topics.set(name, topic)

    return topic
  }

  async drain(rows: (OutboxRow & { attributes: Record<string, string> })[]): Promise<DrainResult> {
    const confirmed: number[] = []
    let bytes = 0
    let error: unknown

    const partitions = partitionRows(rows)

    const results = await Promise.all(
      partitions.map(async (partition) => {
        const done: number[] = []
        let published = 0

        // Fire the whole partition first so the client can batch it, then settle in order:
        // under `ordered` a failed publish fails everything behind it on the key anyway, so
        // the confirmed prefix is exactly the set that is safe to drop from the outbox.
        const inflight = partition.map((row) =>
          this.#topic(row.topic).publishMessage({
            data: Buffer.from(row.payload.buffer, row.payload.byteOffset, row.payload.byteLength),
            attributes: row.attributes,
            orderingKey: row.orderingKey || undefined,
          }),
        )

        for (let i = 0; i < inflight.length; i++) {
          try {
            await inflight[i]
          } catch (e) {
            // Ordered publishing latches the key on error: without this every later publish
            // on that partition rejects immediately, including our own republish.
            if (this.#options.delivery === 'ordered' && partition[i].orderingKey) {
              this.#topic(partition[i].topic).resumePublishing(partition[i].orderingKey)
            }

            return { done, published, error: e }
          }

          done.push(partition[i].rowId)
          published += partition[i].payload.byteLength
        }

        return { done, published, error: undefined as unknown }
      }),
    )

    for (const result of results) {
      confirmed.push(...result.done)
      bytes += result.published
      error ??= result.error
    }

    return { confirmed, bytes, error }
  }

  async close(): Promise<void> {
    for (const topic of this.#topics.values()) {
      await topic.flush().catch(() => undefined)
    }
    this.#topics.clear()
    await this.#client.close()
  }
}

/** Accepts a constructed client or the config to build one, so tests and emulators both work. */
export async function resolvePubsubClient(pubsub: PubSub | ClientConfig): Promise<PubSub> {
  if (typeof (pubsub as PubSub).topic === 'function') return pubsub as PubSub

  const { PubSub: Client } = await import('@google-cloud/pubsub')

  return new Client(pubsub as ClientConfig)
}
