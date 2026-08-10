import type { PubSub, Topic } from '@google-cloud/pubsub'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { testLogger } from '~/testing/index.js'

import { GooglePubsubPublisher, partitionRows } from './publisher.js'
import type { OutboxRow } from './pubsub-state.js'

type WireRow = OutboxRow & { attributes: Record<string, string> }

function row(overrides: Partial<WireRow> = {}): WireRow {
  return {
    rowId: 1,
    topic: 'blocks',
    op: 'upsert',
    id: 'row-1',
    orderingKey: '',
    seq: 1,
    attributes: {},
    payload: new TextEncoder().encode('{}'),
    ...overrides,
  }
}

/** A client whose publishes resolve or reject per row id, with no network anywhere near it. */
function fakeClient(outcome: (id: string) => Promise<string>) {
  const resumed: string[] = []
  const topics = new Map<string, Topic>()

  const client = {
    projectId: 'test',
    topic(name: string) {
      const existing = topics.get(name)
      if (existing) return existing

      const topic = {
        publishMessage: ({ attributes }: { attributes: Record<string, string> }) => outcome(attributes['_id']),
        resumePublishing: (key: string) => resumed.push(key),
        flush: async () => undefined,
        exists: async () => [true],
      } as unknown as Topic
      topics.set(name, topic)

      return topic
    },
    close: async () => undefined,
  } as unknown as PubSub

  return { client, resumed }
}

const unhandled: unknown[] = []
const onUnhandled = (reason: unknown) => unhandled.push(reason)

afterEach(() => {
  process.off('unhandledRejection', onUnhandled)
  unhandled.length = 0
  vi.restoreAllMocks()
})

describe('partitionRows', () => {
  it('keeps identities apart that a separator-joined key would merge', () => {
    const partitions = partitionRows([
      { topic: 'a', orderingKey: 'b c' },
      { topic: 'a b', orderingKey: 'c' },
    ])

    expect(partitions).toHaveLength(2)
  })
})

describe('GooglePubsubPublisher', () => {
  it('confirms only the prefix that published, so a gap is republished not dropped', async () => {
    const { client } = fakeClient(async (id) => {
      if (id === 'b') throw new Error('publish failed')

      return 'message-id'
    })
    const publisher = new GooglePubsubPublisher(client, { delivery: 'lww', topicSetup: 'none' })

    const result = await publisher.drain([
      row({ rowId: 1, id: 'a', attributes: { _id: 'a' } }),
      row({ rowId: 2, id: 'b', attributes: { _id: 'b' } }),
      row({ rowId: 3, id: 'c', attributes: { _id: 'c' } }),
    ])

    expect(result.confirmed).toEqual([1])
    expect(result.error).toMatchObject({ message: 'publish failed' })
  })

  it('observes every publish, so a second failure is not an unhandled rejection', async () => {
    process.on('unhandledRejection', onUnhandled)

    const { client } = fakeClient(async (id) => {
      throw new Error(`publish failed: ${id}`)
    })
    const publisher = new GooglePubsubPublisher(client, { delivery: 'lww', topicSetup: 'none' })

    const result = await publisher.drain([
      row({ rowId: 1, id: 'a', attributes: { _id: 'a' } }),
      row({ rowId: 2, id: 'b', attributes: { _id: 'b' } }),
    ])

    expect(result.confirmed).toEqual([])
    expect(result.error).toMatchObject({ message: 'publish failed: a' })

    // The rejection of the second publish is reported through the drain result, not left to
    // surface later as a process-level unhandled rejection.
    await new Promise((resolve) => setImmediate(resolve))
    expect(unhandled).toEqual([])
  })

  it('unlatches an ordered partition so the republish can proceed', async () => {
    const { client, resumed } = fakeClient(async () => {
      throw new Error('publish failed')
    })
    const publisher = new GooglePubsubPublisher(client, { delivery: 'ordered', topicSetup: 'none' })

    await publisher.drain([row({ orderingKey: 'blocks', attributes: { _id: 'a' } })])

    expect(resumed).toEqual(['blocks'])
  })

  it('publishes partitions independently — one failure does not block another key', async () => {
    const { client } = fakeClient(async (id) => {
      if (id === 'a') throw new Error('publish failed')

      return 'message-id'
    })
    const publisher = new GooglePubsubPublisher(client, { delivery: 'ordered', topicSetup: 'none' })

    const result = await publisher.drain([
      row({ rowId: 1, orderingKey: 'pool-a', attributes: { _id: 'a' } }),
      row({ rowId: 2, orderingKey: 'pool-b', attributes: { _id: 'b' } }),
    ])

    expect(result.confirmed).toEqual([2])
  })

  it('refuses a missing topic under the default setup', async () => {
    const client = {
      projectId: 'test',
      topic: () => ({ exists: async () => [false] }) as unknown as Topic,
      close: async () => undefined,
    } as unknown as PubSub

    const publisher = new GooglePubsubPublisher(client, { delivery: 'lww', topicSetup: 'validate' })

    await expect(publisher.setup(['missing'], testLogger())).rejects.toMatchObject({ code: 'E2401' })
  })
})
