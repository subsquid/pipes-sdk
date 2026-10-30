import { fileURLToPath } from 'node:url'

import * as grpc from '@grpc/grpc-js'
import { loadSync } from '@grpc/proto-loader'

const PROTO = fileURLToPath(new URL('./bus.proto', import.meta.url))

/** A message as the bus took it: what a subscriber would receive. */
export type BusMessage = {
  /** The topic's short name, without `projects/<project>/topics/`. */
  topic: string
  data: Buffer
  attributes: Record<string, string>
  orderingKey: string
}

/**
 * A Pub/Sub stand-in in the test process: it answers the Publisher calls of the Google client and
 * keeps every message it acknowledged, in the order it took them. A killed child cannot take back
 * what the bus holds, so it plays the medium of class C (CN-17): published is published.
 */
export type Bus = {
  /** `host:port`, for the client's `apiEndpoint`. */
  endpoint: string
  projectId: string
  /** Every message the bus acknowledged, in order. The array grows as the bus takes more. */
  messages: readonly BusMessage[]
  close(): Promise<void>
}

type PublishRequest = {
  topic: string
  messages: { data: Buffer; attributes: Record<string, string>; orderingKey: string }[]
}

/** Starts a bus on a free local port that knows the `topics` of `projectId` and no other. */
export async function startBus({ projectId, topics }: { projectId: string; topics: string[] }): Promise<Bus> {
  const definition = loadSync(PROTO, { defaults: true })
  const loaded = grpc.loadPackageDefinition(definition) as unknown as {
    google: { pubsub: { v1: { Publisher: grpc.ServiceClientConstructor } } }
  }

  const prefix = `projects/${projectId}/topics/`
  const known = new Set(topics.map((topic) => `${prefix}${topic}`))
  const messages: BusMessage[] = []

  const unknownTopic = (name: string): grpc.ServiceError =>
    Object.assign(new Error(`the bus has no topic '${name}'`), {
      code: grpc.status.NOT_FOUND,
      details: `the bus has no topic '${name}'`,
      metadata: new grpc.Metadata(),
    })

  const server = new grpc.Server()
  server.addService(loaded.google.pubsub.v1.Publisher.service, {
    GetTopic: (call: grpc.ServerUnaryCall<{ topic: string }, unknown>, done: grpc.sendUnaryData<unknown>) => {
      const name = call.request.topic
      if (!known.has(name)) {
        done(unknownTopic(name))

        return
      }

      done(null, { name })
    },
    Publish: (call: grpc.ServerUnaryCall<PublishRequest, unknown>, done: grpc.sendUnaryData<unknown>) => {
      const { topic, messages: published } = call.request
      if (!known.has(topic)) {
        done(unknownTopic(topic))

        return
      }

      const messageIds: string[] = []
      for (const message of published) {
        messages.push({
          topic: topic.slice(prefix.length),
          data: Buffer.from(message.data),
          attributes: { ...message.attributes },
          orderingKey: message.orderingKey,
        })
        messageIds.push(String(messages.length))
      }

      done(null, { messageIds })
    },
  })

  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync('127.0.0.1:0', grpc.ServerCredentials.createInsecure(), (error, bound) => {
      if (error) {
        reject(error)
      } else {
        resolve(bound)
      }
    })
  })

  return {
    endpoint: `127.0.0.1:${port}`,
    projectId,
    messages,
    close: async () => {
      // A killed child's channel never says goodbye; a graceful shutdown would wait for it.
      server.forceShutdown()
    },
  }
}
