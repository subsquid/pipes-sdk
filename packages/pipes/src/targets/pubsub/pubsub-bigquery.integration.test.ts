import { BigQuery, type Table } from '@google-cloud/bigquery'
import { PubSub, type Subscription, type Topic } from '@google-cloud/pubsub'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { encodeCdcMessage, encodeRow } from './protocol.js'

const PROJECT = process.env['PUBSUB_BIGQUERY_TEST_PROJECT']
const DATASET = process.env['PUBSUB_BIGQUERY_TEST_DATASET'] ?? 'pipes_pubsub_test'
const IS_ENABLED = Boolean(PROJECT)
const projectId = PROJECT as string

const POLL_INITIAL_DELAY_MS = 1_000
const POLL_MAX_DELAY_MS = 10_000
const DELIVERY_TIMEOUT_MS = 180_000

type LandedRow = {
  _id: string
  amount: string
  details: string
  ratio: number
  timestamp: string
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitForRow(options: {
  bigquery: BigQuery
  subscription: Subscription
  tableFqn: string
  id: string
}): Promise<LandedRow> {
  const deadline = Date.now() + DELIVERY_TIMEOUT_MS
  let pollDelay = POLL_INITIAL_DELAY_MS
  let lastError: unknown

  while (Date.now() < deadline) {
    try {
      const [rows] = await options.bigquery.query({
        query: `
          SELECT
            _id,
            amount,
            TO_JSON_STRING(details) AS details,
            ratio,
            FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', timestamp, 'UTC') AS timestamp
          FROM \`${options.tableFqn}\`
          WHERE _id = @id
        `,
        params: { id: options.id },
      })
      const row = rows[0] as LandedRow | undefined
      if (row) return row
    } catch (error) {
      lastError = error
    }

    await delay(pollDelay)
    pollDelay = Math.min(pollDelay * 2, POLL_MAX_DELAY_MS)
  }

  let subscriptionState = 'unknown'
  try {
    const [metadata] = await options.subscription.getMetadata()
    subscriptionState = String(metadata.bigqueryConfig?.state ?? subscriptionState)
  } catch (error) {
    lastError = lastError ?? error
  }

  throw new Error(
    `Pub/Sub did not write row ${options.id} to ${options.tableFqn} within ${DELIVERY_TIMEOUT_MS}ms ` +
      `(subscription state: ${subscriptionState})`,
    { cause: lastError },
  )
}

/**
 * Live contract test for the boundary that the unit codec tests cannot cover.
 *
 * The project must have the Pub/Sub and BigQuery APIs enabled, application-default credentials
 * with permission to create the test resources. Pre-create the test dataset and grant its Pub/Sub
 * service agent BigQuery Data Editor on that dataset, or grant the role at project level.
 *
 * Run from packages/pipes:
 *   PUBSUB_BIGQUERY_TEST_PROJECT=my-gcp-project \
 *   PUBSUB_BIGQUERY_TEST_DATASET=pipes_pubsub_test \
 *   pnpm vitest run src/targets/pubsub/pubsub-bigquery.integration.test.ts
 */
describe.skipIf(!IS_ENABLED)('Pub/Sub canonical CDC -> BigQuery subscription (integration)', () => {
  const suffix = `${Date.now()}_${process.pid}`
  const topicName = `e2e-pubsub-bq-${suffix}`
  const subscriptionName = `e2e-pubsub-bq-${suffix}`
  const tableName = `e2e_pubsub_bq_${suffix}`
  const tableFqn = `${projectId}.${DATASET}.${tableName}`

  let bigquery: BigQuery | undefined
  let pubsub: PubSub | undefined
  let table: Table | undefined
  let topic: Topic | undefined
  let subscription: Subscription | undefined

  beforeAll(async () => {
    bigquery = new BigQuery({ projectId })
    pubsub = new PubSub({ projectId })

    const dataset = bigquery.dataset(DATASET)
    const [datasetExists] = await dataset.exists()
    if (!datasetExists) await bigquery.createDataset(DATASET)

    table = dataset.table(tableName)
    await bigquery.query({
      query: `
        CREATE TABLE \`${tableFqn}\` (
          _id STRING NOT NULL PRIMARY KEY NOT ENFORCED,
          amount STRING,
          details JSON,
          ratio FLOAT64,
          timestamp TIMESTAMP
        )
      `,
    })

    const [createdTopic] = await pubsub.createTopic(topicName)
    topic = createdTopic

    const [createdSubscription] = await topic.createSubscription(subscriptionName, {
      bigqueryConfig: {
        table: `projects/${projectId}/datasets/${DATASET}/tables/${tableName}`,
        useTableSchema: true,
        dropUnknownFields: true,
      },
    })
    subscription = createdSubscription
  }, 60_000)

  afterAll(async () => {
    const cleanupErrors: unknown[] = []
    const cleanups: (() => Promise<unknown>)[] = []

    if (subscription) cleanups.push(() => subscription?.delete() ?? Promise.resolve())
    if (topic) cleanups.push(() => topic?.delete() ?? Promise.resolve())
    if (table) cleanups.push(() => table?.delete({ ignoreNotFound: true }) ?? Promise.resolve())
    if (pubsub) cleanups.push(() => pubsub?.close() ?? Promise.resolve())

    for (const cleanup of cleanups) {
      try {
        await cleanup()
      } catch (error) {
        cleanupErrors.push(error)
      }
    }

    if (cleanupErrors.length > 0) {
      throw new AggregateError(cleanupErrors, 'Failed to clean up Pub/Sub -> BigQuery integration resources')
    }
  }, 60_000)

  it(
    'lands RFC 3339 dates, full-range uint256 strings, JSON strings, and fractional numbers',
    async () => {
      if (!bigquery || !topic || !subscription) throw new Error('integration resources were not initialized')

      const id = 'codec-contract-row'
      const timestamp = new Date('2023-11-14T22:13:20.999Z')
      const amount = 2n ** 256n - 1n
      const payload = encodeRow({
        amount,
        details: JSON.stringify({ key: 'value' }),
        ratio: 0.5,
        timestamp,
      })

      await topic.publishMessage({
        data: encodeCdcMessage({ op: 'upsert', id, seq: 1, payload }),
      })

      await expect(waitForRow({ bigquery, subscription, tableFqn, id })).resolves.toEqual({
        _id: id,
        amount: amount.toString(),
        details: '{"key":"value"}',
        ratio: 0.5,
        timestamp: timestamp.toISOString(),
      })
    },
    DELIVERY_TIMEOUT_MS + 10_000,
  )
})
