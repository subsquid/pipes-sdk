import { describe, expect, it } from 'vitest'

import { evmQuery } from '~/evm/evm-query-builder.js'
import { evmPortalStream } from '~/evm/index.js'
import { mockMetricsServer, readAll } from '~/testing/index.js'
import { withPortal } from '~/testing/portal-fixture.js'
import { StubPortalClient, stubBatch } from '~/testing/stub-portal.js'

function blockOutputs(range: { from: number; to?: number }) {
  return evmQuery()
    .addFields({ block: { number: true, hash: true, timestamp: true } })
    .addRange(range)
}

describe('Pipeline metrics', () => {
  withPortal('should register all expected progress-tracker metrics', async ({ portal: serve }) => {
    const portal = await serve([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0x1', timestamp: 1000 } },
          { header: { number: 2, hash: '0x2', timestamp: 2000 } },
          { header: { number: 3, hash: '0x3', timestamp: 3000 } },
        ],
      },
    ])

    const metrics = mockMetricsServer()

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockOutputs({ from: 0, to: 3 }),
      metrics: metrics.server,
    })

    for await (const _batch of stream) {
      // consume
    }

    expect(metrics.gauge('sqd_processed_block')).toBeDefined()
    expect(metrics.gauge('sqd_end_block')).toBeDefined()
    expect(metrics.gauge('sqd_progress_ratio')).toBeDefined()
    expect(metrics.gauge('sqd_eta_seconds')).toBeDefined()
    expect(metrics.counter('sqd_blocks_processed_total')).toBeDefined()
    expect(metrics.counter('sqd_bytes_downloaded_total')).toBeDefined()
  })

  withPortal('should register all expected portal-source metrics', async ({ portal: serve }) => {
    const portal = await serve([
      {
        statusCode: 200,
        data: [{ header: { number: 1, hash: '0x1', timestamp: 1000 } }],
      },
    ])

    const metrics = mockMetricsServer()

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockOutputs({ from: 0, to: 1 }),
      metrics: metrics.server,
    })

    for await (const _batch of stream) {
      // consume
    }

    expect(metrics.counter('sqd_forks_total')).toBeDefined()
    expect(metrics.counter('sqd_portal_requests_total')).toBeDefined()
    expect(metrics.histogram('sqd_batch_size_blocks')).toBeDefined()
    expect(metrics.histogram('sqd_batch_size_bytes')).toBeDefined()
  })

  withPortal('should track portal request counts with classification and status labels', async ({ portal: serve }) => {
    const portal = await serve([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0x1', timestamp: 1000 } },
          { header: { number: 2, hash: '0x2', timestamp: 2000 } },
        ],
      },
    ])

    const metrics = mockMetricsServer()

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockOutputs({ from: 0, to: 2 }),
      metrics: metrics.server,
    })

    for await (const _batch of stream) {
      // consume
    }

    const requests = metrics.counter('sqd_portal_requests_total')
    expect(requests).toBeDefined()
    expect(requests.total).toBeGreaterThan(0)

    // All requests should have classification=success and status=200
    for (const call of requests.calls) {
      expect(call.labels?.['classification']).toBe('success')
      expect(call.labels?.['status']).toBe('200')
    }
  })

  withPortal('should update progress-tracker metrics on batch processing', async ({ portal: serve }) => {
    const portal = await serve([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0x1', timestamp: 1000 } },
          { header: { number: 2, hash: '0x2', timestamp: 2000 } },
          { header: { number: 3, hash: '0x3', timestamp: 3000 } },
        ],
      },
    ])

    const metrics = mockMetricsServer()

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockOutputs({ from: 0, to: 3 }),
      metrics: metrics.server,
    })

    for await (const _batch of stream) {
      // consume
    }

    expect(metrics.gauge('sqd_processed_block').lastValue).toBe(3)
    expect(metrics.gauge('sqd_end_block').lastValue).toBe(3)
    expect(metrics.gauge('sqd_progress_ratio').lastValue).toBe(1)
    expect(metrics.gauge('sqd_eta_seconds').lastValue).toBe(0)
    expect(metrics.counter('sqd_blocks_processed_total').total).toBeGreaterThanOrEqual(0)
    expect(metrics.counter('sqd_bytes_downloaded_total').total).toBeGreaterThan(0)
  })

  withPortal('should observe batch size metrics', async ({ portal: serve }) => {
    const portal = await serve([
      {
        statusCode: 200,
        data: [
          { header: { number: 1, hash: '0x1', timestamp: 1000 } },
          { header: { number: 2, hash: '0x2', timestamp: 2000 } },
          { header: { number: 3, hash: '0x3', timestamp: 3000 } },
        ],
      },
    ])

    const metrics = mockMetricsServer()

    const stream = evmPortalStream({
      id: 'test',
      portal: portal.url,
      outputs: blockOutputs({ from: 0, to: 3 }),
      metrics: metrics.server,
    })

    for await (const _batch of stream) {
      // consume
    }

    const batchBlocks = metrics.histogram('sqd_batch_size_blocks')
    expect(batchBlocks.observations.length).toBeGreaterThanOrEqual(1)
    const totalBlocks = batchBlocks.observations.reduce((sum, n) => sum + n, 0)
    expect(totalBlocks).toBe(3)

    const batchBytes = metrics.histogram('sqd_batch_size_bytes')
    expect(batchBytes.observations.length).toBe(batchBlocks.observations.length)
    for (const obs of batchBytes.observations) {
      expect(obs).toBeGreaterThan(0)
    }
  })

  // A 204 carries no head headers, and with no finality and no range end nothing else bounds the run.
  it('counts a head-only batch as a request, and keeps the run end at the cursor (WP-13)', async () => {
    const portal = new StubPortalClient(async function* () {
      yield stubBatch([1], { latest: { number: 1 } }, { 200: 1 })
      yield stubBatch([], {}, { 204: 1 })
    })
    const metrics = mockMetricsServer()

    await readAll(evmPortalStream({ id: 'test', portal, outputs: blockOutputs({ from: 1 }), metrics: metrics.server }))

    const statuses = metrics.counter('sqd_portal_requests_total').calls.map((call) => call.labels?.['status'])
    expect(statuses).toEqual(['200', '204'])
    expect(metrics.histogram('sqd_batch_size_blocks').observations).toEqual([1])
    expect(metrics.gauge('sqd_end_block').lastValue).toBe(1)
  })

  it('counts a head-only batch that arrives before the first block (WP-13)', async () => {
    const portal = new StubPortalClient(async function* () {
      yield stubBatch([], {}, { 204: 1 })
      yield stubBatch([1], { latest: { number: 1 } }, { 200: 1 })
      yield stubBatch([], {}, { 204: 1 })
    })
    const metrics = mockMetricsServer()

    await readAll(evmPortalStream({ id: 'test', portal, outputs: blockOutputs({ from: 1 }), metrics: metrics.server }))

    const statuses = metrics.counter('sqd_portal_requests_total').calls.map((call) => call.labels?.['status'])
    expect(statuses).toEqual(['204', '200', '204'])
    expect(metrics.gauge('sqd_processed_block').lastValue).toBe(1)
  })
})
