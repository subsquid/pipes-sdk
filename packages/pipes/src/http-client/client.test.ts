import { describe, expect } from 'vitest'

import { withPortal } from '~/testing/portal-fixture.js'

import { HttpBodyTimeoutError, HttpClient } from './client.js'

async function drain(body: ReadableStream<Uint8Array>) {
  const reader = body.getReader()

  let chunk = await reader.read()
  while (!chunk.done) {
    chunk = await reader.read()
  }
}

describe('HttpClient body timeout', () => {
  withPortal('applies a body timeout that a function picks from the response', async ({ portal: open }) => {
    const portal = await open({ latest: 5 })
    portal.fail({ stallAfter: 0 })

    const res = await new HttpClient().request(`${portal.url}/stream`, {
      method: 'POST',
      json: { type: 'evm', fromBlock: 1 },
      stream: true,
      bodyTimeout: (res) => (res.headers.get('content-type') === 'application/jsonl' ? 100 : undefined),
    })

    await expect(drain(res.body)).rejects.toThrow(new HttpBodyTimeoutError(100))
  })
})
