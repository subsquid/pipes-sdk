import type { ClickHouseClient } from '@clickhouse/client'
import { describe, expect, vi } from 'vitest'

import { evmPortalStream } from '~/evm/index.js'
import { mockBlock } from '~/testing/evm/index.js'
import { blockDecoder } from '~/testing/index.js'
import { withPortal as it } from '~/testing/portal-fixture.js'

import { clickhouseTarget } from './clickouse-target.js'

/** No server: answers every statement with no rows. */
function fakeClient() {
  return {
    close: vi.fn(async () => {}),
    command: vi.fn(async () => ({})),
    insert: vi.fn(async () => ({})),
    query: vi.fn(async () => ({ json: async () => [], stream: async function* () {} })),
  }
}

describe('clickhouseTarget — client ownership', () => {
  it('leaves the client of the caller open when the range ends', async ({ portal }) => {
    const chain = await portal({ blocks: [mockBlock({ number: 1 })] })
    const client = fakeClient()

    await evmPortalStream({
      id: 'ownership',
      portal: chain.url,
      logger: 'silent',
      outputs: blockDecoder({ from: 1, to: 1 }),
    }).pipeTo(
      clickhouseTarget({
        client: client as unknown as ClickHouseClient,
        onData: () => {},
        onRollback: () => {},
      }),
    )

    expect(client.close).not.toHaveBeenCalled()
  })
})
