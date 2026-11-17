import { describe, expect } from 'vitest'

import { BlockValidationError } from '~/core/errors.js'
import { readAll } from '~/testing/index.js'
import { withPortal } from '~/testing/portal-fixture.js'

import {
  HyperliquidReplicaCmdsOutputs,
  hyperliquidReplicaCmdsPortalStream,
} from './hyperliquid-replica-cmds-portal-source.js'
import { hyperliquidReplicaCmdsQuery } from './hyperliquid-replica-cmds-query-builder.js'

const ZERO_HASH = `0x${'0'.repeat(64)}`
const USER = '0x6978e7917dffd1f1815c858ca1528e51a3af1cb5'
const CLOID = '0x9803ba672ed600000000000007639425'
const SIGNATURE = { r: '0x01', s: '0x02', v: 27 }

function header(number: number) {
  return {
    number,
    hash: ZERO_HASH,
    parentHash: ZERO_HASH,
    round: 1529432966 + number,
    parentRound: 1529432965 + number,
    proposer: '0x9dd85fb6cf95a9d755af0853b36fb05e972c71e1',
    timestamp: 1794861532843,
    hardfork: { version: 113, round: 1526234267 },
  }
}

const orderAction = {
  actionIndex: 570,
  user: USER,
  action: {
    type: 'order',
    orders: [{ a: 223, b: true, p: '2.6816', s: '5', r: false, t: { limit: { tif: 'Alo' } }, c: CLOID }],
    grouping: 'na',
  },
  signature: SIGNATURE,
  nonce: 1794861532573,
  vaultAddress: USER,
  status: 'ok',
  response: { type: 'order', data: { statuses: [{ resting: { oid: 607436620785, cloid: CLOID } }] } },
}

const failedAction = {
  actionIndex: 3,
  user: USER,
  action: { type: 'updateLeverage', asset: 86, isCross: true, leverage: 6 },
  signature: SIGNATURE,
  nonce: 1794861532600,
  vaultAddress: null,
  status: 'err',
  response: 'Invalid leverage value',
}

const allActionFields = {
  actionIndex: true,
  user: true,
  action: true,
  signature: true,
  nonce: true,
  vaultAddress: true,
  status: true,
  response: true,
} as const

function streamOf<Out extends HyperliquidReplicaCmdsOutputs>(portal: { url: string }, outputs: Out) {
  return hyperliquidReplicaCmdsPortalStream({ id: 'test', logger: 'silent', portal: portal.url, outputs })
}

describe('hyperliquidReplicaCmdsPortalStream', () => {
  withPortal('decodes the selected action and header fields', async ({ portal: open }) => {
    const portal = await open({
      blocks: [
        { header: header(1), actions: [orderAction] },
        { header: header(2), actions: [failedAction] },
      ],
    })

    const stream = streamOf(
      portal,
      hyperliquidReplicaCmdsQuery()
        .addFields({ block: { number: true, hash: true, round: true }, action: allActionFields })
        .addActionRequest({ range: { from: 1, to: 2 }, request: {} }),
    )

    const blocks = await readAll(stream)

    expect(blocks.map((block) => block.header.number)).toEqual([1, 2])
    expect(blocks[0].header).toEqual({ number: 1, hash: ZERO_HASH, round: 1529432967 })
    expect(blocks[0].actions).toEqual([orderAction])
    expect(blocks[1].actions).toEqual([failedAction])
  })

  withPortal('decodes a null vaultAddress and a string response', async ({ portal: open }) => {
    const portal = await open({ blocks: [{ header: header(1), actions: [failedAction] }] })

    const stream = streamOf(
      portal,
      hyperliquidReplicaCmdsQuery()
        .addFields({ action: { vaultAddress: true, status: true, response: true } })
        .addActionRequest({ range: { from: 1, to: 1 }, request: {} }),
    )

    const [block] = await readAll(stream)

    expect(block.actions).toEqual([{ vaultAddress: null, status: 'err', response: 'Invalid leverage value' }])
  })

  withPortal('decodes a null user on a rejected command', async ({ portal: open }) => {
    const rejectedNoop = {
      actionIndex: 0,
      user: null,
      action: { type: 'noop' },
      status: 'err',
      response: 'User or API Wallet 0x0000000000000000000000000000000000000001 does not exist.',
    }
    const portal = await open({ blocks: [{ header: header(1), actions: [rejectedNoop] }] })

    const stream = streamOf(
      portal,
      hyperliquidReplicaCmdsQuery()
        .addFields({ action: { actionIndex: true, user: true, action: true, status: true, response: true } })
        .addActionRequest({ range: { from: 1, to: 1 }, request: {} }),
    )

    const [block] = await readAll(stream)

    expect(block.actions).toEqual([rejectedNoop])
  })

  withPortal('leaves out fields that were not selected', async ({ portal: open }) => {
    const portal = await open({ blocks: [{ header: header(1), actions: [orderAction] }] })

    const stream = streamOf(
      portal,
      hyperliquidReplicaCmdsQuery()
        .addFields({ block: { round: true }, action: { actionIndex: true, user: true } })
        .addActionRequest({ range: { from: 1, to: 1 }, request: {} }),
    )

    const [block] = await readAll(stream)

    expect(Object.keys(block.header).sort()).toEqual(['hash', 'number', 'round'])
    expect(block.actions).toEqual([{ actionIndex: 570, user: USER }])
    expect(block.actions[0]).not.toHaveProperty('response')
  })

  withPortal('decodes a block without actions as an empty list', async ({ portal: open }) => {
    const portal = await open({ blocks: [{ header: header(1) }] })

    const stream = streamOf(
      portal,
      hyperliquidReplicaCmdsQuery()
        .addFields({ action: { actionIndex: true } })
        .addActionRequest({ range: { from: 1, to: 1 }, request: {} }),
    )

    const [block] = await readAll(stream)

    expect(block.actions).toEqual([])
  })

  withPortal('fails on a status the schema does not allow', async ({ portal: open }) => {
    const portal = await open({ blocks: [{ header: header(1), actions: [{ ...orderAction, status: 'pending' }] }] })

    const stream = streamOf(
      portal,
      hyperliquidReplicaCmdsQuery()
        .addFields({ action: { status: true } })
        .addActionRequest({ range: { from: 1, to: 1 }, request: {} }),
    )

    await expect(readAll(stream)).rejects.toThrow(BlockValidationError)
  })

  withPortal('sends the replica commands query type and the requested family', async ({ portal: open }) => {
    const portal = await open({ blocks: [{ header: header(1), actions: [orderAction] }] })

    const stream = streamOf(
      portal,
      hyperliquidReplicaCmdsQuery()
        .addFields({ action: { actionIndex: true } })
        .addOrderActionRequest({ range: { from: 1, to: 1 }, request: { containsAsset: [223], status: 'ok' } }),
    )

    await readAll(stream)

    const [sent] = portal.requests

    expect(sent.query.type).toBe('hyperliquidReplicaCmds')
    expect(sent.query['orderActions']).toEqual([{ containsAsset: [223], status: 'ok' }])
    expect(sent.query).not.toHaveProperty('actions')
  })
})
