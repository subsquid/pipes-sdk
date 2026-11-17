import { hyperliquidReplicaCmdsPortalStream, hyperliquidReplicaCmdsQuery } from '@subsquid/pipes/hyperliquid'

/**
 * Basic example demonstrating how to fetch the HyperCore commands a specific user sent.
 *
 * The dataset holds about a thousand commands per block, so the example filters by user
 * and reads a bounded range.
 */
async function cli() {
  const stream = hyperliquidReplicaCmdsPortalStream({
    id: 'user-actions',
    portal: process.env['PORTAL_URL'] || 'https://portal.sqd.dev/datasets/hyperliquid-replica-cmds',
    outputs: hyperliquidReplicaCmdsQuery()
      .addFields({
        block: {
          number: true,
          timestamp: true,
        },
        action: {
          actionIndex: true,
          user: true,
          action: true,
          nonce: true,
          vaultAddress: true,
          status: true,
          response: true,
        },
      })
      .addActionRequest({
        request: {
          user: ['0x6978e7917dffd1f1815c858ca1528e51a3af1cb5'],
        },
        range: {
          from: 1_223_800_000,
          to: 1_223_802_000,
        },
      }),
  })

  for await (const { data } of stream) {
    for (const block of data) {
      for (const action of block.actions) {
        console.log(block.header.number, action.action.type, action.status)
      }
    }
  }
}

void cli()
