import { HyperliquidFillsQueryBuilder, hyperliquidFillsPortalStream } from '@subsquid/pipes/hyperliquid'

/**
 * Basic example demonstrating how to fetch hyperliquid fills for a specific user.
 */
async function cli() {
  const stream = hyperliquidFillsPortalStream({
    id: 'user-fills',
    source: process.env['PORTAL_URL'] || 'https://portal.sqd.dev/datasets/hl-node-fills',
    outputs: new HyperliquidFillsQueryBuilder()
      .addFields({
        block: {
          number: true,
          timestamp: true,
        },
        // To get more information about fills abbreviations please check the docs:
        // https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/notation
        fill: {
          tid: true,
          fillIndex: true,
          user: true,
          px: true,
          sz: true,
          side: true,
          coin: true,
          time: true,
          closedPnl: true,
        },
      })
      .addFillRequest({
        request: {},
        range: {
          // Earlier blocks aren't supported yet
          from: 750_000_000,
        },
      }),
  })

  for await (const { data } of stream) {
    for (const block of data) {
      if (block.fills.length === 0) continue

      console.log(block.fills)
    }
  }
}

void cli()
