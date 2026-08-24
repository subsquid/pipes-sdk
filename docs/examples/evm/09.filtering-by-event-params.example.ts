import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'

/**
 * This example demonstrates how to filter EVM events by indexed parameters to reduce data transfer and processing.
 * It shows how to:
 * - Create a data stream from Ethereum mainnet using Portal API
 * - Decode ERC20 transfer and approval events
 * - Filter events by indexed parameters using the `params` property
 */
async function cli() {
  const stream = evmStream({
    id: 'event-params-filter',
    source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs: evmEventDecoder({
      range: { from: '24171448', to: '24171449' },
      events: {
        // Use the AbiEvent instance directly for convenience if you need all the emitted events
        approvals: commonAbis.erc20.events.Approval,
        // Or filter by any of the indexed parameters defined in the contract
        transfers: {
          event: commonAbis.erc20.events.Transfer,
          params: {
            // For every event param you can use an array to match multiple values
            from: ['0x87482e84503639466fad82d1dce97f800a410945'],
            // Or pass a single value directly
            to: '0x10b32a54eeb05d2c9cd1423b4ad90c3671a2ed5f',
          },
        },
      },
    }),
  })

  for await (const { data } of stream) {
    console.log({
      app: data.approvals.length,
      tx: data.transfers.length,
    })
  }
}

void cli()
