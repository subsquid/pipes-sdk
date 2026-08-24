import { contractFactorySqliteStore, evmStream } from '@subsquid/pipes/evm'

import { erc20Transfers, uniswapV3, uniswapV3Decoder } from './decoders'

/**
 * This example demonstrates how to combine multiple data processing pipes
 * to handle both ERC20 transfers and Uniswap V3 swaps simultaneously.
 * It uses Portal API to stream data from Base Mainnet and processes
 * the data in batches while tracking statistics for each type of transaction.
 */
async function cli() {
  // Define block range for data processing
  const range = { from: '20,000,000', to: '+1,000' }

  // Create a combined stream that processes both ERC20 transfers and Uniswap V3 swaps
  // from Base Mainnet using Portal API
  const stream = evmStream({
    id: 'combining-pipes',
    source: 'https://portal.sqd.dev/datasets/base-mainnet',
    outputs: {
      transfers: erc20Transfers({
        range: { from: '20,000,000', to: '+1,000' },
      }),
      uniswapV3: uniswapV3Decoder({
        range,
        factory: {
          address: uniswapV3.base.mainnet.factory,
          database: contractFactorySqliteStore({ path: './uniswap-v3-pools.sqlite' }),
        },
      }),
    },
  })

  // Process streamed data and log statistics for each batch
  // - data.transfers contains processed ERC20 transfers
  // - data.uniswapV3.swaps contains processed Uniswap V3 swaps
  for await (const { data } of stream) {
    console.log('-------------------------------------')
    console.log(`parsed ${data.transfers.length} transfers`)
    console.log(`parsed ${data.uniswapV3.swaps.length} swaps`)
    console.log('-------------------------------------')
  }
}

void cli()
