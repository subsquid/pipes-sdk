import assert from 'assert'

import { contractFactory, contractFactorySqliteStore, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'

import { events as factoryAbi } from './abi/uniswap.v3/factory'
import { events as swapsAbi } from './abi/uniswap.v3/swaps'

/**
 *  This example builds on `03.factory.example.ts` and `09.filtering-by-event-params.example.ts`.
 *  Here, we use event parameter filtering to ensure that only downstream events related to
 *  contract creations matching specific criteria are processed.
 *  In this case, we limit swap decoding to pools whose `token0` is WETH, by filtering factory
 *  events to only those with the given parameter. This keeps pipeline output focused and efficient.
 */
async function cli() {
  const weth = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  const stream = evmStream({
    id: 'factory-event-filter',
    source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs: evmEventDecoder({
      range: { from: '12,369,621' },
      contracts: contractFactory({
        address: '0x1f98431c8ad98523631ae4a59f267346ea31f984',
        event: {
          event: factoryAbi.PoolCreated,
          params: {
            /**
             * Only listen for Swap events from pools where token0 is WETH.
             * You can also pass an array of values to match multiple token0 values.
             */
            token0: weth,
            // token1: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  // You can filter by any indexed parameter!
          },
        },
        childAddressField: 'pool',
        database: contractFactorySqliteStore({
          path: './uniswap3-eth-pools.sqlite',
        }),
      }),
      events: {
        swaps: swapsAbi.Swap,
      },
    }),
  })

  for await (const { data } of stream) {
    for (const swap of data.swaps) {
      assert(swap.factory?.event.token0 === weth)
    }

    console.log(`Parsed ${data.swaps.length} swaps from WETH pools`)
  }
}

void cli()
