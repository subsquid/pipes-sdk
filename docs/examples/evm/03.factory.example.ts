import {
  DecodedEvent,
  contractFactory,
  contractFactorySqliteStore,
  evmEventDecoder,
  evmStream,
} from '@subsquid/pipes/evm'

import { events as factoryAbi } from './abi/uniswap.v3/factory'
import { events as swapsAbi } from './abi/uniswap.v3/swaps'

/**
 * This example demonstrates how to use a Factory pattern to decode Uniswap V3 swaps.
 * It creates an EVM Portal Source to stream Ethereum mainnet data, sets up a Factory
 * to track pool creation events, and decodes swap events from the created pools.
 * The pool addresses are stored in an SQLite database for efficient lookup.
 */

export function transform<T, F>(event: DecodedEvent<T, F>) {
  return {
    ...event.event,
    factoryEvent: event.factory?.event,
  }
}

async function cli() {
  const stream = evmStream({
    id: 'uniswap-v3-pools',
    source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs: evmEventDecoder({
      range: { from: '12,369,621' },
      contracts: contractFactory({
        address: '0x1f98431c8ad98523631ae4a59f267346ea31f984',
        event: factoryAbi.PoolCreated,
        childAddressField: 'pool',
        database: contractFactorySqliteStore({
          path: './uniswap3-eth-pools.sqlite',
        }),
      }),
      events: {
        swaps: swapsAbi.Swap,
        fees: swapsAbi.SetFeeProtocol,
      },
    }),
  })

  for await (const { data } of stream) {
    console.log(`parsed ${data.swaps.length} swaps and ${data.fees.length} fees`)
  }
}

void cli()
