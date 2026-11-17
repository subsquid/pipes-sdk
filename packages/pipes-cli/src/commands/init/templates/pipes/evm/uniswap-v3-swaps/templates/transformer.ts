import Mustache from 'mustache'

import { UniswapV3SwapsPipeTemplateParams } from '../template.config.js'

const template = `import { contractFactory, contractFactorySqliteStore, evmEventDecoder } from '@subsquid/pipes/evm'
import { events as factoryEvents } from './contracts/factory.js'
import { events as poolEvents } from './contracts/pool.js'

const uniswapV3Swaps = evmEventDecoder({
  range: { from: '{{{range.from}}}'{{#range.to}}, to: '{{{range.to}}}'{{/range.to}} },
  contracts: contractFactory({
    address: [
      '{{factoryAddress}}',
    ],
    event: factoryEvents.PoolCreated,
    childAddressField: 'pool',
    database: await contractFactorySqliteStore({
      path: './uniswap3-eth-pools.sqlite',
    }),
  }),
  events: {
    swaps: poolEvents.Swap,
  },
}).pipe(({ swaps }) =>
  swaps.map((s) => ({
    blockNumber: s.block.number,
    txHash: s.rawEvent.transactionHash,
    logIndex: s.rawEvent.logIndex,
    timestamp: s.timestamp.getTime() / 1000,
    pool: s.contract,
    token0: s.factory?.event.token0 ?? '',
    token1: s.factory?.event.token1 ?? '',
    ...s.event,
  })),
)
`

export function renderTemplate(params: UniswapV3SwapsPipeTemplateParams) {
  return Mustache.render(template, params)
}
