import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'
import { PipeContext, devRunner } from '@subsquid/pipes/runtime/node'

export type Params = { dataset: string }

async function transfers({ id, params, metrics, logger }: PipeContext<Params>) {
  const stream = evmStream({
    id,
    source: `https://portal.sqd.dev/datasets/${params.dataset}`,
    metrics,
    logger,
    outputs: evmEventDecoder({
      range: { from: '0' },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
    }),
  })

  for await (const { ctx, data } of stream) {
    ctx.logger.debug(`fetched ${data.transfers.length} transfers`)
  }
}

async function main() {
  const run = devRunner<Params>(
    [
      { id: 'arb', params: { dataset: 'arbitrum-one' }, handler: transfers },
      { id: 'ethereum', params: { dataset: 'ethereum-mainnet' }, handler: transfers },
    ],
    { metrics: { port: 9090 } },
  )

  await run.start()
}

main()
