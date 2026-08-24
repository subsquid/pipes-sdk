import { evmStream } from '@subsquid/pipes/evm'
import pino from 'pino'

import { erc20Transfers } from './decoders'

/**
 * This example demonstrates how to configure custom logging using Pino transports.
 * It sets up a Sentry transport for error reporting and logs ERC20 transfer events
 * from Ethereum mainnet using the Portal API.
 */

async function cli() {
  /*
   * You can use any pino transport, here we use pino-sentry-transport
   * More about pino transports: https://getpino.io/#/docs/transports?id=known-transports
   */

  const transport = pino.transport({
    target: 'pino-sentry-transport',
    options: {
      sentry: {
        dsn: 'https://******@sentry.io/12345',
      },
    },
  })

  const stream = evmStream({
    id: 'custom-logs',
    source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    logger: pino(transport),
    outputs: erc20Transfers(),
  })

  for await (const { data } of stream) {
    console.log(`parsed ${data.length} transfers`)
  }
}

void cli()
