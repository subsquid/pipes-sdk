# @subsquid/pipes

Core package of the **SQD Pipes** ecosystem. Composable streams for building blockchain indexers:
onchain data ingestion, decoding, and storage.

---

## Overview

`@subsquid/pipes` is a TypeScript library for building **blockchain indexers**. A pipeline is built from
three composable parts:

- **Streams** pull data from managed SQD Portal datasets for EVM, Solana, Hyperliquid, Bitcoin, and Tron.
- **Decoders** turn raw blocks, logs, and instructions into strongly-typed objects.
- **Targets** persist or forward the decoded data, managing offsets and chain reorgs.

Storage targets are available for **ClickHouse**, **PostgreSQL** (via Drizzle), **Parquet**, and **BigQuery**.
Observability is built in: Prometheus metrics, Pino-compatible logging, and profiling utilities.

---

## Installation

```bash
npm install @subsquid/pipes
```

---

## Quick start

Stream ERC-20 transfers from an EVM chain and print them:

```ts
import { commonAbis, evmEventDecoder, evmPortalStream } from '@subsquid/pipes/evm'

async function main() {
  const stream = evmPortalStream({
    id: 'erc20-transfers',
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs: evmEventDecoder({
      range: { from: '12,000,000' },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
    }),
  })

  for await (const { data } of stream) {
    console.log(`parsed ${data.transfers.length} transfers`)
  }
}

void main()
```

To persist instead of printing, replace the loop with `.pipeTo(target)`. See the
[ClickHouse](https://github.com/subsquid/pipes-sdk/blob/main/docs/examples/evm/04.clickhouse.example.ts)
and [Drizzle/PostgreSQL](https://github.com/subsquid/pipes-sdk/blob/main/docs/examples/evm/08.drizzle.example.ts)
examples.

---

## Documentation & examples

- **Quickstart & guides:** https://docs.sqd.dev/en/sdk/pipes-sdk/evm/quickstart
- **Examples:** [docs/examples](https://github.com/subsquid/pipes-sdk/tree/main/docs/examples)
  (EVM, Solana, Bitcoin, Hyperliquid, Tron)

Extend the system by implementing custom components against the `Transformer` / `Target` interfaces.

---

## License

MIT © SQD
