# SQD Pipes SDK

[![npm](https://img.shields.io/npm/v/@subsquid/pipes.svg)](https://www.npmjs.com/package/@subsquid/pipes)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)
[![docs](https://img.shields.io/badge/docs-docs.sqd.dev-3b82f6)](https://docs.sqd.dev)

Documentation: https://docs.sqd.dev  ·  Website: https://sqd.dev

SQD Pipes is a TypeScript library for building blockchain indexers. It streams onchain data from SQD
Portal, decodes it in flight, handles chain reorgs, and writes the results to your own storage. A
pipeline is a composition of three parts:

- **Streams** tap managed SQD Portal datasets for EVM, Solana, Hyperliquid, Bitcoin, and Tron.
- **Decoders** turn raw blocks, logs, and instructions into strongly-typed objects.
- **Targets** persist or forward the decoded data, managing offsets and chain reorgs.

Profiling, structured logging, and Prometheus metrics are built in. The same pipeline runs in a CLI, a
backend service, or a long-running worker.

## Install

```bash
pnpm add @subsquid/pipes
```

Or scaffold a ready-to-run project with the CLI:

```bash
pnpm dlx @subsquid/pipes-cli init
```

## Quick start

Stream ERC-20 transfers from Ethereum Mainnet and print them:

```ts
import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'

async function main() {
  const stream = evmStream({
    id: 'erc20-transfers',
    source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
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

Run it with [`tsx`](https://github.com/privatenumber/tsx):

```bash
pnpm dlx tsx erc20-transfers.ts
```

## Persist to a target

Replace the `for await` loop with `.pipeTo(target)`. Each target batches writes, tracks the cursor under
the stream `id`, and rolls back cleanly on chain reorgs.

| Target | Import | Example |
| --- | --- | --- |
| ClickHouse | `@subsquid/pipes/targets/clickhouse` | [04.clickhouse](docs/examples/evm/04.clickhouse.example.ts) |
| PostgreSQL (Drizzle) | `@subsquid/pipes/targets/drizzle/node-postgres` | [08.drizzle](docs/examples/evm/08.drizzle.example.ts) |
| Parquet | `@subsquid/pipes/targets/parquet` | [17.parquet](docs/examples/evm/17.parquet.example.ts) |
| BigQuery | `@subsquid/pipes/targets/bigquery` | [16.bigquery](docs/examples/evm/16.bigquery.example.ts) |
| Google PubSub | `@subsquid/pipes/targets/pubsub` | [18.pubsub](docs/examples/evm/18.pubsub.example.ts) · [landing it in BigQuery](docs/pubsub-bigquery.md) |

```ts
import { createClient } from '@clickhouse/client'
import { clickhouseTarget } from '@subsquid/pipes/targets/clickhouse'

await evmStream({
  id: 'erc20-transfers',
  source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  outputs: evmEventDecoder({
    range: { from: '12,000,000' },
    events: { transfers: commonAbis.erc20.events.Transfer },
  }),
}).pipeTo(
  clickhouseTarget({
    client: createClient({ url: 'http://localhost:8123' }),
    onRollback: async () => {},
    onData: async ({ data }) => {
      // insert data.transfers ...
    },
  }),
)
```

## Learn more

- **Quickstart & guides:** https://docs.sqd.dev/en/sdk/pipes-sdk/evm/quickstart
- **EVM examples:** [docs/examples/evm](docs/examples/evm)
- **Solana examples:** [docs/examples/solana](docs/examples/solana)
- Bitcoin, Hyperliquid, and Tron examples live alongside them under [docs/examples](docs/examples).

Run any example from the repo root with `pnpm tsx <path/to/example.ts>`.

## License

Apache-2.0 © SQD
