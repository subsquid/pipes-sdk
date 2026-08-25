import crypto from 'node:crypto'

import { type MockInstance, afterAll, beforeAll, describe, expect, it, vi } from 'vitest'

import { Config } from '~/types/init.js'
import { ProjectWriter } from '~/utils/project-writer.js'

import { fixtures, overloadedApprovalContract, wethContract } from '../../templates/test-fixtures.js'
import { TransformerBuilder } from './index.js'

describe('EVM Template Builder', () => {
  const projectWriter = new ProjectWriter('mock-folder')
  let spy: MockInstance

  beforeAll(() => {
    spy = vi.spyOn(crypto, 'randomBytes').mockImplementation(() => Buffer.from('a1b2c3d4', 'hex') as any)
  })

  afterAll(() => {
    spy.mockRestore()
  })

  it('should build index.ts file using single pipe template', async () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [fixtures.erc20Transfers()],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    const indexerContent = await new TransformerBuilder(config, projectWriter).render()

    expect(indexerContent).toMatchInlineSnapshot(`
      "import "dotenv/config";
      import { commonAbis, evmEventDecoder, evmStream } from "@subsquid/pipes/evm";
      import { z } from "zod";
      import path from "node:path";
      import { clickhouseTarget } from "@subsquid/pipes/targets/clickhouse";
      import { createClient } from "@clickhouse/client";
      import { serializeJsonWithBigInt, toSnakeCaseKeysArray } from "./utils/index.js";

      const env = z.object({
        CLICKHOUSE_USER: z.string(),
        CLICKHOUSE_PASSWORD: z.string(),
        CLICKHOUSE_URL: z.string(),
        CLICKHOUSE_DATABASE: z.string(),
      }).parse(process.env)

      const erc20Transfers = evmEventDecoder({
        profiler: { name: 'erc20-transfers' }, // Optional: add a profiler to measure the performance of the transformer
        range: { from: '12,369,621' },
        contracts: [
          '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        ],
        events: {
          transfers: commonAbis.erc20.events.Transfer,
        },
      }).pipe(({ transfers }) =>
        transfers.map((transfer) => ({
          blockNumber: transfer.block.number,
          txHash: transfer.rawEvent.transactionHash,
          logIndex: transfer.rawEvent.logIndex,
          timestamp: transfer.timestamp.getTime(),
          from: transfer.event.from,
          to: transfer.event.to,
          value: transfer.event.value,
          tokenAddress: transfer.contract,
        })),
      )

      export async function main() {
        await evmStream({
          id: 'a1b2c3d4',
          source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
          outputs: {
            erc20Transfers,
          },
        })
        .pipeTo(clickhouseTarget({
          client: createClient({
              username: env.CLICKHOUSE_USER,
              password: env.CLICKHOUSE_PASSWORD,
              url: env.CLICKHOUSE_URL,
              database: env.CLICKHOUSE_DATABASE,
              json: {
                  stringify: serializeJsonWithBigInt,
              },
              clickhouse_settings: {
                  date_time_input_format: 'best_effort',
                  date_time_output_format: 'iso',
                  output_format_json_named_tuples_as_objects: 1,
                  output_format_json_quote_64bit_floats: 1,
                  output_format_json_quote_64bit_integers: 1,
                  input_format_skip_unknown_fields: 1,
              },
          }),
          onStart: async ({ store }) => {
            const migrationsDir = path.join(process.cwd(), 'migrations')
            await store.executeFiles(migrationsDir)
          },
          onData: async ({ data, store }) => {
            await store.insert({
              table: 'erc20_transfers',
              values: toSnakeCaseKeysArray(data.erc20Transfers),
              format: 'JSONEachRow',
            });
          },
          onRollback: async ({ safeCursor, store }) => {
            await store.removeAllRows({
              tables: [
                'erc20_transfers',
              ],
              where: 'block_number > {latest:UInt32}',
              params: { latest: safeCursor.number },
            });
          },
        }))
      }

      void main()
      "
    `)
  })

  it('should build index.ts combining multiple pipe templates', async () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [fixtures.erc20Transfers(), fixtures.uniswapV3Swaps()],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    const indexerContent = await new TransformerBuilder(config, projectWriter).render()

    expect(indexerContent).toMatchInlineSnapshot(`
      "import "dotenv/config";
      import { commonAbis, contractFactory, contractFactorySqliteStore, evmEventDecoder, evmStream } from "@subsquid/pipes/evm";
      import { z } from "zod";
      import path from "node:path";
      import { clickhouseTarget } from "@subsquid/pipes/targets/clickhouse";
      import { createClient } from "@clickhouse/client";
      import { serializeJsonWithBigInt, toSnakeCaseKeysArray } from "./utils/index.js";
      import { events as factoryEvents } from "./contracts/factory.js";
      import { events as poolEvents } from "./contracts/pool.js";

      const env = z.object({
        CLICKHOUSE_USER: z.string(),
        CLICKHOUSE_PASSWORD: z.string(),
        CLICKHOUSE_URL: z.string(),
        CLICKHOUSE_DATABASE: z.string(),
      }).parse(process.env)

      const erc20Transfers = evmEventDecoder({
        profiler: { name: 'erc20-transfers' }, // Optional: add a profiler to measure the performance of the transformer
        range: { from: '12,369,621' },
        contracts: [
          '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        ],
        events: {
          transfers: commonAbis.erc20.events.Transfer,
        },
      }).pipe(({ transfers }) =>
        transfers.map((transfer) => ({
          blockNumber: transfer.block.number,
          txHash: transfer.rawEvent.transactionHash,
          logIndex: transfer.rawEvent.logIndex,
          timestamp: transfer.timestamp.getTime(),
          from: transfer.event.from,
          to: transfer.event.to,
          value: transfer.event.value,
          tokenAddress: transfer.contract,
        })),
      )

      const uniswapV3Swaps = evmEventDecoder({
        range: { from: '12,369,621' },
        contracts: contractFactory({
          address: [
            '0x1f98431c8ad98523631ae4a59f267346ea31f984',
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
          timestamp: s.timestamp.getTime(),
          pool: s.contract,
          token0: s.factory?.event.token0 ?? '',
          token1: s.factory?.event.token1 ?? '',
          ...s.event,
        })),
      )

      export async function main() {
        await evmStream({
          id: 'a1b2c3d4',
          source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
          outputs: {
            erc20Transfers,
            uniswapV3Swaps,
          },
        })
        .pipeTo(clickhouseTarget({
          client: createClient({
              username: env.CLICKHOUSE_USER,
              password: env.CLICKHOUSE_PASSWORD,
              url: env.CLICKHOUSE_URL,
              database: env.CLICKHOUSE_DATABASE,
              json: {
                  stringify: serializeJsonWithBigInt,
              },
              clickhouse_settings: {
                  date_time_input_format: 'best_effort',
                  date_time_output_format: 'iso',
                  output_format_json_named_tuples_as_objects: 1,
                  output_format_json_quote_64bit_floats: 1,
                  output_format_json_quote_64bit_integers: 1,
                  input_format_skip_unknown_fields: 1,
              },
          }),
          onStart: async ({ store }) => {
            const migrationsDir = path.join(process.cwd(), 'migrations')
            await store.executeFiles(migrationsDir)
          },
          onData: async ({ data, store }) => {
            await store.insert({
              table: 'erc20_transfers',
              values: toSnakeCaseKeysArray(data.erc20Transfers),
              format: 'JSONEachRow',
            });
            await store.insert({
              table: 'uniswap_v3_swaps',
              values: toSnakeCaseKeysArray(data.uniswapV3Swaps),
              format: 'JSONEachRow',
            });
          },
          onRollback: async ({ safeCursor, store }) => {
            await store.removeAllRows({
              tables: [
                'erc20_transfers',
                'uniswap_v3_swaps',
              ],
              where: 'block_number > {latest:UInt32}',
              params: { latest: safeCursor.number },
            });
          },
        }))
      }

      void main()
      "
    `)
  })

  it('renders a portal + RPC source list and a required RPC_URL when rpcFallback is on', async () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [fixtures.erc20Transfers()],
      target: 'clickhouse',
      packageManager: 'pnpm',
      rpcFallback: true,
    }

    const indexerContent = await new TransformerBuilder(config, projectWriter).render()

    expect(indexerContent).toContain(`source: [
      'https://portal.sqd.dev/datasets/ethereum-mainnet',
      { type: 'rpc', url: env.RPC_URL, name: 'rpc-fallback' },
    ],`)
    expect(indexerContent).not.toContain("source: 'https://portal.sqd.dev")
    expect(indexerContent).toContain('RPC_URL: z.string().min(1),')
  })

  it('disambiguates overloaded events with unique keys + warning comment', async () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [fixtures.evmCustom([overloadedApprovalContract])],
      target: 'postgresql',
      packageManager: 'pnpm',
    }

    const indexerContent = await new TransformerBuilder(config, projectWriter).render()

    expect(indexerContent).toContain('Transfer: overloadedTokenEvents.Transfer,')
    expect(indexerContent).toMatch(/Approval_[0-9a-f]{4}: overloadedTokenEvents\.Approval/)
    const approvalKeyMatches = indexerContent.match(/Approval_[0-9a-f]{4}: overloadedTokenEvents\.Approval,/g) ?? []
    expect(approvalKeyMatches).toHaveLength(2)
    expect(new Set(approvalKeyMatches).size).toBe(2)
    expect(indexerContent).toContain('"Approval" is overloaded in this ABI')
  })

  it('should build custom contract template', async () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [fixtures.evmCustom([wethContract])],
      target: 'postgresql',
      packageManager: 'pnpm',
    }

    const indexerContent = await new TransformerBuilder(config, projectWriter).render()

    expect(indexerContent).toMatchInlineSnapshot(`
      "import "dotenv/config";
      import { evmEventDecoder, evmStream } from "@subsquid/pipes/evm";
      import { z } from "zod";
      import { chunkForInsert, drizzleTarget } from "@subsquid/pipes/targets/drizzle/node-postgres";
      import { drizzle } from "drizzle-orm/node-postgres";
      import { weth9ApprovalTable, weth9TransferTable } from "./schemas.js";
      import { events as weth9Events } from "./contracts/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2.js";
      import { enrichEvents } from "./utils/index.js";

      const env = z.object({
        DB_CONNECTION_STR: z.string(),
      }).parse(process.env)

      const custom = evmEventDecoder({
        range: { from: 'latest' },
        contracts: [
          '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        ],
        /**
         * Or optionally use pass all events object directly to listen to all contract events
         * \`\`\`ts
         * events: myContractEvents,
         * \`\`\`
         */
        events: {
          Approval: weth9Events.Approval,
          Transfer: weth9Events.Transfer,
        },
      }).pipe(enrichEvents)

      export async function main() {
        await evmStream({
          id: 'a1b2c3d4',
          source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
          outputs: {
            custom,
          },
        })
        .pipeTo(drizzleTarget({
          db: drizzle(env.DB_CONNECTION_STR),
          tables: [
            weth9ApprovalTable,
            weth9TransferTable,
          ],
          onData: async ({ tx, data }) => {
            for (const values of chunkForInsert(data.custom.Approval)) {
              await tx.insert(weth9ApprovalTable).values(values)
            }
            for (const values of chunkForInsert(data.custom.Transfer)) {
              await tx.insert(weth9TransferTable).values(values)
            }
          },
        }))
      }

      void main()
      "
    `)
  })
})
