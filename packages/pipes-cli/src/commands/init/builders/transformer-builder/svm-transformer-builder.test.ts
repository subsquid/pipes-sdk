import crypto from 'node:crypto'

import { type MockInstance, afterAll, beforeAll, describe, expect, it, vi } from 'vitest'

import { Config } from '~/types/init.js'
import { ProjectWriter } from '~/utils/project-writer.js'

import { fixtures, whirlpoolContract } from '../../templates/test-fixtures.js'
import { TransformerBuilder } from './index.js'

describe('SVM Template Builder', () => {
  const projectWriter = new ProjectWriter('mock-folder')
  let spy: MockInstance

  beforeAll(() => {
    spy = vi.spyOn(crypto, 'randomBytes').mockImplementation(() => Buffer.from('a1b2c3d4', 'hex') as any)
  })

  afterAll(() => {
    spy.mockRestore()
  })

  it('should build index.ts file using custom template', async () => {
    const config: Config<'svm'> = {
      projectFolder: 'mock-folder',
      networkType: 'svm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [fixtures.svmCustom([whirlpoolContract])],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    const indexerContent = await new TransformerBuilder(config, projectWriter).render()

    expect(indexerContent).toMatchInlineSnapshot(`
      "import "dotenv/config";
      import { solanaInstructionDecoder, solanaPortalStream } from "@subsquid/pipes/solana";
      import { z } from "zod";
      import path from "node:path";
      import { clickhouseTarget } from "@subsquid/pipes/targets/clickhouse";
      import { createClient } from "@clickhouse/client";
      import { enrichEvents, serializeJsonWithBigInt, toSnakeCaseKeysArray } from "./utils/index.js";
      import * as whirpoolProgram from "./contracts/0x0000000000000000000000000000000000000000/index.js";

      const env = z.object({
        CLICKHOUSE_USER: z.string(),
        CLICKHOUSE_PASSWORD: z.string(),
        CLICKHOUSE_URL: z.string(),
        CLICKHOUSE_DATABASE: z.string(),
      }).parse(process.env)

      const custom = solanaInstructionDecoder({
        range: { from: 'latest' },
        program: { ...whirpoolProgram, programId: "0x0000000000000000000000000000000000000000" },
        /**
         * Or pass every instruction of the program:
         * \`\`\`ts
         * instructions: whirpoolProgram.instructions,
         * \`\`\`
         */
        instructions: {
          Swap: whirpoolProgram.instructions.Swap,
        },
      }).pipe(enrichEvents)

      export async function main() {
        await solanaPortalStream({
          id: 'a1b2c3d4',
          portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
          outputs: {
            custom,
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
              table: 'whirpool_swap',
              values: toSnakeCaseKeysArray(data.custom.Swap),
              format: 'JSONEachRow',
            });
          },
          onRollback: async ({ safeCursor, store }) => {
            await store.removeAllRows({
              tables: [
                'whirpool_swap',
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

  it('should build index.ts file using token-balances template', async () => {
    const config: Config<'svm'> = {
      projectFolder: 'mock-folder',
      networkType: 'svm',
      defaultNetwork: 'solana-mainnet',
      templates: [fixtures.tokenBalances()],
      target: 'postgresql',
      packageManager: 'pnpm',
    }

    const indexerContent = await new TransformerBuilder(config, projectWriter).render()

    expect(indexerContent).toMatchInlineSnapshot(`
      "import "dotenv/config";
      import { solanaPortalStream, solanaQuery } from "@subsquid/pipes/solana";
      import { z } from "zod";
      import { chunkForInsert, drizzleTarget } from "@subsquid/pipes/targets/drizzle/node-postgres";
      import { drizzle } from "drizzle-orm/node-postgres";
      import { tokenBalancesTable } from "./schemas.js";

      const env = z.object({
        DB_CONNECTION_STR: z.string(),
      }).parse(process.env)

      interface TokenBalance {
        blockNumber: number
        blockHash: string
        blockTime: number
        tokenAddress: string
        owner: string
        amount: number
      }

      const tokenBalances = solanaQuery()
        .addFields({
          block: {
            number: true,
            hash: true,
            timestamp: true,
          },
          tokenBalance: {
            preDecimals: true,
            preAmount: true,
            postAmount: true,
            preOwner: true,
            postOwner: true,
            preMint: true,
          },
        })
        .addTokenBalanceRequest({
          range: { from: '372,195,730' },
          request: {
            // You can filter in that way. it is much faster, but the query has a limit of 5000 addresses
            // preMint: ['tokenProgramId']
          },
        })
        .build()
        .pipe((data) =>
          data.flatMap((block) =>
            block.tokenBalances.flatMap((balance) => {
              const balances: TokenBalance[] = []

              const tokenAddress = balance.preMint

              if (!tokenAddress) return balances

              balances.push({
                blockNumber: block.header.number,
                blockHash: block.header.hash,
                blockTime: block.header.timestamp,
                tokenAddress: tokenAddress.toString(),
                owner: balance.preOwner?.toString() ?? '',
                amount: Number(balance.preAmount) / 10 ** Number(balance.preDecimals),
              })

              balances.push({
                blockNumber: block.header.number,
                blockHash: block.header.hash,
                blockTime: block.header.timestamp,
                tokenAddress: tokenAddress.toString(),
                owner: balance.postOwner?.toString() ?? '',
                amount: Number(balance.postAmount) / 10 ** Number(balance.preDecimals),
              })

              return balances.filter((balance): balance is TokenBalance => balance.owner !== '')
            }),
          ),
        )

      export async function main() {
        await solanaPortalStream({
          id: 'a1b2c3d4',
          portal: 'https://portal.sqd.dev/datasets/solana-mainnet',
          outputs: {
            tokenBalances,
          },
        })
        .pipeTo(drizzleTarget({
          db: drizzle(env.DB_CONNECTION_STR),
          tables: [
            tokenBalancesTable,
          ],
          onData: async ({ tx, data }) => {
            for (const values of chunkForInsert(data.tokenBalances)) {
              await tx.insert(tokenBalancesTable).values(values)
            }
          },
        }))
      }

      void main()
      "
    `)
  })

  // TODO: implement this test once we have more than one Solana template
  it('should build index.ts combining multiple pipe templates', () => {})
})
