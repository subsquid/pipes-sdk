import { describe, expect, it } from 'vitest'

import { Config } from '~/types/init.js'

import { getTemplate } from '../../templates/registry.js'
import { fixtures, overloadedApprovalContract, seaportContract, wethContract } from '../../templates/test-fixtures.js'
import { buildClickhouseTarget, buildPostgresTarget, buildTarget } from './index.js'

describe('clickhouse target template builder', () => {
  it('should render target for pre-defined template', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.erc20Transfers()],
      defaultNetwork: 'ethereum-mainnet',
      target: 'clickhouse',
      packageManager: 'pnpm',
    }
    expect(buildTarget(config).targetCode).toMatchInlineSnapshot(`
      "
      import path from 'node:path'
      import { clickhouseTarget } from '@subsquid/pipes/targets/clickhouse'
      import { createClient } from '@clickhouse/client'
      import { serializeJsonWithBigInt, toSnakeCaseKeysArray } from './utils/index.js'

      clickhouseTarget({
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
        })"
    `)
  })

  it('should render the target for custom template', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([wethContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    expect(buildTarget(config).targetCode).toMatchInlineSnapshot(`
      "
      import path from 'node:path'
      import { clickhouseTarget } from '@subsquid/pipes/targets/clickhouse'
      import { createClient } from '@clickhouse/client'
      import { serializeJsonWithBigInt, toSnakeCaseKeysArray } from './utils/index.js'

      clickhouseTarget({
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
              table: 'weth_9_approval',
              values: toSnakeCaseKeysArray(data.custom.Approval),
              format: 'JSONEachRow',
            });
            await store.insert({
              table: 'weth_9_transfer',
              values: toSnakeCaseKeysArray(data.custom.Transfer),
              format: 'JSONEachRow',
            });
          },
          onRollback: async ({ safeCursor, store }) => {
            await store.removeAllRows({
              tables: [
                'weth_9_approval',
                'weth_9_transfer',
              ],
              where: 'block_number > {latest:UInt32}',
              params: { latest: safeCursor.number },
            });
          },
        })"
    `)
  })
})

describe('postgres target template builder', () => {
  it('should render target for pre-defined template', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.erc20Transfers()],
      defaultNetwork: 'ethereum-mainnet',
      target: 'postgresql',
      packageManager: 'pnpm',
    }
    expect(buildTarget(config).targetCode).toMatchInlineSnapshot(`
      "
      import { chunkForInsert, drizzleTarget } from '@subsquid/pipes/targets/drizzle/node-postgres'
      import { drizzle } from 'drizzle-orm/node-postgres'
      import {
        erc20TransfersTable,
      } from './schemas.js'

      drizzleTarget({
          db: drizzle(env.DB_CONNECTION_STR),
          tables: [
            erc20TransfersTable,
          ],
          onData: async ({ tx, data }) => {
            for (const values of chunkForInsert(data.erc20Transfers)) {
              await tx.insert(erc20TransfersTable).values(values)
            }
          },
        })"
    `)
  })

  it('should render target for multiple pre-defined templates', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.erc20Transfers(), fixtures.uniswapV3Swaps()],
      defaultNetwork: 'ethereum-mainnet',
      target: 'postgresql',
      packageManager: 'pnpm',
    }
    expect(buildTarget(config).targetCode).toMatchInlineSnapshot(`
      "
      import { chunkForInsert, drizzleTarget } from '@subsquid/pipes/targets/drizzle/node-postgres'
      import { drizzle } from 'drizzle-orm/node-postgres'
      import {
        erc20TransfersTable,
        uniswapV3SwapsTable,
      } from './schemas.js'

      drizzleTarget({
          db: drizzle(env.DB_CONNECTION_STR),
          tables: [
            erc20TransfersTable,
            uniswapV3SwapsTable,
          ],
          onData: async ({ tx, data }) => {
            for (const values of chunkForInsert(data.erc20Transfers)) {
              await tx.insert(erc20TransfersTable).values(values)
            }
            for (const values of chunkForInsert(data.uniswapV3Swaps)) {
              await tx.insert(uniswapV3SwapsTable).values(values)
            }
          },
        })"
    `)
  })

  it('should render the target for custom template', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([wethContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'postgresql',
      packageManager: 'pnpm',
    }

    expect(buildTarget(config).targetCode).toMatchInlineSnapshot(`
      "
      import { chunkForInsert, drizzleTarget } from '@subsquid/pipes/targets/drizzle/node-postgres'
      import { drizzle } from 'drizzle-orm/node-postgres'
      import {
        weth9ApprovalTable,
        weth9TransferTable,
      } from './schemas.js'

      drizzleTarget({
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
        })"
    `)
  })

  it('should render the target for custom and pre-defined templates', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.erc20Transfers(), fixtures.evmCustom([wethContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'postgresql',
      packageManager: 'pnpm',
    }

    expect(buildTarget(config).targetCode).toMatchInlineSnapshot(`
      "
      import { chunkForInsert, drizzleTarget } from '@subsquid/pipes/targets/drizzle/node-postgres'
      import { drizzle } from 'drizzle-orm/node-postgres'
      import {
        erc20TransfersTable,
        weth9ApprovalTable,
        weth9TransferTable,
      } from './schemas.js'

      drizzleTarget({
          db: drizzle(env.DB_CONNECTION_STR),
          tables: [
            erc20TransfersTable,
            weth9ApprovalTable,
            weth9TransferTable,
          ],
          onData: async ({ tx, data }) => {
            for (const values of chunkForInsert(data.erc20Transfers)) {
              await tx.insert(erc20TransfersTable).values(values)
            }
            for (const values of chunkForInsert(data.custom.Approval)) {
              await tx.insert(weth9ApprovalTable).values(values)
            }
            for (const values of chunkForInsert(data.custom.Transfer)) {
              await tx.insert(weth9TransferTable).values(values)
            }
          },
        })"
    `)
  })
})

describe('buildPostgresTarget artifacts', () => {
  const config: Config<'evm'> = {
    projectFolder: 'mock-folder',
    networkType: 'evm',
    templates: [fixtures.erc20Transfers()],
    defaultNetwork: 'ethereum-mainnet',
    target: 'postgresql',
    packageManager: 'pnpm',
  }

  it('returns the postgres env schema', () => {
    expect(buildPostgresTarget(config).envSchema).toMatchInlineSnapshot(`
      "
      import { z } from 'zod'

      const env = z.object({
        DB_CONNECTION_STR: z.string(),
      }).parse(process.env)
      "
    `)
  })

  it('returns files for .env, src/schemas.ts, and drizzle.config.ts', () => {
    const files = buildPostgresTarget(config).files
    expect(files.map((f) => f.path)).toEqual(['.env', 'src/schemas.ts', 'drizzle.config.ts'])
    expect(files.find((f) => f.path === '.env')!.content).toContain('DB_CONNECTION_STR=postgresql://')
    expect(files.find((f) => f.path === 'src/schemas.ts')!.content).toContain('erc20TransfersTable')
    expect(files.find((f) => f.path === 'drizzle.config.ts')!.content.length).toBeGreaterThan(0)
  })

  it('returns a single db:generate post step using the configured package manager', () => {
    expect(buildPostgresTarget(config).postSteps).toEqual([{ kind: 'exec', command: 'pnpm run db:generate' }])
  })

  describe('with rpcFallback', () => {
    const rpcConfig: Config<'evm'> = { ...config, rpcFallback: true }

    it('adds a required RPC_URL to the env schema', () => {
      expect(buildPostgresTarget(rpcConfig).envSchema).toContain('RPC_URL: z.string().min(1),')
      expect(buildPostgresTarget(config).envSchema).not.toContain('RPC_URL')
    })

    it('writes RPC_URL to .env — the prompted URL when given, a blank placeholder otherwise', () => {
      const envOf = (artifacts: ReturnType<typeof buildPostgresTarget>) =>
        artifacts.files.find((f) => f.path === '.env')!.content

      expect(envOf(buildPostgresTarget(rpcConfig))).toContain('RPC_URL=\n')
      expect(envOf(buildPostgresTarget(rpcConfig, { rpcUrl: 'https://rpc.example.com/v2/KEY' }))).toContain(
        'RPC_URL=https://rpc.example.com/v2/KEY\n',
      )
      expect(envOf(buildPostgresTarget(config))).not.toContain('RPC_URL')
    })
  })
})

describe('buildClickhouseTarget artifacts', () => {
  const config: Config<'evm'> = {
    projectFolder: 'mock-folder',
    networkType: 'evm',
    templates: [fixtures.erc20Transfers(), fixtures.evmCustom([wethContract])],
    defaultNetwork: 'ethereum-mainnet',
    target: 'clickhouse',
    packageManager: 'pnpm',
  }

  it('returns the clickhouse env schema', () => {
    expect(buildClickhouseTarget(config).envSchema).toMatchInlineSnapshot(`
      "
      import { z } from 'zod'

      const env = z.object({
        CLICKHOUSE_USER: z.string(),
        CLICKHOUSE_PASSWORD: z.string(),
        CLICKHOUSE_URL: z.string(),
        CLICKHOUSE_DATABASE: z.string(),
      }).parse(process.env)
      "
    `)
  })

  it('returns .env and one migrations file per template', () => {
    const files = buildClickhouseTarget(config).files
    expect(files.map((f) => f.path)).toEqual([
      '.env',
      'migrations/erc20Transfers-migration.sql',
      'migrations/custom-migration.sql',
    ])
    expect(files.find((f) => f.path === '.env')!.content).toContain('CLICKHOUSE_URL=http://localhost:')
  })

  it('returns an empty postSteps array', () => {
    expect(buildClickhouseTarget(config).postSteps).toEqual([])
  })

  describe('with rpcFallback', () => {
    const rpcConfig: Config<'evm'> = { ...config, rpcFallback: true }

    it('adds a required RPC_URL to the env schema', () => {
      expect(buildClickhouseTarget(rpcConfig).envSchema).toContain('RPC_URL: z.string().min(1),')
      expect(buildClickhouseTarget(config).envSchema).not.toContain('RPC_URL')
    })

    it('writes RPC_URL to .env — the prompted URL when given, a blank placeholder otherwise', () => {
      const envOf = (artifacts: ReturnType<typeof buildClickhouseTarget>) =>
        artifacts.files.find((f) => f.path === '.env')!.content

      expect(envOf(buildClickhouseTarget(rpcConfig))).toContain('RPC_URL=\n')
      expect(envOf(buildClickhouseTarget(rpcConfig, { rpcUrl: 'https://rpc.example.com/v2/KEY' }))).toContain(
        'RPC_URL=https://rpc.example.com/v2/KEY\n',
      )
      expect(envOf(buildClickhouseTarget(config))).not.toContain('RPC_URL')
    })
  })
})

describe('shared-decoder contract-address discriminator', () => {
  const usdcContract = {
    contractName: 'USDC',
    contractEvents: wethContract.contractEvents,
    deployments: [{ address: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', range: { from: 'latest' } }],
  }

  it('adds contractAddress column to postgres schema when decoder is shared', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([wethContract, usdcContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'postgresql',
      packageManager: 'pnpm',
    }
    const schema = buildPostgresTarget(config).files.find((f) => f.path === 'src/schemas.ts')!.content
    expect(schema).toContain('contractAddress: char({ length: 42 }).notNull()')
  })

  it('adds contract_address column to clickhouse DDL when decoder is shared', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([wethContract, usdcContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'clickhouse',
      packageManager: 'pnpm',
    }
    const migration = buildClickhouseTarget(config).files.find((f) => f.path.includes('migrations/'))!.content
    expect(migration).toContain('contract_address LowCardinality(FixedString(42))')
  })

  it('omits the discriminator when decoder is per-contract (not shared)', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([wethContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'postgresql',
      packageManager: 'pnpm',
    }
    const schema = buildPostgresTarget(config).files.find((f) => f.path === 'src/schemas.ts')!.content
    expect(schema).not.toContain('contractAddress: char({ length: 42 })')
  })
})

describe('overloaded events', () => {
  it('emits distinct postgres tables for events with the same name', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([overloadedApprovalContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'postgresql',
      packageManager: 'pnpm',
    }
    const schema = buildPostgresTarget(config).files.find((f) => f.path === 'src/schemas.ts')!.content
    const approvalTableDecls = schema.match(/'overloaded_token_approval(_[0-9a-f]{4})?'/g) ?? []
    expect(approvalTableDecls).toHaveLength(2)
    expect(new Set(approvalTableDecls).size).toBe(2)
    expect(schema).toContain("'overloaded_token_transfer'")
  })

  it('emits distinct clickhouse tables for events with the same name', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([overloadedApprovalContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'clickhouse',
      packageManager: 'pnpm',
    }
    const migration = buildClickhouseTarget(config).files.find((f) => f.path.includes('migrations/'))!.content
    const approvalCreates =
      migration.match(/CREATE TABLE IF NOT EXISTS overloaded_token_approval(_[0-9a-f]{4})?/g) ?? []
    expect(approvalCreates).toHaveLength(2)
    expect(new Set(approvalCreates).size).toBe(2)
  })
})

describe('tuple[] event inputs', () => {
  it('renders postgres schema with jsonb columns for tuple-array inputs', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([seaportContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'postgresql',
      packageManager: 'pnpm',
    }
    const files = buildPostgresTarget(config).files.map((f) => ({ path: f.path, content: f.content }))
    const schema = files.find((f) => f.path === 'src/schemas.ts')!
    expect(schema.content).toContain('offer: jsonb()')
    expect(schema.content).toContain('consideration: jsonb()')
  })

  it('renders clickhouse DDL with JSON columns for tuple-array inputs', () => {
    const config: Config<'evm'> = {
      projectFolder: 'mock-folder',
      networkType: 'evm',
      templates: [fixtures.evmCustom([seaportContract])],
      defaultNetwork: 'ethereum-mainnet',
      target: 'clickhouse',
      packageManager: 'pnpm',
    }
    const files = buildClickhouseTarget(config).files.map((f) => ({ path: f.path, content: f.content }))
    const migration = files.find((f) => f.path.includes('migrations/'))!
    expect(migration.content).toMatch(/offer\s+JSON/)
    expect(migration.content).toMatch(/consideration\s+JSON/)
  })
})

describe('target code reads exactly the decoder ids the stream exposes (grouping divergence regressions)', () => {
  const dataPathsIn = (code: string) => [...code.matchAll(/data\.([\w.]+)/g)].map((m) => m[1])

  const divergentCustom = {
    template: getTemplate('evm', 'custom')!,
    params: {
      contracts: [
        {
          contractName: 'Weth9',
          contractEvents: [{ name: 'Transfer', type: 'event', inputs: [] }],
          deployments: [
            { address: '0xAAA1', range: { from: '100' } },
            { address: '0xAAA2', range: { from: '200' } },
          ],
        },
      ],
    },
  }
  const divergentErc20 = {
    template: getTemplate('evm', 'erc20Transfers')!,
    params: {
      deployments: [
        { address: '0xC1', range: { from: 'latest' } },
        { address: '0xC2', range: { from: '5' } },
      ],
    },
  }

  function makeDivergentConfig(target: 'clickhouse' | 'postgresql'): Config<'evm'> {
    return {
      projectFolder: '/tmp/proj',
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      target,
      packageManager: 'pnpm',
      templates: [divergentCustom, divergentErc20],
    }
  }

  const expectedPaths = ['customWeth9.Transfer', 'customWeth92.Transfer', 'erc20Transfers', 'erc20Transfers2']

  it('clickhouse onData covers every decoder, including range-split ones', () => {
    const { targetCode } = buildTarget(makeDivergentConfig('clickhouse'))
    expect(dataPathsIn(targetCode)).toEqual(expectedPaths)
  })

  it('postgres onData covers every decoder, including range-split ones', () => {
    const { targetCode } = buildTarget(makeDivergentConfig('postgresql'))
    expect(dataPathsIn(targetCode)).toEqual(expectedPaths)
  })

  it('clickhouse rollback lists each table once even when several decoders share it', () => {
    const { targetCode } = buildTarget(makeDivergentConfig('clickhouse'))
    const rollback = targetCode.slice(targetCode.indexOf('onRollback'))
    expect(rollback.match(/'erc20_transfers'/g)).toHaveLength(1)
  })
})
