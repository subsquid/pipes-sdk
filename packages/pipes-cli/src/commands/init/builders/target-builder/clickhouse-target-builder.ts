import Mustache from 'mustache'

import type { Config, NetworkType } from '~/types/init.js'

import { clickhouseDefaults } from '../../templates/config-files/dynamic/docker-compose.js'
import { type RenderedTemplate, renderTemplates } from '../render-templates.js'
import { rpcEnvFileLine, rpcEnvSchemaField } from './rpc-env.js'
import type { TargetArtifacts, TargetBuildOptions, TargetFile } from './target-artifacts.js'
import { insertEntries, uniqueTables } from './target-tables.js'

const targetTemplate = `
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
    {{#inserts}}
      await store.insert({
        table: '{{{table}}}',
        values: toSnakeCaseKeysArray(data.{{{dataPath}}}),
        format: 'JSONEachRow',
      });
    {{/inserts}}
    },
    onRollback: async ({ safeCursor, store }) => {
      await store.removeAllRows({
        tables: [
        {{#tables}}
          '{{.}}',
        {{/tables}}
        ],
        where: 'block_number > {latest:UInt32}',
        params: { latest: safeCursor.number },
      });
    },
  })`

function renderEnvSchema(config: Config<NetworkType>): string {
  return `
import { z } from 'zod'

const env = z.object({
  CLICKHOUSE_USER: z.string(),
  CLICKHOUSE_PASSWORD: z.string(),
  CLICKHOUSE_URL: z.string(),
  CLICKHOUSE_DATABASE: z.string(),
${rpcEnvSchemaField(config)}}).parse(process.env)
`
}

function renderEnvFile(config: Config<NetworkType>, options?: TargetBuildOptions): string {
  return `CLICKHOUSE_URL=http://localhost:${clickhouseDefaults.port}
CLICKHOUSE_DATABASE=${clickhouseDefaults.db}
CLICKHOUSE_USER=${clickhouseDefaults.user}
CLICKHOUSE_PASSWORD=${clickhouseDefaults.password}
${rpcEnvFileLine(config, options?.rpcUrl)}`
}

function renderTargetCode(rendered: RenderedTemplate[]): string {
  return Mustache.render(targetTemplate, {
    inserts: insertEntries(rendered),
    tables: uniqueTables(rendered),
  })
}

function renderMigrationFiles(rendered: RenderedTemplate[]): TargetFile[] {
  return rendered.map(({ template, artifacts }) => ({
    path: `migrations/${template.id}-migration.sql`,
    content: artifacts.clickhouseTable,
  }))
}

export function buildClickhouseTarget(config: Config<NetworkType>, options?: TargetBuildOptions): TargetArtifacts {
  const rendered = renderTemplates(config)

  return {
    targetCode: renderTargetCode(rendered),
    envSchema: renderEnvSchema(config),
    files: [
      { path: '.env', content: renderEnvFile(config, options), preserveExisting: true },
      ...renderMigrationFiles(rendered),
    ],
    postSteps: [],
  }
}
