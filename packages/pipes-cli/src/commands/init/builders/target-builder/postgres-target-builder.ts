import Mustache from 'mustache'

import type { Config, NetworkType } from '~/types/init.js'

import { postgresDefaults } from '../../templates/config-files/dynamic/docker-compose.js'
import { drizzleConfigTemplate } from '../../templates/config-files/static/drizzle-config.js'
import { type RenderedTemplate, renderTemplates } from '../render-templates.js'
import { renderSchemasTemplate } from '../schema-builder/index.js'
import type { TargetArtifacts } from './target-artifacts.js'
import { insertEntries, uniqueSchemaNames } from './target-tables.js'

const targetTemplate = `
import { chunkForInsert, drizzleTarget } from '@subsquid/pipes/targets/drizzle/node-postgres'
import { drizzle } from 'drizzle-orm/node-postgres'
import {
  {{#schemaNames}}
  {{.}},
  {{/schemaNames}}
} from './schemas.js'

drizzleTarget({
    db: drizzle(env.DB_CONNECTION_STR),
    tables: [
      {{#schemaNames}}
      {{.}},
      {{/schemaNames}}
    ],
    onData: async ({ tx, data }) => {
    {{#inserts}}
      for (const values of chunkForInsert(data.{{{dataPath}}})) {
        await tx.insert({{schemaName}}).values(values)
      }
    {{/inserts}}
    },
  })`

const envSchema = `
import { z } from 'zod'

const env = z.object({
  DB_CONNECTION_STR: z.string(),
}).parse(process.env)
`

function renderTargetCode(rendered: RenderedTemplate[]): string {
  return Mustache.render(targetTemplate, {
    inserts: insertEntries(rendered),
    schemaNames: uniqueSchemaNames(rendered),
  })
}

function renderEnvFile(): string {
  return `DB_CONNECTION_STR=postgresql://${postgresDefaults.user}:${postgresDefaults.password}@localhost:${postgresDefaults.port}/${postgresDefaults.db}\n`
}

function renderSchemasFile(rendered: RenderedTemplate[]): string {
  return renderSchemasTemplate(rendered.map(({ artifacts }) => artifacts.postgresSchema))
}

export function buildPostgresTarget(config: Config<NetworkType>): TargetArtifacts {
  const rendered = renderTemplates(config)

  return {
    targetCode: renderTargetCode(rendered),
    envSchema,
    files: [
      { path: '.env', content: renderEnvFile(), preserveExisting: true },
      { path: 'src/schemas.ts', content: renderSchemasFile(rendered) },
      { path: 'drizzle.config.ts', content: drizzleConfigTemplate },
    ],
    postSteps: [{ kind: 'exec', command: `${config.packageManager} run db:generate` }],
  }
}
