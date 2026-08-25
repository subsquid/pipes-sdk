import Mustache from 'mustache'

import { PackageManager, Target } from '~/types/init.js'

interface DbConfig {
  user: string
  password: string
  db: string
  port: string
}

export const postgresDefaults: DbConfig = {
  user: 'postgres',
  password: 'password',
  db: 'pipes',
  port: '5432',
}

export const clickhouseDefaults: DbConfig = {
  user: 'default',
  password: 'password',
  db: 'pipes',
  port: '8123',
}

const indexerService = `{{projectName}}:
    build:
      context: .
      dockerfile: Dockerfile
    environment:
    {{#isPostgres}}
      DB_CONNECTION_STR: postgresql://{{user}}:{{password}}@postgres:{{port}}/{{db}}
    {{/isPostgres}}
    {{^isPostgres}}
      CLICKHOUSE_URL: http://clickhouse:{{port}}
      CLICKHOUSE_DATABASE: {{db}}
      CLICKHOUSE_USER: {{user}}
      CLICKHOUSE_PASSWORD: {{password}}
    {{/isPostgres}}
    {{#hasRpcFallback}}
      RPC_URL: \${RPC_URL:-}
    {{/hasRpcFallback}}
    command: ["sh", "-lc", "{{#isPostgres}}{{packageManager}} run db:generate && {{packageManager}} run db:migrate && {{/isPostgres}}node dist/index.js"]
    depends_on:
      {{#isPostgres}}postgres{{/isPostgres}}{{^isPostgres}}clickhouse{{/isPostgres}}:
        condition: service_healthy
    restart: unless-stopped
    profiles: ["with-pipeline"]`

const clickhouseService = `clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "{{port}}:{{port}}"
    environment:
      CLICKHOUSE_DB: {{db}}
      CLICKHOUSE_USER: {{user}}
      CLICKHOUSE_PASSWORD: {{password}}
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 3s
      timeout: 5s
      retries: 5`

const postgresService = `postgres:
    image: postgres:latest
    environment:
      POSTGRES_USER: {{user}}
      POSTGRES_PASSWORD: {{password}}
      POSTGRES_DB: {{db}}
    ports:
      - "{{port}}:{{port}}"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 3s
      timeout: 5s
      retries: 5`

const dockerComposeTemplate = `services:
{{#services}}
  {{{.}}}

{{/services}}`

interface DockerComposeTemplateValues {
  projectName: string
  target: Target
  packageManager: PackageManager
  hasRpcFallback: boolean
}

export function renderDockerCompose(values: DockerComposeTemplateValues): string {
  const isPostgres = values.target === 'postgresql'

  const indexer = Mustache.render(indexerService, {
    ...values,
    ...(values.target === 'postgresql' ? postgresDefaults : clickhouseDefaults),
    isPostgres,
  })

  const db = isPostgres
    ? Mustache.render(postgresService, postgresDefaults)
    : Mustache.render(clickhouseService, clickhouseDefaults)

  return Mustache.render(dockerComposeTemplate, { services: [indexer, db] })
}
