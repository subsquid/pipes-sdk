import { toSnakeCase } from 'drizzle-orm/casing'
import Mustache from 'mustache'

const readmeTemplate = `# {{projectTitle}} - Pipes SDK 🦑

Stream blockchain data from SQD's Portal, transform it however you need, and send it anywhere. Use the built-ins, write your own, or mix both—everything composes together with \`outputs\`.

## Running the pipeline

### Using Docker Compose

\`\`\`bash
docker compose --profile with-pipeline up -d
\`\`\`

### Using Node.js

#### 1. Install dependencies
_If starting a new project using the Pipes CLI, this step is already handled for you_
\`\`\`bash
{{packageManager}} install
\`\`\`

#### 2. Start {{#hasPostgresScripts}}PostgreSQL{{/hasPostgresScripts}} {{^hasPostgresScripts}}ClickHouse{{/hasPostgresScripts}} (Docker)
\`\`\`bash
docker compose up -d
\`\`\`

{{#hasPostgresScripts}}
#### 3. Generate migrations
_If starting a new project using the Pipes CLI, this step is already handled for you_
\`\`\`bash
{{packageManager}} run db:generate
\`\`\`

#### 4. Apply migrations
\`\`\`bash
{{packageManager}} run db:migrate
\`\`\`

#### 5. Start the pipeline
\`\`\`bash
{{packageManager}} run dev
\`\`\`
{{/hasPostgresScripts}}

{{^hasPostgresScripts}}
#### 3. Start the pipeline
\`\`\`bash
{{packageManager}} run dev
\`\`\`
{{/hasPostgresScripts}}
{{#hasRpcFallback}}


## RPC fallback

This pipe is generated with an RPC fallback source: SQD's Portal is the primary
data source, and your RPC endpoint takes over automatically while the Portal is
unavailable. Set the endpoint in \`.env\`:

\`\`\`bash
RPC_URL=https://your-rpc-endpoint.example.com/v2/YOUR_KEY
\`\`\`

\`RPC_URL\` is required — the pipeline refuses to start until it is set. When
running with Docker Compose the variable is passed through to the indexer
container from your shell or the \`.env\` file.
{{/hasRpcFallback}}


## Build and deploy
You can deploy your pipelines built with the Pipes SDK to any cloud provider that supports Node.js.

#### Docker
For providers that support containerization, you can use the \`Dockerfile\` or \`docker-compose.yml\` provided with this template to deploy your pipeline.
\`\`\`bash
docker build -t {{projectName}} . 

# To run the pipeline using docker compose, use the \`--profile with-pipeline\` flag.
docker compose --profile with-pipeline up -d
\`\`\`

#### Node.js
You can also deploy your pipeline that provides node.js runtime, by using the \`package.json\` file provided with this template.
\`\`\`bash
{{packageManager}} build
\`\`\`


## Pipeline metrics
In order to track pipeline metrics the Pipes SDK provides a metrics server along with a UI dashboard.

#### 1. Enable the metrics server in the pipeline's Portal source
Enable the metrics server in the pipeline's Portal source.
\`\`\`ts
import { evmStream, evmEventDecoder } from '@subsquid/pipes/evm'
import { metricsServer } from '@subsquid/pipes/metrics/node'

const pipeline = evmStream({
    id: 'my-pipe',
    source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    /**
     * Enables the metrics server.
     * Default port is 9090, but you can change it by passing a custom port.
     */
    metrics: metricsServer(/* { port: 9091 } */),
    outputs: evmEventDecoder({
        /**
         * \`profiler\` is an optional parameter to attach a specific id to this decoder.
         * Although not required, it's recommended for a better granularity of the metrics collection.
         */
        profiler: { name: 'my-decoder' },
        range: { from: 'latest' },
        events: { /* your events */ },
    }),
})
\`\`\`

#### 2. Run the Pipes UI dashboard
This will start the Pipes UI dashboard at http://localhost:3000. _Learn more about Pipes UI [here](https://docs.sqd.dev/en/sdk/pipes-sdk/evm/guides/basic-development/pipes-ui)_
\`\`\`bash
npx @subsquid/pipes-ui@beta
\`\`\`

#### 3. Custom metrics
Need to track custom metrics? Explore the [custom metrics guide](https://docs.sqd.dev/en/sdk/pipes-sdk/evm/guides/advanced-topics/metrics) in the Pipes SDK docs.


## Going deeper
Check our [documentation](https://docs.sqd.dev/en/sdk/pipes-sdk) to learn more about the Pipes SDK and Portal.
`

interface ReadmeTemplateValues {
  packageManager: string
  projectName: string
  hasPostgresScripts: boolean
  hasRpcFallback: boolean
}

export function renderReadme(values: ReadmeTemplateValues): string {
  return Mustache.render(readmeTemplate, {
    ...values,
    projectTitle: toSnakeCase(values.projectName).replace(/[_-]/g, ' '),
  })
}
