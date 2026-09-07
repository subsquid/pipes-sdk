'use client'

import { ArrowUpRightIcon, Terminal } from 'lucide-react'

import { Alert, AlertDescription, AlertTitle } from '~/components/ui/alert'
import { Code } from '~/components/ui/code'

const DOCS_URL = 'https://beta.docs.sqd.dev'

const example = `import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'
import { metricsServer } from '@subsquid/pipes/metrics/node'

async function cli() {
  const stream = evmStream({
    id: 'erc20-transfers',
    source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs: {
      erc20: evmEventDecoder({
        range: { from: '12,000,000' },
        events: {
          transfers: commonAbis.erc20.events.Transfer,
        },
      }),
    },
    // Enable the metrics server to connect with the Pipe UI dashboard
    metrics: metricsServer(),
  })

  for await (const { data } of stream) {
    console.log(\`parsed \${data.erc20.transfers.length} transfers\`)
  }
}

void cli()`

export function PipelineDisconnected() {
  return (
    <div className="flex-1">
      <Alert variant="destructive">
        <Terminal />
        <AlertTitle>Your pipe is offline!</AlertTitle>
        <AlertDescription>Please ensure that you run a pipeline and expose the metrics server</AlertDescription>
      </Alert>

      <div className="mt-10">
        <h1 className="text-lg font-semibold tracking-tight">Get started with Pipes SDK</h1>

        <div className="mt-6">
          <h4 className="mb-2 text-sm text-muted-foreground font-medium">1. Install npm package</h4>
          <Code language="bash">npm install @subsquid/pipes</Code>
        </div>

        <div className="mt-6">
          <h4 className="mb-2 text-sm text-muted-foreground font-medium">2. Run a simple pipe</h4>
          <Code language="typescript">{example}</Code>
        </div>

        <div className="mt-6">
          <h4 className="text-sm text-muted-foreground font-medium">
            3. Explore the{' '}
            <a
              href={`${DOCS_URL}/en/sdk/pipes-sdk/quickstart`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-400 underline underline-offset-4 hover:text-blue-300"
            >
              documentation
              <ArrowUpRightIcon className="inline h-3.5 w-3.5 ml-0.5 -translate-y-px" />
            </a>
          </h4>
        </div>
      </div>
    </div>
  )
}
