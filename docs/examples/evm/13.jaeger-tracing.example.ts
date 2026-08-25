/**
 * Example: Exporting traces to Jaeger
 *
 * This example shows how to configure OpenTelemetry to export spans to Jaeger
 * via the OTLP HTTP exporter (Jaeger supports OTLP natively since v1.35+).
 * Use `otelProfilerHooks()` as a drop-in replacement for `profiler: true`.
 *
 * Setup:
 *   pnpm add @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
 *
 * Run Jaeger locally:
 * docker run --rm --name jaeger \
 *   -p 16686:16686 \
 *   -p 4317:4317 \
 *   -p 4318:4318 \
 *   -p 5778:5778 \
 *   -p 9411:9411 \
 *   cr.jaegertracing.io/jaegertracing/jaeger:2.15.0
 *
 * Then open http://localhost:16686 and search for the "my-pipe" service.
 */

import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http'
import { NodeSDK } from '@opentelemetry/sdk-node'
import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'
import { opentelemetryProfiler } from '@subsquid/pipes/opentelemetry'

// ─── 1. Bootstrap OTEL SDK ────────────────────────────────────────────────────

const sdk = new NodeSDK({
  serviceName: 'my-pipe',
  traceExporter: new OTLPTraceExporter({
    // Jaeger OTLP HTTP endpoint (default port 4318)
    url: 'http://localhost:4318/v1/traces',
  }),
})

sdk.start()

// ─── 2. Run the pipe ──────────────────────────────────────────────────────────

async function cli() {
  const stream = evmStream({
    id: 'jaeger-tracing',
    source: 'https://portal.sqd.dev/datasets/arbitrum-one',
    // Optionally pass an OTEL context to attach pipe spans to an existing trace:
    //   profiler: opentelemetryProfiler(requestContext)
    profiler: opentelemetryProfiler(),
    outputs: evmEventDecoder({
      range: { from: 'latest' },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
    }),
  })

  for await (const { data } of stream) {
    console.log(data.transfers.length)
  }

  // flush remaining spans before the process exits
  await sdk.shutdown()
}

void cli()
