import { type Context, context, trace } from '@opentelemetry/api'

import { type SpanHooks } from '~/core/index.js'

const tracer = trace.getTracer('subsquid-pipes')

function makeHooks(spanCtx: Context): SpanHooks {
  return {
    onStart(name) {
      const otelSpan = tracer.startSpan(name, {}, spanCtx)
      const childCtx = trace.setSpan(spanCtx, otelSpan)
      const childHooks = makeHooks(childCtx)
      return {
        onStart: childHooks.onStart.bind(childHooks),
        onEnd() {
          otelSpan.end()
        },
      }
    },
    onEnd() {},
  }
}

/**
 * Creates {@link SpanHooks} that export profiler spans to an OpenTelemetry-compatible
 * backend (e.g. Jaeger, Tempo, OTLP collector).
 *
 * Pass the result as `profiler` in source options — it is a drop-in replacement for
 * `profiler: true`.
 *
 * @param parentCtx OTEL context to use as parent. Defaults to `context.active()`,
 *                  which picks up any active span from the ambient context.
 *
 * @example
 * // basic usage
 * evmStream({ profiler: opentelemetryProfiler(), ... })
 *
 * @example
 * // attach to an existing distributed trace (e.g. from an HTTP request)
 * evmStream({ profiler: opentelemetryProfiler(requestContext), ... })
 */
export function opentelemetryProfiler(parentCtx?: Context): SpanHooks {
  return makeHooks(parentCtx ?? context.active())
}
