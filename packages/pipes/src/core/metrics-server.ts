import { BatchContext } from '~/core/portal-source.js'

type Aggregator = 'omit' | 'sum' | 'first' | 'min' | 'max' | 'average'
type CollectFunction<T> = (this: T) => void | Promise<void>

interface MetricConfiguration<T extends string> {
  name: string
  help: string
  labelNames?: T[] | readonly T[]
  aggregator?: Aggregator
  collect?: CollectFunction<any>
  enableExemplars?: boolean
}
type LabelValues<T extends string> = Partial<Record<T, string | number>>

export interface Counter<T extends string = string> {
  inc(labels: LabelValues<T>, value: number): void
  inc(value: number): void
}
export interface CounterConfiguration<T extends string> extends MetricConfiguration<T> {
  collect?: CollectFunction<Counter<T>>
}

export interface Gauge<T extends string = string> {
  set(value: number): void
  set(labels: LabelValues<T>, value: number): void
  /**
   * Clear all label combinations — call before re-setting in `collect` to drop stale series.
   * Optional so a custom `MetricsServer` that predates it still satisfies the interface; callers must
   * invoke it with optional chaining. Without it, stale label series simply aren't pruned.
   */
  reset?(): void
}
export interface GaugeConfiguration<T extends string> extends MetricConfiguration<T> {
  collect?: CollectFunction<Gauge<T>>
}

export interface Histogram<T extends string = string> {
  observe(value: number): void
  observe(labels: LabelValues<T>, value: number): void
}
export interface HistogramConfiguration<T extends string> extends MetricConfiguration<T> {
  buckets?: number[]
  collect?: CollectFunction<Histogram<T>>
}

export interface Summary<T extends string> {
  observe(value: number): void
  observe(labels: LabelValues<T>, value: number): void
}
export interface SummaryConfiguration<T extends string> extends MetricConfiguration<T> {
  percentiles?: number[]
  maxAgeSeconds?: number
  ageBuckets?: number
  pruneAgedBuckets?: boolean
  compressCount?: number
  collect?: CollectFunction<Summary<T>>
}

export type Metrics = {
  counter<T extends string>(options: CounterConfiguration<T>): Counter<T>
  gauge<T extends string>(options: GaugeConfiguration<T>): Gauge<T>
  histogram<T extends string>(options: HistogramConfiguration<T>): Histogram<T>
  summary<T extends string>(options: SummaryConfiguration<T>): Summary<T>
}

export type MetricsServer = {
  start(): void
  stop(): Promise<void>
  registerPipe(id: string): void
  batchProcessed(ctx: BatchContext): void
  metrics: Metrics
}

class NoopCounter<T extends string> implements Counter<T> {
  inc(): void {}
}
class GaugeNoop<T extends string> implements Gauge<T> {
  set(): void {}
  reset(): void {}
}
class HistogramNoop<T extends string> implements Histogram<T> {
  observe(): void {}
}
class NoopSummary<T extends string> implements Summary<T> {
  observe(): void {}
}

export function noopMetricsServer(): MetricsServer {
  const metrics = {
    counter<T extends string>(_options: CounterConfiguration<T>): Counter<T> {
      return new NoopCounter()
    },
    gauge<T extends string>(_options: GaugeConfiguration<T>): Gauge<T> {
      return new GaugeNoop()
    },
    histogram<T extends string>(options: HistogramConfiguration<T>): Histogram<T> {
      return new HistogramNoop()
    },
    summary<T extends string>(_options: SummaryConfiguration<T>): Summary<T> {
      return new NoopSummary()
    },
  }

  return {
    start() {},
    async stop() {},
    registerPipe: () => {},
    batchProcessed() {},
    metrics,
  }
}
