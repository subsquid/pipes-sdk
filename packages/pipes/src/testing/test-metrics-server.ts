import {
  Counter,
  CounterConfiguration,
  Gauge,
  GaugeConfiguration,
  Histogram,
  HistogramConfiguration,
  MetricsServer,
  Summary,
  SummaryConfiguration,
} from '~/core/metrics-server.js'
import { BatchContext } from '~/core/portal-source.js'

type MetricCall = { labels?: Record<string, string | number>; value: number }

export class MockCounter<T extends string = string> implements Counter<T> {
  calls: MetricCall[] = []

  inc(labelsOrValue?: any, value?: number): void {
    if (typeof labelsOrValue === 'number') {
      this.calls.push({ value: labelsOrValue })
    } else {
      this.calls.push({ labels: labelsOrValue, value: value! })
    }
  }

  get total(): number {
    return this.calls.reduce((sum, c) => sum + c.value, 0)
  }
}

export class MockGauge<T extends string = string> implements Gauge<T> {
  calls: MetricCall[] = []

  set(labelsOrValue?: any, value?: number): void {
    if (typeof labelsOrValue === 'number') {
      this.calls.push({ value: labelsOrValue })
    } else {
      this.calls.push({ labels: labelsOrValue, value: value! })
    }
  }

  reset(): void {
    this.calls = []
  }

  get lastValue(): number | undefined {
    return this.calls[this.calls.length - 1]?.value
  }
}

export class MockHistogram<T extends string = string> implements Histogram<T> {
  calls: MetricCall[] = []

  observe(labelsOrValue: any, value?: number): void {
    if (typeof labelsOrValue === 'number') {
      this.calls.push({ value: labelsOrValue })
    } else {
      this.calls.push({ labels: labelsOrValue, value: value! })
    }
  }

  get observations(): number[] {
    return this.calls.map((call) => call.value)
  }
}

class MockSummary<T extends string = string> implements Summary<T> {
  observations: MetricCall[] = []

  observe(labelsOrValue?: any, value?: number): void {
    if (typeof labelsOrValue === 'number') {
      this.observations.push({ value: labelsOrValue })
    } else {
      this.observations.push({ labels: labelsOrValue, value: value! })
    }
  }
}

export function mockMetricsServer() {
  const registered = new Map<string, any>()

  const metrics = {
    counter<T extends string>(options: CounterConfiguration<T>): Counter<T> {
      const existing = registered.get(options.name)
      if (existing) return existing
      const metric = new MockCounter<T>()
      registered.set(options.name, metric)
      return metric
    },
    gauge<T extends string>(options: GaugeConfiguration<T>): Gauge<T> {
      const existing = registered.get(options.name)
      if (existing) return existing
      const metric = new MockGauge<T>()
      registered.set(options.name, metric)
      return metric
    },
    histogram<T extends string>(options: HistogramConfiguration<T>): Histogram<T> {
      const existing = registered.get(options.name)
      if (existing) return existing
      const metric = new MockHistogram<T>()
      registered.set(options.name, metric)
      return metric
    },
    summary<T extends string>(options: SummaryConfiguration<T>): Summary<T> {
      const existing = registered.get(options.name)
      if (existing) return existing
      const metric = new MockSummary<T>()
      registered.set(options.name, metric)
      return metric
    },
  }

  const server: MetricsServer = {
    start() {},
    async stop() {},
    registerPipe() {},
    batchProcessed(_ctx: BatchContext) {},
    metrics,
  }

  return {
    server,
    counter(name: string): MockCounter {
      return registered.get(name) as MockCounter
    },
    gauge(name: string): MockGauge {
      return registered.get(name) as MockGauge
    },
    histogram(name: string): MockHistogram {
      return registered.get(name) as MockHistogram
    },
    summary(name: string): MockSummary {
      return registered.get(name) as MockSummary
    },
    keys(): string[] {
      return [...registered.keys()]
    },
  }
}
