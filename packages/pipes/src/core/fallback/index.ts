export type { CapabilityProbeOptions, ProbeResult } from './capability.js'
export type { FallbackClientOptions, FallbackClientSource, FallbackMetrics } from './client.js'
export { FallbackClient } from './client.js'
export type { FailedCheck, FailureReason, SourceErrorInfo } from './diagnostics.js'
export type { FallbackDetectionOptions, FallbackHealth } from './health.js'
export { AllSourcesDownError } from './health.js'
export type {
  DefaultFallbackStrategyOptions,
  FallbackCommand,
  FallbackEvent,
  FallbackSourceSnapshot,
  FallbackStrategy,
  FallbackStrategyContext,
} from './strategy.js'
