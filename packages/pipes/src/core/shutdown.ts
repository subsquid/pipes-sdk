import { parseDuration } from '~/internal/validation/parse-duration.js'

import {
  PipelineShutdownTimeoutError,
  PipelineStoppedError,
  ShutdownConfigurationError,
  type ShutdownStep,
} from './errors.js'
import type { Logger } from './logger.js'

export type ShutdownOptions = {
  /**
   * The total budget from the first signal: draining, resource cleanup and `onShutdown`.
   * A duration such as `'30s'`, `'1m'` or `'2m 30s'`. Keep it below the deadline of whatever
   * sends the signal, which ends the process with SIGKILL.
   * @default '30s'
   */
  timeout?: string
  /**
   * Runs once after the pipe has drained and released its own resources, also when that failed.
   * It runs on a signal only, not when the range completes.
   */
  onShutdown?: () => unknown
}

/** A function is shorthand for `{ onShutdown: fn }`. `false` leaves shutdown to the application. */
export type ShutdownSetting = false | (() => unknown) | ShutdownOptions

export type PipeToOptions = {
  shutdown?: ShutdownSetting
}

export type ResolvedShutdown = {
  timeout: string
  timeoutMs: number
  onShutdown?: () => unknown
}

const DEFAULT_TIMEOUT = '30s'

/** Reads the `shutdown` option of `pipeTo`. Returns `false` for a pipe that opted out. */
export function resolveShutdown(setting: ShutdownSetting | undefined): ResolvedShutdown | false {
  if (setting === false) {
    return false
  }

  const options: ShutdownOptions = typeof setting === 'function' ? { onShutdown: setting } : (setting ?? {})
  if (typeof options !== 'object' || Array.isArray(options)) {
    throw new ShutdownConfigurationError(
      `Invalid shutdown option: ${String(setting)}. Pass a function, an options object or false.`,
    )
  }

  const { timeout = DEFAULT_TIMEOUT, onShutdown } = options

  const timeoutMs = parseDuration(timeout)
  if (timeoutMs === undefined) {
    throw new ShutdownConfigurationError(
      `Invalid shutdown timeout: ${JSON.stringify(timeout)}. Use a positive duration such as '30s', '1m' or '2m 30s'.`,
    )
  }

  if (onShutdown !== undefined && typeof onShutdown !== 'function') {
    throw new ShutdownConfigurationError('Invalid onShutdown option: it must be a function.')
  }

  return { timeout, timeoutMs, onShutdown }
}

type StopRequest = { signal: NodeJS.Signals; at: number }

const SIGNALS: NodeJS.Signals[] = ['SIGTERM', 'SIGINT']
let controller = new AbortController()
let activeRuns = 0

function onSignal(signal: NodeJS.Signals) {
  if (controller.signal.aborted) return
  controller.abort(Object.assign(new DOMException('The pipe is stopping', 'AbortError'), { signal, at: Date.now() }))
}

function acquireSignal() {
  if (activeRuns++ === 0 && typeof process !== 'undefined' && typeof process.on === 'function') {
    for (const signal of SIGNALS) {
      process.on(signal, onSignal)
    }
  }

  return controller.signal
}

function releaseSignal() {
  if (--activeRuns > 0) return

  if (typeof process !== 'undefined' && typeof process.off === 'function') {
    for (const signal of SIGNALS) {
      process.off(signal, onSignal)
    }
  }
  controller = new AbortController()
}

/**
 * Runs a pipe under the shared signal listeners. A signal aborts the `run` signal, then waits for
 * `run` to settle and for `onShutdown`. A deadline rejects without ending the process (ADR-4, ADR-24).
 * Listeners are removed once no managed runs remain.
 */
export async function withShutdown(
  { timeout, timeoutMs, onShutdown }: ResolvedShutdown,
  logger: Logger,
  run: (signal: AbortSignal) => unknown,
): Promise<void> {
  const signal = acquireSignal()
  let timer: NodeJS.Timeout | undefined
  let timedOut = false
  let step: ShutdownStep = 'drain'
  const errors: unknown[] = []

  let stop = () => {}
  const deadline = new Promise<never>((_, reject) => {
    stop = () => {
      const request: StopRequest = signal.reason
      logger.info(`received ${request.signal}, stopping the pipe (timeout ${timeout})`)
      timer = setTimeout(
        () => {
          timedOut = true
          logger.error(`the pipe did not shut down within ${timeout}: the ${step} step is still running`)
          reject(new PipelineShutdownTimeoutError({ signal: request.signal, timeout, step, cause: errors[0] }))
        },
        Math.max(request.at + timeoutMs - Date.now(), 0),
      )
    }

    if (signal.aborted) stop()
    else signal.addEventListener('abort', stop, { once: true })
  })

  try {
    const finished = (async () => {
      try {
        await run(signal)
      } catch (error) {
        errors.push(error)
        if (timedOut) logger.error({ message: 'the drain step failed after the shutdown timeout', error })
      }

      if (signal.aborted) {
        step = 'onShutdown'
        try {
          await onShutdown?.()
        } catch (error) {
          errors.push(error)
          if (timedOut) logger.error({ message: 'the onShutdown step failed after the shutdown timeout', error })
        }
      }

      if (errors.length === 1) throw errors[0]
      if (errors.length > 1) {
        throw new AggregateError(errors, 'The pipe failed to drain, and onShutdown failed as well')
      }
      if (signal.aborted) {
        const request: StopRequest = signal.reason
        throw new PipelineStoppedError(request.signal)
      }
    })()

    await Promise.race([finished, deadline])
  } finally {
    clearTimeout(timer)
    signal.removeEventListener('abort', stop)
    releaseSignal()
  }
}
