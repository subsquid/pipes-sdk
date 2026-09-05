/** How long a phase may run before the first report. */
const DEFAULT_THRESHOLD_MS = 60_000

/** Upper bound on the gap between repeated reports of the same stall. */
const DEFAULT_MAX_INTERVAL_MS = 15 * 60_000

export type StallReport = {
  /** What the watched code was doing when the clock started. */
  phase: string
  elapsedMs: number
  /** 1 for the first report of this stall, incremented on every repeat. */
  count: number
}

export type StallWatchdogOptions = {
  thresholdMs?: number
  maxIntervalMs?: number
  onStall: (report: StallReport) => void
  /** Called once when a phase that had already reported a stall finally ends. */
  onRecover?: (report: StallReport) => void
}

export interface StallWatchdog {
  /** Start (or restart) the clock for a new phase. */
  begin(phase: string): void
  /** Stop the clock without starting a new phase. */
  end(): void
}

/**
 * Reports code that is taking too long without settling.
 *
 * The gap between reports doubles after every one, up to `maxIntervalMs`, so a wedge that
 * never clears costs a handful of lines an hour rather than one per tick — the whole point
 * is to be readable in a log that nobody is watching at the moment it happens.
 */
export function stallWatchdog({
  thresholdMs = DEFAULT_THRESHOLD_MS,
  maxIntervalMs = DEFAULT_MAX_INTERVAL_MS,
  onStall,
  onRecover,
}: StallWatchdogOptions): StallWatchdog {
  let timer: NodeJS.Timeout | undefined
  let phase = ''
  let startedAt = 0
  let gap = thresholdMs
  let reports = 0

  function arm() {
    timer = setTimeout(fire, gap)
    // A watchdog is never a reason to keep the process alive.
    timer.unref?.()
  }

  function fire() {
    reports += 1
    onStall({ phase, elapsedMs: Date.now() - startedAt, count: reports })

    gap = Math.min(gap * 2, maxIntervalMs)
    arm()
  }

  function settle() {
    if (timer) {
      clearTimeout(timer)
      timer = undefined
    }

    if (reports > 0) {
      onRecover?.({ phase, elapsedMs: Date.now() - startedAt, count: reports })
    }

    reports = 0
    gap = thresholdMs
  }

  return {
    begin(next: string) {
      settle()

      phase = next
      startedAt = Date.now()
      arm()
    },

    end() {
      settle()
      phase = ''
    },
  }
}
