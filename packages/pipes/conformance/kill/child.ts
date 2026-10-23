import { appendFileSync } from 'node:fs'

import { destination, pino } from 'pino'

/**
 * What a killed run's child process uses around its pipe. Everything here writes synchronously,
 * so a line written before the kill is not lost with it.
 */

/** Kills this process with SIGKILL, so nothing unwinds and a store holds what a real crash leaves. */
export function dieNow(): never {
  process.kill(process.pid, 'SIGKILL')

  // SIGKILL lands before anything else runs; blocking here keeps it that way.
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0)
  throw new Error('unreachable')
}

/** Appends one NDJSON line per entry to `journal`, which the parent reads once the child is dead. */
export function journalTo<Entry>(journal: string): (entry: Entry) => void {
  return (entry) => {
    appendFileSync(journal, `${JSON.stringify(entry)}\n`)
  }
}

export function childLogger() {
  return pino(
    { level: process.env['LOG_LEVEL'] || 'info', messageKey: 'message', base: {} },
    destination({ fd: 2, sync: true }),
  )
}
