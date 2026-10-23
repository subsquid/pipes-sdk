import { appendFileSync, readFileSync } from 'node:fs'
import path from 'node:path'

import { destination, pino } from 'pino'

import { type JournalEntry, type KillRunOptions, armKillSwitch } from './kill-switch.js'

/**
 * The child process of a killed run (`runKilled`): S1's pipe, armed to die with SIGKILL at one
 * kill point. Nothing unwinds after the kill, so the store holds what the kill left, as after a
 * real crash. Every batch is journaled before the store sees it, with a synchronous write, so the
 * journal survives the kill and the parent's oracle can follow the run.
 */
const { url, dir, journal, state, point } = JSON.parse(process.argv[2]) as KillRunOptions

const record = (entry: JournalEntry) => {
  appendFileSync(journal, `${JSON.stringify(entry)}\n`)
}

// IB-22: the cursor of the state record is the store's commit point.
const committed = () => {
  let raw: string
  try {
    raw = readFileSync(path.join(dir, state), 'utf8')
  } catch {
    return undefined
  }

  return (JSON.parse(raw) as { cursor: { number: number } }).cursor.number
}

armKillSwitch({
  dir,
  point,
  committed,
  kill: () => {
    record({ killed: point.name })
    process.kill(process.pid, 'SIGKILL')

    // SIGKILL lands before anything else runs; blocking here keeps it that way.
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0)
    throw new Error('unreachable')
  },
})

// Imported once the switch is armed, so nothing in the pipe holds an unwrapped file operation.
const { streamS1 } = await import('./s1.js')

// Synchronous, so a line logged before the kill is not lost with it.
const logger = pino(
  { level: process.env['LOG_LEVEL'] || 'info', messageKey: 'message', base: {} },
  destination({ fd: 2, sync: true }),
)

await streamS1({ url, dir, logger, observe: (observation) => record({ batch: observation }) })
