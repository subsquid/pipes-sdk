import { readFileSync } from 'node:fs'
import path from 'node:path'

import { childLogger, dieNow, journalTo } from '../kill/child.js'
import { type JournalEntry, type KillRunOptions, armKillSwitch } from './kill-switch.js'

/**
 * The child process of a killed run (`runKilled`): a scenario's pipe into Parquet, armed to
 * die with SIGKILL at one kill point. Nothing unwinds after the kill, so the store holds what the
 * kill left, as after a real crash. Every batch is journaled before the store sees it, with a
 * synchronous write, so the journal survives the kill and the parent's oracle can follow the run.
 */
const { range, url, dir, journal, state, point } = JSON.parse(process.argv[2]) as KillRunOptions

const record = journalTo<JournalEntry>(journal)

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

    return dieNow()
  },
})

// Imported once the switch is armed, so nothing in the pipe holds an unwrapped file operation.
const { streamParquet } = await import('./pipe.js')

await streamParquet({
  range,
  url,
  dir,
  logger: childLogger(),
  observe: (observation) => record({ batch: observation }),
})
