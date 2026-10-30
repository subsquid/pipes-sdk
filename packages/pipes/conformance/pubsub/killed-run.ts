import { childLogger, dieNow, journalTo } from '../kill/child.js'
import { type StepJournalEntry, type StepKillRunOptions, armStepKillSwitch } from './kill-switch.js'

/**
 * The child process of a killed class-C run (`runKilled`): a scenario's pipe to the bus, armed to
 * die with SIGKILL at one step. What the bus took stays with the parent, and the state file stays
 * as the kill left it. Every batch and every fork is journaled before the target sees it, with a
 * synchronous write, so the journal survives the kill.
 */
const { range, url, bus, statePath, coldStart, journal, point } = JSON.parse(process.argv[2]) as StepKillRunOptions

const record = journalTo<StepJournalEntry>(journal)

const { killSwitch } = armStepKillSwitch({
  point,
  kill: (evidence) => {
    record({ killed: point.name, ...evidence })

    return dieNow()
  },
})

const { streamPubsub } = await import('./pipe.js')

await streamPubsub({
  range,
  url,
  bus,
  statePath,
  coldStart,
  logger: childLogger(),
  observe: (observation) => {
    record({ batch: observation })
    killSwitch.batch(observation.current.number)
  },
  onFork: (canonical) => {
    record({ fork: canonical })
    killSwitch.fork()
  },
})
