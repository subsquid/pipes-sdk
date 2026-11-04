import { appendFileSync } from 'node:fs'

import { childLogger, dieNow, journalTo } from '../kill/child.js'
import { type CallJournalEntry, type CallKillRunOptions, armCallKillSwitch } from './kill-switch.js'

/**
 * The child process of a killed class-W run (`runKilled`): a scenario's pipe into BigQuery, armed to
 * die with SIGKILL at one client call. Every batch and every fork is journaled before the store sees
 * it, with a synchronous write, so the journal survives the kill; every DML job whose creation
 * resolved leaves its id in the sidecar the same way, because a created job runs on whatever the
 * process does.
 */
const { range, url, projectId, dataset, journal, jobs, point } = JSON.parse(process.argv[2]) as CallKillRunOptions

const record = journalTo<CallJournalEntry>(journal)
const noteJob = (jobId: string) => {
  appendFileSync(jobs, `${jobId}\n`)
}

const { killSwitch } = armCallKillSwitch({
  point,
  kill: (evidence) => {
    record({ killed: point.name, ...evidence })

    return dieNow()
  },
  jobs: noteJob,
})

const { streamBigquery } = await import('./pipe.js')

await streamBigquery({
  range,
  url,
  projectId,
  dataset,
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
