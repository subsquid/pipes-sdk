import { childLogger, dieNow, journalTo } from '../kill/child.js'
import { type RequestJournalEntry, type RequestKillRunOptions, armRequestKillSwitch } from './kill-switch.js'

/**
 * The child process of a killed class-A run (`runKilled`): a scenario's pipe into ClickHouse,
 * armed to die with SIGKILL at one request. Every batch and every fork is journaled before the
 * store sees it, with a synchronous write, so the journal survives the kill.
 */
const { range, url, server, database, application, repair, journal, point } = JSON.parse(
  process.argv[2],
) as RequestKillRunOptions

const record = journalTo<RequestJournalEntry>(journal)

const { killSwitch } = armRequestKillSwitch({
  point,
  kill: (evidence) => {
    record({ killed: point.name, ...evidence })

    return dieNow()
  },
})

const { clickhouseClient, streamClickhouse } = await import('./pipe.js')

const client = clickhouseClient({ server, database, application })

try {
  await streamClickhouse({
    range,
    url,
    client,
    repair,
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
} finally {
  await client.close()
}
