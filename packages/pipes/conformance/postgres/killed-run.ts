import { childLogger, dieNow, journalTo } from '../kill/child.js'
import { type StatementJournalEntry, type StatementKillRunOptions, armStatementKillSwitch } from './kill-switch.js'

/**
 * The child process of a killed class-T run (`runKilled`): a scenario's pipe into Postgres,
 * armed to die with SIGKILL at one statement. The server sees the connection drop and aborts
 * whatever transaction it held open, as after a real crash. Every batch and every fork is
 * journaled before the store sees it, with a synchronous write, so the journal survives the kill.
 */
const { range, url, dsn, schema, application, journal, point } = JSON.parse(process.argv[2]) as StatementKillRunOptions

const record = journalTo<StatementJournalEntry>(journal)

armStatementKillSwitch({
  point,
  kill: (evidence) => {
    record({ killed: point.name, ...evidence })

    return dieNow()
  },
})

const { postgresPool, streamPostgres } = await import('./pipe.js')

const pool = postgresPool({ dsn, schema, application })

try {
  await streamPostgres({
    range,
    url,
    pool,
    schema,
    logger: childLogger(),
    observe: (observation) => record({ batch: observation }),
    onFork: (canonical) => record({ fork: canonical }),
  })
} finally {
  await pool.end()
}
