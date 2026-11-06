import { PipelineStoppedError, createTarget } from '~/core/index.js'
import { evmPortalStream } from '~/evm/index.js'
import { blockDecoder } from '~/testing/index.js'

import { journalTo } from '../kill/child.js'

export type RunMode = {
  /** The application: `plain` awaits the pipe and nothing else, `catching` handles what it rejects with. */
  application: 'plain' | 'catching' | 'staying-alive'
  /** `false` opts out. Otherwise the timeout of the pipe. */
  shutdown: false | string
  /** The target's commit never ends. */
  wedged?: boolean
}

export type RunEntry =
  | { committed: number }
  | { started: true }
  | { callback: true }
  | { completed: true }
  | { rejected: string; signal?: string }

/**
 * The child process of a stopped run: a pipe that a real signal stops. Everything goes to the journal
 * with a synchronous write. `completed` is the code after `await pipeTo(...)`.
 */
const { url, journal, to, mode } = JSON.parse(process.argv[2]) as {
  url: string
  journal: string
  to: number
  mode: RunMode
}

const record = journalTo<RunEntry>(journal)

const target = createTarget<any>({
  write: async ({ read }) => {
    record({ started: true })

    for await (const { ctx } of read()) {
      if (mode.wedged) {
        record({ committed: -1 })
        await new Promise(() => {})
      }

      record({ committed: ctx.stream.state.current.number })
    }
  },
})

const shutdown =
  mode.shutdown === false
    ? false
    : {
        timeout: mode.shutdown,
        onShutdown: () => {
          record({ callback: true })
        },
      }

const run = evmPortalStream({
  id: 'stopped-run',
  portal: url,
  logger: 'silent',
  outputs: blockDecoder({ from: 1, to }),
}).pipeTo(target, { shutdown })

// A wedged target has no handle that would keep the process running.
const keepAlive = mode.wedged || mode.application === 'staying-alive' ? setInterval(() => {}, 1000) : undefined

switch (mode.application) {
  case 'plain':
    await run
    record({ completed: true })
    break

  case 'catching':
    try {
      await run
      record({ completed: true })
    } catch (error) {
      if (!(error instanceof PipelineStoppedError)) {
        throw error
      }

      record({ rejected: error.code, signal: error.signal })
    }
    break

  case 'staying-alive':
    await run.catch((error) => {
      record({ rejected: error.code, signal: error.signal })
    })
    break
}

if (mode.application !== 'staying-alive') {
  clearInterval(keepAlive)
}
