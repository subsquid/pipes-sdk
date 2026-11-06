import { PipelineStoppedError } from '~/core/index.js'

import { childLogger, journalTo } from '../kill/child.js'
import type { Observation } from '../scenarios/erc20.js'
import type { BusAddress } from './pipe.js'
import { streamPubsub } from './pipe.js'

export type StoppedRunEntry = { batch: Observation } | { stopped: string }

/**
 * The child process of a class-C run that a signal from the parent stops: a scenario's pipe to the
 * bus, with the application catching the stop. The process then ends by itself, or not at all if the
 * target left a handle open.
 */
const { range, url, bus, statePath, coldStart, journal } = JSON.parse(process.argv[2]) as {
  range: { from: number; to: number }
  url: string
  bus: BusAddress
  statePath: string
  coldStart: boolean
  journal: string
}

const record = journalTo<StoppedRunEntry>(journal)

try {
  await streamPubsub({
    range,
    url,
    bus,
    statePath,
    coldStart,
    logger: childLogger(),
    observe: (observation) => record({ batch: observation }),
    onFork: () => {},
  })
} catch (error) {
  if (!(error instanceof PipelineStoppedError)) {
    throw error
  }

  record({ stopped: error.signal })
}
