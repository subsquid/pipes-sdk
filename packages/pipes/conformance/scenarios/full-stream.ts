import type { BlockCursor } from '~/core/index.js'
import type { MockChainPortal } from '~/testing/index.js'

import {
  type Cursor,
  type DurabilityClass,
  type OracleRow,
  type OracleState,
  ReferenceModel,
} from '../oracle/reference-model.js'
import { Replay } from '../oracle/replay.js'
import type { BlockRange } from '../oracle/validators.js'
import { RANGES, S1, type S1Simulator, S1_ROWS, endStateOf, follow, rowsAt } from './backfill.js'
import type { Observation } from './erc20.js'
import { S3, type S3Simulator, S3_RANGES, S3_ROWS, s3EndState, s3Oracle, s3RowsUpTo } from './fork.js'

/** The classes that read the full stream and commit at every batch: T and A. */
export type FullStreamClass = Extract<DurabilityClass, 'T' | 'A'>

/** Feeds the oracle a run's batches and forks, as the pipe met them. */
export type Follower = {
  batch(observation: Observation): void
  fork(canonical: BlockCursor[]): void
}

/** What a run of a full-stream class needs to know of its scenario. */
export type FullStreamScenario<Sim extends { chain: MockChainPortal }> = {
  range: { from: number; to: number }
  ranges: BlockRange[]
  oracle: () => ReferenceModel
  /** The state and rows a finished run leaves, from the scenario and the simulator alone. */
  end: (simulator: Sim) => { state: OracleState; rows: OracleRow[] }
  /** The rows a store holds whose cursor is `cursor`, from the scenario alone. */
  rowsUpTo: (cursor: Cursor | undefined) => OracleRow[]
  /** A follower for one run, whose first request is `from` in the portal's log. */
  follower: (model: ReferenceModel, simulator: Sim, from: number) => Follower
}

/** S1 as a full-stream class: one commit per batch, no fork. */
export function s1FullStream(durability: FullStreamClass): FullStreamScenario<S1Simulator> {
  return {
    range: { from: S1.from, to: S1.to },
    ranges: RANGES,
    oracle: () =>
      new ReferenceModel({ durability, mode: 'full', ranges: RANGES, transform: (block) => rowsAt(block.number) }),
    end: (simulator) => ({ state: endStateOf(simulator), rows: S1_ROWS }),
    rowsUpTo: (cursor) => S1_ROWS.filter((row) => row.block <= (cursor?.number ?? -1)),
    follower: (model, simulator) => ({
      batch: (observation) => follow(model, simulator, observation),
      fork: () => {
        throw new Error('S1 takes no fork')
      },
    }),
  }
}

/** S3 as a full-stream class: an unfinalized tail, a floor that rises once, one fork. */
export function s3FullStream(durability: FullStreamClass): FullStreamScenario<S3Simulator> {
  return {
    range: { from: S3.from, to: S3.to },
    ranges: S3_RANGES,
    oracle: () => s3Oracle(durability),
    end: () => ({ state: s3EndState(), rows: S3_ROWS }),
    rowsUpTo: s3RowsUpTo,
    follower: (model, simulator, from) => new Replay(model, simulator.chain.requests, from),
  }
}
