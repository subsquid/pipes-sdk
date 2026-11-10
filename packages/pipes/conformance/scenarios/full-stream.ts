import type { BlockCursor } from '~/core/index.js'
import type { EvmFallbackOptions, EvmSourceSpec } from '~/evm/evm-fallback.js'
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
import { S3, type S3Options, type S3Simulator, S3_RANGES, s3EndState, s3Oracle, s3Rows, s3RowsUpTo } from './fork.js'

/** The classes that read the full stream and commit at every batch: T, W, A and C. */
export type FullStreamClass = Extract<DurabilityClass, 'T' | 'W' | 'A' | 'C'>

/** Feeds the oracle a run's batches and forks, as the pipe met them. */
export type Follower = {
  batch(observation: Observation): void
  fork(canonical: BlockCursor[]): void
}

/** The portal surface a completed run and its oracle consume. */
export type ScenarioPortal = Pick<MockChainPortal, 'url' | 'requests'>

/** What a run of a full-stream class needs to know of its scenario. */
export type FullStreamScenario<Sim extends { chain: ScenarioPortal }> = {
  range: { from: number; to: number }
  ranges: BlockRange[]
  oracle: () => ReferenceModel
  /** The state and rows a finished run leaves, from the scenario and the simulator alone. */
  end: (simulator: Sim) => { state: OracleState; rows: OracleRow[] }
  /** The rows a store holds whose cursor is `cursor`, from the scenario alone. */
  rowsUpTo: (cursor: Cursor | undefined) => OracleRow[]
  /** A follower for one run, whose first request is `from` in the portal's log. */
  follower: (model: ReferenceModel, simulator: Sim, from: number) => Follower
  /** A source list to read instead of `chain.url` (spec/16): portal URLs, or specs such as an RPC source. */
  source?: (simulator: Sim) => { source: EvmSourceSpec[]; fallback?: EvmFallbackOptions }
}

/** What a run reads: one portal URL, or a source list with its fallback options. */
export type RunSource = { source: string | EvmSourceSpec[]; fallback?: EvmFallbackOptions }

/** The scenario's source list, or the simulator's one portal. Every run reads its source here. */
export function sourceOf<Sim extends { chain: ScenarioPortal }>(
  scenario: FullStreamScenario<Sim>,
  simulator: Sim,
): RunSource {
  return scenario.source?.(simulator) ?? { source: simulator.chain.url }
}

/** The simulator's one portal, for a store whose harness reads no source list: it refuses a scenario with one. */
export function onePortalOf<Sim extends { chain: ScenarioPortal }>(
  scenario: FullStreamScenario<Sim>,
  simulator: Sim,
): string {
  if (scenario.source !== undefined) {
    throw new Error('this store runs no source-list scenario: its harness reads one portal')
  }

  return simulator.chain.url
}

/**
 * The source as a killed run's child reads it. A function does not cross the process boundary, so
 * a fallback clock stays behind and the child's fallback runs on the real clock.
 */
export function childSourceOf<Sim extends { chain: ScenarioPortal }>(
  scenario: FullStreamScenario<Sim>,
  simulator: Sim,
): RunSource {
  const source = sourceOf(scenario, simulator)
  if (source.fallback === undefined) {
    return source
  }

  const { clock: _, ...detection } = source.fallback.detection ?? {}

  return { source: source.source, fallback: { ...source.fallback, detection } }
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
export function s3FullStream<Sim extends { chain: ScenarioPortal } = S3Simulator>(
  durability: FullStreamClass,
  options: S3Options = {},
): FullStreamScenario<Sim> {
  return {
    range: { from: S3.from, to: S3.to },
    ranges: S3_RANGES,
    oracle: () => s3Oracle(durability, options),
    end: () => ({ state: s3EndState(options), rows: s3Rows(options) }),
    rowsUpTo: (cursor) => s3RowsUpTo(cursor, options),
    follower: (model, simulator, from) => new Replay(model, simulator.chain.requests, from),
  }
}
