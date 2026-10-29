import { arrayify } from '~/internal/array.js'

import { formatNumber, joinLines } from './formatters.js'

const DOCS_BASE = 'https://docs.sqd.dev/en/sdk/pipes-sdk/errors'

export enum SdkErrorName {
  PipeConfiguration = 'PipeConfiguration',
  ForkHandling = 'ForkHandling',
  TargetConfiguration = 'TargetConfiguration',
  PortalContract = 'PortalContract',
}

export class PipeError extends Error {
  readonly code: string

  constructor(code: string, name: SdkErrorName, message: string | string[], options?: ErrorOptions) {
    super(joinLines([...arrayify(message), '', `See: ${DOCS_BASE}/${code}`]), options)
    this.code = code
    this.name = name
  }
}

// ─── Source errors (E0xxx) ────────────────────────────────────────────────────

/**
 * E0001: Thrown when a pipe is created with an empty or blank id.
 * Targets need a stable, unique ID to persist cursor state across restarts.
 */
export class DefaultPipeIdError extends PipeError {
  constructor() {
    super('E0001', SdkErrorName.PipeConfiguration, [
      'Pipe requires a non-empty id: targets persist progress under it.',
      'Set a stable, unique id in your pipe source options:',
      '',
      '  evmPortalStream({ portal: "...", id: "my-pipe", outputs })',
    ])
  }
}

/**
 * E0002: Thrown when a block range is misconfigured (inverted range, invalid date usage, unresolvable timestamp, etc.).
 */
export class BlockRangeConfigurationError extends PipeError {
  constructor(message: string | string[]) {
    super('E0002', SdkErrorName.PipeConfiguration, message)
  }
}

/**
 * E0003: Thrown when an instruction decoder is built with an unusable instruction set: an
 * instruction that is not one of its `program`'s (a discriminator does not name its program, so
 * another program's instruction would decode this one's data), a `program` with no address,
 * an instruction with no discriminator (it would match every instruction of the program) or
 * with more than one (a malformed ABI entry — an instruction has exactly one), discriminators
 * of mixed widths across the decoder (a decoder covers one ABI, which is single-width), or two
 * instructions sharing a discriminator (Anchor discriminators are program-independent, so a
 * shared one decodes the same raw instruction under both keys).
 */
export class InstructionDecoderConfigurationError extends PipeError {
  constructor(message: string | string[]) {
    super('E0003', SdkErrorName.PipeConfiguration, message)
  }
}

/**
 * E0004: Thrown when an event decoder declares two entries that can match the same log: the same
 * topic0 and topic count, and no indexed parameter that both filter on disjoint values. A log goes to
 * one entry only, so the other would silently miss it.
 */
export class EventDecoderConfigurationError extends PipeError {
  constructor(message: string | string[]) {
    super('E0004', SdkErrorName.PipeConfiguration, message)
  }
}

// ─── Fork handling errors (E1xxx) ─────────────────────────────────────────────

/**
 * E1001: Thrown when a fork is detected but the target does not implement fork handling.
 */
export class TargetForkNotSupportedError extends PipeError {
  constructor() {
    super('E1001', SdkErrorName.ForkHandling, [
      'A blockchain fork was detected, but the target does not support fork handling.',
      'Implement the resolveFork() method on your target to handle chain reorganizations.',
    ])
  }
}

/**
 * E1002: Thrown when the portal reports a fork without the canonical blocks the pipe needs to find the
 * common ancestor. The pipe stops rather than guess where to roll back to.
 */
export class MissingForkAncestorError extends PipeError {
  constructor({ block, portalMessage }: { block?: number; portalMessage?: string } = {}) {
    const at = block === undefined ? '' : ` at block ${formatNumber(block)}`

    super('E1002', SdkErrorName.ForkHandling, [
      `The portal reported a fork${at}, but sent no canonical blocks to find the common ancestor.`,
      ...(portalMessage ? [`The portal said: "${portalMessage}"`] : []),
      'The pipe stopped without rolling anything back.',
      '',
      'Report it against the portal. A restart resumes from the persisted cursor.',
    ])
  }
}

/**
 * E1003: Thrown when a fork is detected but the target resolveFork() returned null.
 */
export class ForkCursorMissingError extends PipeError {
  constructor() {
    super('E1003', SdkErrorName.ForkHandling, [
      'A blockchain fork was detected, but the target resolveFork() did not return a new cursor.',
      'A built-in target returns none when none of its retained blocks is canonical, or when the fork reaches below the finalized block, which is never rolled back.',
      'A custom resolveFork() must return the cursor to resume from after rolling back.',
    ])
  }
}

/**
 * E1004: Thrown when the portal breaks its fork contract — it delivered a canonicalBlocks set whose
 * highest block is below the target's persisted cursor. Rows above it would survive the fork
 * rollback and corrupt the new chain, so the pipe refuses to proceed rather than write bad data.
 */
export class PortalContractViolationError extends PipeError {
  constructor(message: string | string[]) {
    super('E1004', SdkErrorName.ForkHandling, message)
  }
}

/**
 * E1005: Thrown when a fork is reported on `/finalized-stream`. Every block that stream delivers is
 * final, so a 409 there means the portal contradicted its own finality. The pipe stops without
 * rolling anything back — a finalized-only target has no rollback path by design.
 */
export class ForkOnFinalizedStreamError extends PipeError {
  constructor() {
    super('E1005', SdkErrorName.ForkHandling, [
      'A fork was reported on the finalized stream, where every delivered block is already final.',
      'The target commits only finalized data, so nothing was rolled back and the pipe stopped.',
      '',
      'Report this against the portal. To restart, rewind (or clear) the persisted cursor to a block',
      'the portal still considers canonical.',
    ])
  }
}

/**
 * E1006: Thrown when the portal delivers a block outside the window its request allows: at or below
 * a block it already delivered (a duplicate or an out-of-order block), below the requested
 * `fromBlock`, or above `toBlock`. The pipe stops before the block reaches any transformer or target
 * rather than write it twice or out of order (FM-16).
 */
export class PortalBlockOrderError extends PipeError {
  constructor({ block, previous, range }: { block: number; previous?: number; range: { from: number; to?: number } }) {
    super('E1006', SdkErrorName.PortalContract, [
      portalBlockOrderReason({ block, previous, range }),
      'Blocks must arrive in ascending order and inside the requested range. The pipe stopped',
      'before this block reached any transformer or target.',
      '',
      'Report it against the portal. A restart resumes from the persisted cursor.',
    ])
  }
}

function portalBlockOrderReason({
  block,
  previous,
  range,
}: {
  block: number
  previous?: number
  range: { from: number; to?: number }
}) {
  if (previous !== undefined && block <= previous) {
    return `The portal delivered block ${formatNumber(block)} after block ${formatNumber(previous)}: a duplicate or out-of-order block.`
  }

  if (block < range.from) {
    return `The portal delivered block ${formatNumber(block)}, below the requested start ${formatNumber(range.from)}.`
  }

  return `The portal delivered block ${formatNumber(block)}, above the requested end ${formatNumber(range.to ?? Infinity)}.`
}

/** Why a stream response line is not a block: see {@link MalformedPortalResponseError}. */
export type MalformedLine = 'invalid-json' | 'no-block-number' | 'cut-twice'

const LINE_EXCERPT_LENGTH = 120

/**
 * E1007: Thrown when a stream response holds a line the client cannot read as a block: a line that
 * is not valid JSON, a block without a number, or a response that ends inside a block twice in a row.
 * A response cut inside a block once is requested again from that block.
 */
export class MalformedPortalResponseError extends PipeError {
  constructor({ problem, fromBlock, line }: { problem: MalformedLine; fromBlock: number; line?: string }) {
    super('E1007', SdkErrorName.PortalContract, [
      malformedLineReason(problem, fromBlock),
      ...(line === undefined ? [] : [`Line: ${lineExcerpt(line)}`]),
      'The pipe stopped before the line reached any transformer or target.',
      '',
      'Report it against the portal. A restart resumes from the persisted cursor.',
    ])
  }
}

function malformedLineReason(problem: MalformedLine, fromBlock: number) {
  const due = `the next block, at or above block ${formatNumber(fromBlock)}`

  switch (problem) {
    case 'invalid-json':
      return `The portal sent a line that is not valid JSON in place of ${due}.`
    case 'no-block-number':
      return `The portal sent a line without a block number in place of ${due}.`
    case 'cut-twice':
      return `The portal ended its response inside ${due}, twice in a row.`
  }
}

function lineExcerpt(line: string) {
  if (line.length <= LINE_EXCERPT_LENGTH) {
    return line
  }

  return `${line.slice(0, LINE_EXCERPT_LENGTH)}… (${formatNumber(line.length)} characters)`
}

/**
 * E1008: Thrown when a block from the portal does not match the schema of the fields the query selects.
 * The message names the block and the field.
 */
export class BlockValidationError extends PipeError {
  constructor({ block, failure }: { block?: number; failure: string }) {
    const name = block === undefined ? 'A block without a number' : `Block ${formatNumber(block)}`

    super('E1008', SdkErrorName.PortalContract, [
      `${name} does not match the schema of the selected fields: ${failure}.`,
      'The pipe stopped before the block reached any output or target.',
      '',
      'Report it with the block number and the field.',
      'Either the portal sent a wrong value, or the SDK schema rejects a value the chain really has.',
    ])
  }
}
