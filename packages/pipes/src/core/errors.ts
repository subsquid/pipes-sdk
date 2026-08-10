import { arrayify } from '~/internal/array.js'

import { joinLines } from './formatters.js'

const DOCS_BASE = 'https://docs.sqd.dev/en/sdk/pipes-sdk/errors'

export enum SdkErrorName {
  PipeConfiguration = 'PipeConfiguration',
  ForkHandling = 'ForkHandling',
  TargetConfiguration = 'TargetConfiguration',
}

export class PipeError extends Error {
  readonly code: string

  constructor(code: string, name: SdkErrorName, message: string | string[]) {
    super(joinLines([...arrayify(message), '', `See: ${DOCS_BASE}/${code}`]))
    this.code = code
    this.name = name
  }
}

// ─── Source errors (E0xxx) ────────────────────────────────────────────────────

/**
 * E0001: Thrown when a pipe with the default ID is connected to a target.
 * Targets need a stable, unique ID to persist cursor state across restarts.
 */
export class DefaultPipeIdError extends PipeError {
  constructor() {
    super('E0001', SdkErrorName.PipeConfiguration, [
      'Pipe requires a non-default ID when used with targets.',
      'Set a unique id in your pipe source options:',
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
 * E0003: Thrown when an instruction decoder is built with an unusable discriminator set:
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
 * E1002: Thrown when a fork is detected but the canonical block list is empty (internal invariant violation).
 */
export class MissingForkAncestorError extends PipeError {
  constructor() {
    super('E1002', SdkErrorName.ForkHandling, [
      'A blockchain fork was detected, but no canonical blocks were provided to resolve it.',
      'This is an internal error — please report it as a bug.',
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
      'The resolveFork() method must return the cursor to resume from after rolling back.',
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
