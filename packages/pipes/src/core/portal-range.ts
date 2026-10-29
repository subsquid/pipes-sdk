import { BlockRangeConfigurationError } from './errors.js'
import { NaturalRange } from './query-builder.js'

export function parseFormattedBlock(block: string | number) {
  const value = typeof block === 'number' ? block : numberFromString(block)

  const isBlockNumber = Number.isSafeInteger(value) && value >= 0
  if (!isBlockNumber) {
    const given = typeof block === 'string' ? `"${block}"` : String(block)

    throw new BlockRangeConfigurationError(
      `Can't parse a block number from ${given}. Valid examples: 1000000, "1_000_000", "1,000,000"`,
    )
  }

  return value
}

/**
 * Remove commas and underscores
 * 1_000_000 -> 1000000
 * 1,000,000 -> 1000000
 */
function numberFromString(block: string) {
  const digits = block.replace(/[_,]/g, '')

  // `Number('')` is 0, which would read a blank bound as block 0.
  return digits.trim() === '' ? Number.NaN : Number(digits)
}

function parseBlock(block: string | number, offset?: number) {
  if (typeof block === 'number') return parseFormattedBlock(block)

  if (block.startsWith('+') && offset) {
    return offset + parseFormattedBlock(block.substring(1))
  }

  return parseFormattedBlock(block)
}

/**
 * Checks if a string looks like an ISO date (e.g., "2024-01-01", "2024-01-01T00:00:00Z")
 */
function isDateString(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}/.test(value)
}

function toDate(value: string): Date {
  // Treat date-only strings as UTC (e.g., "2024-01-01" → "2024-01-01T00:00:00Z")
  const date = new Date(value.includes('T') ? value : `${value}T00:00:00Z`)
  if (Number.isNaN(date.getTime())) {
    throw new BlockRangeConfigurationError(`Invalid date value: "${value}"`)
  }

  return date
}

function parseBlockOrTimestamp(value: number | string | Date, offset?: number): number | Date {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new BlockRangeConfigurationError('Invalid Date in a block range: the Date object holds no time')
    }

    return value
  }
  if (typeof value === 'string' && isDateString(value)) return toDate(value)

  return parseBlock(value, offset)
}

export function parsePortalRange(range: PortalRange, defaultValue?: PortalRange): NaturalRange {
  range = range || defaultValue

  if (range.from === 'latest') {
    if (range.to instanceof Date || (typeof range.to === 'string' && isDateString(range.to))) {
      throw new BlockRangeConfigurationError(
        "Cannot use a Date for 'to' when 'from' is 'latest'. The portal cannot resolve a timestamp to a block number for blocks that have not been produced yet. Use a block number instead.",
      )
    }
    const to = range.to != null ? parseBlock(range.to) : undefined
    return { from: 'latest', to }
  }

  // `!= null`, not truthiness: NaN must be refused, and `to: 0` is a bound, not its absence.
  const from = range.from != null ? parseBlockOrTimestamp(range.from) : 0
  const to = range.to != null ? parseBlockOrTimestamp(range.to, typeof from === 'number' ? from : undefined) : undefined

  return { from, to }
}

export type PortalRange = {
  from?: number | string | 'latest' | Date
  to?: number | string | Date
}
