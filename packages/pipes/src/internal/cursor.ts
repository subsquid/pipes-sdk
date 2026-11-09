/** Heights are non-negative safe integers; timestamps have different units and precision. */
export function isPersistedHeight(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

/** Database numeric columns may arrive as numbers or decimal strings with an all-zero fraction. */
export function parsePersistedHeight(value: unknown): number | null {
  if (typeof value === 'string') {
    if (!/^\d+(\.0+)?$/.test(value)) return null
    value = Number(value)
  }

  return isPersistedHeight(value) ? value : null
}

/** Structural validation at persisted-state boundaries; optional cursor fields stay optional. */
export function isPersistedCursor(value: unknown): boolean {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false
  const cursor = value as Record<string, unknown>
  const timestamp = cursor['timestamp']

  return (
    !('kind' in cursor) &&
    isPersistedHeight(cursor['number']) &&
    (cursor['hash'] === undefined || typeof cursor['hash'] === 'string') &&
    (timestamp === undefined ||
      (typeof timestamp === 'number' &&
        Number.isFinite(timestamp) &&
        timestamp >= 0 &&
        timestamp <= Number.MAX_SAFE_INTEGER))
  )
}
