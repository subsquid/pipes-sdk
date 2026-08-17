import { Column, Table } from 'drizzle-orm'
import { Reference } from 'drizzle-orm/pg-core/foreign-keys'

import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'

const DRIZZLE_TABLE_NAME = Symbol.for('drizzle:BaseName')
const IS_DRIZZLE_TABLE = Symbol.for('drizzle:IsDrizzleTable')
const DRIZZLE_TABLE_COLS = Symbol.for('drizzle:Columns')
const DRIZZLE_TABLE_EXTRA_BUILDER = Symbol.for('drizzle:ExtraConfigBuilder')
const DRIZZLE_TABLE_EXTRA_COLUMNS = Symbol.for('drizzle:ExtraConfigColumns')
const DRIZZLE_INLINE_FOREIGN_KEYS = Symbol.for('drizzle:PgInlineForeignKeys')

export function isDrizzleTable(table: unknown) {
  if (!table || (typeof table !== 'object' && typeof table !== 'function')) return false

  const isDrizzleTable = (table as any)[IS_DRIZZLE_TABLE]

  return isDrizzleTable === true
}

export function getDrizzleTableName(table: Table): string {
  return ((table as any)[DRIZZLE_TABLE_NAME] as string) || 'unknown'
}

export function getDrizzleTableColumns(table: Table) {
  return ((table as any)[DRIZZLE_TABLE_COLS] as Record<string, Column>) || {}
}

export function getDrizzleTableExtraConfig(table: Table) {
  return (table as any)[DRIZZLE_TABLE_EXTRA_BUILDER] as (columns: Record<string, Column>) => unknown[]
}

export function getDrizzleTableExtraColumns(table: Table) {
  return ((table as any)[DRIZZLE_TABLE_EXTRA_COLUMNS] as Record<string, Column>) || {}
}

/**
 * Resolves a column's real Postgres name.
 *
 * `col.name` holds the database name only for an explicitly named column (`integer('item_id')`).
 * A column declared without one (`integer()`) keeps the JS property key there and gets its real
 * name from the dialect's casing cache, so `drizzle(..., { casing: 'snake_case' })` has to be
 * asked rather than guessed.
 */
export function createColumnNameResolver(db: unknown): (col: Column) => string {
  const casing = (db as any)?.dialect?.casing

  return (col) => {
    if (!col.keyAsName) {
      return col.name
    }

    const resolved = casing?.getColumnCasing?.(col)
    if (!resolved) {
      throw new PostgresTargetError(
        POSTGRES_ERROR_CODES.COLUMN_NAME_UNRESOLVED,
        `Cannot resolve the database name of column "${col.name}". Declare it explicitly, e.g. integer('${col.name}').`,
      )
    }

    return resolved
  }
}

export function getDrizzleForeignKeys(table: Table) {
  return (
    ((table as any)[DRIZZLE_INLINE_FOREIGN_KEYS] as {
      table: Table
      reference: Reference
    }[]) || []
  )
}
