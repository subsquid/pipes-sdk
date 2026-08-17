import { Column, Table, is } from 'drizzle-orm'
import { PrimaryKeyBuilder } from 'drizzle-orm/pg-core'

import {
  getDrizzleForeignKeys,
  getDrizzleTableColumns,
  getDrizzleTableExtraColumns,
  getDrizzleTableExtraConfig,
  getDrizzleTableName,
} from './consts.js'
import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'

export const SNAPSHOT_BLOCK_COLUMN = '___sqd__block_number'
export const SNAPSHOT_OPERATION_COLUMN = '___sqd__operation'

const quote = (name: string) => `"${name}"`

/**
 * Everything the trigger and the rollback statements need about a table, in real Postgres column
 * names. Resolving names once here keeps every generated statement in database space, so nothing
 * downstream has to translate between Drizzle property keys and column names.
 */
export type SnapshotPlan = {
  name: string
  snapshotName: string
  columns: SnapshotColumn[]
  primaryKeys: string[]
}

export type SnapshotColumn = {
  name: string
  sqlType: string
  /** Postgres derives the value and rejects any explicit write, so undo must leave it alone. */
  generated: boolean
  /** `GENERATED ALWAYS AS IDENTITY`: insertable only via OVERRIDING SYSTEM VALUE, never updatable. */
  identityAlways: boolean
}

export function buildSnapshotPlan(table: Table, resolveColumnName: (col: Column) => string): SnapshotPlan {
  const name = getDrizzleTableName(table)
  const columns = getDrizzleTableColumns(table)

  let primaryCols: Column[] = Object.values(columns).filter((c) => c.primary)

  if (primaryCols.length === 0) {
    const extraConfigFn = getDrizzleTableExtraConfig(table)

    if (extraConfigFn) {
      const extra = extraConfigFn(getDrizzleTableExtraColumns(table))

      for (const fn of extra) {
        if (!is(fn, PrimaryKeyBuilder)) {
          continue
        }

        primaryCols = (fn as any).columns
      }
    }
  }

  if (primaryCols.length === 0) {
    throw new PostgresTargetError(
      POSTGRES_ERROR_CODES.MISSING_PRIMARY_KEY,
      `Cannot generate snapshot trigger for table ${name} without primary key columns`,
    )
  }

  return {
    name,
    snapshotName: `${name}__snapshots`,
    columns: Object.values(columns).map((col) => ({
      name: resolveColumnName(col),
      sqlType: col.getSQLType(),
      // Mirrors Drizzle's own insert filter: a 'byDefault' generated column is writable.
      generated: col.generated !== undefined && col.generated.type !== 'byDefault',
      identityAlways: col.generatedIdentity?.type === 'always',
    })),
    primaryKeys: primaryCols.map(resolveColumnName),
  }
}

export function generateTriggerSQL({ name, snapshotName, columns, primaryKeys }: SnapshotPlan) {
  const colsDDL = columns.map((col) => `${quote(col.name)} ${col.sqlType}`).join(',\n  ')
  const snapshotKey = [SNAPSHOT_BLOCK_COLUMN, ...primaryKeys].map(quote).join(',')

  const colNames = columns.map((c) => quote(c.name)).join(', ')
  const oldCols = columns.map((c) => `OLD.${quote(c.name)}`).join(', ')
  const newCols = columns.map((c) => `NEW.${quote(c.name)}`).join(', ')

  return `
-- ===== SNAPSHOT SETUP FOR ${snapshotName} =====
CREATE TABLE IF NOT EXISTS "${snapshotName}" (
  ${colsDDL},
  "${SNAPSHOT_OPERATION_COLUMN}" TEXT NOT NULL,
  "${SNAPSHOT_BLOCK_COLUMN}" BIGINT NOT NULL,
  PRIMARY KEY (${snapshotKey})
);

CREATE OR REPLACE FUNCTION maybe_snapshot_${name}() RETURNS trigger AS $$
DECLARE
  snapshot_enabled BOOLEAN := COALESCE(NULLIF(current_setting('sqd.snapshot_enabled', true), '')::boolean, false);
  block_num BIGINT := COALESCE(NULLIF(current_setting('sqd.snapshot_block_number', true), '')::BIGINT, -1);
BEGIN
   IF snapshot_enabled = true THEN
     IF TG_OP = 'INSERT' THEN
        -- No prior value exists; record the key under 'INSERT' so undo drops the row.
        INSERT INTO "${snapshotName}" (${colNames}, "${SNAPSHOT_BLOCK_COLUMN}", "${SNAPSHOT_OPERATION_COLUMN}")
        VALUES (${newCols}, block_num, 'INSERT')
        ON CONFLICT (${snapshotKey}) DO NOTHING;
     ELSE
        -- UPDATE/DELETE: keep the before-image (OLD) so undo restores the pre-change row.
        -- DO NOTHING preserves the earliest before-image when a row changes twice in one block.
        INSERT INTO "${snapshotName}" (${colNames}, "${SNAPSHOT_BLOCK_COLUMN}", "${SNAPSHOT_OPERATION_COLUMN}")
        VALUES (${oldCols}, block_num, TG_OP)
        ON CONFLICT (${snapshotKey}) DO NOTHING;
     END IF;
   END IF;

   IF TG_OP = 'DELETE' THEN
     RETURN OLD;
   END IF;

   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ${name}_snapshot_trigger ON "${name}";

CREATE TRIGGER ${name}_snapshot_trigger
AFTER INSERT OR UPDATE OR DELETE ON "${name}"
FOR EACH ROW EXECUTE FUNCTION maybe_snapshot_${name}();
`
}

/**
 * The earliest snapshot per row above the fork point holds that row's before-image at the fork
 * boundary, so replaying it rewinds the row to its state at the cursor block — even when the prior
 * value came from a finalized block that was never snapshotted.
 *
 * The operation is filtered *after* `DISTINCT ON`, never inside it: a row inserted and then updated
 * above the fork must still be judged by its first snapshot, otherwise the later UPDATE would
 * resurrect a row that never existed at the cursor.
 */
const earliestSnapshotPerRow = (plan: SnapshotPlan, blockNumber: number, selection: string) => `
  SELECT DISTINCT ON (${plan.primaryKeys.map(quote).join(', ')}) ${selection}
  FROM ${quote(plan.snapshotName)}
  WHERE ${quote(SNAPSHOT_BLOCK_COLUMN)} > ${blockNumber}
  ORDER BY ${plan.primaryKeys.map(quote).join(', ')}, ${quote(SNAPSHOT_BLOCK_COLUMN)} ASC`

/** A row first seen above the fork ('INSERT') had no prior value, so rewinding drops it. */
export function generateForkDeleteSQL(plan: SnapshotPlan, blockNumber: number) {
  const primaryKeys = plan.primaryKeys.map(quote).join(', ')
  const matchKey = plan.primaryKeys.map((c) => `t.${quote(c)} = s.${quote(c)}`).join(' AND ')

  return `
DELETE FROM ${quote(plan.name)} t
USING (${earliestSnapshotPerRow(plan, blockNumber, `${primaryKeys}, ${quote(SNAPSHOT_OPERATION_COLUMN)}`)}
) s
WHERE s.${quote(SNAPSHOT_OPERATION_COLUMN)} = 'INSERT' AND ${matchKey};`
}

/**
 * Restores the before-image kept for an UPDATE/DELETE. The copy stays inside Postgres on purpose:
 * pulling rows into JS and writing them back through the ORM would re-encode values that the driver
 * already returned in wire format.
 */
export function generateForkRestoreSQL(plan: SnapshotPlan, blockNumber: number) {
  const primaryKeys = plan.primaryKeys.map(quote).join(', ')

  const writable = plan.columns.filter((c) => !c.generated)
  const colNames = writable.map((c) => quote(c.name)).join(', ')

  const updatable = writable.filter((c) => !c.identityAlways && !plan.primaryKeys.includes(c.name))
  const onConflict = updatable.length
    ? `DO UPDATE SET ${updatable.map((c) => `${quote(c.name)} = EXCLUDED.${quote(c.name)}`).join(', ')}`
    : 'DO NOTHING'

  // Restoring an identity key means writing a value the sequence owns, which Postgres allows only here.
  const overriding = writable.some((c) => c.identityAlways) ? ' OVERRIDING SYSTEM VALUE' : ''

  return `
INSERT INTO ${quote(plan.name)} (${colNames})${overriding}
SELECT ${colNames}
FROM (${earliestSnapshotPerRow(plan, blockNumber, '*')}
) s
WHERE s.${quote(SNAPSHOT_OPERATION_COLUMN)} <> 'INSERT'
ON CONFLICT (${primaryKeys}) ${onConflict};`
}

/**
 * Returns tables ordered for DELETE operations: children → parents.
 * Edge: parent -> child (table B has FK to A => A -> B).
 * We topologically sort and then reverse to get delete order.
 */
export function orderTablesForDelete(tables: Table[]): Table[] {
  const nameOf = (t: Table) => getDrizzleTableName(t)
  const byName = new Map<string, Table>()
  for (const t of tables) byName.set(nameOf(t), t)

  const nodes = new Set<string>()
  const adj = new Map<string, Set<string>>()
  const indeg = new Map<string, number>()

  for (const t of tables) {
    const tn = nameOf(t)
    nodes.add(tn)
    if (!adj.has(tn)) adj.set(tn, new Set())
    if (!indeg.has(tn)) indeg.set(tn, 0)
  }

  for (const child of tables) {
    const childName = nameOf(child)
    const fks = getDrizzleForeignKeys(child)
    for (const fk of fks) {
      const parent = fk.reference().foreignTable as Table
      const parentName = nameOf(parent)
      if (!byName.has(parentName) || !byName.has(childName)) continue
      if (!adj.get(parentName)!.has(childName)) {
        adj.get(parentName)!.add(childName)
        indeg.set(childName, (indeg.get(childName) ?? 0) + 1)
      }
      if (!adj.has(parentName)) adj.set(parentName, new Set())
      if (!indeg.has(parentName)) indeg.set(parentName, 0)
    }
  }

  // Kahn's algorithm to get parent -> child order
  const q: string[] = []
  for (const [n, d] of indeg) if (d === 0) q.push(n)
  const order: string[] = []
  while (q.length) {
    const u = q.shift()!
    order.push(u)
    for (const v of adj.get(u) ?? []) {
      indeg.set(v, (indeg.get(v) ?? 0) - 1)
      if ((indeg.get(v) ?? 0) === 0) q.push(v)
    }
  }

  if (order.length !== nodes.size) {
    throw new PostgresTargetError(POSTGRES_ERROR_CODES.CIRCULAR_DEPENDENCY, [
      'Circular dependency detected in foreign key references.',
      'Cannot determine a safe order for delete operations.',
      'Please check your table definitions for circular foreign key constraints.',
    ])
  }

  // Reverse to get children→parents
  return order.reverse().map((n) => byName.get(n)!)
}
