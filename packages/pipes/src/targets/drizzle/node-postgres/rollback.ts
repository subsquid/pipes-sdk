import { Column, Table, is } from 'drizzle-orm'
import { PrimaryKeyBuilder } from 'drizzle-orm/pg-core'

import {
  getDrizzleForeignKeys,
  getDrizzleTableColumns,
  getDrizzleTableExtraColumns,
  getDrizzleTableExtraConfig,
  getDrizzleTableName,
  getDrizzleTableSchema,
} from './consts.js'
import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'

export const SNAPSHOT_BLOCK_COLUMN = '___sqd__block_number'
export const SNAPSHOT_OPERATION_COLUMN = '___sqd__operation'

const quote = (name: string) => `"${name}"`
const literal = (value: string) => `'${value.replaceAll("'", "''")}'`

/** The SQLSTATE of each refusal that the set-up script raises. */
const SETUP_SQLSTATE = {
  keyMismatch: 'SQD01',
  valueMissing: 'SQD02',
  columnAmbiguous: 'SQD03',
} as const

const SETUP_REFUSAL_CODES = new Map<string, string>([
  [SETUP_SQLSTATE.keyMismatch, POSTGRES_ERROR_CODES.SNAPSHOT_KEY_MISMATCH],
  [SETUP_SQLSTATE.valueMissing, POSTGRES_ERROR_CODES.SNAPSHOT_VALUE_MISSING],
  [SETUP_SQLSTATE.columnAmbiguous, POSTGRES_ERROR_CODES.SNAPSHOT_COLUMN_AMBIGUOUS],
])

/**
 * Drizzle puts the full script into the message of a failed query. This gives a refusal of the
 * set-up script its code and the text that Postgres raised.
 */
export function asSetupRefusal(error: unknown): PostgresTargetError | undefined {
  for (let current = error; current instanceof Error; current = current.cause) {
    const sqlState = (current as { code?: unknown }).code
    const code = typeof sqlState === 'string' ? SETUP_REFUSAL_CODES.get(sqlState) : undefined

    if (code) {
      return new PostgresTargetError(code, current.message, { cause: error })
    }
  }

  return undefined
}

// A serial column gets a default from a new sequence, and ADD COLUMN fills the old snapshots from it.
const SERIAL_STORAGE_TYPES: Record<string, string> = {
  smallserial: 'smallint',
  serial: 'integer',
  bigserial: 'bigint',
}

/**
 * Everything the trigger and the rollback statements need about a table, in real Postgres column
 * names. Resolving names once here keeps every generated statement in database space, so nothing
 * downstream has to translate between Drizzle property keys and column names.
 */
export type SnapshotPlan = {
  /** The schema of a `pgSchema` table. Without it, Postgres resolves each name through `search_path`. */
  schema: string | undefined
  name: string
  snapshotName: string
  columns: SnapshotColumn[]
  primaryKeys: string[]
}

export type SnapshotColumn = {
  name: string
  /** The type of the snapshot column: a serial type becomes its integer type. */
  sqlType: string
  /** Postgres derives the value and rejects any explicit write, so undo must leave it alone. */
  generated: boolean
  /** `GENERATED ALWAYS AS IDENTITY`: insertable only via OVERRIDING SYSTEM VALUE, never updatable. */
  identityAlways: boolean
}

const qualify = (schema: string | undefined, name: string) => (schema ? `${quote(schema)}.${quote(name)}` : quote(name))

export const trackedTable = (plan: SnapshotPlan) => qualify(plan.schema, plan.name)
export const snapshotTable = (plan: SnapshotPlan) => qualify(plan.schema, plan.snapshotName)

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
    schema: getDrizzleTableSchema(table),
    name,
    snapshotName: `${name}__snapshots`,
    columns: Object.values(columns).map((col) => ({
      name: resolveColumnName(col),
      sqlType: SERIAL_STORAGE_TYPES[col.getSQLType()] ?? col.getSQLType(),
      // Mirrors Drizzle's own insert filter: a 'byDefault' generated column is writable.
      generated: col.generated !== undefined && col.generated.type !== 'byDefault',
      identityAlways: col.generatedIdentity?.type === 'always',
    })),
    primaryKeys: primaryCols.map(resolveColumnName),
  }
}

/**
 * A snapshot table outlives the tracked schema that it had at its creation, so
 * `CREATE TABLE IF NOT EXISTS` alone is not sufficient. This script brings the table in line:
 * - A new tracked column gets a nullable snapshot column. The script copies the value of the tracked
 *   row into each before-image: that is the value that the migration gave the row. The before-image
 *   of a deleted row keeps `NULL`.
 * - The script cannot tell a renamed column from a removed column and a new column. It stops when a
 *   new column comes together with before-image values in a column that the tracked table lost.
 * - It stops when a before-image has `NULL` in a column that is `NOT NULL` in the database, because
 *   a rollback cannot restore that row. A rollback deletes an 'INSERT' row, so that row needs no value.
 * - It rebuilds a table that has a different set of key columns, unless the table holds undo data.
 *
 * Only the rows above `finalizedBlock` are undo data: a fork does not go below that block.
 *
 * Run it under read committed and after `generateTrackedLockSQL`. The checks then see each row that
 * a writer committed, and no trigger adds a row later.
 *
 * A start with no schema change takes no lock on the snapshot table.
 */
function generateSnapshotTableSQL(plan: SnapshotPlan, finalizedBlock: number | undefined) {
  const { columns, primaryKeys } = plan
  const snapshot = snapshotTable(plan)

  const block = quote(SNAPSHOT_BLOCK_COLUMN)
  const operation = quote(SNAPSHOT_OPERATION_COLUMN)

  const snapshotKeyColumns = [SNAPSHOT_BLOCK_COLUMN, ...primaryKeys]
  const snapshotKey = snapshotKeyColumns.map(quote).join(',')

  const textArray = (values: string[]) => `ARRAY[${values.map(literal).join(', ')}]::text[]`
  // Postgres cuts a name at 63 bytes. The cast to name cuts it too, so a long name matches the catalog.
  const nameArray = (values: string[]) => `ARRAY[${values.map(literal).join(', ')}]::name[]`

  const liveRows = finalizedBlock === undefined ? 'true' : `${block} > ${finalizedBlock}`
  const beforeImages = `${liveRows} AND ${operation} <> 'INSERT'`

  const keyMatch = primaryKeys.map((key) => `s.${quote(key)} = t.${quote(key)}`).join(' AND ')

  // A restore leaves a generated column out, so it needs no snapshot value.
  const restoredColumns = columns.filter((col) => !col.generated).map((col) => col.name)

  const colsDDL = columns.map((col) => `${quote(col.name)} ${col.sqlType}`).join(',\n  ')
  const addColumns = columns.map((col) => `ADD COLUMN IF NOT EXISTS ${quote(col.name)} ${col.sqlType}`)

  return `
DO $sqd$
DECLARE
  tracked regclass := to_regclass(${literal(trackedTable(plan))});
  snapshot regclass := to_regclass(${literal(snapshot)});
  expected_key name[] := ${nameArray(snapshotKeyColumns)};
  snapshot_columns name[] := ${nameArray(columns.map((col) => col.name))};
  system_columns name[] := ${nameArray([SNAPSHOT_OPERATION_COLUMN, SNAPSHOT_BLOCK_COLUMN])};
  restored_columns name[] := ${nameArray(restoredColumns)};
  add_columns text[] := ${textArray(addColumns)};
  live_rows text := ${literal(liveRows)};
  before_images text := ${literal(beforeImages)};
  key_match text := ${literal(keyMatch)};
  key_columns name[];
  missing_columns name[];
  missing_add text[];
  lost_columns name[];
  lost_values text;
  fill text;
  null_cases text;
  null_filter text;
  null_columns text[];
  null_block bigint;
  key_matches boolean;
  has_rows boolean;
BEGIN
  IF snapshot IS NULL THEN
    RETURN;
  END IF;

  SELECT array_agg(a.attname) INTO key_columns
  FROM pg_constraint c
  JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
  WHERE c.conrelid = snapshot AND c.contype = 'p';

  -- ON CONFLICT accepts the key columns in any order.
  key_matches := cardinality(key_columns) = cardinality(expected_key)
    AND key_columns @> expected_key AND key_columns <@ expected_key;

  IF key_matches IS NOT TRUE THEN
    EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', snapshot);
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s WHERE %s)', snapshot, live_rows) INTO has_rows;

    IF has_rows THEN
      RAISE EXCEPTION 'Snapshot table % has the primary key %, but the tracked table needs %. The snapshot table holds undo data for unfinalized blocks, and a rebuild deletes this data. Restore the previous primary key of the tracked table. As an alternative, delete all rows of the snapshot table: a fork then cannot undo the writes of these blocks.',
        snapshot, key_columns, expected_key
        USING ERRCODE = ${literal(SETUP_SQLSTATE.keyMismatch)};
    END IF;

    EXECUTE format('DROP TABLE %s', snapshot);

    RETURN;
  END IF;

  SELECT array_agg(c.name), array_agg(c.add_column) INTO missing_columns, missing_add
  FROM unnest(snapshot_columns, add_columns) AS c(name, add_column)
  WHERE NOT EXISTS (
    SELECT 1 FROM pg_attribute a WHERE a.attrelid = snapshot AND a.attname = c.name AND NOT a.attisdropped
  );

  IF missing_columns IS NOT NULL THEN
    EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', snapshot);

    -- A column that the tracked table keeps is not a renamed column.
    SELECT array_agg(a.attname), string_agg(format('%I IS NOT NULL', a.attname), ' OR ')
    INTO lost_columns, lost_values
    FROM pg_attribute a
    WHERE a.attrelid = snapshot AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attname <> ALL (snapshot_columns || system_columns)
      AND NOT EXISTS (
        SELECT 1 FROM pg_attribute t WHERE t.attrelid = tracked AND t.attname = a.attname AND NOT t.attisdropped
      );

    IF lost_columns IS NOT NULL THEN
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s WHERE %s AND (%s))', snapshot, before_images, lost_values)
        INTO has_rows;

      IF has_rows THEN
        RAISE EXCEPTION 'Snapshot table % holds undo data in the columns %, which the tracked table no longer has, and the tracked table has the new columns %. The target cannot tell a renamed column from a removed column. If you renamed a column, rename it in the snapshot table too. If you removed a column, drop it from the snapshot table.',
          snapshot, lost_columns, missing_columns
          USING ERRCODE = ${literal(SETUP_SQLSTATE.columnAmbiguous)};
      END IF;
    END IF;

    EXECUTE format('ALTER TABLE %s %s', snapshot, array_to_string(missing_add, ', '));

    SELECT string_agg(format('%1$I = t.%1$I', c.name), ', ') INTO fill
    FROM unnest(missing_columns) AS c(name)
    WHERE EXISTS (
      SELECT 1 FROM pg_attribute t WHERE t.attrelid = tracked AND t.attname = c.name AND NOT t.attisdropped
    );

    IF fill IS NOT NULL THEN
      EXECUTE format('UPDATE %s s SET %s FROM %s t WHERE %s AND %s', snapshot, fill, tracked, key_match, before_images);
    END IF;
  END IF;

  -- A column can get NOT NULL long after the snapshot table got the column, so each start checks it.
  SELECT string_agg(format('CASE WHEN bool_or(%1$I IS NULL) THEN %1$L END', t.attname), ', '),
    string_agg(format('%I IS NULL', t.attname), ' OR ')
  INTO null_cases, null_filter
  FROM pg_attribute t
  WHERE t.attrelid = tracked AND t.attnotnull AND NOT t.attisdropped AND t.attname = ANY (restored_columns);

  IF null_filter IS NULL THEN
    RETURN;
  END IF;

  EXECUTE format(
    'SELECT array_remove(ARRAY[%s]::text[], NULL), max(%I) FROM %s WHERE %s AND (%s)',
    null_cases, ${literal(SNAPSHOT_BLOCK_COLUMN)}, snapshot, before_images, null_filter
  ) INTO null_columns, null_block;

  IF null_block IS NOT NULL THEN
    RAISE EXCEPTION 'Snapshot table % holds before-images with no value for the NOT NULL columns %, up to block %. A rollback cannot restore these rows. Remove NOT NULL from these columns, and set it again when block % is final.',
      snapshot, null_columns, null_block, null_block
      USING ERRCODE = ${literal(SETUP_SQLSTATE.valueMissing)};
  END IF;
END
$sqd$;

CREATE TABLE IF NOT EXISTS ${snapshot} (
  ${colsDDL},
  ${operation} TEXT NOT NULL,
  ${block} BIGINT NOT NULL,
  PRIMARY KEY (${snapshotKey})
);
`
}

/**
 * The set-up needs this lock on each tracked table to replace the trigger. It takes all of them
 * before it locks a snapshot table. A writer that holds a tracked table then gets each snapshot
 * table, so the writer does not wait for the set-up while the set-up waits for the writer.
 */
export function generateTrackedLockSQL(plans: SnapshotPlan[]) {
  return `LOCK TABLE ${plans.map(trackedTable).join(', ')} IN ACCESS EXCLUSIVE MODE;`
}

export function generateTriggerSQL(plan: SnapshotPlan, finalizedBlock?: number) {
  const { name, columns } = plan
  const tracked = trackedTable(plan)
  const snapshot = snapshotTable(plan)
  const triggerFunction = plan.schema ? `${quote(plan.schema)}.maybe_snapshot_${name}` : `maybe_snapshot_${name}`
  const snapshotKey = [SNAPSHOT_BLOCK_COLUMN, ...plan.primaryKeys].map(quote).join(',')

  const colNames = columns.map((c) => quote(c.name)).join(', ')
  const oldCols = columns.map((c) => `OLD.${quote(c.name)}`).join(', ')
  const newCols = columns.map((c) => `NEW.${quote(c.name)}`).join(', ')

  return `
-- ===== SNAPSHOT SETUP FOR ${plan.snapshotName} =====
${generateSnapshotTableSQL(plan, finalizedBlock)}

CREATE OR REPLACE FUNCTION ${triggerFunction}() RETURNS trigger AS $$
DECLARE
  snapshot_enabled BOOLEAN := COALESCE(NULLIF(current_setting('sqd.snapshot_enabled', true), '')::boolean, false);
  block_num BIGINT := COALESCE(NULLIF(current_setting('sqd.snapshot_block_number', true), '')::BIGINT, -1);
BEGIN
   IF snapshot_enabled = true THEN
     IF TG_OP = 'INSERT' THEN
        -- No prior value exists; record the key under 'INSERT' so undo drops the row.
        INSERT INTO ${snapshot} (${colNames}, "${SNAPSHOT_BLOCK_COLUMN}", "${SNAPSHOT_OPERATION_COLUMN}")
        VALUES (${newCols}, block_num, 'INSERT')
        ON CONFLICT (${snapshotKey}) DO NOTHING;
     ELSE
        -- UPDATE/DELETE: keep the before-image (OLD) so undo restores the pre-change row.
        -- DO NOTHING preserves the earliest before-image when a row changes twice in one block.
        INSERT INTO ${snapshot} (${colNames}, "${SNAPSHOT_BLOCK_COLUMN}", "${SNAPSHOT_OPERATION_COLUMN}")
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

DROP TRIGGER IF EXISTS ${name}_snapshot_trigger ON ${tracked};

CREATE TRIGGER ${name}_snapshot_trigger
AFTER INSERT OR UPDATE OR DELETE ON ${tracked}
FOR EACH ROW EXECUTE FUNCTION ${triggerFunction}();
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
  FROM ${snapshotTable(plan)}
  WHERE ${quote(SNAPSHOT_BLOCK_COLUMN)} > ${blockNumber}
  ORDER BY ${plan.primaryKeys.map(quote).join(', ')}, ${quote(SNAPSHOT_BLOCK_COLUMN)} ASC`

/** A row first seen above the fork ('INSERT') had no prior value, so rewinding drops it. */
export function generateForkDeleteSQL(plan: SnapshotPlan, blockNumber: number) {
  const primaryKeys = plan.primaryKeys.map(quote).join(', ')
  const matchKey = plan.primaryKeys.map((c) => `t.${quote(c)} = s.${quote(c)}`).join(' AND ')

  return `
DELETE FROM ${trackedTable(plan)} t
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
INSERT INTO ${trackedTable(plan)} (${colNames})${overriding}
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
  // Two schemas can each hold a table with the same name.
  const nameOf = (t: Table) => `${getDrizzleTableSchema(t) ?? ''}.${getDrizzleTableName(t)}`
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
