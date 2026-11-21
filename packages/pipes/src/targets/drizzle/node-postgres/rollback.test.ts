import { sql } from 'drizzle-orm'
import {
  PgDialect,
  bigserial,
  integer,
  pgSchema,
  pgTable,
  primaryKey,
  serial,
  smallserial,
  text,
} from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'

import { createColumnNameResolver } from './consts.js'
import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'
import {
  asSetupRefusal,
  buildSnapshotPlan,
  generateForkDeleteSQL,
  generateForkRestoreSQL,
  generateTrackedLockSQL,
  generateTriggerSQL,
  orderTablesForDelete,
} from './rollback.js'

// A dialect is all `createColumnNameResolver` needs, so these stay free of a real connection.
const resolver = (casing?: 'snake_case') => createColumnNameResolver({ dialect: new PgDialect({ casing }) })

describe('buildSnapshotPlan', () => {
  it('takes the database name from an explicitly named column', () => {
    const items = pgTable('items', {
      itemId: integer('item_id').primaryKey(),
      itemValue: integer('item_value').notNull(),
    })

    expect(buildSnapshotPlan(items, resolver())).toMatchInlineSnapshot(`
      {
        "columns": [
          {
            "generated": false,
            "identityAlways": false,
            "name": "item_id",
            "sqlType": "integer",
          },
          {
            "generated": false,
            "identityAlways": false,
            "name": "item_value",
            "sqlType": "integer",
          },
        ],
        "name": "items",
        "primaryKeys": [
          "item_id",
        ],
        "schema": undefined,
        "snapshotName": "items__snapshots",
      }
    `)
  })

  // `col.name` is the JS property key here; only the dialect knows the real column.
  it('takes the database name from the dialect when the column is unnamed', () => {
    const items = pgTable('items', {
      itemId: integer().primaryKey(),
      itemValue: integer().notNull(),
    })

    expect(buildSnapshotPlan(items, resolver('snake_case'))).toMatchInlineSnapshot(`
      {
        "columns": [
          {
            "generated": false,
            "identityAlways": false,
            "name": "item_id",
            "sqlType": "integer",
          },
          {
            "generated": false,
            "identityAlways": false,
            "name": "item_value",
            "sqlType": "integer",
          },
        ],
        "name": "items",
        "primaryKeys": [
          "item_id",
        ],
        "schema": undefined,
        "snapshotName": "items__snapshots",
      }
    `)
  })

  it('resolves composite primary key columns through the dialect too', () => {
    const balances = pgTable(
      'balances',
      {
        accountId: integer(),
        tokenId: integer(),
        balanceValue: integer().notNull(),
      },
      (t) => [primaryKey({ columns: [t.accountId, t.tokenId] })],
    )

    const plan = buildSnapshotPlan(balances, resolver('snake_case'))

    expect(plan.primaryKeys).toEqual(['account_id', 'token_id'])
    expect(plan.columns.map((c) => c.name)).toEqual(['account_id', 'token_id', 'balance_value'])
  })

  it('throws MISSING_PRIMARY_KEY when no primary key is declared', () => {
    const table = pgTable('t', { a: integer('a') })

    try {
      buildSnapshotPlan(table, resolver())
      throw new Error('expected throw')
    } catch (e) {
      expect(e).toBeInstanceOf(PostgresTargetError)
      expect((e as PostgresTargetError).code).toBe(POSTGRES_ERROR_CODES.MISSING_PRIMARY_KEY)
    }
  })

  // A serial type adds a sequence default, and ADD COLUMN would fill old snapshots from it.
  it('stores a serial column as its integer type', () => {
    const counters = pgTable('counters', {
      id: smallserial('id').primaryKey(),
      seq: serial('seq'),
      total: bigserial('total', { mode: 'number' }),
    })

    const types = buildSnapshotPlan(counters, resolver()).columns.map((c) => c.sqlType)

    expect(types).toEqual(['smallint', 'integer', 'bigint'])
  })
})

describe('generateTriggerSQL', () => {
  it('addresses the base table by its real column names under dialect casing', () => {
    const items = pgTable('items', {
      itemId: integer().primaryKey(),
      itemValue: integer().notNull(),
    })

    const sql = generateTriggerSQL(buildSnapshotPlan(items, resolver('snake_case')))

    expect(sql).toContain('VALUES (NEW."item_id", NEW."item_value", block_num, \'INSERT\')')
    expect(sql).toContain('VALUES (OLD."item_id", OLD."item_value", block_num, TG_OP)')
    expect(sql).not.toContain('itemId')
    expect(sql).not.toContain('itemValue')
  })

  it('builds the snapshot table and trigger', () => {
    const items = pgTable('items', {
      itemId: integer('item_id').primaryKey(),
      itemValue: integer('item_value').notNull(),
    })

    expect(generateTriggerSQL(buildSnapshotPlan(items, resolver()))).toMatchInlineSnapshot(`
      "
      -- ===== SNAPSHOT SETUP FOR items__snapshots =====

      DO $sqd$
      DECLARE
        tracked regclass := to_regclass('"items"');
        snapshot regclass := to_regclass('"items__snapshots"');
        expected_key name[] := ARRAY['___sqd__block_number', 'item_id']::name[];
        snapshot_columns name[] := ARRAY['item_id', 'item_value']::name[];
        system_columns name[] := ARRAY['___sqd__operation', '___sqd__block_number']::name[];
        restored_columns name[] := ARRAY['item_id', 'item_value']::name[];
        add_columns text[] := ARRAY['ADD COLUMN IF NOT EXISTS "item_id" integer', 'ADD COLUMN IF NOT EXISTS "item_value" integer']::text[];
        live_rows text := 'true';
        before_images text := 'true AND "___sqd__operation" <> ''INSERT''';
        key_match text := 's."item_id" = t."item_id"';
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
              USING ERRCODE = 'SQD01';
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
                USING ERRCODE = 'SQD03';
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
          null_cases, '___sqd__block_number', snapshot, before_images, null_filter
        ) INTO null_columns, null_block;

        IF null_block IS NOT NULL THEN
          RAISE EXCEPTION 'Snapshot table % holds before-images with no value for the NOT NULL columns %, up to block %. A rollback cannot restore these rows. Remove NOT NULL from these columns, and set it again when block % is final.',
            snapshot, null_columns, null_block, null_block
            USING ERRCODE = 'SQD02';
        END IF;
      END
      $sqd$;

      CREATE TABLE IF NOT EXISTS "items__snapshots" (
        "item_id" integer,
        "item_value" integer,
        "___sqd__operation" TEXT NOT NULL,
        "___sqd__block_number" BIGINT NOT NULL,
        PRIMARY KEY ("___sqd__block_number","item_id")
      );


      CREATE OR REPLACE FUNCTION maybe_snapshot_items() RETURNS trigger AS $$
      DECLARE
        snapshot_enabled BOOLEAN := COALESCE(NULLIF(current_setting('sqd.snapshot_enabled', true), '')::boolean, false);
        block_num BIGINT := COALESCE(NULLIF(current_setting('sqd.snapshot_block_number', true), '')::BIGINT, -1);
      BEGIN
         IF snapshot_enabled = true THEN
           IF TG_OP = 'INSERT' THEN
              -- No prior value exists; record the key under 'INSERT' so undo drops the row.
              INSERT INTO "items__snapshots" ("item_id", "item_value", "___sqd__block_number", "___sqd__operation")
              VALUES (NEW."item_id", NEW."item_value", block_num, 'INSERT')
              ON CONFLICT ("___sqd__block_number","item_id") DO NOTHING;
           ELSE
              -- UPDATE/DELETE: keep the before-image (OLD) so undo restores the pre-change row.
              -- DO NOTHING preserves the earliest before-image when a row changes twice in one block.
              INSERT INTO "items__snapshots" ("item_id", "item_value", "___sqd__block_number", "___sqd__operation")
              VALUES (OLD."item_id", OLD."item_value", block_num, TG_OP)
              ON CONFLICT ("___sqd__block_number","item_id") DO NOTHING;
           END IF;
         END IF;

         IF TG_OP = 'DELETE' THEN
           RETURN OLD;
         END IF;

         RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS items_snapshot_trigger ON "items";

      CREATE TRIGGER items_snapshot_trigger
      AFTER INSERT OR UPDATE OR DELETE ON "items"
      FOR EACH ROW EXECUTE FUNCTION maybe_snapshot_items();
      "
    `)
  })
})

describe('a pgSchema table', () => {
  const items = pgSchema('app').table('items', {
    id: integer('id').primaryKey(),
    value: integer('value').notNull(),
  })

  const plan = buildSnapshotPlan(items, resolver())

  it('gets its schema on each name of the set-up', () => {
    const setup = generateTriggerSQL(plan)

    expect(generateTrackedLockSQL([plan])).toBe('LOCK TABLE "app"."items" IN ACCESS EXCLUSIVE MODE;')
    expect(setup).toContain(`to_regclass('"app"."items__snapshots"')`)
    expect(setup).toContain('CREATE TABLE IF NOT EXISTS "app"."items__snapshots" (')
    expect(setup).toContain('CREATE OR REPLACE FUNCTION "app".maybe_snapshot_items() RETURNS trigger')
    expect(setup).toContain('INSERT INTO "app"."items__snapshots" (')
    expect(setup).toContain('DROP TRIGGER IF EXISTS items_snapshot_trigger ON "app"."items";')
    expect(setup).toContain('FOR EACH ROW EXECUTE FUNCTION "app".maybe_snapshot_items();')
  })

  it('gets its schema on each name of a rollback', () => {
    const rollback = [generateForkDeleteSQL(plan, 1), generateForkRestoreSQL(plan, 1)].join('\n')

    expect(rollback).toContain('DELETE FROM "app"."items" t')
    expect(rollback).toContain('INSERT INTO "app"."items" ("id", "value")')
    expect(rollback).not.toContain('FROM "items__snapshots"')
  })

  it('stays apart from a table with the same name in a different schema', () => {
    const publicItems = pgTable('items', { id: integer('id').primaryKey() })

    expect(orderTablesForDelete([items, publicItems])).toEqual([publicItems, items])
  })
})

describe('set-up checks', () => {
  const items = pgTable('items', {
    id: integer('id').primaryKey(),
    value: integer('value').notNull(),
  })

  const plan = buildSnapshotPlan(items, resolver())

  it('count each row as undo data when the finalized block is not known', () => {
    expect(generateTriggerSQL(plan)).toContain(`live_rows text := 'true';`)
  })

  it('count only the rows above the finalized block', () => {
    expect(generateTriggerSQL(plan, 100)).toContain(`live_rows text := '"___sqd__block_number" > 100';`)
  })

  it('lock each tracked table in one statement', () => {
    const other = buildSnapshotPlan(pgTable('other', { id: integer('id').primaryKey() }), resolver())

    expect(generateTrackedLockSQL([plan, other])).toBe('LOCK TABLE "items", "other" IN ACCESS EXCLUSIVE MODE;')
  })
})

describe('asSetupRefusal', () => {
  // Drizzle puts the driver error into `cause`.
  const failedQuery = (sqlState: string) => {
    const driverError = Object.assign(new Error('the text that Postgres raised'), { code: sqlState })

    return new Error('Failed query: DO $sqd$ ...', { cause: driverError })
  }

  it.for([
    ['SQD01', POSTGRES_ERROR_CODES.SNAPSHOT_KEY_MISMATCH],
    ['SQD02', POSTGRES_ERROR_CODES.SNAPSHOT_VALUE_MISSING],
    ['SQD03', POSTGRES_ERROR_CODES.SNAPSHOT_COLUMN_AMBIGUOUS],
  ])('gives SQLSTATE %s the code %s and the raised text', ([sqlState, code]) => {
    const error = failedQuery(sqlState)
    const refusal = asSetupRefusal(error)

    expect(refusal).toBeInstanceOf(PostgresTargetError)
    expect(refusal).toMatchObject({ code, cause: error })
    expect(refusal?.message).toMatch(/^the text that Postgres raised\n/)
  })

  it('leaves each other error as it is', () => {
    expect(asSetupRefusal(failedQuery('42P01'))).toBeUndefined()
    expect(asSetupRefusal('not an error')).toBeUndefined()
  })
})

describe('fork statements', () => {
  const items = pgTable('items', {
    itemId: integer().primaryKey(),
    itemValue: integer().notNull(),
  })

  it('drops rows whose earliest snapshot above the fork is an INSERT', () => {
    expect(generateForkDeleteSQL(buildSnapshotPlan(items, resolver('snake_case')), 7)).toMatchInlineSnapshot(`
      "
      DELETE FROM "items" t
      USING (
        SELECT DISTINCT ON ("item_id") "item_id", "___sqd__operation"
        FROM "items__snapshots"
        WHERE "___sqd__block_number" > 7
        ORDER BY "item_id", "___sqd__block_number" ASC
      ) s
      WHERE s."___sqd__operation" = 'INSERT' AND t."item_id" = s."item_id";"
    `)
  })

  it('restores before-images without routing values through the ORM', () => {
    expect(generateForkRestoreSQL(buildSnapshotPlan(items, resolver('snake_case')), 7)).toMatchInlineSnapshot(`
      "
      INSERT INTO "items" ("item_id", "item_value")
      SELECT "item_id", "item_value"
      FROM (
        SELECT DISTINCT ON ("item_id") *
        FROM "items__snapshots"
        WHERE "___sqd__block_number" > 7
        ORDER BY "item_id", "___sqd__block_number" ASC
      ) s
      WHERE s."___sqd__operation" <> 'INSERT'
      ON CONFLICT ("item_id") DO UPDATE SET "item_value" = EXCLUDED."item_value";"
    `)
  })

  // Postgres derives the value itself and rejects an explicit write in both clauses.
  it('leaves a stored generated column out of the restore entirely', () => {
    const gen = pgTable('gen', {
      id: integer('id').primaryKey(),
      amount: integer('amount').notNull(),
      doubled: integer('doubled').generatedAlwaysAs(sql`"amount" * 2`),
    })

    const sql_ = generateForkRestoreSQL(buildSnapshotPlan(gen, resolver()), 1)

    expect(sql_).not.toContain('doubled')
    expect(sql_).toContain('INSERT INTO "gen" ("id", "amount")')
    expect(sql_).toContain('ON CONFLICT ("id") DO UPDATE SET "amount" = EXCLUDED."amount"')
  })

  it('claims a GENERATED ALWAYS AS IDENTITY key with OVERRIDING SYSTEM VALUE', () => {
    const ident = pgTable('ident', {
      id: integer('id').generatedAlwaysAsIdentity().primaryKey(),
      amount: integer('amount').notNull(),
    })

    expect(generateForkRestoreSQL(buildSnapshotPlan(ident, resolver()), 1)).toContain(
      'INSERT INTO "ident" ("id", "amount") OVERRIDING SYSTEM VALUE',
    )
  })

  // An all-key table has nothing to overwrite, and an empty SET list is not valid SQL.
  it('falls back to DO NOTHING when every column is part of the primary key', () => {
    const edges = pgTable('edges', { from: text().notNull(), to: text().notNull() }, (t) => [
      primaryKey({ columns: [t.from, t.to] }),
    ])

    expect(generateForkRestoreSQL(buildSnapshotPlan(edges, resolver()), 1)).toContain(
      'ON CONFLICT ("from", "to") DO NOTHING',
    )
  })
})
