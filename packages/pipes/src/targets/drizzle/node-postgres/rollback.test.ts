import { sql } from 'drizzle-orm'
import { PgDialect, integer, pgTable, primaryKey, text } from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'

import { createColumnNameResolver } from './consts.js'
import { POSTGRES_ERROR_CODES, PostgresTargetError } from './errors.js'
import { buildSnapshotPlan, generateForkDeleteSQL, generateForkRestoreSQL, generateTriggerSQL } from './rollback.js'

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
