/**
 * This example demonstrates how to:
 * 1. Fetch ERC20 Transfer events from Ethereum mainnet using Portal API
 * 2. Store them in a PostgreSQL database using Drizzle ORM
 * 3. Expose GraphQL API to query the data
 *
 * Prerequisites:
 * 1. PostgreSQL database running on localhost:5432
 * 2. Database credentials: postgres/postgres
 *
 * To run the indexer:
 * ```bash
 * bun run docs/examples/evm/08.drizzle.example.ts
 * ```
 *
 * To run the GraphQL API:
 * ```bash
 * bun run docs/examples/evm/08.drizzle.example.ts api
 * ```
 *
 * GraphQL API will be available at http://localhost:4000
 * Example query:
 * ```graphql
 * {
 *   transfersTable(limit: 10, orderBy: { blockNumber: {
 *      direction: desc
 *      priority: 1
 *   }}) {
 *     blockNumber
 *     from
 *     to
 *     amount
 *     createdAt
 *   }
 * }
 * ```
 */

import { ApolloServer } from '@apollo/server'
import { startStandaloneServer } from '@apollo/server/standalone'
import { commonAbis, evmEventDecoder, evmStream } from '@subsquid/pipes/evm'
import { metricsServer } from '@subsquid/pipes/metrics/node'
import { chunkForInsert, drizzleTarget } from '@subsquid/pipes/targets/drizzle/node-postgres'
import { buildSchema } from 'drizzle-graphql'
import { drizzle } from 'drizzle-orm/node-postgres'
import { integer, numeric, pgTable, primaryKey, timestamp, varchar } from 'drizzle-orm/pg-core'

export const transfersTable = pgTable(
  'transfers',
  {
    blockNumber: integer().notNull(),
    logIndex: integer().notNull(),
    transactionIndex: integer().notNull(),
    from: varchar().notNull(),
    to: varchar().notNull(),
    amount: numeric({ mode: 'bigint' }).notNull(),
    createdAt: timestamp(),
  },
  (table) => [
    primaryKey({
      columns: [table.blockNumber, table.transactionIndex, table.logIndex],
    }),
  ],
)

type NewTransfer = typeof transfersTable.$inferInsert

const DB_URL = 'postgresql://postgres:postgres@localhost:5432/postgres'

async function main() {
  // Configure Portal API source to fetch data from Ethereum mainnet
  await evmStream({
    id: 'ethereum-transfers',
    source: {
      url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    },
    // Configure decoder to extract ERC20 Transfer events from raw blockchain data
    outputs: evmEventDecoder({
      range: { from: '0' },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
    }),
    metrics: metricsServer(),
  }).pipeTo(
    // Configure Drizzle ORM target to store events in PostgreSQL database
    drizzleTarget({
      db: drizzle(DB_URL),
      /*
       * List of tables that will be used to track snapshots for rollbacks.
       * If you modify any table in onData() callback, you MUST specify it here,
       * otherwise the indexer will throw an error.
       */
      tables: [transfersTable],
      onStart: async ({ db }) => {
        // WARNING: This direct table creation is only suitable for development/example purposes.
        // For production environments, use Drizzle Kit migrations to manage database schema changes
        // in a safe and consistent way.
        // See: https://orm.drizzle.team/kit-docs/overview
        await db.execute(`
              CREATE TABLE IF NOT EXISTS "transfers"
              (
                  "blockNumber"      integer NOT NULL,
                  "logIndex"         integer NOT NULL,
                  "transactionIndex" integer NOT NULL,
                  "from"             varchar NOT NULL,
                  "to"               varchar NOT NULL,
                  "amount"           numeric NOT NULL,
                  "createdAt"        timestamp,
                  CONSTRAINT "transfers_blockNumber_transactionIndex_logIndex_pk" PRIMARY KEY ("blockNumber", "transactionIndex", "logIndex")
              );
          `)
      },
      onData: async ({ tx, data, ctx }) => {
        ctx.logger.debug(`Processing batch with ${data.transfers.length} transfer events...`)

        for (const values of chunkForInsert(data.transfers)) {
          ctx.logger.debug(`Inserting ${values.length} transfer events...`)

          await tx.insert(transfersTable).values(
            values.map(
              (d): NewTransfer => ({
                // Compound ID
                blockNumber: d.block.number,
                logIndex: d.rawEvent.logIndex,
                transactionIndex: d.rawEvent.transactionIndex,
                // ----
                from: d.event.from,
                to: d.event.to,
                amount: d.event.value,
                createdAt: d.timestamp,
              }),
            ),
          )
        }
      },
    }),
  )
}

async function api() {
  // Initialize database connection and create a GraphQL API server
  const db = drizzle({
    connection: DB_URL,
    schema: { transfersTable },
  })
  const { schema } = buildSchema(db)
  const server = new ApolloServer({ schema })
  const { url } = await startStandaloneServer(server)

  console.log(`🚀 Server ready at ${url}`)
}

process.argv[2] === 'api' ? api() : main()
