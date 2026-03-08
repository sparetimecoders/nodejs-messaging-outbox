// MIT License
// Copyright (c) 2026 sparetimecoders

import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import type { Pool, PoolClient } from "pg";
import type { OutboxInserter, OutboxProcessor, OutboxRecord } from "../types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const migrationSQL = readFileSync(
  join(__dirname, "migrations", "001_create_outbox.sql"),
  "utf-8",
);

export interface PostgresStoreOptions {
  /** Skip running the embedded migration on creation. Default: false. */
  skipMigrations?: boolean;
}

/**
 * PostgreSQL implementation of OutboxProcessor and OutboxInserter factory.
 * Uses pg (node-postgres) Pool.
 *
 * For writes: use txInserter(client) to get a transaction-scoped inserter.
 * For relay: pass the store directly as an OutboxProcessor.
 */
export class PostgresStore implements OutboxProcessor {
  private readonly pool: Pool;

  private constructor(pool: Pool) {
    this.pool = pool;
  }

  /**
   * Create a new PostgresStore. By default, runs the embedded migration
   * to create the outbox table. Use skipMigrations to disable.
   */
  static async create(
    pool: Pool,
    options?: PostgresStoreOptions,
  ): Promise<PostgresStore> {
    const store = new PostgresStore(pool);
    if (!options?.skipMigrations) {
      await pool.query(migrationSQL);
    }
    return store;
  }

  /**
   * Returns a transaction-scoped inserter that writes to the given PoolClient.
   * The caller MUST have called client.query("BEGIN") before using this.
   * Use this to insert outbox records within the same transaction as business data.
   */
  txInserter(client: PoolClient): OutboxInserter {
    return {
      insert: (record) => insertRecord(client, record),
    };
  }

  async process(
    batchSize: number,
    fn: (records: OutboxRecord[]) => Promise<string[]>,
  ): Promise<number> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      // Acquire advisory lock (transaction-scoped)
      const lockResult = await client.query<{ acquired: boolean }>(
        "SELECT pg_try_advisory_xact_lock(hashtext('messaging_outbox')) AS acquired",
      );
      if (!lockResult.rows[0]?.acquired) {
        await client.query("ROLLBACK");
        return 0;
      }

      // Fetch and lock unpublished records
      const fetchResult = await client.query<{
        id: string;
        event_type: string;
        routing_key: string;
        payload: string;
        headers: Record<string, string>;
        created_at: Date;
      }>(
        `SELECT id, event_type, routing_key, payload::text, headers, created_at
         FROM messaging_outbox
         ORDER BY created_at ASC, id ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [batchSize],
      );

      if (fetchResult.rows.length === 0) {
        await client.query("ROLLBACK");
        return 0;
      }

      const records: OutboxRecord[] = fetchResult.rows.map((row) => ({
        id: row.id,
        event_type: row.event_type,
        routing_key: row.routing_key,
        payload: row.payload,
        headers: row.headers,
        created_at: row.created_at,
      }));

      const publishedIDs = await fn(records);

      if (publishedIDs.length > 0) {
        await client.query(
          "DELETE FROM messaging_outbox WHERE id = ANY($1::uuid[])",
          [publishedIDs],
        );
      }

      await client.query("COMMIT");
      return publishedIDs.length;
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }
}

async function insertRecord(
  client: { query: PoolClient["query"] },
  record: OutboxRecord,
): Promise<void> {
  await client.query(
    `INSERT INTO messaging_outbox (id, event_type, routing_key, payload, headers, created_at)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      record.id,
      record.event_type,
      record.routing_key,
      record.payload,
      JSON.stringify(record.headers),
      record.created_at,
    ],
  );
}
