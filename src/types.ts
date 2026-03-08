// MIT License
// Copyright (c) 2026 sparetimecoders

/** A single outbox entry stored in the database. */
export interface OutboxRecord {
  id: string;
  event_type: string;
  routing_key: string;
  /** JSON-serialized payload (stored as string to match Go's []byte). */
  payload: string;
  headers: Record<string, string>;
  created_at: Date;
}

/** Input for writing an outbox event. */
export interface OutboxEvent {
  routingKey: string;
  payload: Record<string, unknown>;
  headers?: Record<string, string>;
}

/** Write path: inserts outbox records within a caller-managed transaction. */
export interface OutboxInserter {
  insert(record: OutboxRecord): Promise<void>;
}

/**
 * Read path: runs the relay fetch-publish-delete cycle within a transaction.
 * A PostgreSQL implementation is provided in the postgres sub-package.
 */
export interface OutboxProcessor {
  /**
   * Process runs within a single transaction:
   * 1. Acquire leader lock
   * 2. Fetch up to batchSize unpublished records (FOR UPDATE SKIP LOCKED)
   * 3. Call fn with the records
   * 4. Delete records returned by fn
   * 5. Commit
   *
   * Returns the number of successfully processed records.
   */
  process(
    batchSize: number,
    fn: (records: OutboxRecord[]) => Promise<string[]>,
  ): Promise<number>;
}

/** Publishes a pre-serialized message to the broker. */
export interface RawPublisher {
  publishRaw(
    routingKey: string,
    payload: string,
    headers: Record<string, string>,
  ): Promise<void>;
}

/** Pino-compatible logger subset. */
export interface Logger {
  info(obj: Record<string, unknown>, msg?: string): void;
  info(msg: string): void;
  error(obj: Record<string, unknown>, msg?: string): void;
  error(msg: string): void;
}

/** Relay polling configuration. */
export interface RelayConfig {
  /** Delay between poll cycles when the previous batch was not full. Default: 1000ms. */
  pollIntervalMs?: number;
  /** Maximum number of events fetched per poll cycle. Default: 100. */
  batchSize?: number;
}

/** Controls the relay lifecycle. */
export interface RelayHandle {
  start: () => void;
  stop: () => Promise<void>;
}
