# messaging-outbox

<p align="center">
  <strong>Transactional outbox pattern for the messaging framework -- reliable event publishing with PostgreSQL, CloudEvents, and OpenTelemetry.</strong>
</p>

<p align="center">
  <a href="https://github.com/sparetimecoders/nodejs-messaging-outbox/actions"><img alt="CI" src="https://github.com/sparetimecoders/nodejs-messaging-outbox/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://www.npmjs.com/package/@sparetimecoders/messaging-outbox"><img alt="npm" src="https://img.shields.io/npm/v/@sparetimecoders/messaging-outbox.svg"></a>
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
</p>

---

This package implements the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for the [@sparetimecoders/messaging](https://github.com/sparetimecoders/messaging) framework. Events are written to a database table within the same transaction as business data, then asynchronously relayed to a message broker by a background worker. This guarantees at-least-once delivery without distributed transactions.

A Go implementation is available at [go-messaging-outbox](https://github.com/sparetimecoders/go-messaging-outbox). Both share the same database schema and are interoperable.

## Installation

```sh
npm install @sparetimecoders/messaging-outbox
```

For the PostgreSQL store:

```sh
npm install pg
```

## How It Works

```
App Transaction                 Relay (background)              Broker
+--------------+               +------------------+           +--------+
| BEGIN         |               | BEGIN             |           |        |
| INSERT order  |               | Advisory lock     |           |        |
| INSERT outbox +-------------->| SELECT FOR UPDATE |           |        |
| COMMIT        |               | publishRaw -------+---------->| NATS / |
+--------------+               | DELETE outbox     |           | AMQP   |
                               | COMMIT            |           |        |
                               +------------------+           +--------+
```

1. **Write path**: The application inserts an outbox record in the same transaction as business data using `Writer.write()`.
2. **Relay**: A background relay polls the outbox table, publishes each record via a `RawPublisher`, and deletes it -- all within a single transaction.
3. **Leader election**: A PostgreSQL advisory lock (`pg_try_advisory_xact_lock`) ensures only one relay instance processes at a time.
4. **Concurrency safety**: `SELECT ... FOR UPDATE SKIP LOCKED` prevents duplicate delivery across relay instances.
5. **Hard delete**: Published records are deleted immediately (no `published_at` column).

## Quick Start

```typescript
import { Pool } from "pg";
import { Writer, createRelay } from "@sparetimecoders/messaging-outbox";
import { PostgresStore } from "@sparetimecoders/messaging-outbox/postgres";

const pool = new Pool({ connectionString: "postgres://localhost:5432/mydb" });

// Create the outbox store (runs migration by default)
const store = await PostgresStore.create(pool);

// Write an event within a business transaction
const writer = new Writer("order-service");
const client = await pool.connect();

try {
  await client.query("BEGIN");

  // Insert business data and outbox record in the same transaction
  await client.query("INSERT INTO orders (id, amount) VALUES ($1, $2)", ["abc-123", 42]);
  await writer.write(store.txInserter(client), {
    routingKey: "Order.Created",
    payload: { orderId: "abc-123", amount: 42 },
  });

  await client.query("COMMIT");
} catch (err) {
  await client.query("ROLLBACK");
  throw err;
} finally {
  client.release();
}

// Start the relay (connects to your broker via a RawPublisher adapter)
const relay = createRelay(store, rawPublisher, {
  pollIntervalMs: 1000,
  batchSize: 100,
}, logger);

relay.start();

// Graceful shutdown
process.on("SIGTERM", async () => {
  await relay.stop();
  await pool.end();
});
```

## Writing Events

Use `Writer` to create outbox records with [CloudEvents 1.0](https://cloudevents.io/) headers:

```typescript
const writer = new Writer("order-service");

const client = await pool.connect();
await client.query("BEGIN");

// Business write
await client.query("INSERT INTO orders ...");

// Outbox write (same transaction)
await writer.write(store.txInserter(client), {
  routingKey: "Order.Created",
  payload: { orderId: "abc-123", amount: 42 },
});

await client.query("COMMIT");
client.release();
```

### CloudEvents Headers

Every record includes these headers automatically:

| Header | Value |
|--------|-------|
| `ce-specversion` | `1.0` |
| `ce-type` | Routing key |
| `ce-source` | Service name |
| `ce-id` | UUID |
| `ce-time` | ISO 8601 timestamp |
| `ce-datacontenttype` | `application/json` |

Add custom headers via the `headers` field:

```typescript
await writer.write(inserter, {
  routingKey: "Order.Created",
  payload: { orderId: "abc-123" },
  headers: { "ce-subject": "orders/abc-123" },
});
```

## Running the Relay

The relay polls the outbox table and publishes events to a message broker:

```typescript
import { createRelay } from "@sparetimecoders/messaging-outbox";

const relay = createRelay(store, rawPublisher, {
  pollIntervalMs: 500,  // default: 1000
  batchSize: 200,       // default: 100
}, logger);

relay.start();

// Graceful shutdown (waits for in-flight batch to complete)
await relay.stop();
```

### Adaptive Polling

When a batch is full (published count >= batch size), the relay polls again immediately without waiting. When the batch is partial or empty, it waits for `pollIntervalMs` before the next poll.

### OpenTelemetry Tracing

The relay creates an OpenTelemetry span (`outbox.processEvents`) for each poll cycle with `outbox.batch_size` as a span attribute. Configure your `TracerProvider` as usual -- the relay uses `@opentelemetry/api` and picks up the global provider automatically.

## PostgreSQL Store

The `postgres` sub-package provides a production-ready store using [node-postgres (pg)](https://node-postgres.com/).

```typescript
import { PostgresStore } from "@sparetimecoders/messaging-outbox/postgres";

const store = await PostgresStore.create(pool);
```

### Migrations

By default, `PostgresStore.create()` runs an embedded migration that creates the `messaging_outbox` table and index. To manage migrations externally:

```typescript
const store = await PostgresStore.create(pool, { skipMigrations: true });
```

### Schema

```sql
CREATE TABLE IF NOT EXISTS messaging_outbox (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type  TEXT        NOT NULL,
    routing_key TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    headers     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messaging_outbox_created_at
    ON messaging_outbox (created_at, id);
```

### Interfaces

The store exposes two separate interfaces to prevent misuse:

| Interface | Method | Purpose |
|-----------|--------|---------|
| `OutboxInserter` | `insert(record)` | Write path -- insert within a caller-managed transaction |
| `OutboxProcessor` | `process(batchSize, fn)` | Read path -- relay fetch-publish-delete cycle |

Use `store.txInserter(client)` to get a transaction-scoped `OutboxInserter`. The `PostgresStore` itself implements `OutboxProcessor` for use with the relay.

## Interfaces

### Core Types

```typescript
interface OutboxRecord {
  id: string;
  event_type: string;
  routing_key: string;
  payload: string;              // JSON-serialized
  headers: Record<string, string>;
  created_at: Date;
}

interface OutboxEvent {
  routingKey: string;
  payload: Record<string, unknown>;
  headers?: Record<string, string>;
}

interface OutboxInserter {
  insert(record: OutboxRecord): Promise<void>;
}

interface OutboxProcessor {
  process(batchSize: number,
    fn: (records: OutboxRecord[]) => Promise<string[]>): Promise<number>;
}

interface RawPublisher {
  publishRaw(routingKey: string, payload: string,
    headers: Record<string, string>): Promise<void>;
}

interface RelayConfig {
  pollIntervalMs?: number;  // default: 1000
  batchSize?: number;       // default: 100
}

interface RelayHandle {
  start: () => void;
  stop: () => Promise<void>;
}
```

Implement `RawPublisher` to integrate with any message broker. Implement `OutboxInserter` and `OutboxProcessor` to use a different database backend.

## Development

```sh
# Install dependencies
npm install

# Run tests
npm test

# Build
npm run build
```

## License

MIT
