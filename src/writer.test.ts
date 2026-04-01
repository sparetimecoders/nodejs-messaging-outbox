// MIT License
// Copyright (c) 2026 sparetimecoders

import { describe, it, expect } from "bun:test";
import { Writer } from "./writer.js";
import type { OutboxInserter, OutboxRecord } from "./types.js";

function mockInserter(): OutboxInserter & { inserted: OutboxRecord[] } {
  const inserted: OutboxRecord[] = [];
  return {
    inserted,
    async insert(record: OutboxRecord) {
      inserted.push(record);
    },
  };
}

describe("Writer", () => {
  it("creates a record with CloudEvents headers", async () => {
    const inserter = mockInserter();
    const writer = new Writer("test-service");

    await writer.write(inserter, {
      routingKey: "user.created",
      payload: { name: "alice" },
    });

    expect(inserter.inserted).toHaveLength(1);
    const record = inserter.inserted[0];
    expect(record.routing_key).toBe("user.created");
    expect(record.event_type).toBe("user.created");
    expect(JSON.parse(record.payload)).toEqual({ name: "alice" });
    expect(record.id).toBeTruthy();
    expect(record.headers["ce-source"]).toBe("test-service");
    expect(record.headers["ce-specversion"]).toBe("1.0");
    expect(record.headers["ce-type"]).toBe("user.created");
    expect(record.headers["ce-id"]).toBeTruthy();
    expect(record.headers["ce-time"]).toBeTruthy();
    expect(record.headers["ce-datacontenttype"]).toBe("application/json");
  });

  it("merges extra headers", async () => {
    const inserter = mockInserter();
    const writer = new Writer("test-service");

    await writer.write(inserter, {
      routingKey: "user.created",
      payload: { name: "alice" },
      headers: { "ce-subject": "user/123" },
    });

    expect(inserter.inserted[0].headers["ce-subject"]).toBe("user/123");
  });

  it("does not allow caller to override required CE headers", async () => {
    const inserter = mockInserter();
    const writer = new Writer("test-service");

    await writer.write(inserter, {
      routingKey: "user.created",
      payload: { name: "alice" },
      headers: { "ce-source": "attacker", "ce-type": "evil.event" },
    });

    const headers = inserter.inserted[0].headers;
    expect(headers["ce-source"]).toBe("test-service");
    expect(headers["ce-type"]).toBe("user.created");
  });
});
