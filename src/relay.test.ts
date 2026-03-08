// MIT License
// Copyright (c) 2026 sparetimecoders

import { describe, it, expect, mock, beforeEach, afterEach } from "bun:test";
import { createRelay } from "./relay.js";
import type {
  OutboxProcessor,
  OutboxRecord,
  RawPublisher,
  Logger,
} from "./types.js";

function mockLogger(): Logger {
  return {
    info: mock(),
    error: mock(),
  } as Logger;
}

function makeRecord(id: string, routingKey: string): OutboxRecord {
  return {
    id,
    event_type: routingKey,
    routing_key: routingKey,
    payload: JSON.stringify({ id }),
    headers: { "ce-id": id },
    created_at: new Date(),
  };
}

describe("createRelay", () => {
  beforeEach(() => {
    mock.module("timers", () => ({}));
  });

  it("publishes events and deletes them", async () => {
    const records = [
      makeRecord("1", "user.created"),
      makeRecord("2", "user.updated"),
    ];

    const processFn = mock(async (_batchSize: number, fn: (records: OutboxRecord[]) => Promise<string[]>) => {
      const published = await fn(records);
      return published.length;
    });
    const store: OutboxProcessor = {
      process: processFn,
    };

    const published: Array<{ routingKey: string; payload: string }> = [];
    const publisher: RawPublisher = {
      publishRaw: mock(async (routingKey: string, payload: string) => {
        published.push({ routingKey, payload });
      }),
    };

    const relay = createRelay(
      store,
      publisher,
      { pollIntervalMs: 1000, batchSize: 100 },
      mockLogger(),
    );

    relay.start();
    // Allow the initial poll to execute
    await new Promise((resolve) => setTimeout(resolve, 50));
    await relay.stop();

    expect(published).toHaveLength(2);
    expect(published[0].routingKey).toBe("user.created");
    expect(published[1].routingKey).toBe("user.updated");
    expect(processFn).toHaveBeenCalledTimes(1);
  });

  it("stops when stop() is called", async () => {
    const processFn = mock(async () => 0);
    const store: OutboxProcessor = {
      process: processFn,
    };
    const publisher: RawPublisher = {
      publishRaw: mock(),
    };

    const relay = createRelay(
      store,
      publisher,
      { pollIntervalMs: 100, batchSize: 10 },
      mockLogger(),
    );

    relay.start();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await relay.stop();

    const callCount = processFn.mock.calls.length;
    await new Promise((resolve) => setTimeout(resolve, 200));

    // No more calls after stop
    expect(processFn).toHaveBeenCalledTimes(callCount);
  });
});
