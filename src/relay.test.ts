// MIT License
// Copyright (c) 2026 sparetimecoders

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { createRelay } from "./relay.js";
import type {
  OutboxProcessor,
  OutboxRecord,
  RawPublisher,
  Logger,
} from "./types.js";

function mockLogger(): Logger {
  return {
    info: vi.fn(),
    error: vi.fn(),
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
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("publishes events and deletes them", async () => {
    const records = [
      makeRecord("1", "user.created"),
      makeRecord("2", "user.updated"),
    ];

    const store: OutboxProcessor = {
      process: vi.fn(async (_batchSize, fn) => {
        const published = await fn(records);
        return published.length;
      }),
    };

    const published: Array<{ routingKey: string; payload: string }> = [];
    const publisher: RawPublisher = {
      publishRaw: vi.fn(async (routingKey, payload) => {
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
    await vi.advanceTimersByTimeAsync(0);
    await relay.stop();

    expect(published).toHaveLength(2);
    expect(published[0].routingKey).toBe("user.created");
    expect(published[1].routingKey).toBe("user.updated");
    expect(store.process).toHaveBeenCalledOnce();
  });

  it("stops when stop() is called", async () => {
    const store: OutboxProcessor = {
      process: vi.fn(async () => 0),
    };
    const publisher: RawPublisher = {
      publishRaw: vi.fn(),
    };

    const relay = createRelay(
      store,
      publisher,
      { pollIntervalMs: 100, batchSize: 10 },
      mockLogger(),
    );

    relay.start();
    await vi.advanceTimersByTimeAsync(0);
    await relay.stop();

    const callCount = (store.process as ReturnType<typeof vi.fn>).mock.calls
      .length;
    await vi.advanceTimersByTimeAsync(500);

    // No more calls after stop
    expect(store.process).toHaveBeenCalledTimes(callCount);
  });
});
