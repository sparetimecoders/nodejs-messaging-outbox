// MIT License
// Copyright (c) 2026 sparetimecoders

import { describe, it, expect, mock } from "bun:test";
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
  it("publishes events and deletes them", async () => {
    const records = [
      makeRecord("1", "user.created"),
      makeRecord("2", "user.updated"),
    ];

    let processDone: () => void;
    const processed = new Promise<void>((r) => {
      processDone = r;
    });

    const processFn = mock(
      async (
        _batchSize: number,
        fn: (records: OutboxRecord[]) => Promise<string[]>,
      ) => {
        const published = await fn(records);
        processDone();
        return published.length;
      },
    );
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

    await relay.start();
    await processed;
    await relay.stop();

    expect(published).toHaveLength(2);
    expect(published[0].routingKey).toBe("user.created");
    expect(published[1].routingKey).toBe("user.updated");
    expect(processFn).toHaveBeenCalledTimes(1);
  });

  it("stops when stop() is called", async () => {
    let processDone: () => void;
    const processed = new Promise<void>((r) => {
      processDone = r;
    });

    const processFn = mock(async () => {
      processDone();
      return 0;
    });
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

    await relay.start();
    await processed;
    await relay.stop();

    const callCount = processFn.mock.calls.length;

    // No more calls after stop
    expect(processFn).toHaveBeenCalledTimes(callCount);
  });

  it("rejects batchSize <= 0", () => {
    expect(() =>
      createRelay(
        { process: mock() } as OutboxProcessor,
        { publishRaw: mock() } as RawPublisher,
        { batchSize: 0 },
        mockLogger(),
      ),
    ).toThrow("batchSize must be positive");
  });

  it("rejects pollIntervalMs <= 0", () => {
    expect(() =>
      createRelay(
        { process: mock() } as OutboxProcessor,
        { publishRaw: mock() } as RawPublisher,
        { pollIntervalMs: -1 },
        mockLogger(),
      ),
    ).toThrow("pollIntervalMs must be positive");
  });
});
