// MIT License
// Copyright (c) 2026 sparetimecoders

import { randomUUID } from "node:crypto";
import {
  CESpecVersion,
  CESpecVersionValue,
  CEType,
  CESource,
  CEDataContentType,
  CETime,
  CEID,
} from "@sparetimecoders/messaging";
import type { OutboxInserter, OutboxEvent, OutboxRecord } from "./types.js";

/**
 * Writer creates outbox records with CloudEvents headers.
 * Call insert on the store within your database transaction.
 */
export class Writer {
  private readonly serviceName: string;

  constructor(serviceName: string) {
    this.serviceName = serviceName;
  }

  /**
   * Build an OutboxRecord from an event and insert it via the store.
   * The caller MUST call this within the same database transaction
   * as business writes to guarantee atomicity.
   */
  async write(inserter: OutboxInserter, event: OutboxEvent): Promise<void> {
    const now = new Date();
    const ceHeaders: Record<string, string> = {
      ...event.headers,
      [CESpecVersion]: CESpecVersionValue,
      [CEType]: event.routingKey,
      [CESource]: this.serviceName,
      [CEID]: randomUUID(),
      [CETime]: now.toISOString(),
      [CEDataContentType]: "application/json",
    };

    const record: OutboxRecord = {
      id: ceHeaders[CEID],
      event_type: event.routingKey,
      routing_key: event.routingKey,
      payload: JSON.stringify(event.payload),
      headers: ceHeaders,
      created_at: now,
    };

    await inserter.insert(record);
  }
}
