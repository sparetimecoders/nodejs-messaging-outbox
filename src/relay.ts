// MIT License
// Copyright (c) 2026 sparetimecoders

import { trace, SpanStatusCode } from "@opentelemetry/api";
import type {
  OutboxProcessor,
  RawPublisher,
  RelayConfig,
  RelayHandle,
  Logger,
} from "./types.js";

const tracer = trace.getTracer("outbox-relay");

/**
 * Creates a relay that polls the outbox store and publishes events to the broker.
 * Records are deleted after successful publication within the same transaction.
 */
export function createRelay(
  store: OutboxProcessor,
  publisher: RawPublisher,
  config: RelayConfig,
  log: Logger,
): RelayHandle {
  const pollIntervalMs = config.pollIntervalMs ?? 1000;
  const batchSize = config.batchSize ?? 100;

  let running = false;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  let inflight: Promise<void> | null = null;

  async function processEvents(): Promise<void> {
    if (!running) return;

    let batchWasFull = false;
    await tracer.startActiveSpan("outbox.processEvents", async (span) => {
      try {
        const published = await store.process(
          batchSize,
          async (records) => {
            span.setAttribute("outbox.batch_size", records.length);
            const publishedIDs: string[] = [];

            for (const record of records) {
              if (!running) break;

              try {
                await publisher.publishRaw(
                  record.routing_key,
                  record.payload,
                  record.headers,
                );
                publishedIDs.push(record.id);
              } catch (err) {
                log.error(
                  { err, eventId: record.id },
                  "Failed to publish event",
                );
                break;
              }
            }

            return publishedIDs;
          },
        );

        batchWasFull = published >= batchSize;
      } catch (err) {
        span.recordException(
          err instanceof Error ? err : new Error(String(err)),
        );
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: String(err),
        });
        log.error({ err }, "Error processing outbox events");
      } finally {
        span.end();
      }
    });

    if (running) {
      const delay = batchWasFull ? 0 : pollIntervalMs;
      timeoutId = setTimeout(processEvents, delay);
    }
  }

  return {
    start() {
      if (running) return;
      running = true;
      log.info(
        { pollIntervalMs, batchSize },
        "Outbox relay started",
      );
      inflight = processEvents().finally(() => {
        inflight = null;
      });
    },

    async stop() {
      running = false;
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      if (inflight) {
        await inflight;
      }
      log.info("Outbox relay stopped");
    },
  };
}
