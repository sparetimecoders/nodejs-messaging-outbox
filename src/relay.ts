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
const MAX_BACKOFF_MS = 30_000;

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

  if (batchSize <= 0) throw new Error("batchSize must be positive");
  if (pollIntervalMs <= 0) throw new Error("pollIntervalMs must be positive");

  let running = false;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  let inflight: Promise<void> | null = null;
  let stopPromise: Promise<void> | null = null;
  let consecutiveErrors = 0;

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

        // When the batch was full there may be more events waiting,
        // so we poll again immediately rather than waiting for the
        // next interval. This is a conservative heuristic: it avoids
        // an extra SELECT to check whether more rows exist.
        batchWasFull = published >= batchSize;
        consecutiveErrors = 0;
      } catch (err) {
        consecutiveErrors += 1;
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
      const backoffDelay =
        consecutiveErrors > 0
          ? Math.min(
              pollIntervalMs * Math.pow(2, consecutiveErrors),
              MAX_BACKOFF_MS,
            ) + Math.random() * 1000
          : 0;
      const delay = batchWasFull ? 0 : Math.max(pollIntervalMs, backoffDelay);
      timeoutId = setTimeout(processEvents, delay);
    }
  }

  return {
    async start() {
      if (stopPromise) {
        await stopPromise;
        stopPromise = null;
      }
      if (running) return;
      running = true;
      consecutiveErrors = 0;
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
      const doStop = async (): Promise<void> => {
        if (inflight) {
          await inflight;
        }
        log.info({}, "Outbox relay stopped");
      };
      stopPromise = doStop();
      await stopPromise;
    },
  };
}
