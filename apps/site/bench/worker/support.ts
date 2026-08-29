/// <reference lib="webworker" />
/**
 * Cross-cutting worker plumbing: progress emission, cancellation, and small
 * IndexedDB/arithmetic helpers.
 */
import { protocolVersion, type ProgressResponse } from "@minnowdb/core/worker-protocol";
import { engineIds } from "../protocol";
import type { EngineId, WorkProgress } from "../protocol";

export const cancelledRuns = new Set<string>();

export function validateDatasetSuitePayload(value: unknown): {
  datasetId: string;
  engines: EngineId[];
} {
  if (typeof value !== "object" || value === null) throw new Error("Invalid suite payload");
  const payload = value as { datasetId?: unknown; engines?: unknown };
  if (typeof payload.datasetId !== "string" || payload.datasetId.length === 0) {
    throw new TypeError("Dataset id must be a non-empty string");
  }
  const rawEngines: unknown = payload.engines;
  if (!Array.isArray(rawEngines) || rawEngines.length === 0) {
    throw new Error("Select at least one valid engine");
  }
  const engines = rawEngines.filter(
    (engine: unknown): engine is EngineId =>
      typeof engine === "string" && engineIds.some((candidate) => candidate === engine),
  );
  if (engines.length !== rawEngines.length) throw new Error("Select at least one valid engine");
  return { datasetId: payload.datasetId, engines: [...new Set(engines)] };
}

export function assertNotCancelled(requestId: string): void {
  if (cancelledRuns.has(requestId)) throw new DOMException("Run cancelled", "AbortError");
}

export function progress(requestId: string, value: WorkProgress): void {
  const response: ProgressResponse<WorkProgress> = {
    version: protocolVersion,
    requestId,
    kind: "progress",
    progress: value,
  };
  self.postMessage(response);
}

export async function deleteDatabase(name: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(name);
    request.addEventListener("success", () => resolve(), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("Database deletion failed")),
      { once: true },
    );
    request.addEventListener("blocked", () => reject(new Error("Database deletion was blocked")), {
      once: true,
    });
  });
}

export function indexedDbRequest<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.addEventListener("success", () => resolve(request.result), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("IndexedDB request failed")),
      { once: true },
    );
  });
}

export function indexedDbTransactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.addEventListener("complete", () => resolve(), { once: true });
    transaction.addEventListener(
      "error",
      () => reject(transaction.error ?? new Error("IndexedDB transaction failed")),
      { once: true },
    );
    transaction.addEventListener(
      "abort",
      () => reject(transaction.error ?? new Error("IndexedDB transaction aborted")),
      { once: true },
    );
  });
}

export function sum(values: number[]): number {
  return values.reduce((total, value) => total + value, 0);
}

export function difference(after: number | null, before: number | null): number | null {
  return after === null || before === null ? null : after - before;
}

export function getRequestId(value: unknown): string {
  if (typeof value === "object" && value !== null && "requestId" in value) {
    const requestId = value.requestId;
    if (typeof requestId === "string") return requestId;
  }
  return "unknown";
}

/** Median and p95 over a sorted copy of the samples. */
export function summarizeSamples(samples: number[]): { medianMs: number; p95Ms: number } {
  const sorted = [...samples].sort((left, right) => left - right);
  const medianMs = sorted[Math.floor(sorted.length / 2)] ?? 0;
  const p95Ms = sorted[Math.ceil(sorted.length * 0.95) - 1] ?? medianMs;
  return { medianMs, p95Ms };
}

/**
 * How long one timed window should last. `performance.now()` is deliberately coarse — 100µs on an
 * ordinary origin, 5µs on a cross-origin-isolated one — so timing a single execution of anything
 * quick reads back as a multiple of the clock's step rather than as its cost. Five milliseconds
 * puts even the coarse clock's quantization under 2% of the window.
 */
const TARGET_WINDOW_MS = 5;
const MAX_BATCH = 4_096;

interface RepeatedMeasurement {
  medianMs: number;
  p95Ms: number;
  /** Executions per timed window. One means the operation was slow enough to time directly. */
  batchSize: number;
}

/**
 * Times `run` by the batch: enough executions per window that the clock's resolution stops
 * mattering, divided back down to the cost of one. A query that takes 4µs is measured as 4µs
 * instead of landing on 0.00 or 0.10 depending on which side of a tick it fell.
 *
 * The batch is sized by doubling until a window is long enough, which costs a few extra untimed
 * executions and needs no guess about how fast the engine is.
 */
export async function measureRepeated(
  run: () => Promise<unknown>,
  samples: number,
): Promise<RepeatedMeasurement> {
  let batchSize = 1;
  for (;;) {
    const started = performance.now();
    for (let index = 0; index < batchSize; index += 1) await run();
    const elapsed = performance.now() - started;
    if (elapsed >= TARGET_WINDOW_MS || batchSize >= MAX_BATCH) break;
    // Aim straight at the target from what this window cost, but never trust it to shrink the
    // batch, and never take less than a doubling — a window under the clock's resolution reads
    // as zero and would otherwise divide by nothing.
    const projected = Math.ceil((batchSize * TARGET_WINDOW_MS) / Math.max(elapsed, 0.05));
    batchSize = Math.min(MAX_BATCH, Math.max(batchSize * 2, projected));
  }
  const windows: number[] = [];
  for (let sample = 0; sample < samples; sample += 1) {
    const started = performance.now();
    for (let index = 0; index < batchSize; index += 1) await run();
    windows.push((performance.now() - started) / batchSize);
  }
  return { ...summarizeSamples(windows), batchSize };
}
