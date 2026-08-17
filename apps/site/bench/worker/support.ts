/// <reference lib="webworker" />
/**
 * Cross-cutting worker plumbing: progress emission, cancellation, memory checkpoints
 * answered by the window, and small IndexedDB/arithmetic helpers.
 */
import { protocolVersion, type ProgressResponse } from "@minnowdb/core/worker-protocol";
import { unavailableMemorySample, type MemorySample } from "../memory-probe";
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

/**
 * Memory checkpoints answered by the window. A worker cannot read either memory API
 * itself, so it publishes a checkpoint through the progress channel and waits for the
 * reply keyed by token. Resolves to an unavailable sample rather than hanging when the
 * page cannot answer.
 */
const pendingMemorySamples = new Map<string, (sample: MemorySample) => void>();
let memoryCheckpointCounter = 0;

export function requestMemorySample(requestId: string, label: string): Promise<MemorySample> {
  memoryCheckpointCounter += 1;
  const token = `${requestId}:${String(memoryCheckpointCounter)}`;
  const sample = new Promise<MemorySample>((resolve) => {
    pendingMemorySamples.set(token, resolve);
    // The measurement itself waits for a garbage collection, so allow it real time.
    setTimeout(() => {
      if (pendingMemorySamples.delete(token)) resolve(unavailableMemorySample);
    }, 20_000);
  });
  self.postMessage({
    version: protocolVersion,
    requestId,
    kind: "progress",
    progress: { phase: "memory-checkpoint", token, label, completed: 0, total: 1, message: label },
  } satisfies ProgressResponse);
  return sample;
}

export function resolveMemorySample(token: string, sample: MemorySample): void {
  const resolve = pendingMemorySamples.get(token);
  pendingMemorySamples.delete(token);
  resolve?.(sample);
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

export async function storageEstimate(): Promise<StorageEstimate> {
  try {
    return await navigator.storage.estimate();
  } catch {
    return {};
  }
}

export function sum(values: number[]): number {
  return values.reduce((total, value) => total + value, 0);
}

export function difference(after: number | null, before: number | null): number | null {
  return after === null || before === null ? null : after - before;
}

export function formatRate(rowsPerSecond: number): string {
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(rowsPerSecond)} rows/s`;
}

export function describePlatform(userAgent: string): string {
  if (userAgent.includes("Mac OS X") || userAgent.includes("Macintosh")) return "macOS";
  if (userAgent.includes("Windows")) return "Windows";
  if (userAgent.includes("Android")) return "Android";
  if (userAgent.includes("Linux")) return "Linux";
  return "Unknown";
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
