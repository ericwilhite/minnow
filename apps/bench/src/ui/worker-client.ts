/**
 * Thin request/response client for the benchmark worker. Correlates responses by
 * requestId, forwards progress messages, and answers the worker's memory checkpoints —
 * neither memory API exists in a worker, so the worker delegates each sample to this
 * thread through the progress channel.
 */
import {
  protocolVersion,
  type WorkerOperation,
  type WorkerResponse,
} from "@minnowdb/core/worker-protocol";
import { sampleMemory } from "../memory-probe.js";
import type { WorkProgress } from "../protocol.js";

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  onProgress?: ((progress: WorkProgress) => void) | undefined;
}

export class BenchWorker {
  private readonly worker: Worker;
  private readonly pending = new Map<string, PendingRequest>();

  constructor() {
    this.worker = new Worker(new URL("../worker/index.ts", import.meta.url), { type: "module" });
    this.worker.addEventListener("message", (event: MessageEvent<WorkerResponse>) => {
      this.receive(event.data);
    });
    this.worker.addEventListener("error", (event) => {
      const error = new Error(`Worker error: ${event.message}`);
      for (const request of this.pending.values()) request.reject(error);
      this.pending.clear();
    });
  }

  /** Starts a request and returns its id (for cancel()) alongside the result promise. */
  start<T>(
    operation: WorkerOperation,
    payload: unknown,
    onProgress?: (progress: WorkProgress) => void,
  ): { requestId: string; result: Promise<T> } {
    const requestId = crypto.randomUUID();
    const result = new Promise<T>((resolve, reject) => {
      this.pending.set(requestId, {
        resolve: resolve as (value: unknown) => void,
        reject,
        onProgress,
      });
      this.worker.postMessage({ version: protocolVersion, requestId, operation, payload });
    });
    return { requestId, result };
  }

  request<T>(
    operation: WorkerOperation,
    payload: unknown,
    onProgress?: (progress: WorkProgress) => void,
  ): Promise<T> {
    return this.start<T>(operation, payload, onProgress).result;
  }

  /** Asks the worker to abandon a running request at its next cancellation point. */
  cancel(requestId: string): void {
    this.worker.postMessage({
      version: protocolVersion,
      requestId: crypto.randomUUID(),
      operation: "cancelBenchmark",
      payload: { requestId },
    });
  }

  private receive(message: WorkerResponse): void {
    if (message.kind === "progress") {
      const checkpoint = asMemoryCheckpoint(message.progress);
      if (checkpoint !== undefined) {
        void sampleMemory().then((sample) => {
          this.worker.postMessage({
            version: protocolVersion,
            requestId: crypto.randomUUID(),
            operation: "memorySample",
            payload: { token: checkpoint.token, sample },
          });
        });
        return;
      }
      this.pending.get(message.requestId)?.onProgress?.(message.progress as WorkProgress);
      return;
    }
    const request = this.pending.get(message.requestId);
    if (request === undefined) return;
    this.pending.delete(message.requestId);
    if (message.kind === "failure") {
      request.reject(new Error(`${message.error.name}: ${message.error.message}`));
    } else {
      request.resolve(message.result);
    }
  }
}

function asMemoryCheckpoint(progress: unknown): { token: string } | undefined {
  if (typeof progress !== "object" || progress === null) return undefined;
  const candidate = progress as { phase?: unknown; token?: unknown };
  if (candidate.phase !== "memory-checkpoint" || typeof candidate.token !== "string")
    return undefined;
  return { token: candidate.token };
}
