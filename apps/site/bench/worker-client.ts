/**
 * Thin request/response client for the benchmark worker. Correlates responses by
 * requestId and forwards progress messages.
 */
import {
  protocolVersion,
  type WorkerOperation,
  type WorkerResponse,
} from "@minnowdb/core/worker-protocol";
import type { WorkProgress } from "./protocol";

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  onProgress?: ((progress: WorkProgress) => void) | undefined;
}

export class BenchWorker {
  private readonly worker: Worker;
  private readonly pending = new Map<string, PendingRequest>();

  constructor() {
    this.worker = new Worker(new URL("./worker/index.ts", import.meta.url), { type: "module" });
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

  /** Tears the worker down. Anything still in flight rejects rather than hanging. */
  terminate(): void {
    const error = new Error("The benchmark worker was terminated");
    for (const request of this.pending.values()) request.reject(error);
    this.pending.clear();
    this.worker.terminate();
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
