/** Real-worker proofs for graceful, bounded, and already-failed client cleanup. */
import { MinnowDatabaseClient, type ClientTransport } from "@minnowdb/core/client";
import { serializeError, type SerializedError } from "@minnowdb/core/worker-protocol";

type Mode = "slow" | "stuck" | "init-failure";

class TrackingWorkerTransport implements ClientTransport {
  readonly removed: string[] = [];
  disposeCalls = 0;
  terminations = 0;

  constructor(private readonly worker: Worker) {}

  postMessage(message: unknown, options?: { transfer: ArrayBuffer[] }): void {
    if ((message as { method?: unknown }).method === "dispose") this.disposeCalls += 1;
    if (options === undefined) this.worker.postMessage(message);
    else this.worker.postMessage(message, { transfer: options.transfer });
  }

  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
  addEventListener(
    type: "error" | "messageerror",
    listener: (event?: ErrorEvent | MessageEvent<unknown>) => void,
  ): void;
  addEventListener(
    type: "message" | "error" | "messageerror",
    listener:
      | ((event: MessageEvent<unknown>) => void)
      | ((event?: ErrorEvent | MessageEvent<unknown>) => void),
  ): void {
    this.worker.addEventListener(type, listener as EventListener);
  }

  removeEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
  removeEventListener(
    type: "error" | "messageerror",
    listener: (event?: ErrorEvent | MessageEvent<unknown>) => void,
  ): void;
  removeEventListener(
    type: "message" | "error" | "messageerror",
    listener:
      | ((event: MessageEvent<unknown>) => void)
      | ((event?: ErrorEvent | MessageEvent<unknown>) => void),
  ): void {
    this.removed.push(type);
    this.worker.removeEventListener(type, listener as EventListener);
  }

  terminate(): void {
    this.terminations += 1;
    this.worker.terminate();
  }
}

async function start(mode: Mode): Promise<TrackingWorkerTransport> {
  const worker = new Worker(new URL("./dispose-keepalive-worker.ts", import.meta.url), {
    type: "module",
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Dispose test worker did not start")), 5_000);
      worker.addEventListener(
        "message",
        (event: MessageEvent<{ kind?: unknown }>) => {
          if (event.data.kind !== "dispose-test-ready") return;
          clearTimeout(timer);
          resolve();
        },
        { once: true },
      );
      worker.addEventListener(
        "error",
        (event) => {
          clearTimeout(timer);
          reject(new Error(event.message));
        },
        { once: true },
      );
      worker.postMessage({ mode });
    });
    return new TrackingWorkerTransport(worker);
  } catch (error) {
    worker.terminate();
    throw error;
  }
}

export async function runDisposeKeepalive(): Promise<{ elapsedMs: number }> {
  const transport = await start("slow");
  const client = new MinnowDatabaseClient(transport, { store: { kind: "memory" } });
  try {
    await client.ready();
    const startedAt = performance.now();
    await client.close({ terminateWorker: true });
    return { elapsedMs: performance.now() - startedAt };
  } catch (error) {
    if (transport.terminations === 0) transport.terminate();
    throw error;
  }
}

export async function runDisposeAbsoluteCap(): Promise<{
  elapsedMs: number;
  error: SerializedError;
  disposeCalls: number;
  terminations: number;
}> {
  const transport = await start("stuck");
  const client = new MinnowDatabaseClient(transport, { store: { kind: "memory" } });
  try {
    await client.ready();
    const startedAt = performance.now();
    let error: unknown;
    try {
      await client.close({ terminateWorker: true, timeoutMs: 300 });
    } catch (caught) {
      error = caught;
    }
    if (error === undefined) throw new Error("Permanently stuck disposal unexpectedly completed");
    return {
      elapsedMs: performance.now() - startedAt,
      error: serializeError(error),
      disposeCalls: transport.disposeCalls,
      terminations: transport.terminations,
    };
  } finally {
    if (transport.terminations === 0) transport.terminate();
  }
}

export async function runFailedInitializationClose(): Promise<{
  readyError: SerializedError;
  disposeCalls: number;
  removed: string[];
  terminations: number;
}> {
  const transport = await start("init-failure");
  const client = new MinnowDatabaseClient(transport, { store: { kind: "memory" } });
  try {
    let readyError: unknown;
    try {
      await client.ready();
    } catch (error) {
      readyError = error;
    }
    if (readyError === undefined) throw new Error("Worker initialization unexpectedly completed");
    await client.close({ terminateWorker: true });
    return {
      readyError: serializeError(readyError),
      disposeCalls: transport.disposeCalls,
      removed: transport.removed.sort(),
      terminations: transport.terminations,
    };
  } finally {
    if (transport.terminations === 0) transport.terminate();
  }
}
