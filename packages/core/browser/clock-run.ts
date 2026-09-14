/** Real-browser proof that wall-clock changes cannot defeat the worker request deadline. */
import { MinnowDatabaseClient } from "@minnowdb/core/client";

export interface ClockDeadlineResult {
  readonly settled: boolean;
  readonly errorName: string | undefined;
  readonly elapsedMs: number;
}

async function spawn(): Promise<Worker> {
  const worker = new Worker(new URL("./clock-worker.ts", import.meta.url), { type: "module" });
  // The 100 ms deadline below measures an active RPC, not cold module fetching or worker startup.
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      worker.terminate();
      reject(new Error("Clock worker did not start"));
    }, 10_000);
    worker.addEventListener("message", function ready(event: MessageEvent<unknown>) {
      if (event.data !== "clock-worker-ready") return;
      clearTimeout(timeout);
      worker.removeEventListener("message", ready);
      resolve();
    });
    worker.addEventListener(
      "error",
      (event) => {
        clearTimeout(timeout);
        worker.terminate();
        reject(new Error(event.message));
      },
      { once: true },
    );
  });
  return worker;
}

export async function runBackwardClockDeadline(): Promise<ClockDeadlineResult> {
  const worker = await spawn();
  const client = new MinnowDatabaseClient(worker, {
    store: { kind: "memory" },
    requestTimeoutMs: 100,
  });
  await client.ready();

  const realDateNow = Date.now;
  const started = performance.now();
  const pending: Promise<ClockDeadlineResult> = client.listTables().then(
    (): ClockDeadlineResult => ({
      settled: true,
      errorName: undefined,
      elapsedMs: performance.now() - started,
    }),
    (error: unknown): ClockDeadlineResult => ({
      settled: true,
      errorName: error instanceof Error ? error.name : undefined,
      elapsedMs: performance.now() - started,
    }),
  );
  // Jump a day backward after the request captured its start time. The transport continues to
  // report progress every 20 ms but never answers, so the client's ten-deadline cap must end it.
  Date.now = () => realDateNow() - 86_400_000;
  let guard: ReturnType<typeof setTimeout> | undefined;
  try {
    const guarded = new Promise<ClockDeadlineResult>((resolve) => {
      guard = setTimeout(
        () =>
          resolve({ settled: false, errorName: undefined, elapsedMs: performance.now() - started }),
        4_000,
      );
    });
    return await Promise.race([pending, guarded]);
  } finally {
    if (guard !== undefined) clearTimeout(guard);
    Date.now = realDateNow;
    await client.close({ terminateWorker: true }).catch(() => undefined);
  }
}
