/** Page side of the real-storage owner-ready lane. CPU marks are opt-in and bracket each phase. */
import type { OwnerBenchRequest } from "./next-storage-owner-worker.js";
export async function runOwnerReopen(
  request: OwnerBenchRequest,
  sample: (() => Promise<unknown>) | null = null,
): Promise<{ result: unknown; readyWallMs: number | null; cpu: unknown[] }> {
  const worker = new Worker(new URL("./next-storage-owner-worker.ts", import.meta.url), {
    type: "module",
  });
  const cpu: unknown[] = [];
  let started = 0;
  let readyWallMs: number | null = null;
  try {
    const result = await new Promise<unknown>((resolve, reject) => {
      worker.onerror = (event) => reject(new Error(`owner benchmark worker: ${event.message}`));
      worker.onmessage = (
        event: MessageEvent<{
          boot?: boolean;
          ready?: boolean;
          result?: unknown;
          error?: string;
          phase?: unknown;
          id?: number;
          progress?: string;
        }>,
      ) => {
        const data = event.data;
        if (data.boot)
          void (async () => {
            if (sample !== null) cpu.push(await sample());
            started = performance.now();
            worker.postMessage(request);
          })().catch(reject);
        if (data.ready) {
          readyWallMs = performance.now() - started;
          worker.postMessage({ readyAck: true });
        }
        if (data.progress !== undefined) console.log(`owner-load: ${data.progress}`);
        if (data.phase !== undefined)
          void (async () => {
            if (sample !== null) cpu.push(await sample());
            worker.postMessage({ ack: data.id });
          })().catch(reject);
        if ("result" in data) resolve(data.result);
        if (data.error !== undefined) reject(new Error(data.error));
      };
    });
    return { result, readyWallMs, cpu };
  } finally {
    worker.terminate();
  }
}
