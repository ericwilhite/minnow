/**
 * Page side of browser-tests/next-yield.spec.ts: runs one scenario of next-yield-worker.ts in a
 * fresh dedicated worker and returns what it measured.
 */
import type { YieldKind, YieldResult } from "./next-yield-worker.js";

export async function runYield(kind: YieldKind): Promise<YieldResult> {
  const worker = new Worker(new URL("./next-yield-worker.ts", import.meta.url), { type: "module" });
  try {
    return await new Promise<YieldResult>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`yield worker (${kind}) did not finish`));
      }, 60_000);
      worker.addEventListener("message", (event: MessageEvent<YieldResult | "ready">) => {
        if (event.data === "ready") {
          worker.postMessage(kind);
          return;
        }
        clearTimeout(timer);
        resolve(event.data);
      });
      worker.addEventListener("error", (event) => {
        clearTimeout(timer);
        reject(new Error(event.message));
      });
    });
  } finally {
    worker.terminate();
  }
}
