import { rehydrateError, type SerializedError } from "@minnowdb/core/worker-protocol";

interface Prepared {
  firstRemoval: number;
  secondRemoval: number;
  versions: number[];
  walBytes: number;
}
interface Reopened {
  versions: number[];
  current: number;
  integrity: boolean;
}

export async function runManifestPrefix(
  mode: "prefix" | "replay",
): Promise<{ prepared: Prepared; reopened: Reopened }> {
  const name = `manifest-prefix-${crypto.randomUUID()}`;
  const stage = <T>(phase: "prepare" | "verify") =>
    new Promise<T>((resolve, reject) => {
      const worker = new Worker(new URL("./manifest-prefix-worker.ts", import.meta.url), {
        type: "module",
      });
      const timer = setTimeout(() => {
        worker.terminate();
        reject(new Error(`Manifest ${phase} did not complete`));
      }, 15_000);
      worker.onmessage = (event: MessageEvent<{ result?: T; error?: SerializedError }>) => {
        clearTimeout(timer);
        worker.terminate();
        if (event.data.error !== undefined) reject(rehydrateError(event.data.error, new Map()));
        else if (event.data.result !== undefined) resolve(event.data.result);
        else reject(new Error("Manifest worker returned no result"));
      };
      worker.onerror = (event) => {
        clearTimeout(timer);
        worker.terminate();
        reject(new Error(event.message));
      };
      worker.postMessage({ name, mode, phase });
    });
  const prepared = await stage<Prepared>("prepare");
  const reopened = await stage<Reopened>("verify");
  return { prepared, reopened };
}
