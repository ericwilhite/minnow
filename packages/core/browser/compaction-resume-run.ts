import type { CompactionJobProgress } from "@minnowdb/core";
import { rehydrateError, type SerializedError } from "@minnowdb/core/worker-protocol";

interface Result {
  published: CompactionJobProgress;
  resumed: CompactionJobProgress;
  rows: Array<{ value: number }>;
  integrity: boolean;
}

export function runCompactionResume(kind: "indexeddb" | "opfs"): Promise<Result> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL("./compaction-resume-worker.ts", import.meta.url), {
      type: "module",
    });
    const timer = setTimeout(() => {
      worker.terminate();
      reject(new Error("Compaction resume interleaving did not complete"));
    }, 15_000);
    worker.onmessage = (event: MessageEvent<{ result?: Result; error?: SerializedError }>) => {
      clearTimeout(timer);
      worker.terminate();
      if (event.data.error !== undefined) reject(rehydrateError(event.data.error, new Map()));
      else if (event.data.result !== undefined) resolve(event.data.result);
      else reject(new Error("Compaction resume worker returned no result"));
    };
    worker.onerror = (event) => {
      clearTimeout(timer);
      worker.terminate();
      reject(new Error(event.message));
    };
    worker.postMessage(kind);
  });
}
