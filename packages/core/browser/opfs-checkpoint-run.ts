import { rehydrateError, type SerializedError } from "@minnowdb/core/worker-protocol";

interface RecoveryResult {
  checkpointPair: Array<{ generation: number; lastSeq: number }>;
  tables: string[];
  rowIds?: string[][];
  integrity: boolean;
}

export async function runCheckpointRecovery(
  mode: "renew" | "append" | "mirror",
): Promise<RecoveryResult> {
  const name = `checkpoint-${crypto.randomUUID()}`;
  const start = Date.now() + 60_000;
  const stage = (phase: "prepare" | "verify") =>
    new Promise<RecoveryResult | undefined>((resolve, reject) => {
      const worker = new Worker(new URL("./opfs-checkpoint-worker.ts", import.meta.url), {
        type: "module",
      });
      const timer = setTimeout(() => {
        worker.terminate();
        reject(new Error(`Native checkpoint ${phase} did not complete`));
      }, 15_000);
      worker.onmessage = (
        event: MessageEvent<{ ready?: true; result?: RecoveryResult; error?: SerializedError }>,
      ) => {
        clearTimeout(timer);
        worker.terminate();
        if (event.data.error !== undefined) reject(rehydrateError(event.data.error, new Map()));
        else resolve(event.data.result);
      };
      worker.onerror = (event) => {
        clearTimeout(timer);
        worker.terminate();
        reject(new Error(event.message));
      };
      worker.postMessage({ name, mode, phase, start });
    });
  await stage("prepare");
  const result = await stage("verify");
  if (result === undefined) throw new Error("Recovery returned no result");
  return result;
}
