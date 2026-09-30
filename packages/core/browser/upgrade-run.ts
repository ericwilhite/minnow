export interface NativeUpgradeResult {
  tables: string[];
  blockValues: unknown[];
  walOnlyPreserved: boolean;
  rows: Array<Record<string, unknown>>;
  format: number;
  integrity: boolean;
  olderReaderRefused: boolean;
}

export async function runNativeUpgrade(
  files: Record<string, string>,
): Promise<NativeUpgradeResult> {
  const worker = new Worker(new URL("./upgrade-worker.ts", import.meta.url), { type: "module" });
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await new Promise<NativeUpgradeResult>((resolve, reject) => {
      timeout = setTimeout(() => reject(new Error("Native automatic upgrade timed out")), 30_000);
      worker.onerror = (event) => reject(new Error(event.message));
      worker.onmessage = (
        event: MessageEvent<{ result?: NativeUpgradeResult; error?: string }>,
      ) => {
        if (event.data.result !== undefined) resolve(event.data.result);
        else reject(new Error(event.data.error ?? "Native upgrade worker returned no result"));
      };
      worker.postMessage({ files, name: `native-upgrade-${crypto.randomUUID()}` });
    });
  } finally {
    clearTimeout(timeout);
    worker.terminate();
  }
}
