export interface NativeUpgradeResult {
  tables: string[];
  blockValues: unknown[];
  walOnlyPreserved: boolean;
  rows: Array<Record<string, unknown>>;
  format: number;
  integrity: boolean;
  olderReaderRefused: boolean;
}

/** Every released build that writes an IndexedDB schema this build upgrades. */
export type IndexedDbWriterVersion = "0.10.0" | "0.12.1" | "0.13.1";

export interface IndexedDbUpgradeResult {
  /** The schema the released writer left. */
  releasedSchema: number;
  /** The schema after the current build's first open. */
  schema: number;
  /** The released writer's index lookups answer the same after the upgrade. */
  answersPreserved: boolean;
  /** A row from a commit past the old delta limit, found through its index. */
  newRow: unknown;
  matches: unknown[];
  reopenedSame: boolean;
  integrity: boolean;
  olderReadersRefused: boolean;
  /** The schema after every released reader was refused: they must not change it. */
  finalSchema: number;
}

function runUpgradeWorker<T>(message: Record<string, unknown>, timeoutMs: number): Promise<T> {
  const worker = new Worker(new URL("./upgrade-worker.ts", import.meta.url), { type: "module" });
  let timeout: ReturnType<typeof setTimeout> | undefined;
  return new Promise<T>((resolve, reject) => {
    timeout = setTimeout(() => reject(new Error("Native automatic upgrade timed out")), timeoutMs);
    worker.onerror = (event) => reject(new Error(event.message));
    worker.onmessage = (event: MessageEvent<{ result?: T; error?: string }>) => {
      if (event.data.result !== undefined) resolve(event.data.result);
      else reject(new Error(event.data.error ?? "Native upgrade worker returned no result"));
    };
    worker.postMessage(message);
  }).finally(() => {
    clearTimeout(timeout);
    worker.terminate();
  });
}

export function runNativeUpgrade(files: Record<string, string>): Promise<NativeUpgradeResult> {
  return runUpgradeWorker({ files, name: `native-upgrade-${crypto.randomUUID()}` }, 30_000);
}

/** A released writer's IndexedDB database through the automatic upgrade, in a real browser. */
export function runIndexedDbUpgrade(
  writer: IndexedDbWriterVersion,
): Promise<IndexedDbUpgradeResult> {
  return runUpgradeWorker(
    { kind: "indexeddb", writer, name: `indexeddb-upgrade-${crypto.randomUUID()}` },
    120_000,
  );
}
