export interface NativeUpgradeResult {
  tables: string[];
  blockValues: unknown[];
  walOnlyPreserved: boolean;
  /** The row written after the upgrade, read back at the key a rekey moved it to. */
  rows: Array<Record<string, unknown>>;
  /** Whether the internal key-forwards table the rekey created is marked as such. */
  keyForwards: boolean;
  format: number;
  integrity: boolean;
  olderReaderRefused: boolean;
}

/** A released OPFS writer's own database through the automatic upgrade. */
export interface ReleasedOpfsUpgradeResult {
  /** The layout the released writer left. */
  releasedFormat: number;
  /** The layout after the current build's first open. */
  format: number;
  /** The released writer's rows, index lookups, and sequence read the same after the upgrade. */
  answersPreserved: boolean;
  /** The sequence's next value: the released writer took three. */
  nextValue: unknown;
  /** A row found by its new key after a rekey, after a reopen. */
  rekeyed: unknown;
  keyForwards: boolean;
  integrity: boolean;
  olderReadersRefused: boolean;
  /** The layout after every released reader was refused: they must not change it. */
  finalFormat: number;
}

/** Every released build that writes an IndexedDB schema this build upgrades. */
export type IndexedDbWriterVersion = "0.10.0" | "0.12.1" | "0.13.1" | "0.14.1" | "0.15.0";

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
  /** A row found through its index by its new key after a rekey, after a reopen. */
  rekeyed: unknown;
  keyForwards: boolean;
  /**
   * Rows by `tag` after a column was added to the released writer's populated table and the
   * table was folded: the older rows take the column's default.
   */
  foldedAcrossAddColumn: unknown;
  /**
   * The released writer's full-text index on its append-only table after a fold: the index
   * states, and the full-text matches it answers.
   */
  foldedFullText: { states: string[]; matches: unknown[] };
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

/** The released 0.14.1 writer's OPFS database through the automatic upgrade. */
export function runReleasedOpfsUpgrade(): Promise<ReleasedOpfsUpgradeResult> {
  return runUpgradeWorker(
    { kind: "opfs-released", name: `opfs-released-upgrade-${crypto.randomUUID()}` },
    60_000,
  );
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
