/** Read-only identification of the shipped 0.x stores. This is a refusal, never an upgrade. */
export class LegacyDataError extends Error {
  readonly code = "MINNOW_LEGACY_DATA";
  constructor(
    readonly store: "opfs" | "indexeddb",
    readonly location: string,
  ) {
    super(
      `The database at ${store === "opfs" ? "OPFS" : "IndexedDB"} "${location}" was written by Minnow 0.x; Minnow 1.0 does not open it. The old data has not been changed. Use a new database name or explicitly delete the old database.`,
    );
    this.name = "LegacyDataError";
  }
}

/** Common to every shipped IndexedDB schema I1–I8; inspect shape, not an unrelated app's version. */
export function isLegacyIndexedDb(stores: readonly string[]): boolean {
  return ["catalog", "manifests", "blocks"].every((store) => stores.includes(store));
}

/** Missing databases are aborted during creation, so this probe leaves no database behind. */
export function indexedDbStores(factory: IDBFactory, name: string): Promise<string[] | null> {
  return new Promise((resolve, reject) => {
    let missing = false;
    let settled = false;
    const request = factory.open(name);
    request.onupgradeneeded = () => {
      missing = true;
      request.transaction?.abort();
    };
    request.onsuccess = () => {
      const database = request.result;
      const stores = Array.from(database.objectStoreNames as ArrayLike<string>);
      database.close(); // Also close a late success after onblocked rejected.
      if (!settled) resolve(stores);
      settled = true;
    };
    request.onerror = (event) => {
      if (missing) {
        event.preventDefault();
        if (!settled) resolve(null);
      } else if (!settled) reject(request.error ?? new Error(`Cannot inspect IndexedDB ${name}`));
      settled = true;
    };
    request.onblocked = () => {
      settled = true;
      reject(new Error(`IndexedDB ${name} is blocked by another connection`));
    };
  });
}

export async function refuseLegacyIndexedDb(factory: IDBFactory, name: string): Promise<void> {
  const stores = await indexedDbStores(factory, name);
  if (stores !== null && isLegacyIndexedDb(stores)) throw new LegacyDataError("indexeddb", name);
}

/** Native directory handles enumerate entries; internal storage-kit adapters need not do so. */
export async function refuseLegacyOpfs(
  directory: { keys?(): AsyncIterableIterator<string> },
  location: string,
): Promise<void> {
  if (directory.keys === undefined) return;
  for await (const name of directory.keys())
    if (
      ["format.json", "wal", "checkpoint-a", "checkpoint-b", "extents", "snapshots-v1"].includes(
        name,
      )
    )
      throw new LegacyDataError("opfs", location);
}
