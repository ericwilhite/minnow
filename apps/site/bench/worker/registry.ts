/**
 * Dataset registry: one IndexedDB database recording every generated dataset and its
 * per-engine materializations. Enumeration always comes from here, never from
 * `indexedDB.databases()`. The v1 dashboard registry and its datasets are removed the
 * first time this registry is touched.
 */
import type { DatasetRecord } from "../protocol";
import { deleteDatabase, indexedDbRequest, indexedDbTransactionDone } from "./support";

const REGISTRY_NAME = "minnow-site-datasets-v2";
const REGISTRY_STORE = "datasets";

const V1_REGISTRY_NAME = "minnow-dashboard-datasets-v1";
const V1_STORE = "datasets";

let v1CleanupPromise: Promise<void> | undefined;

function cleanupV1Registry(): Promise<void> {
  v1CleanupPromise ??= (async () => {
    try {
      // The old dashboard deleted PGlite scratch databases by their dataDir label, but
      // Emscripten names them after the mount path, so they leaked. Sweep them first —
      // this must not depend on whether the v1 registry still exists.
      try {
        const databases = await indexedDB.databases();
        for (const database of databases) {
          if (database.name?.startsWith("/pglite/minnow-compare-") === true) {
            await deleteDatabase(database.name).catch(() => undefined);
          }
        }
      } catch {
        // indexedDB.databases() is not universally available.
      }
      // Opening the v1 registry creates it when absent; always close the connection and
      // delete the database afterwards, whether or not it held records.
      const opened = await new Promise<{ database: IDBDatabase; existed: boolean } | undefined>(
        (resolve) => {
          const request = indexedDB.open(V1_REGISTRY_NAME, 1);
          let existed = true;
          request.addEventListener("upgradeneeded", () => {
            existed = false;
          });
          request.addEventListener(
            "success",
            () => resolve({ database: request.result, existed }),
            {
              once: true,
            },
          );
          request.addEventListener("error", () => resolve(undefined), { once: true });
        },
      );
      let databaseNames: string[] = [];
      if (opened !== undefined) {
        try {
          if (opened.existed && opened.database.objectStoreNames.contains(V1_STORE)) {
            const transaction = opened.database.transaction(V1_STORE, "readonly");
            const records = await indexedDbRequest<unknown[]>(
              transaction.objectStore(V1_STORE).getAll() as IDBRequest<unknown[]>,
            );
            databaseNames = records
              .map((record) =>
                typeof record === "object" && record !== null && "databaseName" in record
                  ? String(record.databaseName)
                  : "",
              )
              .filter((name) => name.length > 0);
          }
        } finally {
          opened.database.close();
        }
      }
      for (const name of databaseNames) await deleteDatabase(name).catch(() => undefined);
      await deleteDatabase(V1_REGISTRY_NAME).catch(() => undefined);
    } catch {
      // Best-effort migration; a failure only leaves stale v1 storage behind.
    }
  })();
  return v1CleanupPromise;
}

async function openRegistry(): Promise<IDBDatabase> {
  await cleanupV1Registry();
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(REGISTRY_NAME, 1);
    request.addEventListener("upgradeneeded", () => {
      if (!request.result.objectStoreNames.contains(REGISTRY_STORE)) {
        request.result.createObjectStore(REGISTRY_STORE, { keyPath: "id" });
      }
    });
    request.addEventListener("success", () => resolve(request.result), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("Dataset registry could not be opened")),
      { once: true },
    );
  });
}

export async function putDataset(record: DatasetRecord): Promise<void> {
  const registry = await openRegistry();
  try {
    const transaction = registry.transaction(REGISTRY_STORE, "readwrite");
    transaction.objectStore(REGISTRY_STORE).put(record);
    await indexedDbTransactionDone(transaction);
  } finally {
    registry.close();
  }
}

export async function listDatasets(): Promise<DatasetRecord[]> {
  const registry = await openRegistry();
  try {
    const transaction = registry.transaction(REGISTRY_STORE, "readonly");
    const records = await indexedDbRequest<unknown[]>(
      transaction.objectStore(REGISTRY_STORE).getAll() as IDBRequest<unknown[]>,
    );
    await indexedDbTransactionDone(transaction);
    return records
      .filter(isDatasetRecord)
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt));
  } finally {
    registry.close();
  }
}

export async function getDataset(id: string): Promise<DatasetRecord> {
  const datasets = await listDatasets();
  const record = datasets.find((candidate) => candidate.id === id);
  if (record === undefined) throw new Error(`Unknown dataset: ${id}`);
  return record;
}

export async function removeDatasetRecord(id: string): Promise<void> {
  const registry = await openRegistry();
  try {
    const transaction = registry.transaction(REGISTRY_STORE, "readwrite");
    transaction.objectStore(REGISTRY_STORE).delete(id);
    await indexedDbTransactionDone(transaction);
  } finally {
    registry.close();
  }
}

function isDatasetRecord(value: unknown): value is DatasetRecord {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  return (
    typeof record.id === "string" &&
    typeof record.createdAt === "string" &&
    typeof record.scale === "number" &&
    typeof record.totalRows === "number" &&
    typeof record.tableRows === "object" &&
    record.tableRows !== null &&
    ["raw", "gzip"].includes(String(record.compression)) &&
    typeof record.targetBlockBytes === "number" &&
    ["relaxed", "strict"].includes(String(record.durability)) &&
    typeof record.engines === "object" &&
    record.engines !== null
  );
}
