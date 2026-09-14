import { autoStoreTestHooks, forgetStoreChoice, openAutoStore } from "../dist/engine/auto-store.js";
import { MinnowDatabase } from "@minnowdb/core";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import { deleteOpfsDatabase, opfsDatabaseExists } from "@minnowdb/core/storage/opfs";

interface StorageProbeResult {
  errorName: string;
  indexedDbShadowCreated: boolean;
  rows: unknown[];
}

interface IndexedDbProbeResult {
  errorName: string;
  alternativeOpened: boolean;
  rows: unknown[];
}

interface GarbageCollectionRaceResult {
  insertedAfterSnapshot: boolean;
  droppedBeforeNomination: boolean;
  provenanceRefusals: number;
  missingSegmentCount: number;
  tablesAfterReopen: string[];
  completedJobs: number;
}

interface IndexedDbSequenceResult {
  first: unknown[];
  second: unknown[];
  durableKeyMetadata: boolean;
}

function databaseNames(): Promise<string[]> {
  if (typeof indexedDB.databases !== "function") {
    throw new Error("This browser does not expose indexedDB.databases()");
  }
  return indexedDB
    .databases()
    .then((records) => records.flatMap(({ name }) => (name === undefined ? [] : [name])));
}

function deleteIndexedDbDatabase(name: string): Promise<void> {
  return new Promise((resolve) => {
    const request = indexedDB.deleteDatabase(name);
    request.addEventListener("success", () => resolve(), { once: true });
    request.addEventListener("error", () => resolve(), { once: true });
    request.addEventListener("blocked", () => resolve(), { once: true });
  });
}

export async function runFailedStorageProbe(): Promise<StorageProbeResult> {
  const name = `probe-${crypto.randomUUID()}`;
  const root = await navigator.storage.getDirectory();
  const spawn = () =>
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  let shadow: IndexedDbBlockStore | undefined;
  let seeded: MinnowDatabaseClient | undefined;
  let verifier: MinnowDatabaseClient | undefined;
  try {
    seeded = new MinnowDatabaseClient(spawn(), { store: { kind: "opfs", name } });
    await seeded.execute("CREATE TABLE ledger(id INTEGER PRIMARY KEY, amount INTEGER)");
    await seeded.execute("INSERT INTO ledger VALUES (1, 700)");
    await seeded.close({ terminateWorker: true });
    seeded = undefined;

    const unavailableRoot = {
      getDirectoryHandle: async () => {
        throw new DOMException("OPFS backend unavailable", "UnknownError");
      },
    } as unknown as FileSystemDirectoryHandle;
    // Force the alternative adapter to IndexedDB. Before the fail-closed fix, the uncertain OPFS
    // lookup answered false and this call really opened an empty same-named IndexedDB database.
    autoStoreTestHooks.opfsAvailable = async () => false;
    let errorName = "resolved";
    try {
      const opened = await openAutoStore(
        name,
        async (kind) => {
          if (kind !== "indexeddb") throw new Error(`Unexpected auto store: ${kind}`);
          return IndexedDbBlockStore.open({ name });
        },
        {
          opfsDatabaseExists: (candidate) =>
            opfsDatabaseExists({ name: candidate, root: unavailableRoot }),
        },
      );
      shadow = opened.store;
    } catch (error) {
      errorName = error instanceof Error ? error.name : String(error);
    }

    const indexedDbShadowCreated = (await databaseNames()).includes(name);
    shadow?.close();
    shadow = undefined;
    verifier = new MinnowDatabaseClient(spawn(), { store: { kind: "opfs", name } });
    const rows = (await verifier.query("SELECT * FROM ledger")).rows;
    await verifier.close({ terminateWorker: true });
    verifier = undefined;
    return { errorName, indexedDbShadowCreated, rows };
  } finally {
    delete autoStoreTestHooks.opfsAvailable;
    shadow?.close();
    if (seeded !== undefined) {
      await seeded.close({ terminateWorker: true }).catch(() => undefined);
    }
    if (verifier !== undefined) {
      await verifier.close({ terminateWorker: true }).catch(() => undefined);
    }
    await deleteOpfsDatabase({ name, root }).catch(() => undefined);
    await forgetStoreChoice(name).catch(() => undefined);
    await deleteIndexedDbDatabase(name);
  }
}

export async function runFailedIndexedDbProbe(): Promise<IndexedDbProbeResult> {
  const name = `idb-probe-${crypto.randomUUID()}`;
  const spawn = () =>
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  try {
    const seeded = new MinnowDatabaseClient(spawn(), { store: { kind: "indexeddb", name } });
    try {
      await seeded.execute("CREATE TABLE ledger(id INTEGER PRIMARY KEY, amount INTEGER)");
      await seeded.execute("INSERT INTO ledger VALUES (1, 900)");
    } finally {
      await seeded.close({ terminateWorker: true });
    }

    // The real engine database is schema 2. Hide databases(), then make only the existence probe
    // open it as schema 1, which produces a native asynchronous VersionError. Choice-database
    // calls still reach the browser unchanged so the complete auto-store path runs.
    const probeFactory = new Proxy(indexedDB, {
      get(target, property) {
        if (property === "databases") return undefined;
        if (property === "open") {
          return (candidate: string, version?: number) => {
            if (candidate === name && version === undefined) return target.open(candidate, 1);
            return version === undefined ? target.open(candidate) : target.open(candidate, version);
          };
        }
        const value: unknown = Reflect.get(target, property, target);
        return value;
      },
    });
    autoStoreTestHooks.indexedDB = probeFactory;
    autoStoreTestHooks.opfsAvailable = async () => true;
    let alternativeOpened = false;
    let errorName = "resolved";
    try {
      await openAutoStore(
        name,
        async () => {
          alternativeOpened = true;
          throw new Error("the alternative adapter must not open after an uncertain probe");
        },
        { opfsDatabaseExists: async () => false },
      );
    } catch (error) {
      errorName = error instanceof Error ? error.name : String(error);
    }

    const verifier = new MinnowDatabaseClient(spawn(), { store: { kind: "indexeddb", name } });
    let rows: unknown[];
    try {
      rows = (await verifier.query("SELECT * FROM ledger")).rows;
    } finally {
      await verifier.close({ terminateWorker: true });
    }
    return { errorName, alternativeOpened, rows };
  } finally {
    delete autoStoreTestHooks.indexedDB;
    delete autoStoreTestHooks.opfsAvailable;
    await forgetStoreChoice(name).catch(() => undefined);
    await deleteIndexedDbDatabase(name);
  }
}

export async function runNativeIndexedDbGarbageCollectionRace(): Promise<GarbageCollectionRaceResult> {
  const name = `gc-race-${crypto.randomUUID()}`;
  const collectorStore = await IndexedDbBlockStore.open({ name });
  const writerStore = await IndexedDbBlockStore.open({ name });
  const options = { autoCollect: false, autoCompact: false } as const;
  const writer = new MinnowDatabase(writerStore, options);
  let armed = false;
  let insertedAfterSnapshot = false;
  let droppedBeforeNomination = false;
  let provenanceRefusals = 0;
  const interceptedStore = new Proxy(collectorStore, {
    get(target, property) {
      const value: unknown = Reflect.get(target, property, target);
      if (typeof value !== "function") return value;
      const method = value as (...arguments_: unknown[]) => unknown;
      if (property === "listSegmentPage") {
        return async (...arguments_: unknown[]) => {
          if (armed && !insertedAfterSnapshot) {
            insertedAfterSnapshot = true;
            await writer.execute("INSERT INTO items VALUES (1, 10)");
          }
          return method.apply(target, arguments_);
        };
      }
      if (property === "updateGarbageCollectionPlanning") {
        return async (...arguments_: unknown[]) => {
          const input = arguments_[0] as { candidateSegmentIds?: readonly string[] } | undefined;
          if (armed && !droppedBeforeNomination && (input?.candidateSegmentIds?.length ?? 0) > 0) {
            droppedBeforeNomination = true;
            await writer.execute("DROP TABLE items");
          }
          try {
            return await method.apply(target, arguments_);
          } catch (error) {
            if (
              error instanceof Error &&
              error.message.startsWith("GC segment has no provenance:")
            ) {
              provenanceRefusals += 1;
            }
            throw error;
          }
        };
      }
      return method.bind(target);
    },
  });
  const collector = new MinnowDatabase(interceptedStore, options);
  try {
    await collector.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, amount INTEGER)");
    armed = true;
    const result = await collector.collectGarbage({
      maxPlanningItems: 32,
      maxItemsPerStep: 32,
    });
    const jobs = await collectorStore.listGarbageCollectionJobs();
    await collector.close();
    await writer.close();
    collectorStore.close();
    writerStore.close();

    const reopenedStore = await IndexedDbBlockStore.open({ name });
    try {
      return {
        insertedAfterSnapshot,
        droppedBeforeNomination,
        provenanceRefusals,
        missingSegmentCount: result.missingSegmentCount,
        tablesAfterReopen: (await reopenedStore.listTables()).map(({ name }) => name),
        completedJobs: jobs.filter(({ state }) => state === "completed").length,
      };
    } finally {
      reopenedStore.close();
    }
  } finally {
    await Promise.allSettled([collector.close(), writer.close()]);
    collectorStore.close();
    writerStore.close();
    await deleteIndexedDbDatabase(name);
  }
}

export async function runNativeIndexedDbSequenceReopen(): Promise<IndexedDbSequenceResult> {
  const name = `sequence-${crypto.randomUUID()}`;
  const spawn = () =>
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  let firstClient: MinnowDatabaseClient | undefined;
  let secondClient: MinnowDatabaseClient | undefined;
  let inspector: IndexedDbBlockStore | undefined;
  try {
    firstClient = new MinnowDatabaseClient(spawn(), { store: { kind: "indexeddb", name } });
    await firstClient.execute("CREATE SEQUENCE order_ids");
    const first = (await firstClient.query("SELECT NEXTVAL('order_ids') AS id")).rows;
    await firstClient.close({ terminateWorker: true });
    firstClient = undefined;

    secondClient = new MinnowDatabaseClient(spawn(), { store: { kind: "indexeddb", name } });
    const second = (await secondClient.query("SELECT NEXTVAL('order_ids') AS id")).rows;
    await secondClient.close({ terminateWorker: true });
    secondClient = undefined;

    inspector = await IndexedDbBlockStore.open({ name });
    const sequence = (await inspector.listTables()).find(
      (table) => table.sequence?.name === "order_ids",
    );
    return {
      first,
      second,
      durableKeyMetadata:
        sequence?.uniqueKeyColumnId !== undefined &&
        sequence.uniqueKeyColumnId === sequence.sequence?.columnId,
    };
  } finally {
    if (firstClient !== undefined) {
      await firstClient.close({ terminateWorker: true }).catch(() => undefined);
    }
    if (secondClient !== undefined) {
      await secondClient.close({ terminateWorker: true }).catch(() => undefined);
    }
    inspector?.close();
    await deleteIndexedDbDatabase(name);
  }
}
